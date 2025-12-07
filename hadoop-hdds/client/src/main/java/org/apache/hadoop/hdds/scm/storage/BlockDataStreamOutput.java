/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls.putBlockAsync;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.protocol.DataStreamReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ByteBufferStreamOutput} used by the REST service in combination
 * with the SCMClient to write the value of a key to a sequence
 * of container chunks.  Writes are buffered locally and periodically written to
 * the container as a new chunk.  In order to preserve the semantics that
 * replacement of a pre-existing key is atomic, each instance of the stream has
 * an internal unique identifier.  This unique identifier and a monotonically
 * increasing chunk index form a composite key that is used as the chunk name.
 * After all data is written, a putKey call creates or updates the corresponding
 * container key, and this call includes the full list of chunks that make up
 * the key data.  The list of chunks is updated all at once.  Therefore, a
 * concurrent reader never can see an intermediate state in which different
 * chunks of data from different versions of the key data are interleaved.
 * This class encapsulates all state management for buffering and writing
 * through to the container.
 */
public class BlockDataStreamOutput implements ByteBufferStreamOutput {
  private static final Logger LOG =
      LoggerFactory.getLogger(BlockDataStreamOutput.class);

  public static final int PUT_BLOCK_REQUEST_LENGTH_MAX = 1 << 20;  // 1MB

  public static final String EXCEPTION_MSG =
      "Unexpected Storage Container Exception: ";
  private static final CompletableFuture[] EMPTY_FUTURE_ARRAY = {};

  private AtomicReference<BlockID> blockID;

  private final BlockData.Builder containerBlockData;
  private XceiverClientFactory xceiverClientFactory;
  private XceiverClientRatis xceiverClient;
  private OzoneClientConfig config;

  private int chunkIndex;
  private final AtomicLong chunkOffset = new AtomicLong();

  // Similar to 'BufferPool' but this list maintains only references
  // to the ByteBuffers.
  private List<StreamBuffer> bufferList;

  // The IOException will be set by response handling thread in case there is an
  // exception received in the response. If the exception is set, the next
  // request will fail upfront.
  private final AtomicReference<IOException> ioException;
  private final ExecutorService responseExecutor;

  // the effective length of data flushed so far
  private long totalDataFlushedLength;

  // effective data write attempted so far for the block
  private long writtenDataLength;

  // This object will maintain the commitIndexes and byteBufferList in order
  // Also, corresponding to the logIndex, the corresponding list of buffers will
  // be released from the buffer pool.
  private final StreamCommitWatcher commitWatcher;

  private Queue<CompletableFuture<ContainerCommandResponseProto>>
      putBlockFutures = new LinkedList<>();

  private final List<DatanodeDetails> failedServers;
  private final Checksum checksum;

  private final Token<? extends TokenIdentifier> token;
  private final String tokenString;
  private final DataStreamOutput out;
  private CompletableFuture<DataStreamReply> dataStreamCloseReply;
  private List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
  private static final long SYNC_SIZE = 0; // TODO: disk sync is disabled for now
  private long syncPosition = 0;
  private StreamBuffer currentBuffer;
  private XceiverClientMetrics metrics;
  // buffers for which putBlock is yet to be executed
  private List<StreamBuffer> buffersForPutBlock;
  private boolean isDatastreamPipelineMode;

  /**
   * Creates a new BlockDataStreamOutput.
   *
   * @param blockID              block ID
   * @param xceiverClientManager client manager that controls client
   * @param pipeline             pipeline where block will be written
   */
  public BlockDataStreamOutput(
      BlockID blockID,
      XceiverClientFactory xceiverClientManager,
      Pipeline pipeline,
      OzoneClientConfig config,
      Token<? extends TokenIdentifier> token,
      List<StreamBuffer> bufferList
  ) throws IOException {
    this.xceiverClientFactory = xceiverClientManager;
    this.config = config;
    this.isDatastreamPipelineMode = config.isDatastreamPipelineMode();
    this.blockID = new AtomicReference<>(blockID);
    KeyValue keyValue =
        KeyValue.newBuilder().setKey("TYPE").setValue("KEY").build();
    this.containerBlockData =
        BlockData.newBuilder().setBlockID(blockID.getDatanodeBlockIDProtobuf())
            .addMetadata(keyValue);
    this.xceiverClient =
        (XceiverClientRatis)xceiverClientManager.acquireClient(pipeline, true);
    this.token = token;
    this.tokenString = (this.token == null) ? null :
        this.token.encodeToUrlString();
    // Alternatively, stream setup can be delayed till the first chunk write.
    this.out = setupStream(pipeline);
    this.bufferList = bufferList;

    //number of buffers used before doing a flush/putBlock.
    int flushPeriod = (int) (config.getStreamBufferFlushSize() / config.getStreamBufferSize());

    Preconditions
        .checkArgument(
            (long) flushPeriod * config.getStreamBufferSize() == config
                .getStreamBufferFlushSize());

    // A single thread executor handle the responses of async requests
    responseExecutor = Executors.newSingleThreadExecutor();
    commitWatcher = new StreamCommitWatcher(xceiverClient, bufferList);
    totalDataFlushedLength = 0;
    writtenDataLength = 0;
    failedServers = new ArrayList<>(0);
    ioException = new AtomicReference<>(null);
    checksum = new Checksum(config.getChecksumType(),
        config.getBytesPerChecksum());
    metrics = XceiverClientManager.getXceiverClientMetrics();
  }

  private DataStreamOutput setupStream(Pipeline pipeline) throws IOException {
    // Execute a dummy WriteChunk request to get the path of the target file,
    // but does NOT write any data to it.
    ContainerProtos.WriteChunkRequestProto.Builder writeChunkRequest =
        ContainerProtos.WriteChunkRequestProto.newBuilder()
            .setBlockID(blockID.get().getDatanodeBlockIDProtobuf());

    // TODO: The datanode UUID is not used meaningfully, consider deprecating
    //  it or remove it completely if possible
    String id = pipeline.getFirstNode().getUuidString();
    ContainerProtos.ContainerCommandRequestProto.Builder builder =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.StreamInit)
            .setContainerID(blockID.get().getContainerID())
            .setDatanodeUuid(id).setWriteChunk(writeChunkRequest);

    if (tokenString != null) {
      builder.setEncodedToken(tokenString);
    }

    ContainerCommandRequestMessage message =
        ContainerCommandRequestMessage.toMessage(builder.build(), null);

    if (isDatastreamPipelineMode) {
      return Objects.requireNonNull(xceiverClient.getDataStreamApi(), "xceiverClient.getDataStreamApi() == null")
          .stream(message.getContent().asReadOnlyByteBuffer(),
              RatisHelper.getRoutingTable(pipeline));
    } else {
      return Objects.requireNonNull(xceiverClient.getDataStreamApi(), "xceiverClient.getDataStreamApi() == null")
          .stream(message.getContent().asReadOnlyByteBuffer());
    }
  }

  public BlockID getBlockID() {
    return blockID.get();
  }

  public long getWrittenDataLength() {
    return writtenDataLength;
  }

  public List<DatanodeDetails> getFailedServers() {
    return failedServers;
  }

  public IOException getIoException() {
    return ioException.get();
  }

  @Override
  public void write(ByteBuffer b, int off, int len) throws IOException {
    checkOpen();
    if (b == null) {
      throw new NullPointerException();
    }
    if (len == 0) {
      return;
    }
    while (len > 0) {
      allocateNewBufferIfNeeded();
      int writeLen = Math.min(len, currentBuffer.remaining());
      final StreamBuffer buf = new StreamBuffer(b, off, writeLen);
      currentBuffer.put(buf);
      writeChunkIfNeeded();
      off += writeLen;
      writtenDataLength += writeLen;
      len -= writeLen;
      doFlushIfNeeded();
    }
  }

  private void writeChunkIfNeeded() throws IOException {
    if (currentBuffer.remaining() == 0) {
      writeChunk(currentBuffer);
      currentBuffer = null;
    }
  }

  private void writeChunk(StreamBuffer sb) throws IOException {
    bufferList.add(sb);
    if (buffersForPutBlock == null) {
      buffersForPutBlock = new ArrayList<>();
    }
    buffersForPutBlock.add(sb);
    ByteBuffer dup = sb.duplicate();
    dup.position(0);
    dup.limit(sb.position());
    writeChunkToContainer(dup);
  }

  private void allocateNewBufferIfNeeded() {
    if (currentBuffer == null) {
      currentBuffer =
          StreamBuffer.allocate(config.getDataStreamMinPacketSize());
    }
  }

  private void doFlushIfNeeded() throws IOException {
    long boundary = config.getDataStreamBufferFlushSize() / config
        .getDataStreamMinPacketSize();
    // streamWindow is the maximum number of buffers that
    // are allowed to exist in the bufferList. If buffers in
    // the list exceed this limit , client will till it gets
    // one putBlockResponse (first index) . This is similar to
    // the bufferFull condition in async write path.
    long streamWindow = config.getStreamWindowSize() / config
        .getDataStreamMinPacketSize();
    if (!bufferList.isEmpty() && bufferList.size() % boundary == 0 &&
        buffersForPutBlock != null && !buffersForPutBlock.isEmpty()) {
      updateFlushLength();
      executePutBlock(false, false);
    }
    if (bufferList.size() == streamWindow) {
      try {
        checkOpen();
        if (!putBlockFutures.isEmpty()) {
          putBlockFutures.remove().get();
        }
      } catch (ExecutionException e) {
        handleExecutionException(e);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        handleInterruptedException(ex, true);
      }
      watchForCommit(true);
    }
  }

  private void updateFlushLength() {
    totalDataFlushedLength = writtenDataLength;
  }

  /**
   * Will be called on the retryPath in case closedContainerException/
   * TimeoutException.
   * @param len length of data to write
   * @throws IOException if error occurred
   */

  public void writeOnRetry(long len) throws IOException {
    if (len == 0) {
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Retrying write length {} for blockID {}", len, blockID);
    }
    int count = 0;
    while (len > 0) {
      final StreamBuffer buf = bufferList.get(count);
      final long writeLen = Math.min(buf.position(), len);
      if (buffersForPutBlock == null) {
        buffersForPutBlock = new ArrayList<>();
      }
      buffersForPutBlock.add(buf);
      final ByteBuffer duplicated = buf.duplicate();
      duplicated.position(0);
      duplicated.limit(buf.position());
      writeChunkToContainer(duplicated);
      len -= writeLen;
      count++;
      writtenDataLength += writeLen;
    }


  }

  /**
   * calls watchForCommit API of the Ratis Client. For Standalone client,
   * it is a no op.
   * @param bufferFull flag indicating whether bufferFull condition is hit or
   *              its called as part flush/close
   * @throws IOException IOException in case watch gets timed out
   */
  public void watchForCommit(boolean bufferFull) throws IOException {
    checkOpen();
    try {
      XceiverClientReply reply = bufferFull ?
          commitWatcher.watchOnFirstIndex() :
          commitWatcher.watchOnLastIndex();
      if (reply != null) {
        List<DatanodeDetails> dnList = reply.getDatanodes();
        if (!dnList.isEmpty()) {
          Pipeline pipe = xceiverClient.getPipeline();

          LOG.warn("Failed to commit BlockId {} on {}. Failed nodes: {}",
              blockID, pipe, dnList);
          failedServers.addAll(dnList);
        }
      }
    } catch (IOException ioe) {
      setIoException(ioe);
      throw getIoException();
    }

  }

  /**
   * @param close whether putBlock is happening as part of closing the stream
   * @param force true if no data was written since most recent putBlock and
   *            stream is being closed
   */
  public void executePutBlock(boolean close,
      boolean force) throws IOException {
    checkOpen();
    long flushPos = totalDataFlushedLength;
    final List<StreamBuffer> byteBufferList;
    if (!force) {
      Objects.requireNonNull(bufferList, "bufferList == null");
      byteBufferList = buffersForPutBlock;
      buffersForPutBlock = null;
      Objects.requireNonNull(byteBufferList, "byteBufferList == null");
    } else {
      byteBufferList = null;
    }
    waitFuturesComplete();
    final BlockData blockData = containerBlockData.build();
    if (close) {
      // HDDS-12007 changed datanodes to ignore the following PutBlock request.
      // However, clients still have to send it for maintaining compatibility.
      // Otherwise, new clients won't send a PutBlock.
      // Then, old datanodes will fail since they expect a PutBlock.
      final ContainerCommandRequestProto putBlockRequest
          = ContainerProtocolCalls.getPutBlockRequest(
              xceiverClient.getPipeline(), blockData, true, tokenString);
      dataStreamCloseReply = executePutBlockClose(putBlockRequest,
          PUT_BLOCK_REQUEST_LENGTH_MAX, out);
      dataStreamCloseReply.whenComplete((reply, e) -> {
        if (e != null || reply == null || !reply.isSuccess()) {
          LOG.warn("Failed executePutBlockClose, reply=" + reply, e);
          try {
            executePutBlock(true, false);
          } catch (IOException ex) {
            throw new CompletionException(ex);
          }
        }
      });
    }

    try {
      XceiverClientReply asyncReply =
          putBlockAsync(xceiverClient, blockData, close, tokenString);
      final CompletableFuture<ContainerCommandResponseProto> flushFuture
          = asyncReply.getResponse().thenApplyAsync(e -> {
            try {
              validateResponse(e);
            } catch (IOException sce) {
              throw new CompletionException(sce);
            }
            // if the ioException is not set, putBlock is successful
            if (getIoException() == null && !force) {
              BlockID responseBlockID = BlockID.getFromProtobuf(
                  e.getPutBlock().getCommittedBlockLength().getBlockID());
              Preconditions.checkState(blockID.get().getContainerBlockID()
                  .equals(responseBlockID.getContainerBlockID()));
              // updates the bcsId of the block
              blockID.set(responseBlockID);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Adding index " + asyncReply.getLogIndex() +
                    " commitMap size "
                    + commitWatcher.getCommitIndexMap().size() + " flushLength "
                    + flushPos + " blockID " + blockID);
              }
              // for standalone protocol, logIndex will always be 0.
              commitWatcher
                  .updateCommitInfoMap(asyncReply.getLogIndex(),
                      byteBufferList);
            }
            return e;
          }, responseExecutor).exceptionally(e -> {
            if (LOG.isDebugEnabled()) {
              LOG.debug("putBlock failed for blockID {} with exception {}",
                  blockID, e.getLocalizedMessage());
            }
            CompletionException ce = new CompletionException(e);
            setIoException(ce);
            throw ce;
          });
      putBlockFutures.add(flushFuture);
    } catch (IOException | ExecutionException e) {
      throw new IOException(EXCEPTION_MSG + e.toString(), e);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      handleInterruptedException(ex, false);
    }
  }

  public static CompletableFuture<DataStreamReply> executePutBlockClose(
      ContainerCommandRequestProto putBlockRequest, int max,
      DataStreamOutput out) {
    final ByteBuffer putBlock = ContainerCommandRequestMessage.toMessage(
        putBlockRequest, null).getContent().asReadOnlyByteBuffer();
    final ByteBuffer protoLength = getProtoLength(putBlock, max);
    RatisHelper.debug(putBlock, "putBlock", LOG);
    out.writeAsync(putBlock);
    RatisHelper.debug(protoLength, "protoLength", LOG);
    return out.writeAsync(protoLength, StandardWriteOption.CLOSE);
  }

  public static ByteBuffer getProtoLength(ByteBuffer putBlock, int max) {
    final int protoLength = putBlock.remaining();
    Preconditions.checkState(protoLength <= max,
        "protoLength== %s > max = %s", protoLength, max);
    final ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(protoLength);
    buffer.flip();
    LOG.debug("protoLength = {}", protoLength);
    Preconditions.checkState(buffer.remaining() == 4);
    return buffer.asReadOnlyBuffer();
  }

  @Override
  public void flush() throws IOException {
    if (xceiverClientFactory != null && xceiverClient != null
        && !config.isStreamBufferFlushDelay()) {
      waitFuturesComplete();
    }
  }

  @Override
  public void hflush() throws IOException {
    hsync();
  }

  @Override
  public void hsync() throws IOException {
    try {
      if (!isClosed()) {
        handleFlush(false);
      }
    } catch (Exception e) {

    }
  }

  public void waitFuturesComplete() throws IOException {
    try {
      CompletableFuture.allOf(futures.toArray(EMPTY_FUTURE_ARRAY)).get();
      futures.clear();
    } catch (Exception e) {
      LOG.warn("Failed to write all chunks through stream: " + e);
      throw new IOException(e);
    }
  }

  /**
   * @param close whether the flush is happening as part of closing the stream
   */
  private void handleFlush(boolean close)
      throws IOException, InterruptedException, ExecutionException {
    checkOpen();
    // flush the last chunk data residing on the currentBuffer
    if (totalDataFlushedLength < writtenDataLength) {
      // This can be a partially filled chunk. Since we are flushing the buffer
      // here, we just limit this buffer to the current position. So that next
      // write will happen in new buffer

      if (currentBuffer != null) {
        writeChunk(currentBuffer);
        currentBuffer = null;
      }
      updateFlushLength();
      executePutBlock(close, false);
    } else if (close) {
      // forcing an "empty" putBlock if stream is being closed without new
      // data since latest flush - we need to send the "EOF" flag
      executePutBlock(true, true);
    }
    CompletableFuture.allOf(putBlockFutures.toArray(EMPTY_FUTURE_ARRAY)).get();
    watchForCommit(false);
    // just check again if the exception is hit while waiting for the
    // futures to ensure flush has indeed succeeded

    // irrespective of whether the commitIndex2flushedDataMap is empty
    // or not, ensure there is no exception set
    checkOpen();
  }

  @Override
  public void close() throws IOException {
    if (xceiverClientFactory != null && xceiverClient != null) {
      try {
        handleFlush(true);
        dataStreamCloseReply.get();
      } catch (ExecutionException e) {
        handleExecutionException(e);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        handleInterruptedException(ex, true);
      } finally {
        cleanup(false);
      }

    }
  }

  private void validateResponse(
      ContainerProtos.ContainerCommandResponseProto responseProto)
      throws IOException {
    try {
      // if the ioException is already set, it means a prev request has failed
      // just throw the exception. The current operation will fail with the
      // original error
      IOException exception = getIoException();
      if (exception != null) {
        throw exception;
      }
      ContainerProtocolCalls.validateContainerResponse(responseProto);
    } catch (StorageContainerException sce) {
      setIoException(sce);
      throw sce;
    }
  }

  private void setIoException(Throwable e) {
    IOException ioe = getIoException();
    if (ioe == null) {
      IOException exception =  new IOException(EXCEPTION_MSG + e.toString(), e);
      ioException.compareAndSet(null, exception);
    } else {
      LOG.debug("Previous request had already failed with " + ioe.toString()
          + " so subsequent request also encounters"
          + " Storage Container Exception ", e);
    }
  }

  public void cleanup(boolean invalidateClient) {
    if (xceiverClientFactory != null) {
      xceiverClientFactory.releaseClient(xceiverClient, invalidateClient,
          true);
    }
    xceiverClientFactory = null;
    xceiverClient = null;
    commitWatcher.cleanup();
    responseExecutor.shutdown();
  }

  /**
   * Checks if the stream is open or exception has occurred.
   * If not, throws an exception.
   *
   * @throws IOException if stream is closed
   */
  private void checkOpen() throws IOException {
    if (isClosed()) {
      throw new IOException("BlockDataStreamOutput has been closed.");
    } else if (getIoException() != null) {
      throw getIoException();
    }
  }

  public boolean isClosed() {
    return xceiverClient == null;
  }

  private boolean needSync(long position) {
    if (SYNC_SIZE > 0) {
      // TODO: or position >= fileLength
      if (position - syncPosition >= SYNC_SIZE) {
        syncPosition = position;
        return true;
      }
    }
    return false;
  }

  /**
   * Writes buffered data as a new chunk to the container and saves chunk
   * information to be used later in putKey call.
   *
   * @param buf chunk data to write, from position to limit
   * @throws IOException if there is an I/O error while performing the call
   * @throws OzoneChecksumException if there is an error while computing
   * checksum
   */
  private void writeChunkToContainer(ByteBuffer buf)
      throws IOException {
    final int effectiveChunkSize = buf.remaining();
    final long offset = chunkOffset.getAndAdd(effectiveChunkSize);
    ChecksumData checksumData = checksum.computeChecksum(
        buf.asReadOnlyBuffer());
    ChunkInfo chunkInfo = ChunkInfo.newBuilder()
        .setChunkName(blockID.get().getLocalID() + "_chunk_" + ++chunkIndex)
        .setOffset(offset)
        .setLen(effectiveChunkSize)
        .setChecksumData(checksumData.getProtoBufMessage())
        .build();
    metrics.incrPendingContainerOpsMetrics(ContainerProtos.Type.WriteChunk);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing chunk {} length {} at offset {}",
          chunkInfo.getChunkName(), effectiveChunkSize, offset);
    }

    CompletableFuture<DataStreamReply> future =
        (needSync(offset + effectiveChunkSize) ?
            out.writeAsync(buf, StandardWriteOption.SYNC) :
            out.writeAsync(buf))
            .whenCompleteAsync((r, e) -> {
              metrics.decrPendingContainerOpsMetrics(ContainerProtos.Type.WriteChunk);
              if (e != null || !r.isSuccess()) {
                if (e == null) {
                  e = new IOException("result is not success");
                }
                String msg =
                    "Failed to write chunk " + chunkInfo.getChunkName() +
                        " " + "into block " + blockID;
                LOG.debug("{}, exception: {}", msg, e.getLocalizedMessage());
                CompletionException ce = new CompletionException(msg, e);
                setIoException(ce);
                throw ce;
              } else if (r.isSuccess()) {
                xceiverClient.updateCommitInfosMap(r.getCommitInfos());
              }
            }, responseExecutor);

    futures.add(future);
    containerBlockData.addChunks(chunkInfo);
  }

  /**
   * Handles InterruptedExecution.
   *
   * @param ex
   * @param processExecutionException is optional, if passed as TRUE, then
   * handle ExecutionException else skip it.
   * @throws IOException
   */
  private void handleInterruptedException(Exception ex,
      boolean processExecutionException)
      throws IOException {
    LOG.error("Command execution was interrupted.");
    if (processExecutionException) {
      handleExecutionException(ex);
    } else {
      throw new IOException(EXCEPTION_MSG + ex.toString(), ex);
    }
  }

  /**
   * Handles ExecutionException by adjusting buffers.
   * @param ex
   * @throws IOException
   */
  private void handleExecutionException(Exception ex) throws IOException {
    setIoException(ex);
    throw getIoException();
  }

  public long getTotalAckDataLength() {
    return commitWatcher.getTotalAckDataLength();
  }
}
