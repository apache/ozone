/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls.putBlockAsync;
import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls.writeChunkAsync;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OutputStream} used by the REST service in combination with the
 * SCMClient to write the value of a key to a sequence
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
public class BlockOutputStream extends OutputStream {
  public static final Logger LOG =
      LoggerFactory.getLogger(BlockOutputStream.class);
  public static final String EXCEPTION_MSG =
      "Unexpected Storage Container Exception: ";

  private AtomicReference<BlockID> blockID;
  private final AtomicReference<ChunkInfo> previousChunkInfo
      = new AtomicReference<>();

  private final BlockData.Builder containerBlockData;
  private XceiverClientFactory xceiverClientFactory;
  private XceiverClientSpi xceiverClient;
  private OzoneClientConfig config;
  private StreamBufferArgs streamBufferArgs;

  private int chunkIndex;
  private final AtomicLong chunkOffset = new AtomicLong();
  private final BufferPool bufferPool;
  // The IOException will be set by response handling thread in case there is an
  // exception received in the response. If the exception is set, the next
  // request will fail upfront.
  private final AtomicReference<IOException> ioException;
  private final ExecutorService responseExecutor;

  // the effective length of data flushed so far
  private long totalDataFlushedLength;

  // effective data write attempted so far for the block
  private long writtenDataLength;

  // List containing buffers for which the putBlock call will
  // update the length in the datanodes. This list will just maintain
  // references to the buffers in the BufferPool which will be cleared
  // when the watchForCommit acknowledges a putBlock logIndex has been
  // committed on all datanodes. This list will be a  place holder for buffers
  // which got written between successive putBlock calls.
  private List<ChunkBuffer> bufferList;

  private final List<DatanodeDetails> failedServers;
  private final Checksum checksum;

  //number of buffers used before doing a flush/putBlock.
  private int flushPeriod;
  //bytes remaining to write in the current buffer.
  private int currentBufferRemaining;
  //current buffer allocated to write
  private ChunkBuffer currentBuffer;
  private final Token<? extends TokenIdentifier> token;
  private final String tokenString;
  private int replicationIndex;
  private Pipeline pipeline;
  private final ContainerClientMetrics clientMetrics;

  /**
   * Creates a new BlockOutputStream.
   *
   * @param blockID              block ID
   * @param xceiverClientManager client manager that controls client
   * @param pipeline             pipeline where block will be written
   * @param bufferPool           pool of buffers
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public BlockOutputStream(
      BlockID blockID,
      XceiverClientFactory xceiverClientManager,
      Pipeline pipeline,
      BufferPool bufferPool,
      OzoneClientConfig config,
      Token<? extends TokenIdentifier> token,
      ContainerClientMetrics clientMetrics, StreamBufferArgs streamBufferArgs,
      Supplier<ExecutorService> blockOutputStreamResourceProvider
  ) throws IOException {
    this.xceiverClientFactory = xceiverClientManager;
    this.config = config;
    this.blockID = new AtomicReference<>(blockID);
    replicationIndex = pipeline.getReplicaIndex(pipeline.getClosestNode());
    KeyValue keyValue =
        KeyValue.newBuilder().setKey("TYPE").setValue("KEY").build();

    ContainerProtos.DatanodeBlockID.Builder blkIDBuilder =
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(blockID.getContainerID())
            .setLocalID(blockID.getLocalID())
            .setBlockCommitSequenceId(blockID.getBlockCommitSequenceId());
    if (replicationIndex > 0) {
      blkIDBuilder.setReplicaIndex(replicationIndex);
    }
    this.containerBlockData = BlockData.newBuilder().setBlockID(
        blkIDBuilder.build()).addMetadata(keyValue);
    this.xceiverClient = xceiverClientManager.acquireClient(pipeline);
    this.bufferPool = bufferPool;
    this.token = token;
    this.tokenString = (this.token == null) ? null :
        this.token.encodeToUrlString();

    //number of buffers used before doing a flush
    refreshCurrentBuffer();
    flushPeriod = (int) (streamBufferArgs.getStreamBufferFlushSize() / streamBufferArgs
        .getStreamBufferSize());

    Preconditions
        .checkArgument(
            (long) flushPeriod * streamBufferArgs.getStreamBufferSize() == streamBufferArgs
                .getStreamBufferFlushSize());

    this.responseExecutor = blockOutputStreamResourceProvider.get();
    bufferList = null;
    totalDataFlushedLength = 0;
    writtenDataLength = 0;
    failedServers = new ArrayList<>(0);
    ioException = new AtomicReference<>(null);
    checksum = new Checksum(config.getChecksumType(),
        config.getBytesPerChecksum());
    this.clientMetrics = clientMetrics;
    this.pipeline = pipeline;
    this.streamBufferArgs = streamBufferArgs;
  }

  void refreshCurrentBuffer() {
    ChunkBuffer old = currentBuffer;
    currentBuffer = bufferPool.getCurrentBuffer();
    if (old != currentBuffer) {
      LOG.warn("Switching buffer, old={} new={}", old, currentBuffer);
    }
    currentBufferRemaining =
        currentBuffer != null ? currentBuffer.remaining() : 0;
  }

  public BlockID getBlockID() {
    return blockID.get();
  }

  public long getTotalAckDataLength() {
    return 0;
  }

  public long getWrittenDataLength() {
    return writtenDataLength;
  }

  public List<DatanodeDetails> getFailedServers() {
    return failedServers;
  }

  @VisibleForTesting
  public XceiverClientSpi getXceiverClient() {
    return xceiverClient;
  }

  @VisibleForTesting
  public long getTotalDataFlushedLength() {
    return totalDataFlushedLength;
  }

  @VisibleForTesting
  public BufferPool getBufferPool() {
    return bufferPool;
  }

  public IOException getIoException() {
    return ioException.get();
  }

  public BlockData.Builder getContainerBlockData() {
    return this.containerBlockData;
  }

  public Pipeline getPipeline() {
    return this.pipeline;
  }

  protected String getTokenString() {
    return this.tokenString;
  }

  ExecutorService getResponseExecutor() {
    return this.responseExecutor;
  }

  @Override
  public void write(int b) throws IOException {
    checkOpen();
    allocateNewBufferIfNeeded();
    currentBuffer.put((byte) b);
    currentBufferRemaining--;
    writeChunkIfNeeded();
    writtenDataLength++;
    doFlushOrWatchIfNeeded();
  }

  private void writeChunkIfNeeded() throws IOException {
    if (currentBufferRemaining == 0) {
      writeChunk(currentBuffer);
    }
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    checkOpen();
    if (b == null) {
      throw new NullPointerException();
    }
    if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return;
    }

    while (len > 0) {
      allocateNewBufferIfNeeded();
      final int writeLen = Math.min(currentBufferRemaining, len);
      LOG.warn("write: off={}, len={}, currentBuffer={}, currentBufferRemaining={}", off, len, currentBuffer, currentBufferRemaining);
      currentBuffer.put(b, off, writeLen);
      currentBufferRemaining -= writeLen;
      writeChunkIfNeeded();
      off += writeLen;
      len -= writeLen;
      updateWrittenDataLength(writeLen);
      doFlushOrWatchIfNeeded();
    }
    LOG.warn("after write, totalDataFlushedLength={}, writtenDataLength={}", totalDataFlushedLength, writtenDataLength);
  }

  public void updateWrittenDataLength(int writeLen) {
    writtenDataLength += writeLen;
  }

  private void doFlushOrWatchIfNeeded() throws IOException {
    if (currentBufferRemaining == 0) {
      PutBlockResult putBlockResult = null;
      if (bufferPool.getNumberOfUsedBuffers() % flushPeriod == 0) {
        updateFlushLength();
        putBlockResult = executePutBlock(false, false);
      }
      // Data in the bufferPool can not exceed streamBufferMaxSize
      if (bufferPool.getNumberOfUsedBuffers() == bufferPool.getCapacity()) {
        if (putBlockResult != null) {
          handleFullBuffer(putBlockResult.commitIndexFuture);
        } else {
          // TODO: clarify if this is feasible and what's needed.
        }
      }
    }
  }

  private void allocateNewBufferIfNeeded() {
    if (currentBufferRemaining == 0) {
      ChunkBuffer old = currentBuffer;
      currentBuffer = bufferPool.allocateBuffer(config.getBufferIncrement());
      currentBufferRemaining = currentBuffer.remaining();
      if (old != currentBuffer) {
        LOG.warn("Allocating buffer, old={} new={}", old, currentBuffer);
      }
    }
  }

  private void allocateNewBuffer() {
    ChunkBuffer old = currentBuffer;
    currentBuffer = bufferPool.allocateBuffer(config.getBufferIncrement());
    currentBufferRemaining = currentBuffer.remaining();
    if (old != currentBuffer) {
      LOG.warn("Force allocating buffer, old={} new={}", old, currentBuffer);
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

  // In this case, the data is already cached in the currentBuffer.
  public void writeOnRetry(long len) throws IOException {
    if (len == 0) {
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Retrying write length {} for blockID {}", len, blockID);
    }
    Preconditions.checkArgument(len <= streamBufferArgs.getStreamBufferMaxSize());
    int count = 0;
    while (len > 0) {
      ChunkBuffer buffer = bufferPool.getBuffer(count);
      long writeLen = Math.min(buffer.position(), len);
      if (!buffer.hasRemaining()) {
        writeChunk(buffer);
      }
      len -= writeLen;
      count++;
      writtenDataLength += writeLen;
      // we should not call isBufferFull/shouldFlush here.
      // The buffer might already be full as whole data is already cached in
      // the buffer. We should just validate
      // if we wrote data of size streamBufferMaxSize/streamBufferFlushSize to
      // call for handling full buffer/flush buffer condition.
      if (writtenDataLength % streamBufferArgs.getStreamBufferFlushSize() == 0) {
        // reset the position to zero as now we will be reading the
        // next buffer in the list
        updateFlushLength();
        executePutBlock(false, false);
      }
      if (writtenDataLength == streamBufferArgs.getStreamBufferMaxSize()) {
        handleFullBuffer(CompletableFuture.completedFuture(0L));  // TODO: correct commitLog
      }
    }
  }

  /**
   * This is a blocking call. It will wait for the flush till the commit index
   * at the head of the commitIndex2flushedDataMap gets replicated to all or
   * majority.
   * @throws IOException
   */
  private void handleFullBuffer(CompletableFuture<Long> commitLogFuture) throws IOException {
    waitForFlushAndCommit(commitLogFuture);
  }

  void waitForFlushAndCommit(CompletableFuture<Long> commitLogFuture) throws IOException {
    checkOpen();
    long commitLog = 0;
    try {
      commitLog = commitLogFuture.get();
      waitOnFlushFutures();
    } catch (ExecutionException e) {
      handleExecutionException(e);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      handleInterruptedException(ex, true);
    }
    watchForCommit(commitLog);
  }

  void releaseBuffersOnException() {
  }

  // It may happen that once the exception is encountered , we still might
  // have successfully flushed up to a certain index. Make sure the buffers
  // only contain data which have not been sufficiently replicated
  private void adjustBuffersOnException() {
    releaseBuffersOnException();
    refreshCurrentBuffer();
  }

  XceiverClientReply sendWatchForCommit(boolean bufferFull)
      throws IOException {
    return null;
  }

  XceiverClientReply sendWatchForCommit(long commitIndex)
      throws IOException {
    return null;
  }


  /**
   * calls watchForCommit API of the Ratis Client. For Standalone client,
   * it is a no op.
   * @param commitIndex
   * @throws IOException IOException in case watch gets timed out
   */
  private void watchForCommit(long commitIndex) throws IOException {
    checkOpen();
    LOG.warn("!!! Entering watchForCommit(commitIndex = {})", commitIndex);
    try {
      final XceiverClientReply reply = sendWatchForCommit(commitIndex);
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
    LOG.warn("!!! Leaving watchForCommit(commitIndex = {})", commitIndex);
    // TODO scream: why is it here?
    refreshCurrentBuffer();
  }

  void updateCommitInfo(XceiverClientReply reply, List<ChunkBuffer> buffers) {
  }

  static class PutBlockResult {
    final long flushPosition;
    final CompletableFuture<Long> commitIndexFuture;
    final CompletableFuture<ContainerCommandResponseProto> responseFuture;

    PutBlockResult(long flushPosition, CompletableFuture<Long> commitIndexFuture,
        CompletableFuture<ContainerCommandResponseProto> responseFuture) {
      this.flushPosition = flushPosition;
      this.commitIndexFuture = commitIndexFuture;
      this.responseFuture = responseFuture;
    }
  }

  /**
   * @param close whether putBlock is happening as part of closing the stream
   * @param force true if no data was written since most recent putBlock and
   *            stream is being closed
   */
  PutBlockResult executePutBlock(boolean close, boolean force) throws IOException {
    checkOpen();
    final long flushPos = totalDataFlushedLength;
    final List<ChunkBuffer> byteBufferList;
    if (!force) {
      Preconditions.checkNotNull(bufferList);
      byteBufferList = bufferList;
      bufferList = null;
      Preconditions.checkNotNull(byteBufferList);
    } else {
      byteBufferList = null;
    }

    CompletableFuture<ContainerCommandResponseProto> flushFuture = null;
    final CompletableFuture<Long> commitIndexFuture = new CompletableFuture<>();
    try {
      BlockData blockData = containerBlockData.build();
      XceiverClientReply asyncReply = putBlockAsync(xceiverClient, blockData, close, tokenString);
      CompletableFuture<ContainerCommandResponseProto> future = asyncReply.getResponse();
      flushFuture = future.whenComplete(
          (r, e) -> commitIndexFuture.complete(asyncReply.getLogIndex()))
          .thenApplyAsync(e -> {
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
            LOG.debug(
                "Adding index " + asyncReply.getLogIndex() + " flushLength "
                    + flushPos + " numBuffers " + byteBufferList.size()
                    + " blockID " + blockID + " bufferPool size" + bufferPool
                    .getSize() + " currentBufferIndex " + bufferPool
                    .getCurrentBufferIndex());
          }
          // for standalone protocol, logIndex will always be 0.
          updateCommitInfo(asyncReply, byteBufferList);
        }
        return e;
      }, responseExecutor).exceptionally(e -> {
        if (LOG.isDebugEnabled()) {
          LOG.debug("putBlock failed for blockID {} with exception {}",
                  blockID, e.getLocalizedMessage());
        }
        CompletionException ce =  new CompletionException(e);
        setIoException(ce);
        throw ce;
      });
    } catch (IOException | ExecutionException e) {
      throw new IOException(EXCEPTION_MSG + e.toString(), e);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      handleInterruptedException(ex, false);
      // never reach
      return null;
    }
    putFlushFuture(flushPos, flushFuture);
    return new PutBlockResult(flushPos, commitIndexFuture, flushFuture);
  }

  void putFlushFuture(long flushPos,
      CompletableFuture<ContainerCommandResponseProto> flushFuture) {
  }

  @Override
  public void flush() throws IOException {
    if (xceiverClientFactory != null && xceiverClient != null
        && bufferPool != null && bufferPool.getSize() > 0
        && (!streamBufferArgs.isStreamBufferFlushDelay() ||
            writtenDataLength - totalDataFlushedLength
                >= streamBufferArgs.getStreamBufferSize())) {
      handleFlush(false);
    }
  }

  private void writeChunk(ChunkBuffer buffer)
      throws IOException {
    // This data in the buffer will be pushed to datanode and a reference will
    // be added to the bufferList. Once putBlock gets executed, this list will
    // be marked null. Hence, during first writeChunk call after every putBlock
    // call or during the first call to writeChunk here, the list will be null.
    LOG.warn("writeChunk {}", buffer);

    if (bufferList == null) {
      bufferList = new ArrayList<>();
    }
    bufferList.add(buffer);
    writeChunkToContainer(buffer);
  }

  /**
   * @param close whether the flush is happening as part of closing the stream
   */
  protected void handleFlush(boolean close) throws IOException {
    try {
      handleFlushInternal(close);
    } catch (ExecutionException e) {
      handleExecutionException(e);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      handleInterruptedException(ex, true);
    } catch (Throwable e) {
      String msg = "Failed to flush. error: " + e.getMessage();
      LOG.error(msg, e);
      throw e;
    } finally {
      if (close) {
        cleanup(false);
      }
    }
  }

  private void handleFlushInternal(boolean close)
      throws IOException, InterruptedException, ExecutionException {
    checkOpen();
    PutBlockResult putBlockResult = null;
    synchronized (this) {
      LOG.warn("flushing, totalDataFlushedLength={}, writtenDataLength={}", totalDataFlushedLength, writtenDataLength);
      // flush the last chunk data residing on the currentBuffer
      if (totalDataFlushedLength < writtenDataLength) {
        refreshCurrentBuffer();
        Preconditions.checkArgument(currentBuffer.position() > 0);
        if (currentBuffer.hasRemaining()) {
          writeChunk(currentBuffer);
          allocateNewBuffer();
        }
        // This can be a partially filled chunk. Since we are flushing the buffer
        // here, we just limit this buffer to the current position. So that next
        // write will happen in new buffer
        updateFlushLength();
        putBlockResult = executePutBlock(close, false);
      } else if (close) {
        // forcing an "empty" putBlock if stream is being closed without new
        // data since latest flush - we need to send the "EOF" flag
        putBlockResult = executePutBlock(true, true);
      }
    }
    // TODO why wait for all flush futures?
    waitOnFlushFutures();


    if (putBlockResult != null) {
      long commitIndex = putBlockResult.commitIndexFuture.get();
      LOG.info("commitIndex={}, flush position={}", commitIndex, putBlockResult.flushPosition);
      watchForCommit(commitIndex);
    } else {
      // todo, what would we wait if hsycn doesn't create writeChunk/PutBlock?
//      watchForCommit(false);
    }
    // just check again if the exception is hit while waiting for the
    // futures to ensure flush has indeed succeeded

    // irrespective of whether the commitIndex2flushedDataMap is empty
    // or not, ensure there is no exception set
    checkOpen();
  }

  @Override
  public void close() throws IOException {
    if (xceiverClientFactory != null && xceiverClient != null) {
      if (bufferPool != null && bufferPool.getSize() > 0) {
        handleFlush(true);
        // TODO: Turn the below buffer empty check on when Standalone pipeline
        // is removed in the write path in tests
        // Preconditions.checkArgument(buffer.position() == 0);
        // bufferPool.checkBufferPoolEmpty();
      } else {
        cleanup(false);
      }
    }
  }

  CompletableFuture<Void> getCombinedFlushFuture() {
    return CompletableFuture.completedFuture(null);  // dummy
  }

  void waitOnFlushFutures() throws InterruptedException, ExecutionException {
  }

  void validateResponse(
      ContainerCommandResponseProto responseProto)
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


  public void setIoException(Exception e) {
    IOException ioe = getIoException();
    if (ioe == null) {
      IOException exception =  new IOException(EXCEPTION_MSG + e.toString(), e);
      ioException.compareAndSet(null, exception);
      LOG.debug("Exception: for block ID: " + blockID,  e);
    } else {
      LOG.debug("Previous request had already failed with {} " +
              "so subsequent request also encounters " +
              "Storage Container Exception {}", ioe, e);
    }
  }

  void cleanup() {
  }

  public void cleanup(boolean invalidateClient) {
    if (xceiverClientFactory != null) {
      xceiverClientFactory.releaseClient(xceiverClient, invalidateClient);
    }
    xceiverClientFactory = null;
    xceiverClient = null;
    cleanup();

    if (bufferList !=  null) {
      bufferList.clear();
    }
    bufferList = null;
  }

  /**
   * Checks if the stream is open or exception has occurred.
   * If not, throws an exception.
   *
   * @throws IOException if stream is closed
   */
  void checkOpen() throws IOException {
    if (isClosed()) {
      throw new IOException("BlockOutputStream has been closed.");
    } else if (getIoException() != null) {
      adjustBuffersOnException();
      throw getIoException();
    }
  }

  public boolean isClosed() {
    return xceiverClient == null;
  }

  /**
   * Writes buffered data as a new chunk to the container and saves chunk
   * information to be used later in putKey call.
   *
   * @throws IOException if there is an I/O error while performing the call
   * @throws OzoneChecksumException if there is an error while computing
   * checksum
   * @return
   */
  CompletableFuture<ContainerCommandResponseProto> writeChunkToContainer(
      ChunkBuffer chunk) throws IOException {
    int effectiveChunkSize = chunk.remaining();
    final long offset = chunkOffset.getAndAdd(effectiveChunkSize);
    final ByteString data = chunk.toByteString(
        bufferPool.byteStringConversion());
    ChecksumData checksumData = checksum.computeChecksum(chunk);
    ChunkInfo chunkInfo = ChunkInfo.newBuilder()
        .setChunkName(blockID.get().getLocalID() + "_chunk_" + ++chunkIndex)
        .setOffset(offset)
        .setLen(effectiveChunkSize)
        .setChecksumData(checksumData.getProtoBufMessage())
        .build();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing chunk {} length {} at offset {}",
          chunkInfo.getChunkName(), effectiveChunkSize, offset);
    }

    final ChunkInfo previous = previousChunkInfo.getAndSet(chunkInfo);
    final long expectedOffset = previous == null ? 0
        : chunkInfo.getChunkName().equals(previous.getChunkName()) ?
        previous.getOffset() : previous.getOffset() + previous.getLen();
    if (chunkInfo.getOffset() != expectedOffset) {
      throw new IOException("Unexpected offset: "
          + chunkInfo.getOffset() + "(actual) != "
          + expectedOffset + "(expected), "
          + blockID + ", chunkInfo = " + chunkInfo
          + ", previous = " + previous);
    }

    try {
      XceiverClientReply asyncReply = writeChunkAsync(xceiverClient, chunkInfo,
          blockID.get(), data, tokenString, replicationIndex);
      CompletableFuture<ContainerCommandResponseProto>
          respFuture = asyncReply.getResponse();
      CompletableFuture<ContainerCommandResponseProto>
          validateFuture = respFuture.thenApplyAsync(e -> {
            try {
              validateResponse(e);
            } catch (IOException sce) {
              respFuture.completeExceptionally(sce);
            }
            return e;
          }, responseExecutor).exceptionally(e -> {
            String msg = "Failed to write chunk " + chunkInfo.getChunkName() +
                " into block " + blockID;
            LOG.debug("{}, exception: {}", msg, e.getLocalizedMessage());
            CompletionException ce = new CompletionException(msg, e);
            setIoException(ce);
            throw ce;
          });
      containerBlockData.addChunks(chunkInfo);
      clientMetrics.recordWriteChunk(pipeline, chunkInfo.getLen());
      return validateFuture;
    } catch (IOException | ExecutionException e) {
      throw new IOException(EXCEPTION_MSG + e.toString(), e);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      handleInterruptedException(ex, false);
    }
    return null;
  }

  /**
   * Handles InterruptedExecution.
   *
   * @param ex
   * @param processExecutionException is optional, if passed as TRUE, then
   * handle ExecutionException else skip it.
   * @throws IOException
   */
  void handleInterruptedException(Exception ex,
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
    adjustBuffersOnException();
    throw getIoException();
  }

  /**
   * Get the Replication Index.
   * @return replicationIndex
   */
  public int getReplicationIndex() {
    return replicationIndex;
  }
}
