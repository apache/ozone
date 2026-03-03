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

import static org.apache.hadoop.hdds.HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC;
import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls.putBlockAsync;
import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls.writeChunkAsync;
import static org.apache.hadoop.ozone.OzoneConsts.INCREMENTAL_CHUNK_LIST;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
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
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.hadoop.util.Time;
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
  static final Logger LOG =
      LoggerFactory.getLogger(BlockOutputStream.class);
  public static final String EXCEPTION_MSG =
      "Unexpected Storage Container Exception: ";
  public static final KeyValue INCREMENTAL_CHUNK_LIST_KV =
      KeyValue.newBuilder().setKey(INCREMENTAL_CHUNK_LIST).build();
  public static final String FULL_CHUNK = "full";
  public static final KeyValue FULL_CHUNK_KV =
      KeyValue.newBuilder().setKey(FULL_CHUNK).build();

  private AtomicReference<BlockID> blockID;
  // planned block full size
  private long blockSize;
  private AtomicBoolean eofSent = new AtomicBoolean(false);
  private final AtomicReference<ChunkInfo> previousChunkInfo
      = new AtomicReference<>();

  private final BlockData.Builder containerBlockData;
  private volatile XceiverClientFactory xceiverClientFactory;
  private XceiverClientSpi xceiverClient;
  private OzoneClientConfig config;
  private StreamBufferArgs streamBufferArgs;

  private int chunkIndex;
  private final AtomicLong chunkOffset = new AtomicLong();
  private final BufferPool bufferPool;
  private static final DirectBufferPool DIRECT_BUFFER_POOL = new DirectBufferPool();
  // The IOException will be set by response handling thread in case there is an
  // exception received in the response. If the exception is set, the next
  // request will fail upfront.
  private final AtomicReference<IOException> ioException;
  private final ExecutorService responseExecutor;

  // the effective length of data sent to datanodes (via writeChunk).
  private long totalWriteChunkLength;

  // The effective length of data flushed to datanodes (via putBlock).
  private long totalPutBlockLength;

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
  // last chunk holds the buffer after the last complete chunk, which may be
  // different from currentBuffer. We need this to calculate checksum.
  private ByteBuffer lastChunkBuffer;
  private long lastChunkOffset;
  private final Token<? extends TokenIdentifier> token;
  private final String tokenString;
  private int replicationIndex;
  private Pipeline pipeline;
  private final ContainerClientMetrics clientMetrics;
  private boolean allowPutBlockPiggybacking;
  private boolean supportIncrementalChunkList;

  private CompletableFuture<Void> lastFlushFuture;
  private CompletableFuture<Void> allPendingFlushFutures = CompletableFuture.completedFuture(null);

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
      long blockSize,
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
    this.blockSize = blockSize;
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
    this.pipeline = pipeline;
    // tell DataNode I will send incremental chunk list
    this.supportIncrementalChunkList = canEnableIncrementalChunkList();
    LOG.debug("incrementalChunkList is {}", supportIncrementalChunkList);
    if (supportIncrementalChunkList) {
      this.containerBlockData.addMetadata(INCREMENTAL_CHUNK_LIST_KV);
      this.lastChunkBuffer = DIRECT_BUFFER_POOL.getBuffer(config.getStreamBufferSize());
      this.lastChunkOffset = 0;
    } else {
      this.lastChunkBuffer = null;
    }
    this.xceiverClient = xceiverClientManager.acquireClient(pipeline);
    this.bufferPool = bufferPool;
    this.token = token;
    this.tokenString = (this.token == null) ? null :
        this.token.encodeToUrlString();

    //number of buffers used before doing a flush
    currentBuffer = null;
    currentBufferRemaining = 0;
    flushPeriod = (int) (streamBufferArgs.getStreamBufferFlushSize() / streamBufferArgs
        .getStreamBufferSize());

    Preconditions
        .checkArgument(
            (long) flushPeriod * streamBufferArgs.getStreamBufferSize() == streamBufferArgs
                .getStreamBufferFlushSize());

    this.responseExecutor = blockOutputStreamResourceProvider.get();
    bufferList = null;
    totalWriteChunkLength = 0;
    totalPutBlockLength = 0;
    writtenDataLength = 0;
    failedServers = new CopyOnWriteArrayList<>();
    ioException = new AtomicReference<>(null);
    this.checksum = new Checksum(config.getChecksumType(), config.getBytesPerChecksum(), true);
    this.clientMetrics = clientMetrics;
    this.streamBufferArgs = streamBufferArgs;
    this.allowPutBlockPiggybacking = canEnablePutblockPiggybacking();
    LOG.debug("PutBlock piggybacking is {}", allowPutBlockPiggybacking);
  }

  /**
   * Helper method to check if incremental chunk list can be enabled.
   * Prints debug messages if it cannot be enabled.
   */
  private boolean canEnableIncrementalChunkList() {
    boolean confEnableIncrementalChunkList = config.getIncrementalChunkList();
    if (!confEnableIncrementalChunkList) {
      return false;
    }

    if (!(this instanceof RatisBlockOutputStream)) {
      // Note: EC does not support incremental chunk list
      LOG.debug("Unable to enable incrementalChunkList because BlockOutputStream is not a RatisBlockOutputStream");
      return false;
    }
    if (!allDataNodesSupportPiggybacking()) {
      // Not all datanodes support piggybacking and incremental chunk list.
      LOG.debug("Unable to enable incrementalChunkList because not all datanodes support piggybacking");
      return false;
    }
    return confEnableIncrementalChunkList;
  }

  /**
   * Helper method to check if PutBlock piggybacking can be enabled.
   * Prints debug message if it cannot be enabled.
   */
  private boolean canEnablePutblockPiggybacking() {
    boolean confEnablePutblockPiggybacking = config.getEnablePutblockPiggybacking();
    if (!confEnablePutblockPiggybacking) {
      return false;
    }

    if (!allDataNodesSupportPiggybacking()) {
      // Not all datanodes support piggybacking and incremental chunk list.
      LOG.debug("Unable to enable PutBlock piggybacking because not all datanodes support piggybacking");
      return false;
    }
    return confEnablePutblockPiggybacking;
  }

  private boolean allDataNodesSupportPiggybacking() {
    // return true only if all DataNodes in the pipeline are on a version
    // that supports PutBlock piggybacking.
    for (DatanodeDetails dn : pipeline.getNodes()) {
      LOG.debug("dn = {}, version = {}", dn, dn.getCurrentVersion());
      if (!COMBINED_PUTBLOCK_WRITECHUNK_RPC.isSupportedBy(
          dn.getCurrentVersion())) {
        return false;
      }
    }
    return true;
  }

  public BlockID getBlockID() {
    return blockID.get();
  }

  public long getTotalAckDataLength() {
    return 0;
  }

  public synchronized long getWrittenDataLength() {
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
    return totalPutBlockLength;
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
    synchronized (this) {
      allocateNewBufferIfNeeded();
      currentBuffer.put((byte) b);
      currentBufferRemaining--;
      updateWrittenDataLength(1);
      writeChunkIfNeeded();
      doFlushOrWatchIfNeeded();
    }
  }

  private void writeChunkIfNeeded() throws IOException {
    if (currentBufferRemaining == 0) {
      LOG.debug("WriteChunk from write(), buffer = {}", currentBuffer);
      clientMetrics.getWriteChunksDuringWrite().incr();
      writeChunk(currentBuffer);
      updateWriteChunkLength();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkOpen();
    if (b == null) {
      throw new NullPointerException();
    }
    if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException("Offset=" + off + " and len="
          + len + " don't match the array length of " + b.length);
    }
    if (len == 0) {
      return;
    }
    synchronized (this) {
      while (len > 0) {
        allocateNewBufferIfNeeded();
        final int writeLen = Math.min(currentBufferRemaining, len);
        currentBuffer.put(b, off, writeLen);
        currentBufferRemaining -= writeLen;
        updateWrittenDataLength(writeLen);
        writeChunkIfNeeded();
        off += writeLen;
        len -= writeLen;
        doFlushOrWatchIfNeeded();
      }
    }
  }

  protected synchronized void updateWrittenDataLength(int writeLen) {
    writtenDataLength += writeLen;
  }

  private void doFlushOrWatchIfNeeded() throws IOException {
    if (currentBufferRemaining == 0) {
      if (bufferPool.getNumberOfUsedBuffers() % flushPeriod == 0) {
        updatePutBlockLength();
        CompletableFuture<PutBlockResult> putBlockFuture = executePutBlock(false, false);
        recordWatchForCommitAsync(putBlockFuture);
        clientMetrics.getFlushesDuringWrite().incr();
      }

      if (bufferPool.isAtCapacity()) {
        handleFullBuffer();
      }
    }
  }

  private void recordWatchForCommitAsync(CompletableFuture<PutBlockResult> putBlockResultFuture) {
    final CompletableFuture<Void> flushFuture = putBlockResultFuture.thenCompose(x -> watchForCommit(x.commitIndex));

    Preconditions.checkState(Thread.holdsLock(this));
    this.lastFlushFuture = flushFuture;
    this.allPendingFlushFutures = allPendingFlushFutures.thenCombine(flushFuture, (last, curr) -> null);
  }

  private void allocateNewBufferIfNeeded() throws IOException {
    if (currentBufferRemaining == 0) {
      try {
        currentBuffer = bufferPool.allocateBuffer(config.getBufferIncrement());
        currentBufferRemaining = currentBuffer.remaining();
        LOG.debug("Allocated new buffer {}, used = {}, capacity = {}", currentBuffer,
            bufferPool.getNumberOfUsedBuffers(), bufferPool.getCapacity());
      } catch (InterruptedException e) {
        handleInterruptedException(e, false);
      }
    }
  }

  private void updateWriteChunkLength() {
    Preconditions.checkState(Thread.holdsLock(this));
    totalWriteChunkLength = writtenDataLength;
  }

  private void updatePutBlockLength() {
    Preconditions.checkState(Thread.holdsLock(this));
    totalPutBlockLength = totalWriteChunkLength;
  }

  /**
   * Will be called on the retryPath in case closedContainerException/
   * TimeoutException.
   * @param len length of data to write
   * @throws IOException if error occurred
   */
  public synchronized void writeOnRetry(long len) throws IOException {
    if (len == 0) {
      return;
    }

    // In this case, the data from the failing (previous) block already cached in the allocated buffers in
    // the BufferPool. For each pending buffers in the BufferPool, we sequentially flush it and wait synchronously.

    List<ChunkBuffer> allocatedBuffers = bufferPool.getAllocatedBuffers();
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: Retrying write length {} on target blockID {}, {} buffers", this, len, blockID,
          allocatedBuffers.size());
    }
    Preconditions.checkArgument(len <= streamBufferArgs.getStreamBufferMaxSize());
    int count = 0;
    while (len > 0) {
      ChunkBuffer buffer = allocatedBuffers.get(count);
      long writeLen = Math.min(buffer.position(), len);
      len -= writeLen;
      count++;
      writtenDataLength += writeLen;
      updateWriteChunkLength();
      updatePutBlockLength();
      LOG.debug("Write chunk on retry buffer = {}", buffer);
      CompletableFuture<PutBlockResult> putBlockFuture;
      if (allowPutBlockPiggybacking) {
        putBlockFuture = writeChunkAndPutBlock(buffer, false);
      } else {
        writeChunk(buffer);
        putBlockFuture = executePutBlock(false, false);
      }
      CompletableFuture<Void> watchForCommitAsync =
          putBlockFuture.thenCompose(x -> watchForCommit(x.commitIndex));
      try {
        watchForCommitAsync.get();
      } catch (InterruptedException e) {
        handleInterruptedException(e, true);
      } catch (ExecutionException e) {
        handleExecutionException(e);
      }
    }
  }

  /**
   * This is a blocking call. It will wait for the flush till the commit index
   * at the head of the commitIndex2flushedDataMap gets replicated to all or
   * majority.
   * @throws IOException
   */
  private void handleFullBuffer() throws IOException {
    try {
      checkOpen();
      waitOnFlushFuture();
    } catch (ExecutionException e) {
      handleExecutionException(e);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      handleInterruptedException(ex, true);
    }
  }

  void releaseBuffersOnException() {
  }

  /**
   * Send a watch request to wait until the given index became committed.
   * When watch is not needed (e.g. EC), this is a NOOP.
   *
   * @param index the log index to wait for.
   * @return the future of the reply.
   */
  CompletableFuture<XceiverClientReply> sendWatchForCommit(long index) {
    return CompletableFuture.completedFuture(null);
  }

  private CompletableFuture<Void> watchForCommit(long commitIndex) {
    try {
      checkOpen();
    } catch (IOException e) {
      throw new FlushRuntimeException(e);
    }

    LOG.debug("Entering watchForCommit commitIndex = {}", commitIndex);
    final long start = Time.monotonicNowNanos();
    return sendWatchForCommit(commitIndex)
        .thenAccept(this::checkReply)
        .exceptionally(e -> {
          throw new FlushRuntimeException(setIoException(e));
        })
        .whenComplete((r, e) -> {
          LOG.debug("Leaving watchForCommit commitIndex = {}", commitIndex);
          clientMetrics.getHsyncWatchForCommitNs().add(Time.monotonicNowNanos() - start);
        });
  }

  private void checkReply(XceiverClientReply reply) {
    if (reply == null) {
      return;
    }
    final List<DatanodeDetails> dnList = reply.getDatanodes();
    if (dnList.isEmpty()) {
      return;
    }

    LOG.warn("Failed to commit BlockId {} on {}. Failed nodes: {}",
        blockID, xceiverClient.getPipeline(), dnList);
    failedServers.addAll(dnList);
  }

  void updateCommitInfo(XceiverClientReply reply, List<ChunkBuffer> buffers) {
  }

  /**
   * @param close whether putBlock is happening as part of closing the stream
   * @param force true if no data was written since most recent putBlock and
   *            stream is being closed
   */
  CompletableFuture<PutBlockResult> executePutBlock(boolean close,
      boolean force) throws IOException {
    checkOpen();
    long flushPos = totalWriteChunkLength;
    final List<ChunkBuffer> byteBufferList;
    if (!force) {
      Objects.requireNonNull(bufferList, "bufferList == null");
      byteBufferList = bufferList;
      bufferList = null;
      Objects.requireNonNull(byteBufferList, "byteBufferList == null");
    } else {
      byteBufferList = null;
    }

    final CompletableFuture<ContainerCommandResponseProto> flushFuture;
    final XceiverClientReply asyncReply;
    try {
      // Note: checksum was previously appended to containerBlockData by WriteChunk
      BlockData blockData = containerBlockData.build();
      LOG.debug("sending PutBlock {} flushPos {}", blockData, flushPos);

      if (supportIncrementalChunkList) {
        // remove any chunks in the containerBlockData list.
        // since they are sent.
        containerBlockData.clearChunks();
      }

      // if block is full, send the eof
      boolean isBlockFull = (blockSize != -1 && flushPos == blockSize);
      asyncReply = putBlockAsync(xceiverClient, blockData, close || isBlockFull, tokenString);
      CompletableFuture<ContainerCommandResponseProto> future = asyncReply.getResponse();
      flushFuture = future.thenApplyAsync(e -> {
        try {
          validateResponse(e);
        } catch (IOException sce) {
          throw new CompletionException(sce);
        }
        // if the ioException is not set, putBlock is successful
        if (getIoException() == null && !force) {
          handleSuccessfulPutBlock(e.getPutBlock().getCommittedBlockLength(),
              asyncReply, flushPos, byteBufferList);
          eofSent.set(close || isBlockFull);
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
      // never reach, just to make compiler happy.
      return null;
    }
    return flushFuture.thenApply(r -> new PutBlockResult(asyncReply.getLogIndex(), r));
  }

  @Override
  public void flush() throws IOException {
    if (xceiverClientFactory != null && xceiverClient != null
        && bufferPool != null && bufferPool.getSize() > 0
        && (!streamBufferArgs.isStreamBufferFlushDelay() ||
            unflushedLength() >= streamBufferArgs.getStreamBufferSize())) {
      handleFlush(false);
    }
  }

  private synchronized long unflushedLength() {
    return writtenDataLength - totalPutBlockLength;
  }

  private void writeChunkCommon(ChunkBuffer buffer)
      throws IOException {
    // This data in the buffer will be pushed to datanode and a reference will
    // be added to the bufferList. Once putBlock gets executed, this list will
    // be marked null. Hence, during first writeChunk call after every putBlock
    // call or during the first call to writeChunk here, the list will be null.

    if (bufferList == null) {
      bufferList = new ArrayList<>();
    }
    bufferList.add(buffer);
  }

  private void writeChunk(ChunkBuffer buffer) throws IOException {
    writeChunkCommon(buffer);
    writeChunkToContainer(buffer.duplicate(0, buffer.position()), false, false);
  }

  private CompletableFuture<PutBlockResult> writeChunkAndPutBlock(ChunkBuffer buffer, boolean close)
      throws IOException {
    LOG.debug("WriteChunk and Putblock from flush, buffer={}", buffer);
    writeChunkCommon(buffer);
    return writeChunkToContainer(buffer.duplicate(0, buffer.position()), true, close);
  }

  /**
   * @param close whether the flush is happening as part of closing the stream
   */
  protected void handleFlush(boolean close) throws IOException {
    try {
      handleFlushInternal(close);
      if (close) {
        waitForAllPendingFlushes();
      }
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
    LOG.debug("Start handleFlushInternal close={}", close);
    CompletableFuture<Void> toWaitFor = captureLatencyNs(clientMetrics.getHsyncSynchronizedWorkNs(),
        () -> handleFlushInternalSynchronized(close));

    if (toWaitFor != null) {
      LOG.debug("Waiting for flush");
      try {
        long startWaiting = Time.monotonicNowNanos();
        toWaitFor.get();
        clientMetrics.getHsyncWaitForFlushNs().add(Time.monotonicNowNanos() - startWaiting);
      } catch (ExecutionException ex) {
        if (ex.getCause() instanceof FlushRuntimeException) {
          throw ((FlushRuntimeException) ex.getCause()).cause;
        } else {
          throw ex;
        }
      }
      LOG.debug("Flush done.");
    }

    if (close) {
      // When closing, must wait for all flush futures to complete.
      allPendingFlushFutures.get();
    }
  }

  public void waitForAllPendingFlushes() throws IOException {
    // When closing, must wait for all flush futures to complete.
    try {
      allPendingFlushFutures.get();
    } catch (InterruptedException e) {
      handleInterruptedException(e, true);
    } catch (ExecutionException e) {
      handleExecutionException(e);
    }
  }

  private synchronized CompletableFuture<Void> handleFlushInternalSynchronized(boolean close) throws IOException {
    long start = Time.monotonicNowNanos();
    CompletableFuture<PutBlockResult> putBlockResultFuture = null;
    // flush the last chunk data residing on the currentBuffer
    if (totalWriteChunkLength < writtenDataLength) {
      Preconditions.checkArgument(currentBuffer.position() > 0);

      // This can be a partially filled chunk. Since we are flushing the buffer
      // here, we just limit this buffer to the current position. So that next
      // write will happen in new buffer
      updateWriteChunkLength();
      updatePutBlockLength();
      if (currentBuffer.hasRemaining()) {
        if (allowPutBlockPiggybacking) {
          putBlockResultFuture = writeChunkAndPutBlock(currentBuffer, close);
        } else {
          writeChunk(currentBuffer);
          putBlockResultFuture = executePutBlock(close, false);
        }
        if (!close) {
          // reset current buffer so that the next write will allocate a new one.
          currentBuffer = null;
          currentBufferRemaining = 0;
        }
      } else {
        putBlockResultFuture = executePutBlock(close, false);
        // set lastFuture.
      }
    } else if (totalPutBlockLength < totalWriteChunkLength) {
      // There're no pending written data, but there're uncommitted data.
      updatePutBlockLength();
      putBlockResultFuture = executePutBlock(close, false);
    } else if (close && !eofSent.get()) {
      // forcing an "empty" putBlock if stream is being closed without new
      // data since latest flush - we need to send the "EOF" flag
      updatePutBlockLength();
      putBlockResultFuture = executePutBlock(true, true);
    } else {
      LOG.debug("Flushing without data");
    }
    if (putBlockResultFuture != null) {
      recordWatchForCommitAsync(putBlockResultFuture);
    }
    clientMetrics.getHsyncSendWriteChunkNs().add(Time.monotonicNowNanos() - start);
    return lastFlushFuture;
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
        waitForAllPendingFlushes();
        cleanup(false);
      }
    }
  }

  void waitOnFlushFuture() throws InterruptedException, ExecutionException {
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

  public IOException setIoException(Throwable e) {
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
    return getIoException();
  }

  void cleanup() {
  }

  public synchronized void cleanup(boolean invalidateClient) {
    if (xceiverClientFactory != null) {
      xceiverClientFactory.releaseClient(xceiverClient, invalidateClient);
    }
    xceiverClientFactory = null;
    xceiverClient = null;
    cleanup();

    if (bufferList != null) {
      bufferList.clear();
    }
    bufferList = null;
    if (lastChunkBuffer != null) {
      DIRECT_BUFFER_POOL.returnBuffer(lastChunkBuffer);
      lastChunkBuffer = null;
      // Clear checksum cache
      checksum.clearChecksumCache();
    }
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
  CompletableFuture<ContainerCommandResponseProto> writeChunkToContainer(ChunkBuffer chunk) throws IOException {
    return writeChunkToContainer(chunk, false, false).thenApply(x -> x.response);
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
  private CompletableFuture<PutBlockResult> writeChunkToContainer(
      ChunkBuffer chunk, boolean putBlockPiggybacking, boolean close) throws IOException {
    int effectiveChunkSize = chunk.remaining();
    final long offset = chunkOffset.getAndAdd(effectiveChunkSize);
    final ByteString data = chunk.toByteString(
        bufferPool.byteStringConversion());
    // chunk is incremental, don't cache its checksum
    ChecksumData checksumData = checksum.computeChecksum(chunk, false);
    // side note: checksum object is shared with PutBlock's (blockData) checksum calc,
    // current impl does not support caching both
    ChunkInfo chunkInfo = ChunkInfo.newBuilder()
        .setChunkName(blockID.get().getLocalID() + "_chunk_" + ++chunkIndex)
        .setOffset(offset)
        .setLen(effectiveChunkSize)
        .setChecksumData(checksumData.getProtoBufMessage())
        .build();

    long flushPos = totalWriteChunkLength;

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

    final List<ChunkBuffer> byteBufferList;
    CompletableFuture<ContainerCommandResponseProto>
        validateFuture = null;
    XceiverClientReply asyncReply;
    try {
      BlockData blockData = null;

      if (supportIncrementalChunkList) {
        updateBlockDataForWriteChunk(chunk);
      } else {
        containerBlockData.addChunks(chunkInfo);
      }
      if (putBlockPiggybacking) {
        Objects.requireNonNull(bufferList, "bufferList == null");
        byteBufferList = bufferList;
        bufferList = null;
        Objects.requireNonNull(byteBufferList, "byteBufferList == null");

        blockData = containerBlockData.build();
        LOG.debug("piggyback chunk list {}", blockData);

        if (supportIncrementalChunkList) {
          // remove any chunks in the containerBlockData list.
          // since they are sent.
          containerBlockData.clearChunks();
        }
      } else {
        byteBufferList = null;
      }

      asyncReply = writeChunkAsync(xceiverClient, chunkInfo,
          blockID.get(), data, tokenString, replicationIndex, blockData, close);
      CompletableFuture<ContainerCommandResponseProto>
          respFuture = asyncReply.getResponse();
      validateFuture = respFuture.thenApplyAsync(e -> {
        try {
          validateResponse(e);
        } catch (IOException sce) {
          respFuture.completeExceptionally(sce);
        }
        // if the ioException is not set, putBlock is successful
        if (getIoException() == null && putBlockPiggybacking) {
          handleSuccessfulPutBlock(e.getWriteChunk().getCommittedBlockLength(),
              asyncReply, flushPos, byteBufferList);
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
      clientMetrics.recordWriteChunk(pipeline, chunkInfo.getLen());

    } catch (IOException | ExecutionException e) {
      throw new IOException(EXCEPTION_MSG + e.toString(), e);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      handleInterruptedException(ex, false);
      // never reach.
      return null;
    }
    return validateFuture.thenApply(x -> new PutBlockResult(asyncReply.getLogIndex(), x));
  }

  private void handleSuccessfulPutBlock(
      ContainerProtos.GetCommittedBlockLengthResponseProto e,
      XceiverClientReply asyncReply, long flushPos,
      List<ChunkBuffer> byteBufferList) {
    BlockID responseBlockID = BlockID.getFromProtobuf(
        e.getBlockID());
    Preconditions.checkState(blockID.get().getContainerBlockID()
        .equals(responseBlockID.getContainerBlockID()));
    // updates the bcsId of the block
    blockID.set(responseBlockID);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Adding index " + asyncReply.getLogIndex() + " flushLength "
              + flushPos + " numBuffers " + byteBufferList.size()
              + " blockID " + blockID + " bufferPool size " + bufferPool
              .getSize());
    }
    // for standalone protocol, logIndex will always be 0.
    updateCommitInfo(asyncReply, byteBufferList);
  }

  /**
   * Update container block data, which is later sent to DataNodes via PutBlock,
   * using the new chunks sent out via WriteChunk.
   *
   * This method is only used when incremental chunk list is enabled.
   * @param chunk the chunk buffer to be sent out by WriteChunk.
   * @throws OzoneChecksumException
   */
  private void updateBlockDataForWriteChunk(ChunkBuffer chunk)
      throws OzoneChecksumException {
    // Update lastChunkBuffer using the new chunk data.
    // This is used to calculate checksum for the last partial chunk in
    // containerBlockData which will used by PutBlock.

    // the last partial chunk in containerBlockData will be replaced.
    // So remove it.
    removeLastPartialChunk();
    chunk.rewind();
    LOG.debug("Adding chunk pos {} limit {} remaining {}." +
            "lastChunkBuffer pos {} limit {} remaining {} lastChunkOffset = {}",
        chunk.position(), chunk.limit(), chunk.remaining(),
        lastChunkBuffer.position(), lastChunkBuffer.limit(),
        lastChunkBuffer.remaining(), lastChunkOffset);

    // Append the chunk to the last chunk buffer.
    // if the resulting size exceeds limit (4MB),
    // drop the full chunk and keep the rest.
    if (lastChunkBuffer.position() + chunk.remaining() <=
        lastChunkBuffer.capacity()) {
      appendLastChunkBuffer(chunk, 0, chunk.remaining());
    } else {
      int remainingBufferSize =
          lastChunkBuffer.capacity() - lastChunkBuffer.position();
      appendLastChunkBuffer(chunk, 0, remainingBufferSize);
      updateBlockDataWithLastChunkBuffer();
      // TODO: Optional refactoring: Can attach ChecksumCache to lastChunkBuffer rather than Checksum
      appendLastChunkBuffer(chunk, remainingBufferSize,
          chunk.remaining() - remainingBufferSize);
    }
    LOG.debug("after append, lastChunkBuffer={} lastChunkOffset={}",
        lastChunkBuffer, lastChunkOffset);

    updateBlockDataWithLastChunkBuffer();
  }

  private void updateBlockDataWithLastChunkBuffer()
      throws OzoneChecksumException {
    // create chunk info for lastChunkBuffer
    ChunkInfo lastChunkInfo = createChunkInfo(lastChunkOffset);
    LOG.debug("lastChunkInfo = {}", lastChunkInfo);
    long lastChunkSize = lastChunkInfo.getLen();
    addToBlockData(lastChunkInfo);
    // Set ByteBuffer limit to capacity, pos to 0. Does not erase data
    lastChunkBuffer.clear();

    if (lastChunkSize == config.getStreamBufferSize()) {
      lastChunkOffset += config.getStreamBufferSize();
      // Reached stream buffer size (chunk size), starting new chunk, need to clear checksum cache
      checksum.clearChecksumCache();
    } else {
      lastChunkBuffer.position((int) lastChunkSize);
    }
  }

  private void appendLastChunkBuffer(ChunkBuffer chunkBuffer, int offset,
      int length) {
    LOG.debug("copying to last chunk buffer offset={} length={}",
        offset, length);
    int pos = 0;
    int uncopied = length;
    for (ByteBuffer bb : chunkBuffer.asByteBufferList()) {
      if (pos + bb.remaining() >= offset) {
        int copyStart = offset < pos ? 0 : offset - pos;
        int copyLen = Math.min(uncopied, bb.remaining());
        try {
          LOG.debug("put into last chunk buffer start = {} len = {}",
              copyStart, copyLen);
          int origPos = bb.position();
          int origLimit = bb.limit();
          bb.position(copyStart).limit(copyStart + copyLen);
          lastChunkBuffer.put(bb);
          bb.position(origPos).limit(origLimit);
        } catch (BufferOverflowException e) {
          LOG.error("appending from " + copyStart + " for len=" + copyLen +
              ". lastChunkBuffer remaining=" + lastChunkBuffer.remaining() +
              " pos=" + lastChunkBuffer.position() +
              " limit=" + lastChunkBuffer.limit() +
              " capacity=" + lastChunkBuffer.capacity());
          throw e;
        }

        uncopied -= copyLen;
      }

      pos += bb.remaining();
      if (pos >= offset + length) {
        return;
      }
      if (uncopied == 0) {
        return;
      }
    }
  }

  private void removeLastPartialChunk() {
    // remove the last chunk if it's partial.
    if (containerBlockData.getChunksList().isEmpty()) {
      return;
    }
    int lastChunkIndex = containerBlockData.getChunksCount() - 1;
    ChunkInfo lastChunkInBlockData = containerBlockData.getChunks(
        lastChunkIndex);
    if (!isFullChunk(lastChunkInBlockData)) {
      containerBlockData.removeChunks(lastChunkIndex);
    }
  }

  private ChunkInfo createChunkInfo(long lastPartialChunkOffset)
      throws OzoneChecksumException {
    lastChunkBuffer.flip();
    int revisedChunkSize = lastChunkBuffer.remaining();
    // create the chunk info to be sent in PutBlock.
    // checksum cache is utilized for this computation
    // this checksum is stored in blockData and later transferred in PutBlock
    ChecksumData revisedChecksumData = checksum.computeChecksum(lastChunkBuffer, true);

    long chunkID = lastPartialChunkOffset / config.getStreamBufferSize();
    ChunkInfo.Builder revisedChunkInfo = ChunkInfo.newBuilder()
        .setChunkName(blockID.get().getLocalID() + "_chunk_" + chunkID)
        .setOffset(lastPartialChunkOffset)
        .setLen(revisedChunkSize)
        .setChecksumData(revisedChecksumData.getProtoBufMessage());
    // if full chunk
    if (revisedChunkSize == config.getStreamBufferSize()) {
      revisedChunkInfo.addMetadata(FULL_CHUNK_KV);
    }
    return revisedChunkInfo.build();
  }

  private boolean isFullChunk(ChunkInfo chunkInfo) {
    Preconditions.checkState(
        chunkInfo.getLen() <= config.getStreamBufferSize());
    return chunkInfo.getLen() == config.getStreamBufferSize();
  }

  private void addToBlockData(ChunkInfo revisedChunkInfo) {
    LOG.debug("containerBlockData chunk: {}", containerBlockData);
    if (containerBlockData.getChunksCount() > 0) {
      ChunkInfo lastChunk = containerBlockData.getChunks(
          containerBlockData.getChunksCount() - 1);
      LOG.debug("revisedChunkInfo chunk: {}", revisedChunkInfo);
      Preconditions.checkState(lastChunk.getOffset() + lastChunk.getLen() ==
          revisedChunkInfo.getOffset(),
            "lastChunk.getOffset() + lastChunk.getLen() " +
                "!= revisedChunkInfo.getOffset()");
    }
    containerBlockData.addChunks(revisedChunkInfo);
  }

  @VisibleForTesting
  public void setXceiverClient(XceiverClientSpi xceiverClient) {
    this.xceiverClient = xceiverClient;
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
    throw getIoException();
  }

  protected synchronized CompletableFuture<Void> getLastFlushFuture() {
    return lastFlushFuture;
  }

  /**
   * Get the Replication Index.
   * @return replicationIndex
   */
  public int getReplicationIndex() {
    return replicationIndex;
  }

  static class PutBlockResult {
    private final long commitIndex;
    private final ContainerCommandResponseProto response;

    PutBlockResult(long commitIndex, ContainerCommandResponseProto response) {
      this.commitIndex = commitIndex;
      this.response = response;
    }

    ContainerCommandResponseProto getResponse() {
      return response;
    }
  }

  /**
   * RuntimeException to wrap watchForCommit errors when running asynchronously.
   */
  private static class FlushRuntimeException extends RuntimeException {
    private final IOException cause;

    FlushRuntimeException(IOException cause) {
      this.cause = cause;
    }
  }
}
