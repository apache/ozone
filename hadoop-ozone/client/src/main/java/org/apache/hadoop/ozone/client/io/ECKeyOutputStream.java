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

package org.apache.hadoop.ozone.client.io;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ECBlockOutputStream;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.ozone.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.ozone.erasurecode.rawcoder.util.CodecUtil;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ECKeyOutputStream handles the EC writes by writing the data into underlying
 * block output streams chunk by chunk.
 */
public final class ECKeyOutputStream extends KeyOutputStream
    implements KeyMetadataAware {

  private static final Logger LOG = LoggerFactory.getLogger(KeyOutputStream.class);

  private OzoneClientConfig config;
  private ECChunkBuffers ecChunkBufferCache;
  private final BlockingQueue<ECChunkBuffers> ecStripeQueue;
  private int chunkIndex;
  private int ecChunkSize;
  private final int numDataBlks;
  private final int numParityBlks;
  private final ByteBufferPool bufferPool;
  private final RawErasureEncoder encoder;
  private final Future<Boolean> flushFuture;
  private final AtomicLong flushCheckpoint;

  /**
   * Indicates if an atomic write is required. When set to true,
   * the amount of data written must match the declared size during the commit.
   * A mismatch will prevent the commit from succeeding.
   * This is essential for operations like S3 put to ensure atomicity.
   */
  private boolean atomicKeyCreation;

  private volatile boolean closed;
  private volatile boolean closing;
  // how much of data is actually written yet to underlying stream
  private long offset;
  // how much data has been ingested into the stream
  private long writeOffset;

  private Runnable preCommit = () -> {
  };

  @Override
  public void setPreCommit(@Nonnull Runnable preCommit) {
    this.preCommit = preCommit;
  }

  @VisibleForTesting
  public void insertFlushCheckpoint(long version) throws IOException {
    addStripeToQueue(new CheckpointDummyStripe(version));
  }

  @VisibleForTesting
  public long getFlushCheckpoint() {
    return flushCheckpoint.get();
  }

  private ECKeyOutputStream(Builder builder) {
    super(builder.getReplicationConfig(), new ECBlockOutputStreamEntryPool(builder));
    this.config = builder.getClientConfig();
    this.bufferPool = builder.getByteBufferPool();
    // For EC, cell/chunk size and buffer size can be same for now.
    ecChunkSize = builder.getReplicationConfig().getEcChunkSize();
    this.numDataBlks = builder.getReplicationConfig().getData();
    this.numParityBlks = builder.getReplicationConfig().getParity();
    ecChunkBufferCache = new ECChunkBuffers(
        ecChunkSize, numDataBlks, numParityBlks, bufferPool);
    chunkIndex = 0;
    ecStripeQueue = new ArrayBlockingQueue<>(config.getEcStripeQueueSize());

    this.writeOffset = 0;
    this.encoder = CodecUtil.createRawEncoderWithFallback(
        builder.getReplicationConfig());
    S3Auth s3Auth = builder.getS3CredentialsProvider().get();
    ThreadLocal<S3Auth> s3CredentialsProvider =
        builder.getS3CredentialsProvider();
    this.flushFuture = builder.getExecutorServiceSupplier().get().submit(() -> {
      s3CredentialsProvider.set(s3Auth);
      return flushStripeFromQueue();
    });
    this.flushCheckpoint = new AtomicLong(0);
    this.atomicKeyCreation = builder.getAtomicKeyCreation();
  }

  @Override
  protected ECBlockOutputStreamEntryPool getBlockOutputStreamEntryPool() {
    return (ECBlockOutputStreamEntryPool) super.getBlockOutputStreamEntryPool();
  }

  /**
   * Try to write the bytes sequence b[off:off+len) to underlying EC block
   * streams.
   *
   * @param b   byte data
   * @param off starting offset
   * @param len length to write
   * @throws IOException
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkNotClosed();
    if (b == null) {
      throw new NullPointerException();
    }
    if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }
    try {
      int writtenLen = 0;
      while (writtenLen < len) {
        writtenLen += handleWrite(b, off + writtenLen, len - writtenLen);
      }
    } catch (Exception e) {
      markStreamClosed();
      throw e;
    }
    writeOffset += len;
  }

  private void rollbackAndReset(ECChunkBuffers stripe) throws IOException {
    // Rollback the length/offset updated as part of this failed stripe write.
    final ByteBuffer[] dataBuffers = stripe.getDataBuffers();
    offset -= Arrays.stream(dataBuffers).mapToInt(Buffer::limit).sum();

    final ECBlockOutputStreamEntryPool blockOutputStreamEntryPool = getBlockOutputStreamEntryPool();
    final ECBlockOutputStreamEntry failedStreamEntry =
        blockOutputStreamEntryPool.getCurrentStreamEntry();
    failedStreamEntry.resetToFirstEntry();
    failedStreamEntry.resetToAckedPosition();
    // All pre-allocated blocks from the same pipeline
    // should be dropped to eliminate worthless retries.
    blockOutputStreamEntryPool.discardPreallocatedBlocks(-1,
        failedStreamEntry.getPipeline().getId());
    // Let's close the current entry.
    failedStreamEntry.close();
  }

  private void logStreamError(List<ECBlockOutputStream> failedStreams,
                              String operation) {
    if (!LOG.isWarnEnabled()) {
      return;
    }

    Set<Integer> failedStreamIndexSet =
            failedStreams.stream().map(ECBlockOutputStream::getReplicationIndex)
                    .collect(Collectors.toSet());

    String failedStreamsString = IntStream.range(1,
                    numDataBlks + numParityBlks + 1)
            .mapToObj(index -> failedStreamIndexSet.contains(index)
                    ? "F" : "S")
            .collect(Collectors.joining(" "));
    LOG.warn("{} failed: {}", operation, failedStreamsString);
    for (ECBlockOutputStream stream : failedStreams) {
      LOG.warn("Failure for replica index: {}, DatanodeDetails: {}",
              stream.getReplicationIndex(), stream.getDatanodeDetails(),
              stream.getIoException());
    }
  }

  private StripeWriteStatus commitStripeWrite(ECChunkBuffers stripe)
      throws IOException {

    final ECBlockOutputStreamEntry streamEntry = getBlockOutputStreamEntryPool().getCurrentStreamEntry();
    List<ECBlockOutputStream> failedStreams =
        streamEntry.streamsWithWriteFailure();
    if (!failedStreams.isEmpty()) {
      logStreamError(failedStreams, "EC stripe write");
      excludePipelineAndFailedDN(streamEntry.getPipeline(), failedStreams);
      return StripeWriteStatus.FAILED;
    }

    // By this time, we should have finished full stripe. So, lets call
    // executePutBlock for all.
    final boolean isLastStripe = streamEntry.getRemaining() <= 0 ||
        stripe.getLastDataCell().limit() < ecChunkSize;
    ByteString checksum = streamEntry.calculateChecksum();
    streamEntry.executePutBlock(isLastStripe,
        streamEntry.getCurrentPosition(), checksum);

    failedStreams = streamEntry.streamsWithPutBlockFailure();
    if (!failedStreams.isEmpty()) {
      logStreamError(failedStreams, "Put block");
      excludePipelineAndFailedDN(streamEntry.getPipeline(), failedStreams);
      return StripeWriteStatus.FAILED;
    }
    streamEntry.updateBlockGroupToAckedPosition(
        streamEntry.getCurrentPosition());
    stripe.clear();

    if (streamEntry.getRemaining() <= 0) {
      streamEntry.close();
    } else {
      streamEntry.resetToFirstEntry();
    }

    return StripeWriteStatus.SUCCESS;
  }

  private void excludePipelineAndFailedDN(Pipeline pipeline,
      List<ECBlockOutputStream> failedStreams) {

    // Exclude the failed pipeline
    final ECBlockOutputStreamEntryPool blockOutputStreamEntryPool = getBlockOutputStreamEntryPool();
    blockOutputStreamEntryPool.getExcludeList().addPipeline(pipeline.getId());

    // If the failure is NOT caused by other reasons (e.g. container full),
    // we assume it is caused by DN failure and exclude the failed DN.
    failedStreams.stream()
        .filter(s -> !checkIfContainerToExclude(
            HddsClientUtils.checkForException(s.getIoException())))
        .forEach(s -> blockOutputStreamEntryPool.getExcludeList()
            .addDatanode(s.getDatanodeDetails()));
  }

  @Override
  protected boolean checkIfContainerToExclude(Throwable t) {
    return super.checkIfContainerToExclude(t)
        && t instanceof ContainerNotOpenException;
  }

  private void generateParityCells() throws IOException {
    final ByteBuffer[] dataBuffers = ecChunkBufferCache.getDataBuffers();
    final ByteBuffer[] parityBuffers = ecChunkBufferCache.getParityBuffers();

    // parityCellSize = min(ecChunkSize, stripeSize)
    //                = min(cellSize, sum(dataBuffers positions))
    //                = min(dataBuffers[0].limit(), dataBuffers[0].position())
    //                = dataBuffers[0].position()
    final int parityCellSize = dataBuffers[0].position();
    int firstNonFullIndex = dataBuffers.length;
    int firstNonFullLength = 0;

    for (int i = 0; i < dataBuffers.length; i++) {
      if (dataBuffers[i].position() != ecChunkSize) {
        firstNonFullIndex = i;
        firstNonFullLength = dataBuffers[i].position();
        break;
      }
    }
    for (int i = firstNonFullIndex + 1; i < dataBuffers.length; i++) {
      Preconditions.checkState(dataBuffers[i].position() == 0,
          "Illegal stripe state: cell {} is not full while cell {} has data",
          firstNonFullIndex, i);
    }

    // Add padding to dataBuffers for encode if stripe is not full.
    for (int i = firstNonFullIndex; i < dataBuffers.length; i++) {
      padBufferToLimit(dataBuffers[i], parityCellSize);
    }

    for (ByteBuffer b : parityBuffers) {
      b.limit(parityCellSize);
    }
    for (ByteBuffer b : dataBuffers) {
      b.flip();
    }
    encoder.encode(dataBuffers, parityBuffers);

    // Remove padding from dataBuffers for (re)write data cells.
    if (firstNonFullIndex < dataBuffers.length) {
      dataBuffers[firstNonFullIndex].limit(firstNonFullLength);
    }
    for (int i = firstNonFullIndex + 1; i < dataBuffers.length; i++) {
      dataBuffers[i].limit(0);
    }
  }

  private void writeDataCells(ECChunkBuffers stripe) throws IOException {
    final ECBlockOutputStreamEntryPool blockOutputStreamEntryPool = getBlockOutputStreamEntryPool();
    blockOutputStreamEntryPool.allocateBlockIfNeeded(false);
    ByteBuffer[] dataCells = stripe.getDataBuffers();
    for (int i = 0; i < numDataBlks; i++) {
      if (dataCells[i].limit() > 0) {
        handleOutputStreamWrite(dataCells[i], false);
      }
      blockOutputStreamEntryPool.getCurrentStreamEntry().useNextBlockStream();
    }
  }

  private void writeParityCells(ECChunkBuffers stripe) {
    // Move the stream entry cursor to parity block index
    final ECBlockOutputStreamEntryPool blockOutputStreamEntryPool = getBlockOutputStreamEntryPool();
    blockOutputStreamEntryPool
        .getCurrentStreamEntry().forceToFirstParityBlock();
    ByteBuffer[] parityCells = stripe.getParityBuffers();
    for (int i = 0; i < numParityBlks; i++) {
      handleOutputStreamWrite(parityCells[i], true);
      blockOutputStreamEntryPool.getCurrentStreamEntry().useNextBlockStream();
    }
  }

  private int handleWrite(byte[] b, int off, int len) throws IOException {
    int bufferRem = ecChunkBufferCache.dataBuffers[chunkIndex].remaining();
    final int writeLen = Math.min(len, Math.min(bufferRem, ecChunkSize));
    int pos = ecChunkBufferCache.addToDataBuffer(chunkIndex, b, off, writeLen);

    // if this cell is full, use next buffer
    if (pos == ecChunkSize) {
      chunkIndex++;

      // if this is last data cell in the stripe,
      // compute parity cells and write data
      if (chunkIndex == numDataBlks) {
        generateParityCells();
        addStripeToQueue(ecChunkBufferCache);
        ecChunkBufferCache = new ECChunkBuffers(ecChunkSize,
            numDataBlks, numParityBlks, bufferPool);
        chunkIndex = 0;
      }
    }
    return writeLen;
  }

  private void handleOutputStreamWrite(ByteBuffer buffer, boolean isParity) {
    try {
      // Since it's a full cell, let's write all content from buffer.
      // At a time we write max cell size in EC. So, it should safe to cast
      // the len to int to use the super class defined write API.
      // The len cannot be bigger than cell buffer size.
      assert buffer.limit() <= ecChunkSize : "The buffer size: " +
          buffer.limit() + " should not exceed EC chunk size: " + ecChunkSize;
      writeToOutputStream(getBlockOutputStreamEntryPool().getCurrentStreamEntry(),
          buffer.array(), buffer.limit(), 0, isParity);
    } catch (Exception e) {
      markStreamAsFailed(e);
    }
  }

  private void writeToOutputStream(ECBlockOutputStreamEntry current,
      byte[] b, int writeLen, int off, boolean isParity)
      throws IOException {
    if (closing) {
      throw new IOException("Stream is closing, avoid re-opening streams");
    }
    try {
      if (!isParity) {
        // In case if exception while writing, this length will be updated back
        // as part of handleStripeFailure.
        offset += writeLen;
      }
      current.write(b, off, writeLen);
    } catch (IOException ioe) {
      LOG.debug(
          "Exception while writing the cell buffers. The writeLen: " + writeLen
              + ". The block internal index is: "
              + current
              .getCurrentStreamIdx(), ioe);
      handleException(current, ioe);
    }
  }

  private void handleException(BlockOutputStreamEntry streamEntry,
      IOException exception) throws IOException {
    Throwable t = HddsClientUtils.checkForException(exception);
    Objects.requireNonNull(t, "t == null");
    boolean containerExclusionException = checkIfContainerToExclude(t);
    if (containerExclusionException) {
      getBlockOutputStreamEntryPool().getExcludeList().addPipeline(streamEntry.getPipeline().getId());
    }
    markStreamAsFailed(exception);
  }

  private void markStreamClosed() {
    closing = true;
  }

  private void markStreamAsFailed(Exception e) {
    getBlockOutputStreamEntryPool().getCurrentStreamEntry().markFailed(e);
  }

  @Override
  public void flush() {
    LOG.debug("ECKeyOutputStream does not support flush.");
  }

  @Override
  public void hflush() {
    throw new NotImplementedException("ECKeyOutputStream does not support hflush.");
  }

  @Override
  public void hsync() {
    throw new NotImplementedException("ECKeyOutputStream does not support hsync.");
  }

  private void closeCurrentStreamEntry()
      throws IOException {
    final ECBlockOutputStreamEntryPool blockOutputStreamEntryPool = getBlockOutputStreamEntryPool();
    if (!blockOutputStreamEntryPool.isEmpty()) {
      while (true) {
        try {
          BlockOutputStreamEntry entry =
              blockOutputStreamEntryPool.getCurrentStreamEntry();
          if (entry != null) {
            try {
              entry.close();
            } catch (IOException ioe) {
              handleException(entry, ioe);
              continue;
            }
          }
          return;
        } catch (Exception e) {
          markStreamClosed();
          throw e;
        }
      }
    }
  }

  /**
   * Commit the key to OM, this will add the blocks as the new key blocks.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    final ECBlockOutputStreamEntryPool blockOutputStreamEntryPool = getBlockOutputStreamEntryPool();
    try {
      if (!closing) {
        // If stripe buffer is not empty, encode and flush the stripe.
        if (ecChunkBufferCache.getFirstDataCell().position() > 0) {
          generateParityCells();
          addStripeToQueue(ecChunkBufferCache);
        }
        // Send EOF mark to flush thread.
        addStripeToQueue(new EOFDummyStripe());

        // Wait for all the stripes to be written.
        flushFuture.get();

        Preconditions.checkArgument(writeOffset == offset,
            "Expected writeOffset= " + writeOffset
                + " Expected offset=" + offset);
        if (atomicKeyCreation) {
          long expectedSize = blockOutputStreamEntryPool.getDataSize();
          Preconditions.checkState(expectedSize == offset, String.format(
              "Expected: %d and actual %d write sizes do not match",
                  expectedSize, offset));
        }
        preCommit.run();
        blockOutputStreamEntryPool.commitKey(offset);
      }
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else {
        throw new IOException(cause);
      }
    } catch (InterruptedException e) {
      throw new IOException("Flushing thread was interrupted", e);
    } finally {
      closeCurrentStreamEntry();
      blockOutputStreamEntryPool.cleanup();
    }
  }

  private void addStripeToQueue(ECChunkBuffers stripe) throws IOException {
    try {
      do {
        // If flushFuture is done, it means that the flush thread has
        // encountered an exception. Call get() to throw that exception here.
        if (flushFuture.isDone()) {
          flushFuture.get();
          // We should never reach here.
          throw new IOException("Flush thread has ended before stream close");
        }
      } while (!ecStripeQueue.offer(stripe, 1, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while adding stripe to queue", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  private boolean flushStripeFromQueue() throws IOException {
    try {
      ECChunkBuffers stripe = ecStripeQueue.take();
      while (!closing && !(stripe instanceof EOFDummyStripe)) {
        if (stripe instanceof CheckpointDummyStripe) {
          flushCheckpoint.set(((CheckpointDummyStripe) stripe).version);
        } else {
          flushStripeToDatanodes(stripe);
          stripe.release();
        }
        stripe = ecStripeQueue.take();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while polling stripe from queue", e);
    }
    return true;
  }

  private void flushStripeToDatanodes(ECChunkBuffers stripe)
      throws IOException {
    int maxRetry = config.getMaxECStripeWriteRetries();
    for (int i = 0; i <= maxRetry; i++) {
      writeDataCells(stripe);
      writeParityCells(stripe);
      if (commitStripeWrite(stripe) == StripeWriteStatus.SUCCESS) {
        return;
      }
      // In case of failure, cleanup before retry
      rollbackAndReset(stripe);
    }
    throw new IOException("Completed max allowed retries " +
        maxRetry + " on stripe failures.");
  }

  public static void padBufferToLimit(ByteBuffer buf, int limit) {
    int pos = buf.position();
    if (pos >= limit) {
      return;
    }
    Arrays.fill(buf.array(), pos, limit, (byte)0);
    buf.position(limit);
  }

  /**
   * Builder class of ECKeyOutputStream.
   */
  public static class Builder extends KeyOutputStream.Builder {
    private ECReplicationConfig replicationConfig;
    private ByteBufferPool byteBufferPool;

    private ThreadLocal<S3Auth> s3CredentialsProvider;

    @Override
    public ECReplicationConfig getReplicationConfig() {
      return replicationConfig;
    }

    public ECKeyOutputStream.Builder setReplicationConfig(
        ECReplicationConfig replConfig) {
      this.replicationConfig = replConfig;
      return this;
    }

    public ByteBufferPool getByteBufferPool() {
      return byteBufferPool;
    }

    public ECKeyOutputStream.Builder setByteBufferPool(
        ByteBufferPool bufferPool) {
      this.byteBufferPool = bufferPool;
      return this;
    }

    public ECKeyOutputStream.Builder setS3CredentialsProvider(
        ThreadLocal<S3Auth> s3CredentialsThreadLocal) {
      this.s3CredentialsProvider = s3CredentialsThreadLocal;
      return this;
    }

    public ThreadLocal<S3Auth> getS3CredentialsProvider() {
      return s3CredentialsProvider;
    }

    @Override
    public ECKeyOutputStream build() {
      return new ECKeyOutputStream(this);
    }
  }

  /**
   * Verify that the output stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   *
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closing || closed) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED + " Key: "
          + getBlockOutputStreamEntryPool().getKeyName());
    }
  }

  private static class EOFDummyStripe extends ECChunkBuffers {
    EOFDummyStripe() {
    }
  }

  private static class CheckpointDummyStripe extends ECChunkBuffers {
    private final long version;

    CheckpointDummyStripe(long version) {
      super();
      this.version = version;
    }
  }

  private static class ECChunkBuffers {
    private final ByteBuffer[] dataBuffers;
    private final ByteBuffer[] parityBuffers;
    private int cellSize;
    private ByteBufferPool byteBufferPool;

    ECChunkBuffers() {
      dataBuffers = null;
      parityBuffers = null;
    }

    ECChunkBuffers(int cellSize, int numData, int numParity,
        ByteBufferPool byteBufferPool) {
      this.cellSize = cellSize;
      dataBuffers = new ByteBuffer[numData];
      parityBuffers = new ByteBuffer[numParity];
      this.byteBufferPool = byteBufferPool;
      allocateBuffers(dataBuffers, this.cellSize);
      allocateBuffers(parityBuffers, this.cellSize);
    }

    private ByteBuffer[] getDataBuffers() {
      return dataBuffers;
    }

    private ByteBuffer[] getParityBuffers() {
      return parityBuffers;
    }

    private ByteBuffer getFirstDataCell() {
      return dataBuffers[0];
    }

    private ByteBuffer getLastDataCell() {
      return dataBuffers[dataBuffers.length - 1];
    }

    private int addToDataBuffer(int i, byte[] b, int off, int len) {
      final ByteBuffer buf = dataBuffers[i];
      final int pos = buf.position() + len;
      Preconditions.checkState(pos <= cellSize,
          "Position(" + pos + ") is greater than the cellSize("
              + cellSize + ").");
      buf.put(b, off, len);
      return pos;
    }

    private void clear() {
      clearBuffers(dataBuffers);
      clearBuffers(parityBuffers);
    }

    private void release() {
      releaseBuffers(dataBuffers);
      releaseBuffers(parityBuffers);
    }

    private void allocateBuffers(ByteBuffer[] buffers, int bufferSize) {
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = byteBufferPool.getBuffer(false, cellSize);
        buffers[i].limit(bufferSize);
      }
    }

    private void clearBuffers(ByteBuffer[] buffers) {
      for (ByteBuffer buffer : buffers) {
        buffer.clear();
        buffer.limit(cellSize);
      }
    }

    private void releaseBuffers(ByteBuffer[] buffers) {
      for (int i = 0; i < buffers.length; i++) {
        if (buffers[i] != null) {
          byteBufferPool.putBuffer(buffers[i]);
          buffers[i] = null;
        }
      }
    }
  }

  private enum StripeWriteStatus {
    SUCCESS,
    FAILED
  }
}
