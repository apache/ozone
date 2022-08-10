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
package org.apache.hadoop.ozone.client.io;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ECBlockOutputStream;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.ozone.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.ozone.erasurecode.rawcoder.util.CodecUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ECKeyOutputStream handles the EC writes by writing the data into underlying
 * block output streams chunk by chunk.
 */
public final class ECKeyOutputStream extends KeyOutputStream {
  private OzoneClientConfig config;
  private ECChunkBuffers ecChunkBufferCache;
  private int ecChunkSize;
  private final int numDataBlks;
  private final int numParityBlks;
  private final ByteBufferPool bufferPool;
  private final RawErasureEncoder encoder;

  private enum StripeWriteStatus {
    SUCCESS,
    FAILED
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(KeyOutputStream.class);

  private boolean closed;
  // how much of data is actually written yet to underlying stream
  private long offset;
  // how much data has been ingested into the stream
  private long writeOffset;
  private final ECBlockOutputStreamEntryPool blockOutputStreamEntryPool;

  @VisibleForTesting
  public List<BlockOutputStreamEntry> getStreamEntries() {
    return blockOutputStreamEntryPool.getStreamEntries();
  }

  @VisibleForTesting
  public XceiverClientFactory getXceiverClientFactory() {
    return blockOutputStreamEntryPool.getXceiverClientFactory();
  }

  @VisibleForTesting
  public List<OmKeyLocationInfo> getLocationInfoList() {
    return blockOutputStreamEntryPool.getLocationInfoList();
  }

  private ECKeyOutputStream(Builder builder) {
    super(builder.getClientMetrics());
    this.config = builder.getClientConfig();
    this.bufferPool = builder.getByteBufferPool();
    // For EC, cell/chunk size and buffer size can be same for now.
    ecChunkSize = builder.getReplicationConfig().getEcChunkSize();
    this.config.setStreamBufferMaxSize(ecChunkSize);
    this.config.setStreamBufferFlushSize(ecChunkSize);
    this.config.setStreamBufferSize(ecChunkSize);
    this.numDataBlks = builder.getReplicationConfig().getData();
    this.numParityBlks = builder.getReplicationConfig().getParity();
    ecChunkBufferCache = new ECChunkBuffers(
        ecChunkSize, numDataBlks, numParityBlks, bufferPool);
    OmKeyInfo info = builder.getOpenHandler().getKeyInfo();
    blockOutputStreamEntryPool =
        new ECBlockOutputStreamEntryPool(config,
            builder.getOmClient(), builder.getRequestID(),
            builder.getReplicationConfig(),
            builder.getMultipartUploadID(), builder.getMultipartNumber(),
            builder.isMultipartKey(),
            info, builder.isUnsafeByteBufferConversionEnabled(),
            builder.getXceiverManager(), builder.getOpenHandler().getId(),
            builder.getClientMetrics());

    this.writeOffset = 0;
    this.encoder = CodecUtil.createRawEncoderWithFallback(
        builder.getReplicationConfig());
  }

  /**
   * When a key is opened, it is possible that there are some blocks already
   * allocated to it for this open session. In this case, to make use of these
   * blocks, we need to add these blocks to stream entries. But, a key's version
   * also includes blocks from previous versions, we need to avoid adding these
   * old blocks to stream entries, because these old blocks should not be picked
   * for write. To do this, the following method checks that, only those
   * blocks created in this particular open version are added to stream entries.
   *
   * @param version     the set of blocks that are pre-allocated.
   * @param openVersion the version corresponding to the pre-allocation.
   * @throws IOException
   */
  public void addPreallocateBlocks(OmKeyLocationInfoGroup version,
      long openVersion) throws IOException {
    blockOutputStreamEntryPool.addPreallocateBlocks(version, openVersion);
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
      throw new IOException(e.getMessage());
    }
    writeOffset += len;
  }

  private StripeWriteStatus rewriteStripeToNewBlockGroup() throws IOException {
    // Rollback the length/offset updated as part of this failed stripe write.
    final ByteBuffer[] dataBuffers = ecChunkBufferCache.getDataBuffers();
    offset -= Arrays.stream(dataBuffers).mapToInt(Buffer::limit).sum();

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

    // Let's rewrite the last stripe, so that it will be written to new block
    // group.
    // TODO: we can improve to write partial stripe failures. In that case,
    //  we just need to write only available buffers.
    blockOutputStreamEntryPool.allocateBlockIfNeeded();
    final ECBlockOutputStreamEntry currentStreamEntry =
        blockOutputStreamEntryPool.getCurrentStreamEntry();
    for (int i = 0; i < numDataBlks; i++) {
      if (dataBuffers[i].limit() > 0) {
        handleOutputStreamWrite(i, dataBuffers[i].limit(), false);
      }
      currentStreamEntry.useNextBlockStream();
    }
    return handleParityWrites();
  }

  private void encodeAndWriteParityCells() throws IOException {
    generateParityCells();
    if (handleParityWrites() == StripeWriteStatus.FAILED) {
      retryStripeWrite(config.getMaxECStripeWriteRetries());
    }
  }

  private void logStreamError(List<ECBlockOutputStream> failedStreams,
                              String operation) {
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

  private StripeWriteStatus handleParityWrites() throws IOException {
    writeParityCells();

    ECBlockOutputStreamEntry streamEntry =
        blockOutputStreamEntryPool.getCurrentStreamEntry();
    List<ECBlockOutputStream> failedStreams =
        streamEntry.streamsWithWriteFailure();
    if (!failedStreams.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        logStreamError(failedStreams, "EC stripe write");
      }
      excludePipelineAndFailedDN(streamEntry.getPipeline(), failedStreams);
      return StripeWriteStatus.FAILED;
    }

    // By this time, we should have finished full stripe. So, lets call
    // executePutBlock for all.
    // TODO: we should alter the put block calls to share CRC to each stream.
    final boolean isLastStripe = streamEntry.getRemaining() <= 0 ||
        ecChunkBufferCache.getLastDataCell().limit() < ecChunkSize;
    streamEntry.executePutBlock(isLastStripe, streamEntry.getCurrentPosition());

    failedStreams = streamEntry.streamsWithPutBlockFailure();
    if (!failedStreams.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        logStreamError(failedStreams, "Put block");
      }
      excludePipelineAndFailedDN(streamEntry.getPipeline(), failedStreams);
      return StripeWriteStatus.FAILED;
    }
    streamEntry.updateBlockGroupToAckedPosition(
        streamEntry.getCurrentPosition());
    ecChunkBufferCache.clear();

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

  private void writeParityCells() {
    // Move the stream entry cursor to parity block index
    blockOutputStreamEntryPool
        .getCurrentStreamEntry().forceToFirstParityBlock();
    ByteBuffer[] parityCells = ecChunkBufferCache.getParityBuffers();
    for (int i = 0; i < numParityBlks; i++) {
      handleOutputStreamWrite(numDataBlks + i, parityCells[i].limit(), true);
      blockOutputStreamEntryPool.getCurrentStreamEntry().useNextBlockStream();
    }
  }

  private int handleWrite(byte[] b, int off, int len) throws IOException {

    blockOutputStreamEntryPool.allocateBlockIfNeeded();

    int currIdx = blockOutputStreamEntryPool
        .getCurrentStreamEntry().getCurrentStreamIdx();
    int bufferRem = ecChunkBufferCache.dataBuffers[currIdx].remaining();
    final int writeLen = Math.min(len, Math.min(bufferRem, ecChunkSize));
    int pos = ecChunkBufferCache.addToDataBuffer(currIdx, b, off, writeLen);

    // if this cell is full, send data to the OutputStream
    if (pos == ecChunkSize) {
      handleOutputStreamWrite(currIdx, pos, false);
      blockOutputStreamEntryPool.getCurrentStreamEntry().useNextBlockStream();

      // if this is last data cell in the stripe,
      // compute and write the parity cells
      if (currIdx == numDataBlks - 1) {
        encodeAndWriteParityCells();
      }
    }
    return writeLen;
  }

  private void handleOutputStreamWrite(int currIdx, int len, boolean isParity) {
    ByteBuffer bytesToWrite = isParity ?
        ecChunkBufferCache.getParityBuffers()[currIdx - numDataBlks] :
        ecChunkBufferCache.getDataBuffers()[currIdx];
    try {
      // Since it's a full cell, let's write all content from buffer.
      // At a time we write max cell size in EC. So, it should safe to cast
      // the len to int to use the super class defined write API.
      // The len cannot be bigger than cell buffer size.
      assert len <= ecChunkSize : " The len: " + len + ". EC chunk size: "
          + ecChunkSize;
      assert len <= bytesToWrite
          .limit() : " The len: " + len + ". Chunk buffer limit: "
          + bytesToWrite.limit();
      writeToOutputStream(blockOutputStreamEntryPool.getCurrentStreamEntry(),
          bytesToWrite.array(), len, 0, isParity);
    } catch (Exception e) {
      markStreamAsFailed(e);
    }
  }

  private long writeToOutputStream(ECBlockOutputStreamEntry current,
      byte[] b, int writeLen, int off, boolean isParity)
      throws IOException {
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
    return writeLen;
  }

  private void handleException(BlockOutputStreamEntry streamEntry,
      IOException exception) throws IOException {
    Throwable t = HddsClientUtils.checkForException(exception);
    Preconditions.checkNotNull(t);
    boolean containerExclusionException = checkIfContainerToExclude(t);
    if (containerExclusionException) {
      blockOutputStreamEntryPool.getExcludeList()
          .addPipeline(streamEntry.getPipeline().getId());
    }
    markStreamAsFailed(exception);
  }

  private void markStreamClosed() {
    blockOutputStreamEntryPool.cleanup();
    closed = true;
  }

  private void markStreamAsFailed(Exception e) {
    blockOutputStreamEntryPool.getCurrentStreamEntry().markFailed(e);
  }

  @Override
  public void flush() {
    LOG.debug("ECKeyOutputStream does not support flush.");
  }

  private void closeCurrentStreamEntry()
      throws IOException {
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
    try {
      // If stripe buffer is not empty, encode and flush the stripe.
      if (ecChunkBufferCache.getFirstDataCell().position() > 0) {
        final int index = blockOutputStreamEntryPool.getCurrentStreamEntry()
            .getCurrentStreamIdx();
        ByteBuffer lastCell = ecChunkBufferCache.getDataBuffers()[index];

        // Finish writing the current partial cached chunk
        if (lastCell.position() % ecChunkSize != 0) {
          handleOutputStreamWrite(index, lastCell.position(), false);
        }

        encodeAndWriteParityCells();
      }

      closeCurrentStreamEntry();
      Preconditions.checkArgument(writeOffset == offset,
          "Expected writeOffset= " + writeOffset
              + " Expected offset=" + offset);
      blockOutputStreamEntryPool.commitKey(offset);
    } finally {
      blockOutputStreamEntryPool.cleanup();
    }
    ecChunkBufferCache.release();
  }

  private void retryStripeWrite(int times) throws IOException {
    for (int i = 0; i < times; i++) {
      if (rewriteStripeToNewBlockGroup() == StripeWriteStatus.SUCCESS) {
        return;
      }
    }
    throw new IOException("Completed max allowed retries " +
        times + " on stripe failures.");
  }

  public static void padBufferToLimit(ByteBuffer buf, int limit) {
    int pos = buf.position();
    if (pos >= limit) {
      return;
    }
    Arrays.fill(buf.array(), pos, limit, (byte)0);
    buf.position(limit);
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    return blockOutputStreamEntryPool.getCommitUploadPartInfo();
  }

  @VisibleForTesting
  public ExcludeList getExcludeList() {
    return blockOutputStreamEntryPool.getExcludeList();
  }

  /**
   * Builder class of ECKeyOutputStream.
   */
  public static class Builder extends KeyOutputStream.Builder {
    private ECReplicationConfig replicationConfig;
    private ByteBufferPool byteBufferPool;

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
    if (closed) {
      throw new IOException(
          ": " + FSExceptionMessages.STREAM_IS_CLOSED + " Key: "
              + blockOutputStreamEntryPool.getKeyName());
    }
  }

  private static class ECChunkBuffers {
    private final ByteBuffer[] dataBuffers;
    private final ByteBuffer[] parityBuffers;
    private int cellSize;
    private ByteBufferPool byteBufferPool;

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
      for (int i = 0; i < buffers.length; i++) {
        buffers[i].clear();
        buffers[i].limit(cellSize);
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
}
