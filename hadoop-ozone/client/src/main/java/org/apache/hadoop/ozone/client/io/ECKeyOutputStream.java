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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.storage.ECBlockOutputStream;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;

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
public class ECKeyOutputStream extends KeyOutputStream {
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

  @SuppressWarnings({"parameternumber", "squid:S00107"})
  public ECKeyOutputStream(OzoneClientConfig config, OpenKeySession handler,
      XceiverClientFactory xceiverClientManager, OzoneManagerProtocol omClient,
      int chunkSize, String requestId, ECReplicationConfig replicationConfig,
      String uploadID, int partNumber, boolean isMultipart,
      boolean unsafeByteBufferConversion, ByteBufferPool byteBufferPool) {
    this.config = config;
    this.bufferPool = byteBufferPool;
    // For EC, cell/chunk size and buffer size can be same for now.
    ecChunkSize = replicationConfig.getEcChunkSize();
    this.config.setStreamBufferMaxSize(ecChunkSize);
    this.config.setStreamBufferFlushSize(ecChunkSize);
    this.config.setStreamBufferSize(ecChunkSize);
    this.numDataBlks = replicationConfig.getData();
    this.numParityBlks = replicationConfig.getParity();
    ecChunkBufferCache = new ECChunkBuffers(
        ecChunkSize, numDataBlks, numParityBlks, bufferPool);
    OmKeyInfo info = handler.getKeyInfo();
    blockOutputStreamEntryPool =
        new ECBlockOutputStreamEntryPool(config, omClient, requestId,
            replicationConfig, uploadID, partNumber, isMultipart, info,
            unsafeByteBufferConversion, xceiverClientManager, handler.getId());

    this.writeOffset = 0;
    this.encoder = CodecUtil.createRawEncoderWithFallback(replicationConfig);
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
    int rem = len;
    while (rem > 0) {
      try {
        blockOutputStreamEntryPool.allocateBlockIfNeeded();
        int currentStreamIdx = blockOutputStreamEntryPool
            .getCurrentStreamEntry().getCurrentStreamIdx();
        int bufferRem =
            ecChunkBufferCache.dataBuffers[currentStreamIdx].remaining();
        int expectedWriteLen = Math.min(rem, Math.min(bufferRem, ecChunkSize));
        int oldPos =
            ecChunkBufferCache.dataBuffers[currentStreamIdx].position();
        int pos =
            handleDataWrite(currentStreamIdx, b, off, expectedWriteLen,
                oldPos + expectedWriteLen == ecChunkSize);
        checkAndWriteParityCells(pos);
        long writtenLength = pos - oldPos;
        rem -= writtenLength;
        off += writtenLength;
      } catch (Exception e) {
        markStreamClosed();
        throw new IOException(e.getMessage());
      }
    }
    writeOffset += len;
  }

  private StripeWriteStatus rewriteStripeToNewBlockGroup(
      long failedStripeDataSize, boolean close) throws IOException {
    int[] failedDataStripeChunkLens = new int[numDataBlks];
    int[] failedParityStripeChunkLens = new int[numParityBlks];
    final ByteBuffer[] dataBuffers = ecChunkBufferCache.getDataBuffers();
    for (int i = 0; i < numDataBlks; i++) {
      failedDataStripeChunkLens[i] = dataBuffers[i].limit();
    }
    final ByteBuffer[] parityBuffers = ecChunkBufferCache.getParityBuffers();
    for (int i = 0; i < numParityBlks; i++) {
      failedParityStripeChunkLens[i] = parityBuffers[i].limit();
    }

    // Rollback the length/offset updated as part of this failed stripe write.
    offset -= failedStripeDataSize;

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
    long totalLenToWrite = failedStripeDataSize;
    for (int i = 0; i < numDataBlks; i++) {
      int currentLen = (int) (totalLenToWrite < failedDataStripeChunkLens[i] ?
          totalLenToWrite :
          failedDataStripeChunkLens[i]);
      if (currentLen > 0) {
        handleOutputStreamWrite(i, currentLen, false);
      }
      currentStreamEntry.useNextBlockStream();
      totalLenToWrite -= currentLen;
    }
    for (int i = 0; i < (numParityBlks); i++) {
      handleOutputStreamWrite(i + numDataBlks, failedParityStripeChunkLens[i],
          true);
      currentStreamEntry.useNextBlockStream();
    }

    if (hasWriteFailure()) {
      handleFailedStreams(false);
      return StripeWriteStatus.FAILED;
    }
    currentStreamEntry.executePutBlock(close);

    if (hasPutBlockFailure()) {
      handleFailedStreams(true);
      return StripeWriteStatus.FAILED;
    }
    ECBlockOutputStreamEntry newBlockGroupStreamEntry =
        blockOutputStreamEntryPool.getCurrentStreamEntry();
    newBlockGroupStreamEntry
        .updateBlockGroupToAckedPosition(failedStripeDataSize);
    ecChunkBufferCache.clear();

    if (newBlockGroupStreamEntry.getRemaining() <= 0) {
      // In most cases this should not happen except in the case stripe size and
      // block size same.
      newBlockGroupStreamEntry.close();
    } else {
      newBlockGroupStreamEntry.resetToFirstEntry();
    }

    return StripeWriteStatus.SUCCESS;
  }

  private void checkAndWriteParityCells(int lastDataBuffPos)
      throws IOException {
    // Check data blocks finished
    // If index > numDataBlks
    ECBlockOutputStreamEntry currentStreamEntry =
        blockOutputStreamEntryPool.getCurrentStreamEntry();
    int currentStreamIdx = currentStreamEntry.getCurrentStreamIdx();
    if (currentStreamIdx == numDataBlks && lastDataBuffPos == ecChunkSize) {
      //Lets encode and write
      boolean shouldClose = currentStreamEntry.getRemaining() <= 0;
      if (handleParityWrites(ecChunkSize, shouldClose)
          == StripeWriteStatus.FAILED) {
        handleStripeFailure(numDataBlks * ecChunkSize, shouldClose);
      } else {
        // At this stage stripe write is successful.
        currentStreamEntry.updateBlockGroupToAckedPosition(
            currentStreamEntry.getCurrentPosition());
      }

    }
  }

  private StripeWriteStatus handleParityWrites(int parityCellSize,
      boolean isLastStripe) throws IOException {
    writeParityCells(parityCellSize);
    if (hasWriteFailure()) {
      handleFailedStreams(false);
      return StripeWriteStatus.FAILED;
    }

    // By this time, we should have finished full stripe. So, lets call
    // executePutBlock for all.
    // TODO: we should alter the put block calls to share CRC to each stream.
    ECBlockOutputStreamEntry streamEntry =
        blockOutputStreamEntryPool.getCurrentStreamEntry();
    streamEntry
        .executePutBlock(isLastStripe);

    if (hasPutBlockFailure()) {
      handleFailedStreams(true);
      return StripeWriteStatus.FAILED;
    }
    ecChunkBufferCache.clear();

    if (streamEntry.getRemaining() <= 0) {
      streamEntry.close();
    } else {
      streamEntry.resetToFirstEntry();
    }

    return StripeWriteStatus.SUCCESS;
  }

  private boolean hasWriteFailure() {
    return !blockOutputStreamEntryPool.getCurrentStreamEntry()
        .streamsWithWriteFailure().isEmpty();
  }

  private boolean hasPutBlockFailure() {
    return !blockOutputStreamEntryPool.getCurrentStreamEntry()
        .streamsWithPutBlockFailure().isEmpty();
  }

  private void handleFailedStreams(boolean forPutBlock) {
    ECBlockOutputStreamEntry currentStreamEntry =
        blockOutputStreamEntryPool.getCurrentStreamEntry();
    List<ECBlockOutputStream> failedStreams = forPutBlock
        ? currentStreamEntry.streamsWithPutBlockFailure()
        : currentStreamEntry.streamsWithWriteFailure();

    // Since writes are async, let's check the failures once.
    boolean containerToExcludeAll = true;
    for (ECBlockOutputStream failedStream : failedStreams) {
      Throwable cause = HddsClientUtils.checkForException(
          failedStream.getIoException());
      Preconditions.checkNotNull(cause);
      if (!checkIfContainerToExclude(cause)) {
        blockOutputStreamEntryPool.getExcludeList()
            .addDatanode(failedStream.getDatanodeDetails());
        containerToExcludeAll = false;
      }
    }

    // NOTE: For now, this is mainly for ContainerNotOpenException
    // due to container full, but may also for those cases that
    // a DN do respond but with one with certain failures.
    // In such cases we don't treat the replied DNs as failed.
    if (containerToExcludeAll) {
      blockOutputStreamEntryPool.getExcludeList()
          .addPipeline(currentStreamEntry.getPipeline().getId());
    }
  }

  @Override
  protected boolean checkIfContainerToExclude(Throwable t) {
    return super.checkIfContainerToExclude(t)
        && t instanceof ContainerNotOpenException;
  }

  void writeParityCells(int parityCellSize) throws IOException {
    final ByteBuffer[] buffers = ecChunkBufferCache.getDataBuffers();
    final ByteBuffer[] parityBuffers = ecChunkBufferCache.getParityBuffers();

    for (ByteBuffer b : parityBuffers) {
      b.limit(parityCellSize);
    }
    for (ByteBuffer b : buffers) {
      b.flip();
    }
    encoder.encode(buffers, parityBuffers);
    blockOutputStreamEntryPool
        .getCurrentStreamEntry().forceToFirstParityBlock();
    for (int i =
         numDataBlks; i < (this.numDataBlks + this.numParityBlks); i++) {
      // Move the stream entry cursor to parity block index
      handleParityWrite(i, parityCellSize);
    }
  }

  private int handleDataWrite(int currIdx, byte[] b, int off, long len,
      boolean isFullCell) {
    int pos = ecChunkBufferCache.addToDataBuffer(currIdx, b, off, (int) len);

    if (isFullCell) {
      Preconditions.checkArgument(pos == ecChunkSize,
          "When full cell passed, the pos: " + pos
              + " should match to ec chunk size.");
      handleOutputStreamWrite(currIdx, pos, false);
      blockOutputStreamEntryPool.getCurrentStreamEntry().useNextBlockStream();
    }
    return pos;
  }

  private void handleParityWrite(int currIdx, int len) {
    handleOutputStreamWrite(currIdx, len, true);
    blockOutputStreamEntryPool.getCurrentStreamEntry().useNextBlockStream();
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
      final long lastStripeSize = getCurrentDataStripeSize();
      if (isPartialStripe(lastStripeSize)) {
        ByteBuffer bytesToWrite =
            ecChunkBufferCache.getDataBuffers()[blockOutputStreamEntryPool
                .getCurrentStreamEntry().getCurrentStreamIdx()];

        // Finish writing the current partial cached chunk
        if (bytesToWrite.position() % ecChunkSize != 0) {
          final ECBlockOutputStreamEntry current =
              blockOutputStreamEntryPool.getCurrentStreamEntry();
          try {
            byte[] array = bytesToWrite.array();
            writeToOutputStream(current, array,
                bytesToWrite.position(), 0, false);
          } catch (Exception e) {
            markStreamAsFailed(e);
          }
        }

        final int parityCellSize =
            (int) (lastStripeSize < ecChunkSize ? lastStripeSize : ecChunkSize);
        addPadding(parityCellSize);
        if (handleParityWrites(parityCellSize, true)
            == StripeWriteStatus.FAILED) {
          handleStripeFailure(lastStripeSize, true);
        } else {
          blockOutputStreamEntryPool.getCurrentStreamEntry()
              .updateBlockGroupToAckedPosition(
                  blockOutputStreamEntryPool.getCurrentStreamEntry()
                      .getCurrentPosition());
        }

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

  private void handleStripeFailure(long lastStripeSize, boolean isClose)
      throws IOException {
    StripeWriteStatus stripeWriteStatus;
    for (int i = 0; i < this.config.getMaxECStripeWriteRetries(); i++) {
      stripeWriteStatus = rewriteStripeToNewBlockGroup(lastStripeSize, isClose);
      if (stripeWriteStatus == StripeWriteStatus.SUCCESS) {
        return;
      }
    }
    throw new IOException("Completed max allowed retries " + this.config
        .getMaxECStripeWriteRetries() + " on stripe failures.");

  }

  private void addPadding(int parityCellSize) {
    ByteBuffer[] buffers = ecChunkBufferCache.getDataBuffers();

    for (int i = 1; i < numDataBlks; i++) {
      final int position = buffers[i].position();
      assert position <= parityCellSize : "If an internal block is smaller"
          + " than parity block, then its last cell should be small than last"
          + " parity cell";
      padBufferToLimit(buffers[i], parityCellSize);
    }
  }

  public static void padBufferToLimit(ByteBuffer buf, int limit) {
    int pos = buf.position();
    if (pos >= limit) {
      return;
    }
    Arrays.fill(buf.array(), pos, limit, (byte)0);
    buf.position(limit);
  }

  private boolean isPartialStripe(long stripeSize) {
    return stripeSize > 0 && stripeSize < (numDataBlks * ecChunkSize);
  }

  private long getCurrentDataStripeSize() {
    final ByteBuffer[] dataBuffers = ecChunkBufferCache.getDataBuffers();
    long lastStripeSize = 0;
    for (int i = 0; i < numDataBlks; i++) {
      lastStripeSize += dataBuffers[i].position();
    }
    return lastStripeSize;
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
  public static class Builder {
    private OpenKeySession openHandler;
    private XceiverClientFactory xceiverManager;
    private OzoneManagerProtocol omClient;
    private int chunkSize;
    private String requestID;
    private String multipartUploadID;
    private int multipartNumber;
    private boolean isMultipartKey;
    private boolean unsafeByteBufferConversion;
    private OzoneClientConfig clientConfig;
    private ECReplicationConfig replicationConfig;
    private ByteBufferPool byteBufferPool;

    public Builder setMultipartUploadID(String uploadID) {
      this.multipartUploadID = uploadID;
      return this;
    }

    public Builder setMultipartNumber(int partNumber) {
      this.multipartNumber = partNumber;
      return this;
    }

    public Builder setHandler(OpenKeySession handler) {
      this.openHandler = handler;
      return this;
    }

    public Builder setXceiverClientManager(XceiverClientFactory manager) {
      this.xceiverManager = manager;
      return this;
    }

    public Builder setOmClient(OzoneManagerProtocol client) {
      this.omClient = client;
      return this;
    }

    public Builder setChunkSize(int size) {
      this.chunkSize = size;
      return this;
    }

    public Builder setRequestID(String id) {
      this.requestID = id;
      return this;
    }

    public Builder setIsMultipartKey(boolean isMultipart) {
      this.isMultipartKey = isMultipart;
      return this;
    }

    public Builder setConfig(OzoneClientConfig config) {
      this.clientConfig = config;
      return this;
    }

    public Builder enableUnsafeByteBufferConversion(boolean enabled) {
      this.unsafeByteBufferConversion = enabled;
      return this;
    }

    public ECKeyOutputStream.Builder setReplicationConfig(
        ECReplicationConfig replConfig) {
      this.replicationConfig = replConfig;
      return this;
    }

    public ECKeyOutputStream.Builder setByteBufferPool(
        ByteBufferPool bufferPool) {
      this.byteBufferPool = bufferPool;
      return this;
    }

    public ECKeyOutputStream build() {
      return new ECKeyOutputStream(clientConfig, openHandler, xceiverManager,
          omClient, chunkSize, requestID, replicationConfig, multipartUploadID,
          multipartNumber, isMultipartKey, unsafeByteBufferConversion,
          byteBufferPool);
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
