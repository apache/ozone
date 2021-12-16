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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.storage.ECBlockOutputStream;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.ozone.erasurecode.CodecRegistry;
import org.apache.ozone.erasurecode.rawcoder.RawErasureEncoder;
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
  private static final ByteBufferPool BUFFER_POOL = new ElasticByteBufferPool();
  private final RawErasureEncoder encoder;

  private enum StripeWriteStatus {
    SUCCESS,
    FAILED
  }

  private long currentBlockGroupLen = 0;

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
      boolean unsafeByteBufferConversion) {
    this.config = config;
    // For EC, cell/chunk size and buffer size can be same for now.
    ecChunkSize = replicationConfig.getEcChunkSize();
    this.config.setStreamBufferMaxSize(ecChunkSize);
    this.config.setStreamBufferFlushSize(ecChunkSize);
    this.config.setStreamBufferSize(ecChunkSize);
    this.numDataBlks = replicationConfig.getData();
    this.numParityBlks = replicationConfig.getParity();
    ecChunkBufferCache =
        new ECChunkBuffers(ecChunkSize, numDataBlks, numParityBlks);
    OmKeyInfo info = handler.getKeyInfo();
    blockOutputStreamEntryPool =
        new ECBlockOutputStreamEntryPool(config, omClient, requestId,
            replicationConfig, uploadID, partNumber, isMultipart, info,
            unsafeByteBufferConversion, xceiverClientManager, handler.getId());

    this.writeOffset = 0;
    this.encoder = CodecRegistry.getInstance()
        .getCodecFactory(replicationConfig.getCodec().name().toLowerCase())
        .createEncoder(replicationConfig);
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
    if (len == 0) {
      return;
    }
    blockOutputStreamEntryPool.allocateBlockIfNeeded();

    int currentStreamIdx = blockOutputStreamEntryPool.getCurrentStreamEntry()
        .getCurrentStreamIdx();
    int currentChunkBufferRemainingLength =
        ecChunkBufferCache.dataBuffers[currentStreamIdx].remaining();
    int currentChunkBufferLen =
        ecChunkBufferCache.dataBuffers[currentStreamIdx]
            .position();
    int maxLenToCurrChunkBuffer = Math.min(len, ecChunkSize);
    int currentWriterChunkLenToWrite =
        Math.min(currentChunkBufferRemainingLength, maxLenToCurrChunkBuffer);
    int pos = handleDataWrite(currentStreamIdx, b, off,
        currentWriterChunkLenToWrite,
        currentChunkBufferLen + currentWriterChunkLenToWrite == ecChunkSize);
    checkAndWriteParityCells(pos, false);
    int remLen = len - currentWriterChunkLenToWrite;
    int iters = remLen / ecChunkSize;
    int lastCellSize = remLen % ecChunkSize;
    off += currentWriterChunkLenToWrite;

    while (iters > 0) {
      currentStreamIdx = blockOutputStreamEntryPool.getCurrentStreamEntry()
          .getCurrentStreamIdx();
      pos = handleDataWrite(currentStreamIdx, b, off, ecChunkSize, true);
      off += ecChunkSize;
      iters--;
      checkAndWriteParityCells(pos, iters > 0 || remLen > 0);
    }

    if (lastCellSize > 0) {
      currentStreamIdx = blockOutputStreamEntryPool.getCurrentStreamEntry()
          .getCurrentStreamIdx();
      pos = handleDataWrite(currentStreamIdx, b, off,
          lastCellSize, false);
      checkAndWriteParityCells(pos, false);
    }
    writeOffset += len;
  }

  private StripeWriteStatus rewriteStripeToNewBlockGroup(int chunkSize,
      int failedStripeDataSize, boolean allocateBlockIfFull)
      throws IOException {
    long[] failedDataStripeChunkLens = new long[numDataBlks];
    long[] failedParityStripeChunkLens = new long[numParityBlks];
    final ByteBuffer[] dataBuffers = ecChunkBufferCache.getDataBuffers();
    for (int i = 0; i < numDataBlks; i++) {
      failedDataStripeChunkLens[i] = dataBuffers[i].limit();
    }
    final ByteBuffer[] parityBuffers = ecChunkBufferCache.getParityBuffers();
    for (int i = 0; i <  numParityBlks; i++) {
      failedParityStripeChunkLens[i] = parityBuffers[i].limit();
    }

    blockOutputStreamEntryPool.getCurrentStreamEntry().resetToFirstEntry();
    // Rollback the length/offset updated as part of this failed stripe write.
    offset -= failedStripeDataSize;
    blockOutputStreamEntryPool.getCurrentStreamEntry()
        .resetToAckedPosition();

    // Let's close the current entry.
    blockOutputStreamEntryPool.getCurrentStreamEntry().close();
    currentBlockGroupLen = 0;

    // Let's rewrite the last stripe, so that it will be written to new block
    // group.
    // TODO: we can improve to write partial stripe failures. In that case,
    //  we just need to write only available buffers.
    blockOutputStreamEntryPool.allocateBlockIfNeeded();
    final ECBlockOutputStreamEntry currentStreamEntry =
        blockOutputStreamEntryPool.getCurrentStreamEntry();
    long totalLenToWrite = failedStripeDataSize;
    for (int i = 0; i < numDataBlks; i++) {
      long currentLen = totalLenToWrite < failedDataStripeChunkLens[i] ?
          totalLenToWrite :
          failedDataStripeChunkLens[i];
      if (currentLen > 0) {
        handleOutputStreamWrite(i, currentLen, true, false);
      }
      currentStreamEntry.useNextBlockStream();
      totalLenToWrite -= currentLen;
    }
    for (int i = 0; i < (numParityBlks); i++) {
      handleOutputStreamWrite(i + numDataBlks, failedParityStripeChunkLens[i],
          true, true);
      currentStreamEntry.useNextBlockStream();
    }

    if (hasWriteFailure()) {
      return StripeWriteStatus.FAILED;
    }
    currentStreamEntry.executePutBlock();

    if (hasPutBlockFailure()) {
      return StripeWriteStatus.FAILED;
    }
    ECBlockOutputStreamEntry newBlockGroupStreamEntry =
        blockOutputStreamEntryPool.getCurrentStreamEntry();
    newBlockGroupStreamEntry
        .updateBlockGroupToAckedPosition(failedStripeDataSize);
    ecChunkBufferCache.clear(chunkSize);
    ecChunkBufferCache.release();

    if (newBlockGroupStreamEntry.getRemaining() <= 0) {
      // In most cases this should not happen except in the case stripe size and
      // block size same.
      newBlockGroupStreamEntry.close();
      if (allocateBlockIfFull) {
        blockOutputStreamEntryPool.allocateBlockIfNeeded();
      }
      currentBlockGroupLen = 0;
    } else {
      newBlockGroupStreamEntry.resetToFirstEntry();
    }

    return StripeWriteStatus.SUCCESS;
  }

  private void checkAndWriteParityCells(int lastDataBuffPos,
      boolean allocateBlockIfFull) throws IOException {
    //check data blocks finished
    //If index is > datanum blks
    ECBlockOutputStreamEntry currentStreamEntry =
        blockOutputStreamEntryPool.getCurrentStreamEntry();
    int currentStreamIdx = currentStreamEntry.getCurrentStreamIdx();
    if (currentStreamIdx == numDataBlks && lastDataBuffPos == ecChunkSize) {
      //Lets encode and write
      if (handleParityWrites(ecChunkSize,
          allocateBlockIfFull) == StripeWriteStatus.FAILED) {
        handleStripeFailure(numDataBlks * ecChunkSize, ecChunkSize,
            allocateBlockIfFull);
      } else {
        // At this stage stripe write is successful.
        currentStreamEntry.updateBlockGroupToAckedPosition(
            currentStreamEntry.getCurrentPosition());
      }

    }
  }

  private StripeWriteStatus handleParityWrites(int parityCellSize,
      boolean allocateBlockIfFull)
      throws IOException {
    writeParityCells(parityCellSize);
    if (hasWriteFailure()) {
      return StripeWriteStatus.FAILED;
    }

    // By this time, we should have finished full stripe. So, lets call
    // executePutBlock for all.
    // TODO: we should alter the put block calls to share CRC to each stream.
    ECBlockOutputStreamEntry streamEntry =
        blockOutputStreamEntryPool.getCurrentStreamEntry();
    streamEntry.executePutBlock();

    if (hasPutBlockFailure()) {
      return StripeWriteStatus.FAILED;
    }
    ecChunkBufferCache.clear(parityCellSize);

    if (streamEntry.getRemaining() <= 0) {
      streamEntry.close();
      if (allocateBlockIfFull) {
        blockOutputStreamEntryPool.allocateBlockIfNeeded();
      }
      currentBlockGroupLen = 0;
    } else {
      streamEntry.resetToFirstEntry();
    }

    return StripeWriteStatus.SUCCESS;
  }

  private boolean hasWriteFailure() {
    List<ECBlockOutputStream> failedStreams =
        blockOutputStreamEntryPool.getCurrentStreamEntry()
            .streamsWithWriteFailure();
    // Since writes are async, let's check the failures once.
    if (failedStreams.size() > 0) {
      addToExcludeNodesList(failedStreams);
      return true;
    }
    return false;
  }

  private boolean hasPutBlockFailure() {
    List<ECBlockOutputStream> failedStreams =
        blockOutputStreamEntryPool.getCurrentStreamEntry()
            .streamsWithPutBlockFailure();
    // Since writes are async, let's check the failures once.
    if (failedStreams.size() > 0) {
      addToExcludeNodesList(failedStreams);
      return true;
    }
    return false;
  }

  private void addToExcludeNodesList(List<ECBlockOutputStream> failedStreams) {
    for (ECBlockOutputStream failedStream : failedStreams) {
      blockOutputStreamEntryPool.getExcludeList()
          .addDatanode(failedStream.getDatanodeDetails());
    }
  }

  void writeParityCells(int parityCellSize) throws IOException {
    final ByteBuffer[] buffers = ecChunkBufferCache.getDataBuffers();
    ecChunkBufferCache.allocateParityBuffers(parityCellSize);
    final ByteBuffer[] parityBuffers = ecChunkBufferCache.getParityBuffers();

    for(int i=0; i< buffers.length; i++){
      buffers[i].flip();
    }
    encoder.encode(buffers, parityBuffers);
    blockOutputStreamEntryPool
        .getCurrentStreamEntry().forceToFirstParityBlock();
    for (int i =
         numDataBlks; i < (this.numDataBlks + this.numParityBlks); i++) {
      // Move the stream entry cursor to parity block index
      handleParityWrite(i, parityBuffers[i - numDataBlks].array(), 0,
          parityCellSize, true);
    }
  }

  private int handleDataWrite(int currIdx, byte[] b, int off, long len,
      boolean isFullCell) throws IOException {
    int pos = ecChunkBufferCache.addToDataBuffer(currIdx, b, off, (int) len);
    handleOutputStreamWrite(currIdx, len, isFullCell, false);

    if(pos == ecChunkSize){
      blockOutputStreamEntryPool.getCurrentStreamEntry().useNextBlockStream();
    }
    return pos;
  }

  private void handleParityWrite(int currIdx, byte[] b, int off, long len,
      boolean isFullCell) throws IOException {
    handleOutputStreamWrite(currIdx, len, isFullCell, true);
    blockOutputStreamEntryPool.getCurrentStreamEntry().useNextBlockStream();
  }

  private void handleOutputStreamWrite(int currIdx, long len,
      boolean isFullCell, boolean isParity) {

    BlockOutputStreamEntry current =
        blockOutputStreamEntryPool.getCurrentStreamEntry();
    int writeLengthToCurrStream =
        Math.min((int) len, (int) current.getRemaining());
    currentBlockGroupLen += isParity ? 0 : writeLengthToCurrStream;

    if (isFullCell) {
      ByteBuffer bytesToWrite = isParity ?
          ecChunkBufferCache.getParityBuffers()[currIdx - numDataBlks] :
          ecChunkBufferCache.getDataBuffers()[currIdx];
      try {
        // Since it's a fullcell, let's write all content from buffer.
        writeToOutputStream(current, len, bytesToWrite.array(),
            bytesToWrite.array().length, 0, isParity);
      } catch (Exception e) {
        markStreamAsFailed(e);
      }
    }
  }

  private int writeToOutputStream(BlockOutputStreamEntry current, long len,
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
      LOG.debug("Exception:: writeLen: " + writeLen + ", total len:" + len,
          ioe);
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
    // In EC, we will just close the current stream.
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
    throw new NotImplementedException("The flush API is not implemented yet.");
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
      if(isPartialStripe()){
        ByteBuffer bytesToWrite =
            ecChunkBufferCache.getDataBuffers()[blockOutputStreamEntryPool
                .getCurrentStreamEntry().getCurrentStreamIdx()];

        // Finish writing the current partial cached chunk
        if (bytesToWrite.position() % ecChunkSize != 0) {
          final BlockOutputStreamEntry current =
              blockOutputStreamEntryPool.getCurrentStreamEntry();
          try {
            byte[] array = bytesToWrite.array();
            writeToOutputStream(current, bytesToWrite.position(), array,
                bytesToWrite.position(), 0, false);
          } catch (Exception e) {
            markStreamAsFailed(e);
          }
        }
        final int lastStripeSize =
            (int) (currentBlockGroupLen % (numDataBlks * ecChunkSize));

        final int parityCellSize =
            lastStripeSize < ecChunkSize ? lastStripeSize : ecChunkSize;
        addPadding(parityCellSize);
        if (handleParityWrites(parityCellSize,
            false) == StripeWriteStatus.FAILED) {
          handleStripeFailure(lastStripeSize, parityCellSize, false);
        } else {
          blockOutputStreamEntryPool.getCurrentStreamEntry()
              .updateBlockGroupToAckedPosition(
                  blockOutputStreamEntryPool.getCurrentStreamEntry()
                      .getCurrentPosition());
        }

      }

      closeCurrentStreamEntry();
      Preconditions.checkArgument(writeOffset == offset);
      blockOutputStreamEntryPool.getCurrentStreamEntry().close();
      blockOutputStreamEntryPool.commitKey(offset);
    } finally {
      blockOutputStreamEntryPool.cleanup();
    }
    ecChunkBufferCache.release();
  }

  private void handleStripeFailure(int lastStripeSize, int parityCellSize,
      boolean allocateBlockIfFull)
      throws IOException {
    StripeWriteStatus stripeWriteStatus;
    for (int i = 0; i < this.config.getMaxECStripeWriteRetries(); i++) {
      stripeWriteStatus =
          rewriteStripeToNewBlockGroup(parityCellSize, lastStripeSize,
              allocateBlockIfFull);
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

  private boolean isPartialStripe() {
    return currentBlockGroupLen % (numDataBlks * ecChunkSize) > 0;
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

    public ECKeyOutputStream build() {
      return new ECKeyOutputStream(clientConfig, openHandler, xceiverManager,
          omClient, chunkSize, requestID, replicationConfig, multipartUploadID,
          multipartNumber, isMultipartKey, unsafeByteBufferConversion);
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

    ECChunkBuffers(int cellSize, int numData, int numParity) {
      this.cellSize = cellSize;
      dataBuffers = new ByteBuffer[numData];
      parityBuffers = new ByteBuffer[numParity];
      allocateBuffers(cellSize, dataBuffers);
    }

    private ByteBuffer[] getDataBuffers() {
      return dataBuffers;
    }

    private ByteBuffer[] getParityBuffers() {
      return parityBuffers;
    }

    public void allocateParityBuffers(int size){
      allocateBuffers(size, parityBuffers);
    }

    private int addToDataBuffer(int i, byte[] b, int off, int len) {
      final ByteBuffer buf = dataBuffers[i];
      final int pos = buf.position() + len;
      Preconditions.checkState(pos <= cellSize,
          "Position("+pos+") is greater than the cellSize("+cellSize+").");
      buf.put(b, off, len);
      return pos;
    }

    private void clear(int size) {
      clearBuffers(size, dataBuffers);
      clearBuffers(size, parityBuffers);
    }

    private void release() {
      releaseBuffers(dataBuffers);
      releaseBuffers(parityBuffers);
    }

    private static void allocateBuffers(int cellSize, ByteBuffer[] buffers) {
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = BUFFER_POOL.getBuffer(false, cellSize);
        buffers[i].limit(cellSize);
      }
    }

    private static void clearBuffers(int cellSize, ByteBuffer[] buffers) {
      for (int i = 0; i < buffers.length; i++) {
        buffers[i].clear();
        buffers[i].limit(cellSize);
      }
    }

    private static void releaseBuffers(ByteBuffer[] buffers) {
      for (int i = 0; i < buffers.length; i++) {
        if (buffers[i] != null) {
          BUFFER_POOL.putBuffer(buffers[i]);
          buffers[i] = null;
        }
      }
    }
  }
}
