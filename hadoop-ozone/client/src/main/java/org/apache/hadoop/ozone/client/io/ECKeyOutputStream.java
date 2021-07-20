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
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
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
  private int ecChunkSize = 1024;
  private ECReplicationConfig ecReplicationConfig;
  private static final ByteBufferPool BUFFER_POOL = new ElasticByteBufferPool();
  private final RawErasureEncoder encoder;
  // TODO: EC: Currently using the below EC Schema. This has to be modified and
  //  created dynamically once OM return the configured scheme details.
  private static final String DEFAULT_CODEC_NAME = "rs";

  private long currentBlockGroupLen = 0;
  /**
   * Defines stream action while calling handleFlushOrClose.
   */
  enum StreamAction {
    FLUSH, CLOSE, FULL
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(KeyOutputStream.class);

  private boolean closed;
  private FileEncryptionInfo feInfo;
  // how much of data is actually written yet to underlying stream
  private long offset;
  // how much data has been ingested into the stream
  private long writeOffset;
  // whether an exception is encountered while write and whole write could
  // not succeed
  private boolean isException;
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
    this.config.setStreamBufferMaxSize(ecChunkSize);
    this.config.setStreamBufferFlushSize(ecChunkSize);
    this.config.setStreamBufferSize(ecChunkSize);
    this.ecReplicationConfig = replicationConfig;
    ecChunkBufferCache =
        new ECChunkBuffers(ecChunkSize,
            ecReplicationConfig.getData(),
            ecReplicationConfig.getParity());
    OmKeyInfo info = handler.getKeyInfo();
    blockOutputStreamEntryPool =
        new ECBlockOutputStreamEntryPool(config, omClient, requestId,
            replicationConfig, uploadID, partNumber, isMultipart, info,
            unsafeByteBufferConversion, xceiverClientManager, handler.getId());

    // Retrieve the file encryption key info, null if file is not in
    // encrypted bucket.
    this.feInfo = info.getFileEncryptionInfo();
    this.isException = false;
    this.writeOffset = 0;
    this.encoder = CodecRegistry.getInstance()
        .getCodecFactory(ecReplicationConfig.getCodecName())
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

    int currentChunkBufferRemainingLength =
        ecChunkBufferCache.dataBuffers[blockOutputStreamEntryPool.getCurrIdx()]
            .remaining();
    int currentChunkBufferLen =
        ecChunkBufferCache.dataBuffers[blockOutputStreamEntryPool.getCurrIdx()]
            .position();
    int maxLenToCurrChunkBuffer = (int) Math.min(len, ecChunkSize);
    int currentWriterChunkLenToWrite =
        Math.min(currentChunkBufferRemainingLength, maxLenToCurrChunkBuffer);
    handleWrite(b, off, currentWriterChunkLenToWrite,
        currentChunkBufferLen + currentWriterChunkLenToWrite == ecChunkSize,
        false);
    checkAndWriteParityCells();

    int remLen = len - currentWriterChunkLenToWrite;
    int iters = remLen / ecChunkSize;
    int lastCellSize = remLen % ecChunkSize;
    while (iters > 0) {
      handleWrite(b, off, ecChunkSize, true, false);
      off += ecChunkSize;
      iters--;
      checkAndWriteParityCells();
    }

    if (lastCellSize > 0) {
      handleWrite(b, off, lastCellSize, false, false);
      checkAndWriteParityCells();
    }
    writeOffset += len;
  }

  private void checkAndWriteParityCells() throws IOException {
    //check data blocks finished
    //If index is > datanum blks
    if (blockOutputStreamEntryPool.getCurrIdx() ==
        ecReplicationConfig.getData()) {
      //Lets encode and write
      //encoder.encode();
      writeParityCells();
      // By this time, we should have finished full stripe. So, lets call
      // executePutBlock for all.
      // TODO: we should alter the put block calls to share CRC to each stream.
      blockOutputStreamEntryPool.executePutBlockForAll();
      ecChunkBufferCache.clear();

      // check if block ends?
      if (currentBlockGroupLen ==
          ecReplicationConfig.getData() * blockOutputStreamEntryPool
              .getStreamEntries().get(blockOutputStreamEntryPool.getCurrIdx())
              .getLength()) {
        blockOutputStreamEntryPool.endECBlock();
        currentBlockGroupLen = 0;
      }
    }
  }

  void writeParityCells() throws IOException {
    final ByteBuffer[] buffers = ecChunkBufferCache.getDataBuffers();
    //encode the data cells
    final int numDataBlks = ecReplicationConfig.getData();
    final int numParityBlks = ecReplicationConfig.getParity();
    for (int i = 0; i < numDataBlks; i++) {
      buffers[i].flip();
    }

    final ByteBuffer[] parityBuffers = ecChunkBufferCache.getParityBuffers();
    encoder.encode(buffers, parityBuffers);
    for (int i =
         numDataBlks; i < (numDataBlks + numParityBlks); i++) {
      handleWrite(parityBuffers[i - numDataBlks].array(), 0, ecChunkSize, true,
          true);
    }
  }

  private void handleWrite(byte[] b, int off, long len, boolean isFullCell,
      boolean isParity) throws IOException {
    if (!isParity) {
      ecChunkBufferCache
          .addToDataBuffer(blockOutputStreamEntryPool.getCurrIdx(), b, off,
              (int) len);
    }
    BlockOutputStreamEntry current =
        blockOutputStreamEntryPool.allocateBlockIfNeeded();
    int writeLengthToCurrStream =
        Math.min((int) len, (int) current.getRemaining());
    currentBlockGroupLen += isParity ? 0 : writeLengthToCurrStream;
    if (current.getRemaining() <= 0) {
      // since the current block is already written close the stream.
      closeCurrentStream(StreamAction.CLOSE);
    }

    len -= writeLengthToCurrStream;
    if (isFullCell) {
      ByteBuffer bytesToWrite = isParity ?
          ecChunkBufferCache.getParityBuffers()[blockOutputStreamEntryPool
              .getCurrIdx() - ecReplicationConfig.getData()] :
          ecChunkBufferCache.getDataBuffers()[blockOutputStreamEntryPool
              .getCurrIdx()];
      try {
        writeToOutputStream(current, len, bytesToWrite.array(),
            bytesToWrite.array().length, 0, current.getWrittenDataLength(),
            isParity);
      } catch (Exception e) {
        markStreamClosed();
      }

      blockOutputStreamEntryPool
          .updateToNextStream(ecReplicationConfig.getData() +
              ecReplicationConfig.getParity());
    }
  }

  private int writeToOutputStream(BlockOutputStreamEntry current, long len,
      byte[] b, int writeLen, int off, long currentPos, boolean isParity)
      throws IOException {
    try {
      current.write(b, off, writeLen);
      if (!isParity) {
        offset += writeLen;
      }
    } catch (IOException ioe) {
      // for the current iteration, totalDataWritten - currentPos gives the
      // amount of data already written to the buffer

      // In the retryPath, the total data to be written will always be equal
      // to or less than the max length of the buffer allocated.
      // The len specified here is the combined sum of the data length of
      // the buffers
      Preconditions.checkState(len <= config.getStreamBufferMaxSize());
      int dataWritten = (int) (current.getWrittenDataLength() - currentPos);
      writeLen = dataWritten;

      if (!isParity) {
        offset += writeLen;
      }
      LOG.debug("writeLen {}, total len {}", writeLen, len);
      handleException(current, ioe);
    }
    return writeLen;
  }

  private void handleException(BlockOutputStreamEntry streamEntry,
      IOException exception) throws IOException {
    Throwable t = HddsClientUtils.checkForException(exception);
    Preconditions.checkNotNull(t);
    // In EC, we will just close the current stream.
    streamEntry.close();
  }

  private void markStreamClosed() {
    blockOutputStreamEntryPool.cleanup();
    closed = true;
  }

  @Override
  public void flush() throws IOException {
    checkNotClosed();
    handleFlushOrClose(StreamAction.FLUSH);
  }

  /**
   * Close or Flush the latest outputStream depending upon the action.
   * This function gets called when while write is going on, the current stream
   * gets full or explicit flush or close request is made by client.
   *
   * @param op Flag which decides whether to call close or flush on the
   *           outputStream.
   * @throws IOException In case, flush or close fails with exception.
   */
  @SuppressWarnings("squid:S1141")
  private void handleFlushOrClose(StreamAction op) throws IOException {
    if (!blockOutputStreamEntryPool.isEmpty()) {
      while (true) {
        try {
          BlockOutputStreamEntry entry =
              blockOutputStreamEntryPool.getCurrentStreamEntry();
          if (entry != null) {
            try {
              handleStreamAction(entry, op);
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

  private void handleFlushOrCloseAllStreams(StreamAction op)
      throws IOException {
    if (!blockOutputStreamEntryPool.isEmpty()) {
      List<BlockOutputStreamEntry> allStreamEntries =
          blockOutputStreamEntryPool.getStreamEntries();
      for (int i = 0; i < allStreamEntries.size(); i++) {
        while (true) {
          try {
            BlockOutputStreamEntry entry = allStreamEntries.get(i);
            if (entry != null) {
              try {
                handleStreamAction(entry, op);
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
  }

  private void closeCurrentStream(StreamAction op) throws IOException {
    if (!blockOutputStreamEntryPool.isEmpty()) {
      List<BlockOutputStreamEntry> allStreamEntries =
          blockOutputStreamEntryPool.getStreamEntries();
      for (int i = 0; i < allStreamEntries.size(); i++) {
        while (true) {
          try {
            BlockOutputStreamEntry entry = allStreamEntries.get(i);
            if (entry != null) {
              try {
                handleStreamAction(entry, op);
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
  }

  private void handleStreamAction(BlockOutputStreamEntry entry, StreamAction op)
      throws IOException {
    Collection<DatanodeDetails> failedServers = entry.getFailedServers();
    // failed servers can be null in case there is no data written in
    // the stream
    if (!failedServers.isEmpty()) {
      blockOutputStreamEntryPool.getExcludeList().addDatanodes(failedServers);
    }
    switch (op) {
    case CLOSE:
      entry.close();
      break;
    case FULL:
      if (entry.getRemaining() == 0) {
        entry.close();
      }
      break;
    case FLUSH:
      entry.flush();
      break;
    default:
      throw new IOException("Invalid Operation");
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
      handleFlushOrCloseAllStreams(StreamAction.CLOSE);
      if (!isException) {
        Preconditions.checkArgument(writeOffset == offset);
      }
      blockOutputStreamEntryPool.endECBlock();
      blockOutputStreamEntryPool.commitKey(offset);
    } finally {
      blockOutputStreamEntryPool.cleanupAll();
    }
    ecChunkBufferCache.release();
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    return blockOutputStreamEntryPool.getCommitUploadPartInfo();
  }

  public FileEncryptionInfo getFileEncryptionInfo() {
    return feInfo;
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
      allocateBuffers(cellSize, parityBuffers);
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
      Preconditions.checkState(pos <= cellSize);
      buf.put(b, off, len);
      return pos;
    }

    private void clear() {
      clearBuffers(cellSize, dataBuffers);
      clearBuffers(cellSize, parityBuffers);
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
