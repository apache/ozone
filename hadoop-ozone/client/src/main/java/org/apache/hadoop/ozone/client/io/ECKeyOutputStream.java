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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodecOptions;
import org.apache.hadoop.io.erasurecode.codec.RSErasureCodec;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ECKeyOutputStream handles the EC writes by writing the data into underlying
 * block output streams chunk by chunk.
 */
public class ECKeyOutputStream extends KeyOutputStream {
  private OzoneClientConfig config;
  private ECChunkBuffers ecChunkBufferCache;
  private int cellSize = 1000;
  private int numDataBlks = 3;
  private int numParityBlks = 2;
  private static final ByteBufferPool BUFFER_POOL = new ElasticByteBufferPool();
  private final RawErasureEncoder encoder;
  // TODO: EC: Currently using the below EC Schema. This has to be modified and
  //  created dynamically once OM return the configured scheme details.
  private static final String DEFAULT_CODEC_NAME = "rs";
  private ECSchema schema =
      new ECSchema(DEFAULT_CODEC_NAME, numDataBlks, numParityBlks);
  private ErasureCodecOptions options = new ErasureCodecOptions(schema);
  private RSErasureCodec codec =
      new RSErasureCodec(new Configuration(), options);

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
  private final Map<Class<? extends Throwable>, RetryPolicy> retryPolicyMap;
  private int retryCount;
  // how much of data is actually written yet to underlying stream
  private long offset;
  // how much data has been ingested into the stream
  private long writeOffset;
  // whether an exception is encountered while write and whole write could
  // not succeed
  private boolean isException;
  private final BlockOutputStreamEntryPool blockOutputStreamEntryPool;

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

  @VisibleForTesting
  public int getRetryCount() {
    return retryCount;
  }

  @SuppressWarnings({"parameternumber", "squid:S00107"})
  public ECKeyOutputStream(OzoneClientConfig config, OpenKeySession handler,
      XceiverClientFactory xceiverClientManager, OzoneManagerProtocol omClient,
      int chunkSize, String requestId, ReplicationConfig replicationConfig,
      String uploadID, int partNumber, boolean isMultipart,
      boolean unsafeByteBufferConversion) {
    ecChunkBufferCache =
        new ECChunkBuffers(cellSize, numDataBlks, numParityBlks);
    this.config = config;
    OmKeyInfo info = handler.getKeyInfo();
    blockOutputStreamEntryPool =
        new BlockOutputStreamEntryPool(config, omClient, requestId,
            replicationConfig, uploadID, partNumber, isMultipart, info,
            unsafeByteBufferConversion, xceiverClientManager, handler.getId(),
            true);

    // Retrieve the file encryption key info, null if file is not in
    // encrypted bucket.
    this.feInfo = info.getFileEncryptionInfo();
    this.retryPolicyMap = HddsClientUtils
        .getRetryPolicyByException(config.getMaxRetryCount(),
            config.getRetryInterval());
    this.retryCount = 0;
    this.isException = false;
    this.writeOffset = 0;
    encoder = CodecUtil.createRawEncoder(new Configuration(),
        SystemErasureCodingPolicies.getPolicies().get(1).getCodecName(),
        codec.getCoderOptions());
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

  @Override
  public void write(int b) throws IOException {
    byte[] buf = new byte[1];
    buf[0] = (byte) b;
    write(buf, 0, 1);
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
        ecChunkBufferCache.buffers[blockOutputStreamEntryPool.getCurrIdx()]
            .remaining();
    int currentChunkBufferLen =
        ecChunkBufferCache.buffers[blockOutputStreamEntryPool.getCurrIdx()]
            .position();
    int maxLenToCurrChunkBuffer = (int) Math.min(len, cellSize);
    int currentWriterChunkLenToWrite =
        Math.min(currentChunkBufferRemainingLength, maxLenToCurrChunkBuffer);
    handleWrite(b, off, currentWriterChunkLenToWrite,
        currentChunkBufferLen + currentWriterChunkLenToWrite == cellSize,
        false);
    checkAndWriteParityCells();

    int remLen = len - currentWriterChunkLenToWrite;
    int iters = remLen / cellSize;
    int lastCellSize = remLen % cellSize;
    while (iters > 0) {
      handleWrite(b, off, cellSize, true, false);
      off += cellSize;
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
    if (blockOutputStreamEntryPool.getCurrIdx() == numDataBlks) {
      //Lets encode and write
      //encoder.encode();
      writeParityCells();
      // check if block ends?
      if (currentBlockGroupLen == numDataBlks * blockOutputStreamEntryPool
          .getStreamEntries().get(blockOutputStreamEntryPool.getCurrIdx())
          .getLength()) {
        blockOutputStreamEntryPool.endECBlock(numDataBlks);
        currentBlockGroupLen = 0;
      }
    }
  }

  void writeParityCells() throws IOException {
    final ByteBuffer[] buffers = ecChunkBufferCache.getBuffers();
    //encode the data cells
    for (int i = 0; i < numDataBlks; i++) {
      buffers[i].flip();
    }
    encode(encoder, numDataBlks, buffers);
    for (int i = numDataBlks; i < numDataBlks + 2; i++) {
      handleWrite(buffers[i].array(), 0, cellSize, true, true);
    }

    ecChunkBufferCache.flipAllDataBuffers();
    ecChunkBufferCache.clear();
  }

  private static void encode(RawErasureEncoder encoder, int numData,
      ByteBuffer[] buffers) throws IOException {
    final ByteBuffer[] dataBuffers = new ByteBuffer[numData];
    final ByteBuffer[] parityBuffers = new ByteBuffer[buffers.length - numData];
    System.arraycopy(buffers, 0, dataBuffers, 0, dataBuffers.length);
    System.arraycopy(buffers, numData, parityBuffers, 0, parityBuffers.length);

    encoder.encode(dataBuffers, parityBuffers);
  }

  private void handleWrite(byte[] b, int off, long len, boolean isFullCell,
      boolean isParity) throws IOException {
    ecChunkBufferCache
        .addTo(blockOutputStreamEntryPool.getCurrIdx(), b, off, (int) len);
    while (len > 0) {
      try {

        BlockOutputStreamEntry current =
            blockOutputStreamEntryPool.allocateBlockIfNeeded();
        // length(len) will be in int range if the call is happening through
        // write API of blockOutputStream. Length can be in long range if it
        // comes via Exception path.
        int expectedWriteLen =
            Math.min((int) len, (int) current.getRemaining());
        long currentPos = current.getWrittenDataLength();
        // writeLen will be updated based on whether the write was succeeded
        // or if it sees an exception, how much the actual write was
        // acknowledged.
        int writtenLength =
            writeToOutputStream(current, len, b, expectedWriteLen, off,
                currentPos, isParity);
        currentBlockGroupLen += isParity ? 0 : writtenLength;
        if (current.getRemaining() <= 0) {
          // since the current block is already written close the stream.
          closeCurrentStream(StreamAction.CLOSE);
        }

        len -= writtenLength;
        off += writtenLength;

      } catch (Exception e) {
        markStreamClosed();
        throw new IOException(e);
      }

      if (isFullCell) {
        handleFlushOrClose(StreamAction.FLUSH);
        blockOutputStreamEntryPool.updateToNextStream(numDataBlks + 2);
      }
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
      blockOutputStreamEntryPool.endECBlock(numDataBlks);
      //TODO: offset should not consider parity blocks length
      blockOutputStreamEntryPool.commitKey(offset);
    } finally {
      blockOutputStreamEntryPool.cleanupAll();
    }
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
    private ReplicationConfig replicationConfig;

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
        ReplicationConfig replConfig) {
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
    private final ByteBuffer[] buffers;
    private final int dataBlks;
    private final int parityBlks;
    private final int totalBlks;
    private int cellSize;

    ECChunkBuffers(int cellSize, int numData, int numParity) {
      this.cellSize = cellSize;
      this.parityBlks = numParity;
      this.dataBlks = numData;
      this.totalBlks = this.dataBlks + this.parityBlks;
      buffers = new ByteBuffer[this.totalBlks];
      for (int i = 0; i < buffers.length; i++) {
        buffers[i] = BUFFER_POOL.getBuffer(false, cellSize);
        buffers[i].limit(cellSize);
      }
    }

    private ByteBuffer[] getBuffers() {
      return buffers;
    }

    private int addTo(int i, byte[] b, int off, int len) {
      final ByteBuffer buf = buffers[i];
      final int pos = buf.position() + len;
      Preconditions.checkState(pos <= cellSize);
      buf.put(b, off, len);
      return pos;
    }

    private void clear() {
      for (int i = 0; i < this.totalBlks; i++) {
        buffers[i].clear();
        buffers[i].limit(cellSize);
      }
    }

    private void release() {
      for (int i = 0; i < this.totalBlks; i++) {
        if (buffers[i] != null) {
          BUFFER_POOL.putBuffer(buffers[i]);
          buffers[i] = null;
        }
      }
    }

    private void flipDataBuffers() {
      for (int i = 0; i < this.totalBlks; i++) {
        buffers[i].flip();
      }
    }

    private void flipAllDataBuffers() {
      for (int i = 0; i < this.totalBlks; i++) {
        buffers[i].flip();
      }
    }
  }
}
