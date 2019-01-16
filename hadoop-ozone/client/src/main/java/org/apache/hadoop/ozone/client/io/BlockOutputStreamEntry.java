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

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Helper class used inside {@link BlockOutputStream}.
 * */
public class BlockOutputStreamEntry extends OutputStream {

  private OutputStream outputStream;
  private BlockID blockID;
  private final String key;
  private final XceiverClientManager xceiverClientManager;
  private final XceiverClientSpi xceiverClient;
  private final Checksum checksum;
  private final String requestId;
  private final int chunkSize;
  // total number of bytes that should be written to this stream
  private final long length;
  // the current position of this stream 0 <= currentPosition < length
  private long currentPosition;
  private Token<OzoneBlockTokenIdentifier> token;

  private final long streamBufferFlushSize;
  private final long streamBufferMaxSize;
  private final long watchTimeout;
  private List<ByteBuffer> bufferList;

  private BlockOutputStreamEntry(BlockID blockID, String key,
      XceiverClientManager xceiverClientManager,
      XceiverClientSpi xceiverClient, String requestId, int chunkSize,
      long length, long streamBufferFlushSize, long streamBufferMaxSize,
      long watchTimeout, List<ByteBuffer> bufferList, Checksum checksum,
      Token<OzoneBlockTokenIdentifier> token) {
    this.outputStream = null;
    this.blockID = blockID;
    this.key = key;
    this.xceiverClientManager = xceiverClientManager;
    this.xceiverClient = xceiverClient;
    this.requestId = requestId;
    this.chunkSize = chunkSize;
    this.token = token;
    this.length = length;
    this.currentPosition = 0;
    this.streamBufferFlushSize = streamBufferFlushSize;
    this.streamBufferMaxSize = streamBufferMaxSize;
    this.watchTimeout = watchTimeout;
    this.bufferList = bufferList;
    this.checksum = checksum;
  }

  /**
   * For testing purpose, taking a some random created stream instance.
   *
   * @param outputStream a existing writable output stream
   * @param length the length of data to write to the stream
   */
  BlockOutputStreamEntry(OutputStream outputStream, long length,
                         Checksum checksum) {
    this.outputStream = outputStream;
    this.blockID = null;
    this.key = null;
    this.xceiverClientManager = null;
    this.xceiverClient = null;
    this.requestId = null;
    this.chunkSize = -1;
    this.token = null;
    this.length = length;
    this.currentPosition = 0;
    streamBufferFlushSize = 0;
    streamBufferMaxSize = 0;
    bufferList = null;
    watchTimeout = 0;
    this.checksum = checksum;
  }

  long getLength() {
    return length;
  }

  Token<OzoneBlockTokenIdentifier> getToken() {
    return token;
  }

  long getRemaining() {
    return length - currentPosition;
  }

  private void checkStream() throws IOException {
    if (this.outputStream == null) {
      if (getToken() != null) {
        UserGroupInformation.getCurrentUser().addToken(getToken());
      }
      this.outputStream =
          new BlockOutputStream(blockID, key, xceiverClientManager,
              xceiverClient, requestId, chunkSize, streamBufferFlushSize,
              streamBufferMaxSize, watchTimeout, bufferList, checksum);
    }
  }

  @Override
  public void write(int b) throws IOException {
    checkStream();
    outputStream.write(b);
    this.currentPosition += 1;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkStream();
    outputStream.write(b, off, len);
    this.currentPosition += len;
  }

  @Override
  public void flush() throws IOException {
    if (this.outputStream != null) {
      this.outputStream.flush();
    }
  }

  @Override
  public void close() throws IOException {
    if (this.outputStream != null) {
      this.outputStream.close();
      // after closing the chunkOutPutStream, blockId would have been
      // reconstructed with updated bcsId
      if (this.outputStream instanceof BlockOutputStream) {
        this.blockID = ((BlockOutputStream) outputStream).getBlockID();
      }
    }
  }

  long getTotalSuccessfulFlushedData() throws IOException {
    if (this.outputStream instanceof BlockOutputStream) {
      BlockOutputStream out = (BlockOutputStream) this.outputStream;
      blockID = out.getBlockID();
      return out.getTotalSuccessfulFlushedData();
    } else if (outputStream == null) {
        // For a pre allocated block for which no write has been initiated,
        // the OutputStream will be null here.
        // In such cases, the default blockCommitSequenceId will be 0
        return 0;
    }
    throw new IOException("Invalid Output Stream for Key: " + key);
  }

  long getWrittenDataLength() throws IOException {
    if (this.outputStream instanceof BlockOutputStream) {
      BlockOutputStream out = (BlockOutputStream) this.outputStream;
      return out.getWrittenDataLength();
    } else if (outputStream == null) {
      // For a pre allocated block for which no write has been initiated,
      // the OutputStream will be null here.
      // In such cases, the default blockCommitSequenceId will be 0
      return 0;
    }
    throw new IOException("Invalid Output Stream for Key: " + key);
  }

  void cleanup() throws IOException{
    checkStream();
    if (this.outputStream instanceof BlockOutputStream) {
      BlockOutputStream out = (BlockOutputStream) this.outputStream;
      out.cleanup();
    }
  }

  void writeOnRetry(long len) throws IOException {
    checkStream();
    if (this.outputStream instanceof BlockOutputStream) {
      BlockOutputStream out = (BlockOutputStream) this.outputStream;
      out.writeOnRetry(len);
      this.currentPosition += len;
    } else {
      throw new IOException("Invalid Output Stream for Key: " + key);
    }
  }

  /**
   * Builder class for ChunkGroupOutputStreamEntry.
   * */
  public static class Builder {

    private BlockID blockID;
    private String key;
    private XceiverClientManager xceiverClientManager;
    private XceiverClientSpi xceiverClient;
    private String requestId;
    private int chunkSize;
    private long length;
    private long streamBufferFlushSize;
    private long streamBufferMaxSize;
    private long watchTimeout;
    private List<ByteBuffer> bufferList;
    private Token<OzoneBlockTokenIdentifier> token;
    private Checksum checksum;

    public Builder setChecksum(Checksum cs) {
      this.checksum = cs;
      return this;
    }

    public Builder setBlockID(BlockID bID) {
      this.blockID = bID;
      return this;
    }

    public Builder setKey(String keys) {
      this.key = keys;
      return this;
    }

    public Builder setXceiverClientManager(XceiverClientManager
        xClientManager) {
      this.xceiverClientManager = xClientManager;
      return this;
    }

    public Builder setXceiverClient(XceiverClientSpi client) {
      this.xceiverClient = client;
      return this;
    }

    public Builder setRequestId(String request) {
      this.requestId = request;
      return this;
    }

    public Builder setChunkSize(int cSize) {
      this.chunkSize = cSize;
      return this;
    }

    public Builder setLength(long len) {
      this.length = len;
      return this;
    }

    public Builder setStreamBufferFlushSize(long bufferFlushSize) {
      this.streamBufferFlushSize = bufferFlushSize;
      return this;
    }

    public Builder setStreamBufferMaxSize(long bufferMaxSize) {
      this.streamBufferMaxSize = bufferMaxSize;
      return this;
    }

    public Builder setWatchTimeout(long timeout) {
      this.watchTimeout = timeout;
      return this;
    }

    public Builder setBufferList(List<ByteBuffer> bufferList) {
      this.bufferList = bufferList;
      return this;
    }

    public Builder setToken(Token<OzoneBlockTokenIdentifier> bToken) {
      this.token = bToken;
      return this;
    }

    public BlockOutputStreamEntry build() {
      return new BlockOutputStreamEntry(blockID, key,
          xceiverClientManager, xceiverClient, requestId, chunkSize,
          length, streamBufferFlushSize, streamBufferMaxSize, watchTimeout,
          bufferList, checksum, token);
    }
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }

  public BlockID getBlockID() {
    return blockID;
  }

  public String getKey() {
    return key;
  }

  public XceiverClientManager getXceiverClientManager() {
    return xceiverClientManager;
  }

  public XceiverClientSpi getXceiverClient() {
    return xceiverClient;
  }

  public Checksum getChecksum() {
    return checksum;
  }

  public String getRequestId() {
    return requestId;
  }

  public int getChunkSize() {
    return chunkSize;
  }

  public long getCurrentPosition() {
    return currentPosition;
  }

  public long getStreamBufferFlushSize() {
    return streamBufferFlushSize;
  }

  public long getStreamBufferMaxSize() {
    return streamBufferMaxSize;
  }

  public long getWatchTimeout() {
    return watchTimeout;
  }

  public List<ByteBuffer> getBufferList() {
    return bufferList;
  }

  public void setCurrentPosition(long curPosition) {
    this.currentPosition = curPosition;
  }
}


