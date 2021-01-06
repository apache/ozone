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
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import com.google.common.annotations.VisibleForTesting;

/**
 * Helper class used inside {@link BlockOutputStream}.
 * */
public final class BlockOutputStreamEntry extends OutputStream {

  private final OzoneClientConfig config;
  private OutputStream outputStream;
  private BlockID blockID;
  private final String key;
  private final XceiverClientFactory xceiverClientManager;
  private final Pipeline pipeline;
  // total number of bytes that should be written to this stream
  private final long length;
  // the current position of this stream 0 <= currentPosition < length
  private long currentPosition;
  private final Token<OzoneBlockTokenIdentifier> token;

  private BufferPool bufferPool;

  @SuppressWarnings({"parameternumber", "squid:S00107"})
  private BlockOutputStreamEntry(
      BlockID blockID, String key,
      XceiverClientFactory xceiverClientManager,
      Pipeline pipeline,
      long length,
      BufferPool bufferPool,
      Token<OzoneBlockTokenIdentifier> token,
      OzoneClientConfig config
  ) {
    this.config = config;
    this.outputStream = null;
    this.blockID = blockID;
    this.key = key;
    this.xceiverClientManager = xceiverClientManager;
    this.pipeline = pipeline;
    this.token = token;
    this.length = length;
    this.currentPosition = 0;
    this.bufferPool = bufferPool;
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

  /**
   * BlockOutputStream is initialized in this function. This makes sure that
   * xceiverClient initialization is not done during preallocation and only
   * done when data is written.
   * @throws IOException if xceiverClient initialization fails
   */
  private void checkStream() throws IOException {
    if (this.outputStream == null) {
      this.outputStream =
          new BlockOutputStream(blockID, xceiverClientManager,
              pipeline, bufferPool, config, token);
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
      this.blockID = ((BlockOutputStream) outputStream).getBlockID();
    }
  }

  boolean isClosed() {
    if (outputStream != null) {
      return  ((BlockOutputStream) outputStream).isClosed();
    }
    return false;
  }

  long getTotalAckDataLength() {
    if (outputStream != null) {
      BlockOutputStream out = (BlockOutputStream) this.outputStream;
      blockID = out.getBlockID();
      return out.getTotalAckDataLength();
    } else {
      // For a pre allocated block for which no write has been initiated,
      // the OutputStream will be null here.
      // In such cases, the default blockCommitSequenceId will be 0
      return 0;
    }
  }

  Collection<DatanodeDetails> getFailedServers() {
    if (outputStream != null) {
      BlockOutputStream out = (BlockOutputStream) this.outputStream;
      return out.getFailedServers();
    }
    return Collections.emptyList();
  }

  long getWrittenDataLength() {
    if (outputStream != null) {
      BlockOutputStream out = (BlockOutputStream) this.outputStream;
      return out.getWrittenDataLength();
    } else {
      // For a pre allocated block for which no write has been initiated,
      // the OutputStream will be null here.
      // In such cases, the default blockCommitSequenceId will be 0
      return 0;
    }
  }

  void cleanup(boolean invalidateClient) throws IOException {
    checkStream();
    BlockOutputStream out = (BlockOutputStream) this.outputStream;
    out.cleanup(invalidateClient);

  }

  void writeOnRetry(long len) throws IOException {
    checkStream();
    BlockOutputStream out = (BlockOutputStream) this.outputStream;
    out.writeOnRetry(len);
    this.currentPosition += len;

  }

  /**
   * Builder class for ChunkGroupOutputStreamEntry.
   * */
  public static class Builder {

    private BlockID blockID;
    private String key;
    private XceiverClientFactory xceiverClientManager;
    private Pipeline pipeline;
    private long length;
    private BufferPool bufferPool;
    private Token<OzoneBlockTokenIdentifier> token;
    private OzoneClientConfig config;

    public Builder setBlockID(BlockID bID) {
      this.blockID = bID;
      return this;
    }

    public Builder setKey(String keys) {
      this.key = keys;
      return this;
    }

    public Builder setXceiverClientManager(
        XceiverClientFactory
        xClientManager) {
      this.xceiverClientManager = xClientManager;
      return this;
    }

    public Builder setPipeline(Pipeline ppln) {
      this.pipeline = ppln;
      return this;
    }


    public Builder setLength(long len) {
      this.length = len;
      return this;
    }


    public Builder setBufferPool(BufferPool pool) {
      this.bufferPool = pool;
      return this;
    }

    public Builder setConfig(OzoneClientConfig clientConfig) {
      this.config = clientConfig;
      return this;
    }

    public Builder setToken(Token<OzoneBlockTokenIdentifier> bToken) {
      this.token = bToken;
      return this;
    }

    public BlockOutputStreamEntry build() {
      return new BlockOutputStreamEntry(blockID,
          key,
          xceiverClientManager,
          pipeline,
          length,
          bufferPool,
          token, config);
    }
  }

  @VisibleForTesting
  public OutputStream getOutputStream() {
    return outputStream;
  }

  public BlockID getBlockID() {
    return blockID;
  }

  public String getKey() {
    return key;
  }

  public XceiverClientFactory getXceiverClientManager() {
    return xceiverClientManager;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public long getCurrentPosition() {
    return currentPosition;
  }

  public BufferPool getBufferPool() {
    return bufferPool;
  }

  public void setCurrentPosition(long curPosition) {
    this.currentPosition = curPosition;
  }
}


