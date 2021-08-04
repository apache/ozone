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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockDataStreamOutput;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.hdds.scm.storage.DataStreamOutput;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;

/**
 * Helper class used inside {@link BlockDataStreamOutput}.
 * */
public final class BlockDataStreamOutputEntry extends OutputStream
    implements DataStreamOutput {

  private final OzoneClientConfig config;
  private DataStreamOutput dataStreamOutput;
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
  private BlockDataStreamOutputEntry(
      BlockID blockID, String key,
      XceiverClientFactory xceiverClientManager,
      Pipeline pipeline,
      long length,
      BufferPool bufferPool,
      Token<OzoneBlockTokenIdentifier> token,
      OzoneClientConfig config
  ) {
    this.config = config;
    this.dataStreamOutput = null;
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
   * BlockDataStreamOutput is initialized in this function. This makes sure that
   * xceiverClient initialization is not done during preallocation and only
   * done when data is written.
   * @throws IOException if xceiverClient initialization fails
   */
  private void checkStream() throws IOException {
    if (this.dataStreamOutput == null) {
      this.dataStreamOutput =
          new BlockDataStreamOutput(blockID, xceiverClientManager,
              pipeline, bufferPool, config, token);
    }
  }

  @Override
  public void write(ByteBuf b) throws IOException {
    checkStream();
    final int len = b.readableBytes();
    dataStreamOutput.write(b);
    this.currentPosition += len;
  }

  @Override
  public void write(int b) throws IOException {
    byte[] buf = new byte[1];
    buf[0] = (byte) b;
    write(buf);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    write(Unpooled.wrappedBuffer(b), off, len);
  }

  @Override
  public void flush() throws IOException {
    if (this.dataStreamOutput != null) {
      this.dataStreamOutput.flush();
    }
  }

  @Override
  public void close() throws IOException {
    if (this.dataStreamOutput != null) {
      this.dataStreamOutput.close();
      // after closing the chunkOutPutStream, blockId would have been
      // reconstructed with updated bcsId
      this.blockID = ((BlockDataStreamOutput) dataStreamOutput).getBlockID();
    }
  }

  boolean isClosed() {
    if (dataStreamOutput != null) {
      return  ((BlockDataStreamOutput) dataStreamOutput).isClosed();
    }
    return false;
  }

  long getTotalAckDataLength() {
    if (dataStreamOutput != null) {
      BlockDataStreamOutput out = (BlockDataStreamOutput) this.dataStreamOutput;
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
    if (dataStreamOutput != null) {
      BlockDataStreamOutput out = (BlockDataStreamOutput) this.dataStreamOutput;
      return out.getFailedServers();
    }
    return Collections.emptyList();
  }

  long getWrittenDataLength() {
    if (dataStreamOutput != null) {
      BlockDataStreamOutput out = (BlockDataStreamOutput) this.dataStreamOutput;
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
    BlockDataStreamOutput out = (BlockDataStreamOutput) this.dataStreamOutput;
    out.cleanup(invalidateClient);

  }

  void writeOnRetry(long len) throws IOException {
    checkStream();
    BlockDataStreamOutput out = (BlockDataStreamOutput) this.dataStreamOutput;
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

    public BlockDataStreamOutputEntry build() {
      return new BlockDataStreamOutputEntry(blockID,
          key,
          xceiverClientManager,
          pipeline,
          length,
          bufferPool,
          token, config);
    }
  }

  @VisibleForTesting
  public DataStreamOutput getDataStreamOutput() {
    return dataStreamOutput;
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


