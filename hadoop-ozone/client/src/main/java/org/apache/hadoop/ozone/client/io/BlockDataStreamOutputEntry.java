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
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockDataStreamOutput;
import org.apache.hadoop.hdds.scm.storage.ByteBufStreamOutput;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Helper class used inside {@link BlockDataStreamOutput}.
 * */
public final class BlockDataStreamOutputEntry
    implements ByteBufStreamOutput {

  private final OzoneClientConfig config;
  private ByteBufStreamOutput byteBufStreamOutput;
  private BlockID blockID;
  private final String key;
  private final XceiverClientFactory xceiverClientManager;
  private final Pipeline pipeline;
  // total number of bytes that should be written to this stream
  private final long length;
  // the current position of this stream 0 <= currentPosition < length
  private long currentPosition;
  private final Token<OzoneBlockTokenIdentifier> token;

  @SuppressWarnings({"parameternumber", "squid:S00107"})
  private BlockDataStreamOutputEntry(
      BlockID blockID, String key,
      XceiverClientFactory xceiverClientManager,
      Pipeline pipeline,
      long length,
      Token<OzoneBlockTokenIdentifier> token,
      OzoneClientConfig config
  ) {
    this.config = config;
    this.byteBufStreamOutput = null;
    this.blockID = blockID;
    this.key = key;
    this.xceiverClientManager = xceiverClientManager;
    this.pipeline = pipeline;
    this.token = token;
    this.length = length;
    this.currentPosition = 0;
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
    if (this.byteBufStreamOutput == null) {
      this.byteBufStreamOutput =
          new BlockDataStreamOutput(blockID, xceiverClientManager,
              pipeline, config, token);
    }
  }

  @Override
  public void write(ByteBuf b) throws IOException {
    checkStream();
    final int len = b.readableBytes();
    byteBufStreamOutput.write(b);
    this.currentPosition += len;
  }

  @Override
  public void flush() throws IOException {
    if (this.byteBufStreamOutput != null) {
      this.byteBufStreamOutput.flush();
    }
  }

  @Override
  public void close() throws IOException {
    if (this.byteBufStreamOutput != null) {
      this.byteBufStreamOutput.close();
      // after closing the chunkOutPutStream, blockId would have been
      // reconstructed with updated bcsId
      this.blockID =
          ((BlockDataStreamOutput) byteBufStreamOutput).getBlockID();
    }
  }

  boolean isClosed() {
    if (byteBufStreamOutput != null) {
      return  ((BlockDataStreamOutput) byteBufStreamOutput).isClosed();
    }
    return false;
  }

  Collection<DatanodeDetails> getFailedServers() {
    if (byteBufStreamOutput != null) {
      BlockDataStreamOutput out =
          (BlockDataStreamOutput) this.byteBufStreamOutput;
      return out.getFailedServers();
    }
    return Collections.emptyList();
  }

  long getWrittenDataLength() {
    if (byteBufStreamOutput != null) {
      BlockDataStreamOutput out =
          (BlockDataStreamOutput) this.byteBufStreamOutput;
      return out.getWrittenDataLength();
    } else {
      // For a pre allocated block for which no write has been initiated,
      // the ByteBufStreamOutput will be null here.
      // In such cases, the default blockCommitSequenceId will be 0
      return 0;
    }
  }

  void cleanup(boolean invalidateClient) throws IOException {
    checkStream();
    BlockDataStreamOutput out =
        (BlockDataStreamOutput) this.byteBufStreamOutput;
    out.cleanup(invalidateClient);

  }

  void writeOnRetry(long len) throws IOException {
    checkStream();
    BlockDataStreamOutput out =
        (BlockDataStreamOutput) this.byteBufStreamOutput;
    out.writeOnRetry(len);
    this.currentPosition += len;

  }

  /**
   * Builder class for BlockDataStreamOutputEntry.
   * */
  public static class Builder {

    private BlockID blockID;
    private String key;
    private XceiverClientFactory xceiverClientManager;
    private Pipeline pipeline;
    private long length;
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
          token, config);
    }
  }

  @VisibleForTesting
  public ByteBufStreamOutput getByteBufStreamOutput() {
    return byteBufStreamOutput;
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

  public void setCurrentPosition(long curPosition) {
    this.currentPosition = curPosition;
  }
}


