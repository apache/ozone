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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.hdds.scm.storage.ECBlockOutputStream;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Helper for {@link ECBlockOutputStream}.
 */
public class ECBlockOutputStreamEntry extends BlockOutputStreamEntry {
  private final boolean isParityStreamEntry;
  private boolean isFailed = false;
  private ECBlockOutputStream out;

  @SuppressWarnings({"parameternumber", "squid:S00107"})
  ECBlockOutputStreamEntry(BlockID blockID, String key,
      XceiverClientFactory xceiverClientManager, Pipeline pipeline, long length,
      BufferPool bufferPool, Token<OzoneBlockTokenIdentifier> token,
      OzoneClientConfig config, boolean isParityStream) {
    super(blockID, key, xceiverClientManager, pipeline, length, bufferPool,
        token, config);
    this.isParityStreamEntry = isParityStream;
  }

  @Override
  ECBlockOutputStream createOutputStream() throws IOException {
    this.out = new ECBlockOutputStream(getBlockID(), getXceiverClientManager(),
        getPipeline(), getBufferPool(), getConf(), getToken());
    return this.out;
  }

  CompletableFuture<ContainerProtos.
      ContainerCommandResponseProto> executePutBlock()
      throws IOException {
    return this.out.executePutBlock(false, false);
  }

  public boolean isParityStreamEntry() {
    return this.isParityStreamEntry;
  }

  public void markFailed() {
    this.isFailed = true;
  }

  public boolean isFailed() {
    return this.isFailed;
  }

  /**
   * Builder class for ChunkGroupOutputStreamEntry.
   */
  public static class Builder {

    private BlockID blockID;
    private String key;
    private XceiverClientFactory xceiverClientManager;
    private Pipeline pipeline;
    private long length;
    private BufferPool bufferPool;
    private Token<OzoneBlockTokenIdentifier> token;
    private OzoneClientConfig config;
    private boolean isParityStreamEntry;

    public ECBlockOutputStreamEntry.Builder setBlockID(BlockID bID) {
      this.blockID = bID;
      return this;
    }

    public ECBlockOutputStreamEntry.Builder setKey(String keys) {
      this.key = keys;
      return this;
    }

    public ECBlockOutputStreamEntry.Builder setXceiverClientManager(
        XceiverClientFactory xClientManager) {
      this.xceiverClientManager = xClientManager;
      return this;
    }

    public ECBlockOutputStreamEntry.Builder setPipeline(Pipeline ppln) {
      this.pipeline = ppln;
      return this;
    }

    public ECBlockOutputStreamEntry.Builder setLength(long len) {
      this.length = len;
      return this;
    }

    public ECBlockOutputStreamEntry.Builder setBufferPool(BufferPool pool) {
      this.bufferPool = pool;
      return this;
    }

    public ECBlockOutputStreamEntry.Builder setConfig(
        OzoneClientConfig clientConfig) {
      this.config = clientConfig;
      return this;
    }

    public ECBlockOutputStreamEntry.Builder setToken(
        Token<OzoneBlockTokenIdentifier> bToken) {
      this.token = bToken;
      return this;
    }

    public ECBlockOutputStreamEntry.Builder setIsParityStreamEntry(
        boolean isParity) {
      this.isParityStreamEntry = isParity;
      return this;
    }

    public ECBlockOutputStreamEntry build() {
      return new ECBlockOutputStreamEntry(blockID, key, xceiverClientManager,
          pipeline, length, bufferPool, token, config, isParityStreamEntry);
    }
  }
}
