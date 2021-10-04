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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.hdds.scm.storage.ECBlockOutputStream;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.util.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper for {@link ECBlockOutputStream}.
 */
public class ECBlockOutputStreamEntry extends BlockOutputStreamEntry{
  private ECBlockOutputStream[] blockOutputStreams;
  private final ECReplicationConfig replicationConfig;
  private Map<DatanodeDetails, Integer> replicaIndicies = new HashMap<>();

  private int currentStreamIdx = 0;
  @SuppressWarnings({"parameternumber", "squid:S00107"})
  ECBlockOutputStreamEntry(BlockID blockID, String key,
      XceiverClientFactory xceiverClientManager, Pipeline pipeline, long length,
      BufferPool bufferPool, Token<OzoneBlockTokenIdentifier> token,
      OzoneClientConfig config) {
    super(blockID, key, xceiverClientManager, pipeline, length, bufferPool,
        token, config);
    Preconditions.assertInstanceOf(
        pipeline.getReplicationConfig(), ECReplicationConfig.class);
    this.replicationConfig =
        (ECReplicationConfig) pipeline.getReplicationConfig();
  }

  @Override
  void createOutputStream() throws IOException {
    Pipeline ecPipeline = getPipeline();
    List<DatanodeDetails> nodes = getPipeline().getNodes();
    blockOutputStreams =
        new ECBlockOutputStream[nodes.size()];
    for (int i = 0; i< getPipeline().getNodes().size(); i++) {
      blockOutputStreams[i] = new ECBlockOutputStream(
          getBlockID(),
          getXceiverClientManager(),
          createSingleECBlockPipeline(ecPipeline, nodes.get(i), i+1),
          getBufferPool(),
          getConf(),
          getToken());
    }
  }

  @Override
  public OutputStream getOutputStream() {
    if (!isInitialized()) {
      return null;
    }
    return blockOutputStreams[currentStreamIdx];
  }

  @Override
  boolean isInitialized() {
    return blockOutputStreams != null;
  }

  public int getCurrentStreamIdx() {
    return currentStreamIdx;
  }

  public void useNextBlockStream() {
    currentStreamIdx++;
  }

  public void forceToFirstParityBlock(){
    currentStreamIdx = replicationConfig.getData();
  }

  @Override
  void incCurrentPosition() {
    if (isWritingParity()) {
      return;
    }
    super.incCurrentPosition();
  }

  @Override
  void incCurrentPosition(long len) {
    if (isWritingParity()){
      return;
    }
    super.incCurrentPosition(len);
  }

  @Override
  public void flush() throws IOException {
    if (!isInitialized()) {
      return;
    }
    for(int i=0; i<=currentStreamIdx && i<blockOutputStreams.length; i++) {
      blockOutputStreams[i].flush();
    }
  }

  @Override
  boolean isClosed() {
    if (!isInitialized()) {
      return false;
    }
    return blockStreams().allMatch(BlockOutputStream::isClosed);
  }

  @Override
  public void close() throws IOException {
    if (!isInitialized()) {
      return;
    }
    for (ECBlockOutputStream stream : blockOutputStreams) {
      stream.close();
    }
    updateBlockID(underlyingBlockID());
  }

  @Override
  long getTotalAckDataLength() {
    if (!isInitialized()) {
      return 0;
    }
    // blockID is the same for EC blocks inside one block group managed by
    // this entry.
    updateBlockID(underlyingBlockID());
    return blockStreams()
        .mapToLong(BlockOutputStream::getTotalAckDataLength)
        .sum();
  }

  @Override
  long getWrittenDataLength() {
    if (!isInitialized()) {
      return 0;
    }
    return blockStreams()
        .mapToLong(BlockOutputStream::getWrittenDataLength)
        .sum();
  }

  @Override
  Collection<DatanodeDetails> getFailedServers() {
    if (!isInitialized()) {
      return Collections.emptyList();
    }

    return blockStreams()
        .flatMap(outputStream -> outputStream.getFailedServers().stream())
        .collect(Collectors.toList());
  }

  private Pipeline createSingleECBlockPipeline(Pipeline ecPipeline,
      DatanodeDetails node, int replicaIndex) {
    Map<DatanodeDetails, Integer> indiciesForSinglePipeline = new HashMap<>();
    indiciesForSinglePipeline.put(node, replicaIndex);
    replicaIndicies.put(node, replicaIndex);
    return Pipeline.newBuilder()
        .setId(ecPipeline.getId())
        .setReplicationConfig(ecPipeline.getReplicationConfig())
        .setState(ecPipeline.getPipelineState())
        .setNodes(ImmutableList.of(node))
        .setReplicaIndexes(indiciesForSinglePipeline)
        .build();
  }

  @Override
  Pipeline getPipelineForOMLocationReport() {
    Pipeline original = getPipeline();
    return Pipeline.newBuilder()
        .setId(original.getId())
        .setReplicationConfig(original.getReplicationConfig())
        .setState(original.getPipelineState())
        .setNodes(original.getNodes())
        .setReplicaIndexes(replicaIndicies)
        .build();
  }

  void executePutBlock() throws IOException {
    if (!isInitialized()) {
      return;
    }
    int failedStreams = 0;
    for (ECBlockOutputStream stream : blockOutputStreams) {
      if (!stream.isClosed()) {
        stream.executePutBlock(false, true);
      } else {
        failedStreams++;
      }
      if(failedStreams > replicationConfig.getParity()) {
        throw new IOException(
            "There are " + failedStreams + " block write failures,"
                + " supported tolerance: " + replicationConfig.getParity());
      }
    }
  }

  private BlockID underlyingBlockID() {
    return blockOutputStreams[0].getBlockID();
  }

  private boolean isWritingParity() {
    return currentStreamIdx >= replicationConfig.getData();
  }

  private Stream<ECBlockOutputStream> blockStreams() {
    return Arrays.stream(blockOutputStreams);
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

    public ECBlockOutputStreamEntry.Builder setBlockID(BlockID bID) {
      this.blockID = bID;
      return this;
    }

    public ECBlockOutputStreamEntry.Builder setKey(String keys) {
      this.key = keys;
      return this;
    }

    public ECBlockOutputStreamEntry.Builder setXceiverClientManager(
        XceiverClientFactory
            xClientManager) {
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

    public ECBlockOutputStreamEntry build() {
      return new ECBlockOutputStreamEntry(blockID,
          key,
          xceiverClientManager,
          pipeline,
          length,
          bufferPool,
          token, config);
    }
  }
}
