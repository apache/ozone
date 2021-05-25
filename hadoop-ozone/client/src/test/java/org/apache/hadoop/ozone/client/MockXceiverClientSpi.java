/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto.Builder;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Mocked version of the datanode client.
 */
public class MockXceiverClientSpi extends XceiverClientSpi {

  private final Pipeline pipeline;

  private final MockDatanodeStorage datanodeStorage;

  public MockXceiverClientSpi(Pipeline pipeline, MockDatanodeStorage storage) {
    this.pipeline = pipeline;
    this.datanodeStorage = storage;
  }

  @Override
  public void connect() throws Exception {

  }

  @Override
  public void connect(String encodedToken) throws Exception {

  }

  @Override
  public void close() {

  }

  @Override
  public Pipeline getPipeline() {
    return this.pipeline;
  }

  @Override
  public XceiverClientReply sendCommandAsync(
      ContainerCommandRequestProto request) {
    switch (request.getCmdType()) {
    case WriteChunk:
      return result(request,
          r -> r.setWriteChunk(writeChunk(request.getWriteChunk())));
    case ReadChunk:
      return result(request,
          r -> r.setReadChunk(readChunk(request.getReadChunk())));
    case PutBlock:
      return result(request,
          r -> r.setPutBlock(putBlock(request.getPutBlock())));
    case GetBlock:
      return result(request,
          r -> r.setGetBlock(getBlock(request.getGetBlock())));
    default:
      throw new IllegalArgumentException(
          "Mock version of datanode call " + request.getCmdType()
              + " is not yet implemented");
    }
  }

  private ReadChunkResponseProto readChunk(ReadChunkRequestProto readChunk) {
    return ReadChunkResponseProto.newBuilder()
        .setChunkData(datanodeStorage
            .readChunkInfo(readChunk.getBlockID(), readChunk.getChunkData()))
        .setData(datanodeStorage
            .readChunkData(readChunk.getBlockID(), readChunk.getChunkData()))
        .setBlockID(readChunk.getBlockID())
        .build();
  }

  private GetBlockResponseProto getBlock(GetBlockRequestProto getBlock) {
    return GetBlockResponseProto.newBuilder()
        .setBlockData(datanodeStorage.getBlock(getBlock.getBlockID()))
        .build();
  }

  private PutBlockResponseProto putBlock(PutBlockRequestProto putBlock) {
    long length = 0;
    for (ChunkInfo chunk : putBlock.getBlockData().getChunksList()) {
      length += chunk.getLen();
    }

    datanodeStorage.putBlock(putBlock.getBlockData().getBlockID(),
        putBlock.getBlockData());

    return PutBlockResponseProto.newBuilder()
        .setCommittedBlockLength(
            GetCommittedBlockLengthResponseProto.newBuilder()
                .setBlockID(putBlock.getBlockData().getBlockID())
                .setBlockLength(length)
                .build())
        .build();
  }

  private XceiverClientReply result(
      ContainerCommandRequestProto request,
      Function<ContainerCommandResponseProto.Builder,
          ContainerCommandResponseProto.Builder> function) {

    Builder builder = ContainerCommandResponseProto.newBuilder()
        .setResult(Result.SUCCESS)
        .setCmdType(request.getCmdType());
    builder = function.apply(builder);

    XceiverClientReply reply = new XceiverClientReply(
        CompletableFuture.completedFuture(builder.build()));
    return reply;
  }

  private WriteChunkResponseProto writeChunk(
      WriteChunkRequestProto writeChunk) {
    datanodeStorage
        .writeChunk(writeChunk.getBlockID(), writeChunk.getChunkData(),
            writeChunk.getData());
    return WriteChunkResponseProto.newBuilder()
        .build();
  }

  @Override
  public ReplicationType getPipelineType() {
    return pipeline.getType();
  }

  @Override
  public XceiverClientReply watchForCommit(long index) {
    return null;
  }

  @Override
  public long getReplicatedMinCommitIndex() {
    return 0;
  }

  @Override
  public Map<DatanodeDetails, ContainerCommandResponseProto>
      sendCommandOnAllNodes(ContainerCommandRequestProto request) {
    return null;
  }
}
