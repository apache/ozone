/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
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
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

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
          r -> {
            try {
              return r.setWriteChunk(writeChunk(request.getWriteChunk()));
            } catch (ContainerNotOpenException e) {
              return r.setResult(Result.CLOSED_CONTAINER_IO);
            } catch (IOException e) {
              return r.setResult(Result.IO_EXCEPTION);
            }
          });
    case ReadChunk:
      return result(request,
          r -> r.setReadChunk(readChunk(request.getReadChunk())));
    case PutBlock:
      return result(request,
          r -> r.setPutBlock(putBlock(request.getPutBlock())));
    case GetBlock:
      return result(request,
          r -> r.setGetBlock(getBlock(request.getGetBlock())));
    case ListBlock:
      return result(request,
          r -> r.setListBlock(listBlock(request.getContainerID())));
    default:
      throw new IllegalArgumentException(
          "Mock version of datanode call " + request.getCmdType()
              + " is not yet implemented");
    }
  }

  private ReadChunkResponseProto readChunk(ReadChunkRequestProto readChunk) {
    return ReadChunkResponseProto.newBuilder()
        .setChunkData(readChunk.getChunkData())
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

  private ContainerProtos.ListBlockResponseProto listBlock(long containerID) {
    return ContainerProtos.ListBlockResponseProto.newBuilder()
        .addAllBlockData(datanodeStorage.listBlock(containerID)).build();
  }

  private PutBlockResponseProto putBlock(PutBlockRequestProto putBlock) {
    return PutBlockResponseProto.newBuilder()
        .setCommittedBlockLength(
            doPutBlock(putBlock.getBlockData()))
        .build();
  }

  private GetCommittedBlockLengthResponseProto doPutBlock(
      ContainerProtos.BlockData blockData) {
    long length = 0;
    for (ChunkInfo chunk : blockData.getChunksList()) {
      length += chunk.getLen();
    }

    datanodeStorage.putBlock(blockData.getBlockID(),
        blockData);

    return GetCommittedBlockLengthResponseProto.newBuilder()
                .setBlockID(blockData.getBlockID())
                .setBlockLength(length)
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
      WriteChunkRequestProto writeChunk) throws IOException {
    datanodeStorage
        .writeChunk(writeChunk.getBlockID(), writeChunk.getChunkData(),
            writeChunk.getData());

    WriteChunkResponseProto.Builder builder =
        WriteChunkResponseProto.newBuilder();
    if (writeChunk.hasBlock()) {
      ContainerProtos.BlockData
          blockData = writeChunk.getBlock().getBlockData();
      builder.setCommittedBlockLength(doPutBlock(blockData));
    }
    return builder.build();
  }

  @Override
  public ReplicationType getPipelineType() {
    return pipeline.getType();
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
