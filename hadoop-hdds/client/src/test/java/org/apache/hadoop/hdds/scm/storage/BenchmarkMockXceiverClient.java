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

package org.apache.hadoop.hdds.scm.storage;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.ClientVersion;

/**
 * In-memory xceiver client for {@link BlockOutputStreamWriteBenchmark}.
 */
final class BenchmarkMockXceiverClient extends XceiverClientSpi {

  private final Pipeline pipeline;
  private final AtomicLong logIndex = new AtomicLong();

  BenchmarkMockXceiverClient(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  @Override
  public void connect() {
  }

  @Override
  public void close() {
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public XceiverClientReply sendCommandAsync(ContainerCommandRequestProto request) {
    if (!request.hasVersion()) {
      request = ContainerCommandRequestProto.newBuilder(request)
          .setVersion(ClientVersion.CURRENT.toProtoValue()).build();
    }
    final ContainerCommandResponseProto.Builder builder =
        ContainerCommandResponseProto.newBuilder()
            .setResult(Result.SUCCESS)
            .setCmdType(request.getCmdType());
    if (request.getCmdType() == Type.PutBlock) {
      builder.setPutBlock(PutBlockResponseProto.newBuilder()
          .setCommittedBlockLength(
              GetCommittedBlockLengthResponseProto.newBuilder()
                  .setBlockID(request.getPutBlock().getBlockData().getBlockID())
                  .setBlockLength(request.getPutBlock().getBlockData().getSize())
                  .build())
          .build());
    }
    final XceiverClientReply reply = new XceiverClientReply(
        CompletableFuture.completedFuture(builder.build()));
    reply.setLogIndex(logIndex.incrementAndGet());
    return reply;
  }

  @Override
  public ReplicationType getPipelineType() {
    return pipeline.getType();
  }

  @Override
  public CompletableFuture<XceiverClientReply> watchForCommit(long index) {
    final ContainerCommandResponseProto response =
        ContainerCommandResponseProto.newBuilder()
            .setCmdType(Type.WriteChunk)
            .setResult(Result.SUCCESS)
            .build();
    final XceiverClientReply reply =
        new XceiverClientReply(CompletableFuture.completedFuture(response));
    reply.setLogIndex(index);
    return CompletableFuture.completedFuture(reply);
  }

  @Override
  public long getReplicatedMinCommitIndex() {
    return logIndex.get();
  }

  @Override
  public Map<DatanodeDetails, ContainerCommandResponseProto> sendCommandOnAllNodes(
      ContainerCommandRequestProto request) {
    return null;
  }
}
