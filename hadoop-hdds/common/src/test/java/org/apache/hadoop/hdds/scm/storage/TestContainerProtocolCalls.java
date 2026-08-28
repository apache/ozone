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

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Test that {@link ContainerProtocolCalls} stamps the pipeline write version on
 * outgoing write requests (HDDS-15718).
 */
public class TestContainerProtocolCalls {

  private static final int VERSION = HDDSVersion.ZDU.serialize();

  private static Pipeline pipelineWithVersion(int version) {
    List<DatanodeDetails> nodes = Arrays.asList(
        randomDatanodeDetails(), randomDatanodeDetails(), randomDatanodeDetails());
    nodes.forEach(dn -> dn.setCurrentVersion(HDDSVersion.deserialize(version)));
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(Pipeline.PipelineState.OPEN)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setNodes(nodes)
        .build();
  }

  private static XceiverClientSpi clientWith(Pipeline pipeline) {
    XceiverClientSpi client = mock(XceiverClientSpi.class);
    when(client.getPipeline()).thenReturn(pipeline);
    return client;
  }

  private static ChunkInfo chunkInfo(byte[] data) throws IOException {
    return ChunkInfo.newBuilder()
        .setChunkName("chunk")
        .setOffset(0)
        .setLen(data.length)
        .setChecksumData(new Checksum(ChecksumType.CRC32, 256)
            .computeChecksum(data).getProtoBufMessage())
        .build();
  }

  @Test
  void putBlockRequestCarriesPipelineWriteVersion() throws IOException {
    Pipeline pipeline = pipelineWithVersion(VERSION);
    BlockData blockData = BlockData.newBuilder()
        .setBlockID(DatanodeBlockID.newBuilder()
            .setContainerID(1).setLocalID(1).build())
        .build();

    ContainerCommandRequestProto request =
        ContainerProtocolCalls.getPutBlockRequest(pipeline, blockData, true, null);

    assertEquals(VERSION, request.getWritePipelineVersion());
  }

  @Test
  void writeChunkRequestCarriesPipelineWriteVersion() throws Exception {
    Pipeline pipeline = pipelineWithVersion(VERSION);
    XceiverClientSpi client = clientWith(pipeline);
    when(client.sendCommandAsync(any())).thenReturn(mock(XceiverClientReply.class));
    byte[] data = "data".getBytes(StandardCharsets.UTF_8);

    ContainerProtocolCalls.writeChunkAsync(client, chunkInfo(data), new BlockID(1, 1),
        ByteString.copyFrom(data), null, 0, null, false, true);

    ArgumentCaptor<ContainerCommandRequestProto> captor =
        ArgumentCaptor.forClass(ContainerCommandRequestProto.class);
    verify(client).sendCommandAsync(captor.capture());
    assertEquals(VERSION, captor.getValue().getWritePipelineVersion());
  }

  @Test
  void writeSmallFileRequestCarriesPipelineWriteVersion() throws IOException {
    Pipeline pipeline = pipelineWithVersion(VERSION);
    XceiverClientSpi client = clientWith(pipeline);
    when(client.sendCommand(any(), anyList())).thenReturn(
        response(Type.PutSmallFile));

    ContainerProtocolCalls.writeSmallFile(client, new BlockID(1, 1),
        "data".getBytes(StandardCharsets.UTF_8), null);

    assertEquals(VERSION, sentRequest(client).getWritePipelineVersion());
  }

  @Test
  void finalizeBlockRequestCarriesPipelineWriteVersion() throws IOException {
    Pipeline pipeline = pipelineWithVersion(VERSION);
    XceiverClientSpi client = clientWith(pipeline);
    when(client.sendCommand(any(), anyList())).thenReturn(
        response(Type.FinalizeBlock));

    ContainerProtocolCalls.finalizeBlock(client,
        DatanodeBlockID.newBuilder().setContainerID(1).setLocalID(1).build(), null);

    assertEquals(VERSION, sentRequest(client).getWritePipelineVersion());
  }

  private static ContainerCommandResponseProto response(Type type) {
    return ContainerCommandResponseProto.newBuilder()
        .setCmdType(type)
        .setResult(Result.SUCCESS)
        .build();
  }

  private static ContainerCommandRequestProto sentRequest(XceiverClientSpi client)
      throws IOException {
    ArgumentCaptor<ContainerCommandRequestProto> captor =
        ArgumentCaptor.forClass(ContainerCommandRequestProto.class);
    verify(client).sendCommand(captor.capture(), anyList());
    return captor.getValue();
  }
}
