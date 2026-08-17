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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.junit.jupiter.api.Test;

/**
 * Test that {@link ContainerProtocolCalls} stamps the pipeline write version on
 * outgoing write requests (HDDS-15718).
 */
public class TestContainerProtocolCalls {

  private static Pipeline pipelineWithVersion(int version) {
    List<DatanodeDetails> nodes = Arrays.asList(
        randomDatanodeDetails(), randomDatanodeDetails(), randomDatanodeDetails());
    nodes.forEach(dn -> dn.setCurrentVersion(version));
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(Pipeline.PipelineState.OPEN)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setNodes(nodes)
        .build();
  }

  @Test
  void putBlockRequestCarriesPipelineWriteVersion() throws IOException {
    Pipeline pipeline = pipelineWithVersion(HDDSVersion.ZDU.serialize());
    BlockData blockData = BlockData.newBuilder()
        .setBlockID(DatanodeBlockID.newBuilder()
            .setContainerID(1).setLocalID(1).build())
        .build();

    ContainerCommandRequestProto request =
        ContainerProtocolCalls.getPutBlockRequest(pipeline, blockData, true, null);

    assertEquals(pipeline.getFirstNode().getCurrentVersion(),
        request.getWritePipelineVersion());
    assertEquals(HDDSVersion.ZDU.serialize(), request.getWritePipelineVersion());
  }
}
