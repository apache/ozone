/*
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
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.ALL_PORTS;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.V0_PORTS;
import static org.apache.hadoop.hdds.protocol.TestDatanodeDetails.assertPorts;
import static org.apache.hadoop.ozone.ClientVersion.DEFAULT_VERSION;
import static org.apache.hadoop.ozone.ClientVersion.VERSION_HANDLES_UNKNOWN_DN_PORTS;

/**
 * Test for {@link Pipeline}.
 */
public class TestPipeline {

  @Test
  public void protoIncludesNewPortsOnlyForV1() throws IOException {
    Pipeline subject = MockPipeline.createPipeline(3);

    HddsProtos.Pipeline proto =
        subject.getProtobufMessage(DEFAULT_VERSION.toProtoValue());
    for (HddsProtos.DatanodeDetailsProto dn : proto.getMembersList()) {
      assertPorts(dn, V0_PORTS);
    }

    HddsProtos.Pipeline protoV1 = subject.getProtobufMessage(
        VERSION_HANDLES_UNKNOWN_DN_PORTS.toProtoValue());
    for (HddsProtos.DatanodeDetailsProto dn : protoV1.getMembersList()) {
      assertPorts(dn, ALL_PORTS);
    }
  }

  @Test
  public void getProtobufMessageEC() throws IOException {
    Pipeline subject = MockPipeline.createPipeline(3);

    //when EC config is empty/null
    HddsProtos.Pipeline protobufMessage = subject.getProtobufMessage(1);
    Assert.assertEquals(0, protobufMessage.getEcReplicationConfig().getData());


    //when EC config is NOT empty
    subject = MockPipeline.createEcPipeline();

    protobufMessage = subject.getProtobufMessage(1);
    Assert.assertEquals(3, protobufMessage.getEcReplicationConfig().getData());
    Assert
        .assertEquals(2, protobufMessage.getEcReplicationConfig().getParity());

  }

  @Test
  public void testReplicaIndexesSerialisedCorrectly() throws IOException {
    Pipeline pipeline = MockPipeline.createEcPipeline();
    HddsProtos.Pipeline protobufMessage = pipeline.getProtobufMessage(1);
    Pipeline reloadedPipeline = Pipeline.getFromProtobuf(protobufMessage);

    for (DatanodeDetails dn : pipeline.getNodes()) {
      Assert.assertEquals(pipeline.getReplicaIndex(dn),
          reloadedPipeline.getReplicaIndex(dn));
    }
  }

  @Test
  public void testECPipelineIsAlwaysHealthy() throws IOException {
    Pipeline pipeline = MockPipeline.createEcPipeline();
    Assert.assertTrue(pipeline.isHealthy());
  }

  @Test
  public void testBuilderCopiesAllFieldsFromOtherPipeline() {
    Pipeline original = MockPipeline.createEcPipeline();
    Pipeline copied = Pipeline.newBuilder(original).build();
    Assert.assertEquals(original.getId(), copied.getId());
    Assert.assertEquals(original.getReplicationConfig(),
        copied.getReplicationConfig());
    Assert.assertEquals(original.getPipelineState(), copied.getPipelineState());
    Assert.assertEquals(original.getId(), copied.getId());
    Assert.assertEquals(original.getId(), copied.getId());
    Assert.assertEquals(original.getId(), copied.getId());
    Assert.assertEquals(original.getNodeSet(), copied.getNodeSet());
    Assert.assertEquals(original.getNodesInOrder(), copied.getNodesInOrder());
    Assert.assertEquals(original.getLeaderId(), copied.getLeaderId());
    Assert.assertEquals(original.getCreationTimestamp(),
        copied.getCreationTimestamp());
    Assert.assertEquals(original.getSuggestedLeaderId(),
        copied.getSuggestedLeaderId());
    for (DatanodeDetails dn : original.getNodes()) {
      Assert.assertEquals(original.getReplicaIndex(dn),
          copied.getReplicaIndex(dn));
    }
  }
}
