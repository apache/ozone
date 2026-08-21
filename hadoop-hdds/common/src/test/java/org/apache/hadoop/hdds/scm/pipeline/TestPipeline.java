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

package org.apache.hadoop.hdds.scm.pipeline;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.ALL_PORTS;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.V0_PORTS;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.TestDatanodeDetails.assertPorts;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.ClientVersion.DEFAULT_VERSION;
import static org.apache.hadoop.ozone.ClientVersion.VERSION_HANDLES_UNKNOWN_DN_PORTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.ClientVersion;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link Pipeline}.
 */
public class TestPipeline {

  @Test
  public void protoIncludesNewPortsOnlyForV1() throws IOException {
    Pipeline subject = MockPipeline.createPipeline(3);

    HddsProtos.Pipeline proto =
        subject.getProtobufMessage(DEFAULT_VERSION);
    for (HddsProtos.DatanodeDetailsProto dn : proto.getMembersList()) {
      assertPorts(dn, V0_PORTS);
    }

    HddsProtos.Pipeline protoV1 = subject.getProtobufMessage(
        VERSION_HANDLES_UNKNOWN_DN_PORTS);
    for (HddsProtos.DatanodeDetailsProto dn : protoV1.getMembersList()) {
      assertPorts(dn, ALL_PORTS);
    }
  }

  @Test
  public void testDefaultPreservesCurrentVersion() {
    // Read path: each member's own reported currentVersion must survive serialization.
    Map<DatanodeID, Integer> expected = new HashMap<>();
    DatanodeDetails dn0 = randomDatanodeDetails();
    dn0.setCurrentVersion(HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC);
    DatanodeDetails dn1 = randomDatanodeDetails();
    dn1.setCurrentVersion(HDDSVersion.STREAM_BLOCK_SUPPORT);
    DatanodeDetails dn2 = randomDatanodeDetails();
    dn2.setCurrentVersion(HDDSVersion.SOFTWARE_VERSION);
    expected.put(dn0.getID(), HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC.serialize());
    expected.put(dn1.getID(), HDDSVersion.STREAM_BLOCK_SUPPORT.serialize());
    expected.put(dn2.getID(), HDDSVersion.SOFTWARE_VERSION.serialize());

    Pipeline subject = MockPipeline.createPipeline(Arrays.asList(dn0, dn1, dn2));

    assertPerMemberVersions(expected, subject.getProtobufMessage(ClientVersion.CURRENT));
  }

  @Test
  public void testOverrideAllMemberVersions() {
    // Write path: the explicit datanodeVersion overrides every member's currentVersion.
    final HDDSVersion override = HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC;
    DatanodeDetails dn0 = randomDatanodeDetails();
    dn0.setCurrentVersion(override);
    DatanodeDetails dn1 = randomDatanodeDetails();
    dn1.setCurrentVersion(HDDSVersion.STREAM_BLOCK_SUPPORT);
    DatanodeDetails dn2 = randomDatanodeDetails();
    dn2.setCurrentVersion(HDDSVersion.SOFTWARE_VERSION);

    Pipeline subject = MockPipeline.createPipeline(Arrays.asList(dn0, dn1, dn2));

    HddsProtos.Pipeline proto =
        subject.getProtobufMessage(ClientVersion.CURRENT, ALL_PORTS, override);
    for (HddsProtos.DatanodeDetailsProto member : proto.getMembersList()) {
      assertEquals(override.serialize(), member.getCurrentVersion());
    }
  }

  @Test
  public void testOverrideEachMemberVersion() {
    // Read path: the translator substitutes each member's live currentVersion via a per-datanode map,
    // overriding the stale version frozen into the pipeline member.
    DatanodeDetails dn0 = randomDatanodeDetails();
    DatanodeDetails dn1 = randomDatanodeDetails();
    DatanodeDetails dn2 = randomDatanodeDetails();
    for (DatanodeDetails dn : Arrays.asList(dn0, dn1, dn2)) {
      dn.setCurrentVersion(HDDSVersion.DEFAULT_VERSION);
    }

    Map<DatanodeID, ComponentVersion> overrides = new HashMap<>();
    overrides.put(dn0.getID(), HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC);
    overrides.put(dn1.getID(), HDDSVersion.STREAM_BLOCK_SUPPORT);
    overrides.put(dn2.getID(), HDDSVersion.SOFTWARE_VERSION);

    Pipeline subject = MockPipeline.createPipeline(Arrays.asList(dn0, dn1, dn2));

    Map<DatanodeID, Integer> expected = new HashMap<>();
    expected.put(dn0.getID(), HDDSVersion.COMBINED_PUTBLOCK_WRITECHUNK_RPC.serialize());
    expected.put(dn1.getID(), HDDSVersion.STREAM_BLOCK_SUPPORT.serialize());
    expected.put(dn2.getID(), HDDSVersion.SOFTWARE_VERSION.serialize());
    assertPerMemberVersions(expected, subject.getProtobufMessage(DEFAULT_VERSION, ALL_PORTS, overrides));
  }

  private static void assertPerMemberVersions(Map<DatanodeID, Integer> expected, HddsProtos.Pipeline proto) {
    assertEquals(expected.size(), proto.getMembersCount());
    for (HddsProtos.DatanodeDetailsProto member : proto.getMembersList()) {
      DatanodeID id = DatanodeDetails.getFromProtoBuf(member).getID();
      assertEquals(expected.get(id).intValue(), member.getCurrentVersion());
    }
  }

  @Test
  public void getProtobufMessageEC() throws IOException {
    Pipeline subject = MockPipeline.createPipeline(3);

    //when EC config is empty/null
    HddsProtos.Pipeline protobufMessage = subject.getProtobufMessage(VERSION_HANDLES_UNKNOWN_DN_PORTS);
    assertEquals(0, protobufMessage.getEcReplicationConfig().getData());


    //when EC config is NOT empty
    subject = MockPipeline.createEcPipeline();

    protobufMessage = subject.getProtobufMessage(VERSION_HANDLES_UNKNOWN_DN_PORTS);
    assertEquals(3, protobufMessage.getEcReplicationConfig().getData());
    assertEquals(2, protobufMessage.getEcReplicationConfig().getParity());

  }

  @Test
  public void testReplicaIndexesSerialisedCorrectly() {
    Pipeline pipeline = MockPipeline.createEcPipeline();
    HddsProtos.Pipeline protobufMessage = pipeline.getProtobufMessage(VERSION_HANDLES_UNKNOWN_DN_PORTS);
    Pipeline reloadedPipeline = Pipeline.getFromProtobuf(protobufMessage);

    for (DatanodeDetails dn : pipeline.getNodes()) {
      assertEquals(pipeline.getReplicaIndex(dn),
          reloadedPipeline.getReplicaIndex(dn));
    }
  }

  @Test
  public void testECPipelineIsAlwaysHealthy() {
    Pipeline pipeline = MockPipeline.createEcPipeline();
    assertTrue(pipeline.isHealthy());
  }

  @Test
  public void testBuilderCopiesAllFieldsFromOtherPipeline() {
    Pipeline original = MockPipeline.createEcPipeline();
    Pipeline copied = original.toBuilder().build();
    assertEquals(original.getId(), copied.getId());
    assertEquals(original.getReplicationConfig(),
        copied.getReplicationConfig());
    assertEquals(original.getPipelineState(), copied.getPipelineState());
    assertEquals(original.getNodeSet(), copied.getNodeSet());
    assertEquals(original.getNodesInOrder(), copied.getNodesInOrder());
    assertEquals(original.getLeaderId(), copied.getLeaderId());
    assertEquals(original.getCreationTimestamp(),
        copied.getCreationTimestamp());
    assertEquals(original.getSuggestedLeaderId(),
        copied.getSuggestedLeaderId());
    for (DatanodeDetails dn : original.getNodes()) {
      assertEquals(original.getReplicaIndex(dn),
          copied.getReplicaIndex(dn));
    }
  }

  @Test
  void idChangedIfNodesReplaced() {
    Pipeline original = MockPipeline.createRatisPipeline();

    Pipeline withDifferentNodes = original.toBuilder()
        .setNodes(Arrays.asList(randomDatanodeDetails(), randomDatanodeDetails(), randomDatanodeDetails()))
        .build();

    assertNotEquals(original.getId(), withDifferentNodes.getId());
    withDifferentNodes.getNodes()
        .forEach(node -> assertNotEquals(node.getID().toPipelineID(), withDifferentNodes.getId()));
  }

  @Test
  void testCopyForReadFromNode() {
    Pipeline subject = MockPipeline.createRatisPipeline();
    DatanodeDetails node = subject.getNodes().iterator().next();

    Pipeline copy = subject.copyForReadFromNode(node);

    assertEquals(singletonList(node), copy.getNodes());
    assertEquals(node.getID().toPipelineID(), copy.getId());
    assertEquals(subject.getReplicaIndex(node), copy.getReplicaIndex(node));
    assertEquals(StandaloneReplicationConfig.getInstance(ONE), copy.getReplicationConfig());
  }

  @Test
  void testCopyForReadFromNodeRejectsUnknownNode() {
    Pipeline subject = MockPipeline.createRatisPipeline();
    assertThrows(IllegalStateException.class, () -> subject.copyForReadFromNode(randomDatanodeDetails()));
  }
}
