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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test that {@link XceiverServerRatis#addGroup} passes the {@link ReplicationType} to {@link ContainerStateMachine},
 * and that {@link XceiverServerRatis#removeGroup(RaftGroupId, org.apache.ratis.grpc.GrpcTlsConfig)}
 * removes the group from the peers only for {@link ReplicationType#RATIS}.
 */
public class TestXceiverServerRatisReplicationType {
  @TempDir
  private File dir;
  @TempDir
  private File peerDir;
  private DatanodeDetails dn;
  private XceiverServerRatis server;
  private XceiverServerRatis peerServer;

  @BeforeEach
  void setup() throws Exception {
    dn = MockPipeline.createPipeline(1).getFirstNode();
    server = newServer(dn, dir);
  }

  private static XceiverServerRatis newServer(DatanodeDetails datanode, File storageDir) throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, storageDir.getAbsolutePath());
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_RANDOM_PORT, true);
    final XceiverServerRatis s = ContainerTestUtils.newXceiverServerRatis(datanode, conf);
    // start() updates the ports of the datanode to the actual ports
    s.start();
    return s;
  }

  @AfterEach
  void teardown() {
    if (server != null) {
      server.stop();
    }
    if (peerServer != null) {
      peerServer.stop();
    }
  }

  @Test
  void testRatis() throws Exception {
    final PipelineID pipelineID = PipelineID.randomId();
    server.addGroup(pipelineID, ReplicationType.RATIS, Collections.singletonList(dn), Collections.singletonList(0));
    assertReplicationType(ReplicationType.RATIS, pipelineID);
  }

  @Test
  void testChained() throws Exception {
    final PipelineID pipelineID = PipelineID.randomId();
    server.addGroup(pipelineID, ReplicationType.CHAINED, Collections.singletonList(dn), Collections.singletonList(0));
    assertReplicationType(ReplicationType.CHAINED, pipelineID);
  }

  @ParameterizedTest
  @EnumSource(value = ReplicationType.class, names = {"RATIS", "CHAINED"})
  void testRemoveGroup(ReplicationType type) throws Exception {
    final DatanodeDetails peerDn = MockPipeline.createPipeline(1).getFirstNode();
    peerServer = newServer(peerDn, peerDir);

    final PipelineID pipelineID = PipelineID.randomId();
    final RaftGroupId groupId = pipelineID.getGroupId();
    final List<DatanodeDetails> peers = Arrays.asList(dn, peerDn);
    final List<Integer> priorities = Arrays.asList(0, 0);
    server.addGroup(pipelineID, type, peers, priorities);
    peerServer.addGroup(pipelineID, type, peers, priorities);
    assertTrue(hasGroup(peerServer, groupId));

    server.removeGroup(groupId, null);

    assertFalse(hasGroup(server, groupId));
    // For RATIS, the group is also removed from the peer; for CHAINED, only locally.
    assertEquals(type == ReplicationType.CHAINED, hasGroup(peerServer, groupId));
  }

  private static boolean hasGroup(XceiverServerRatis s, RaftGroupId groupId) {
    for (RaftGroupId id : s.getServer().getGroupIds()) {
      if (id.equals(groupId)) {
        return true;
      }
    }
    return false;
  }

  private void assertReplicationType(ReplicationType expected, PipelineID pipelineID) throws Exception {
    final RaftGroupId groupId = RaftGroupId.valueOf(pipelineID.getId());
    final ContainerStateMachine stateMachine = assertInstanceOf(ContainerStateMachine.class,
        server.getServerDivision(groupId).getStateMachine());
    assertEquals(expected, stateMachine.getReplicationType());
    assertEquals(expected, server.getReplicationType(pipelineID.getGroupId()));
  }
}
