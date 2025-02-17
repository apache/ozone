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

package org.apache.hadoop.hdds.scm.container.report;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 *
 */
public class TestContainerReportValidator {

  private ContainerReplicaProto getContainerReplica(
          ContainerID containerID, int replicaIndex, DatanodeDetails dn) {
    return HddsTestUtils.createContainerReplica(containerID,
            State.CLOSED, dn.getUuidString(), 10000L, 2L,
            replicaIndex);

  }

  @Test
  public void testValidECReplicaIndex() {
    ContainerInfo containerInfo = HddsTestUtils.getECContainer(
            HddsProtos.LifeCycleState.CLOSED, PipelineID.randomId(),
            new ECReplicationConfig(3, 2));

    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    ContainerReplicaProto replica = getContainerReplica(
            containerInfo.containerID(), 1, dn);
    assertTrue(ContainerReportValidator.validate(containerInfo, dn,
            replica));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 6, 100, -1})
  public void testInValidECReplicaIndex(int replicaIndex) {
    ContainerInfo containerInfo = HddsTestUtils.getECContainer(
            HddsProtos.LifeCycleState.CLOSED, PipelineID.randomId(),
            new ECReplicationConfig(3, 2));

    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    ContainerReplicaProto replica = getContainerReplica(
            containerInfo.containerID(), replicaIndex, dn);
    assertFalse(ContainerReportValidator.validate(containerInfo, dn,
            replica));
  }

}
