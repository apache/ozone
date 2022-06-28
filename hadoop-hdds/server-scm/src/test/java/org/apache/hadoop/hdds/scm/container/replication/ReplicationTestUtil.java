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
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

/**
 * Helper class to provide common methods used to test ReplicationManager.
 */
public final class ReplicationTestUtil {

  private ReplicationTestUtil() {
  }

  public static Set<ContainerReplica> createReplicas(ContainerID containerID,
      Pair<HddsProtos.NodeOperationalState, Integer>... nodes) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (Pair<HddsProtos.NodeOperationalState, Integer> p : nodes) {
      replicas.add(
          createContainerReplica(containerID, p.getRight(), p.getLeft()));
    }
    return replicas;
  }

  public static Set<ContainerReplica> createReplicas(ContainerID containerID,
      int... indexes) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (int i : indexes) {
      replicas.add(createContainerReplica(
          containerID, i, IN_SERVICE));
    }
    return replicas;
  }

  public static ContainerReplica createContainerReplica(ContainerID containerID,
      int replicaIndex, HddsProtos.NodeOperationalState opState) {
    ContainerReplica.ContainerReplicaBuilder builder
        = ContainerReplica.newBuilder();
    DatanodeDetails datanodeDetails
        = MockDatanodeDetails.randomDatanodeDetails();
    datanodeDetails.setPersistedOpState(opState);
    builder.setContainerID(containerID);
    builder.setReplicaIndex(replicaIndex);
    builder.setKeyCount(123);
    builder.setBytesUsed(1234);
    builder.setContainerState(StorageContainerDatanodeProtocolProtos
        .ContainerReplicaProto.State.CLOSED);
    builder.setDatanodeDetails(datanodeDetails);
    builder.setSequenceId(0);
    builder.setOriginNodeId(datanodeDetails.getUuid());
    return builder.build();
  }

  public static ContainerInfo createContainerInfo(ReplicationConfig repConfig) {
    return createContainerInfo(repConfig, 1, HddsProtos.LifeCycleState.CLOSED);
  }

  public static ContainerInfo createContainerInfo(
      ReplicationConfig replicationConfig, long containerID,
      HddsProtos.LifeCycleState containerState) {
    ContainerInfo.Builder builder = new ContainerInfo.Builder();
    builder.setContainerID(containerID);
    builder.setOwner("Ozone");
    builder.setPipelineID(PipelineID.randomId());
    builder.setReplicationConfig(replicationConfig);
    builder.setState(containerState);
    return builder.build();
  }

}
