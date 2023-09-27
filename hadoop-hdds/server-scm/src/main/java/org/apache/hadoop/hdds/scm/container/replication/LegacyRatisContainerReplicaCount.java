/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.NodeManager;

import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

/**
 * When HDDS-6447 was done to improve the LegacyReplicationManager, work on
 * the new replication manager had already started. When this class was added,
 * the LegacyReplicationManager needed separate handling for healthy and
 * unhealthy container replicas, but the new replication manager did not yet
 * have this functionality. This class is used by the
 * LegacyReplicationManager to allow {@link RatisContainerReplicaCount} to
 * function for both use cases. When the new replication manager is finished
 * and LegacyReplicationManager is removed, this class should be deleted and
 * all necessary functionality consolidated to
 * {@link RatisContainerReplicaCount}
 */
public class LegacyRatisContainerReplicaCount extends
    RatisContainerReplicaCount {
  public LegacyRatisContainerReplicaCount(ContainerInfo container,
                                    Set<ContainerReplica> replicas,
                                    int inFlightAdd,
                                    int inFlightDelete, int replicationFactor,
                                    int minHealthyForMaintenance) {
    super(container, replicas, inFlightAdd, inFlightDelete, replicationFactor,
        minHealthyForMaintenance);
  }

  @Override
  protected int healthyReplicaCountAdapter() {
    return -getMisMatchedReplicaCount();
  }

  /**
   * For LegacyReplicationManager, unhealthy replicas are all replicas that
   * don't match the container's state. For a CLOSED container with replicas
   * {CLOSED, CLOSING, UNHEALTHY, OPEN}, unhealthy replica count is 3. 2
   * mismatches (CLOSING, OPEN) + 1 UNHEALTHY = 3.
   */
  @Override
  public int getUnhealthyReplicaCountAdapter() {
    return getMisMatchedReplicaCount();
  }

  @Override
  public boolean isHealthyEnoughForOffline() {
    LifeCycleState containerState = getContainer().getState();
    return (containerState == LifeCycleState.CLOSED
        || containerState == LifeCycleState.QUASI_CLOSED)
        && getReplicas().stream()
        .filter(r -> r.getDatanodeDetails().getPersistedOpState() == IN_SERVICE)
        .filter(r -> r.getState() !=
            ContainerReplicaProto.State.UNHEALTHY)
        .allMatch(r -> ReplicationManager.compareState(
            containerState, r.getState()));
  }

  /**
   * For Legacy Replication Manager and Ratis Containers, this method checks
   * if the container is sufficiently replicated. It also checks whether
   * there are any UNHEALTHY replicas that need to be replicated.
   * @param datanode Not used in this implementation
   * @param nodeManager An instance of NodeManager, used to check the health
   * status of a node
   * @return true if the container is sufficiently replicated and there are
   * no UNHEALTHY replicas that need to be replicated, false otherwise
   */
  @Override
  public boolean isSufficientlyReplicatedForOffline(DatanodeDetails datanode,
      NodeManager nodeManager) {
    return super.isSufficientlyReplicated() &&
        super.getVulnerableUnhealthyReplicas(nodeManager).isEmpty();
  }
}
