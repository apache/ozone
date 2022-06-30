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
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.replication.LegacyReplicationManager;

import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

/**
 * Common interface for EC and non-EC container replica counts.
 * TODO pull up more methods if needed
 */
public interface ContainerReplicaCount {
  ContainerInfo getContainer();

  Set<ContainerReplica> getReplicas();

  boolean isSufficientlyReplicated();

  boolean isOverReplicated();

  int getDecommissionCount();

  int getMaintenanceCount();

  /**
   * Calculates the delta of replicas which need to be created or removed
   * to ensure the container is correctly replicated when considered inflight
   * adds and deletes.
   *
   * @return Delta of replicas needed. Negative indicates over replication and
   *         containers should be removed. Positive indicates over replication
   *         and zero indicates the containers has replicationFactor healthy
   *         replica
   */
  int additionalReplicaNeeded();

  /**
   * Returns true if the container is healthy, meaning all replica which are not
   * in a decommission or maintenance state are in the same state as the
   * container and in QUASI_CLOSED or in CLOSED state.
   *
   * @return true if the container is healthy, false otherwise
   */
  default boolean isHealthy() {
    HddsProtos.LifeCycleState containerState = getContainer().getState();
    return (containerState == HddsProtos.LifeCycleState.CLOSED
        || containerState == HddsProtos.LifeCycleState.QUASI_CLOSED)
        && getReplicas().stream()
        .filter(r -> r.getDatanodeDetails().getPersistedOpState() == IN_SERVICE)
        .allMatch(r -> LegacyReplicationManager.compareState(
            containerState, r.getState()));

  }

  /**
   * Returns true is there are no replicas of the container available, ie the
   * set of container replicas has zero entries.
   *
   * @return true if there are no replicas, false otherwise.
   */
  default boolean isMissing() {
    return getReplicas().isEmpty();
  }
}
