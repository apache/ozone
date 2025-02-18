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

package org.apache.hadoop.hdds.scm.container.replication;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.NodeManager;

/**
 * Common interface for EC and non-EC container replica counts.
 * TODO pull up more methods if needed
 */
public interface ContainerReplicaCount {
  ContainerInfo getContainer();

  List<ContainerReplica> getReplicas();

  boolean isSufficientlyReplicated();

  /**
   * Checks if a container has enough replicas to allow the specified
   * datanode to be taken offline. This method is the interface between the
   * decommissioning flow and Replication Manager.
   * @param datanode the datanode being taken offline
   * @param nodeManager an instance of {@link NodeManager}
   * @return true if the datanode can be taken offline, otherwise false
   */
  boolean isSufficientlyReplicatedForOffline(DatanodeDetails datanode,
      NodeManager nodeManager);

  boolean isOverReplicated();

  int getDecommissionCount();

  int getMaintenanceCount();

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
        .allMatch(r -> ReplicationManager.compareState(
            containerState, r.getState()));

  }

  boolean isHealthyEnoughForOffline();

  /**
   * Return true if there are insufficient replicas to recover this container.
   *
   * @return true if there are insufficient replicas, false otherwise.
   */
  boolean isUnrecoverable();
}
