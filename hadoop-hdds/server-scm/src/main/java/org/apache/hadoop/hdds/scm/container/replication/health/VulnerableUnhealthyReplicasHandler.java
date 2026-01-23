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

package org.apache.hadoop.hdds.scm.container.replication.health;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;

import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.RatisContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A QUASI_CLOSED container may have some UNHEALTHY replicas with the
 * same Sequence ID as the container and on unique origins. RM should try to maintain one
 * copy of such replicas.
 */
public class VulnerableUnhealthyReplicasHandler extends AbstractCheck {
  private static final Logger LOG = LoggerFactory.getLogger(VulnerableUnhealthyReplicasHandler.class);
  private final ReplicationManager replicationManager;

  public VulnerableUnhealthyReplicasHandler(ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
  }

  /**
   * Checks if the container is QUASI_CLOSED has some vulnerable UNHEALTHY replicas that need to replicated to
   * other Datanodes. These replicas have the same sequence ID as the container while other healthy replicas don't.
   * Or, these replicas have unique origin Datanodes. If the node hosting such a replica is being taken offline, then
   * the replica may have to be replicated to another node.
   * @param request ContainerCheckRequest object representing the container
   * @return true if some vulnerable UNHEALTHY replicas were found, else false
   */
  @Override
  public boolean handle(ContainerCheckRequest request) {
    ContainerInfo container = request.getContainerInfo();
    if (container.getReplicationType() != RATIS) {
      // This handler is only for Ratis containers.
      return false;
    }
    if (container.getState() != HddsProtos.LifeCycleState.QUASI_CLOSED) {
      return false;
    }
    Set<ContainerReplica> replicas = request.getContainerReplicas();
    LOG.debug("Checking whether container {} with replicas {} has vulnerable UNHEALTHY replicas.", container, replicas);
    RatisContainerReplicaCount replicaCount =
        new RatisContainerReplicaCount(container, replicas, request.getPendingOps(), request.getMaintenanceRedundancy(),
            true);

    List<ContainerReplica> vulnerableUnhealthy = replicaCount.getVulnerableUnhealthyReplicas(dn -> {
      try {
        return replicationManager.getNodeStatus(dn);
      } catch (NodeNotFoundException e) {
        LOG.warn("Exception for datanode {} while handling vulnerable replicas for container {}, with all replicas" +
            " {}.", dn, container, replicaCount.getReplicas(), e);
        return null;
      }
    });

    if (!vulnerableUnhealthy.isEmpty()) {
      LOG.info("Found vulnerable UNHEALTHY replicas {} for container {}.", vulnerableUnhealthy, container);
      ReplicationManagerReport report = request.getReport();
      report.incrementAndSample(ReplicationManagerReport.HealthState.UNDER_REPLICATED, container.containerID());
      if (!request.isReadOnly()) {
        ContainerHealthResult.UnderReplicatedHealthResult underRepResult =
            replicaCount.toUnderHealthResult();
        underRepResult.setHasVulnerableUnhealthy(true);
        request.getReplicationQueue().enqueue(underRepResult);
      }
      return true;
    }

    return false;
  }

}
