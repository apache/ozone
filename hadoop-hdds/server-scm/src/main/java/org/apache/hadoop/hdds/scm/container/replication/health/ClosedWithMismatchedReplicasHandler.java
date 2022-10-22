/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handler to process containers which are closed, but some replicas are still
 * open or closing. This handler will send a command to the datanodes for each
 * mis-matched replica to close it.
 */
public class ClosedWithMismatchedReplicasHandler extends AbstractCheck {

  private ReplicationManager replicationManager;

  public ClosedWithMismatchedReplicasHandler(
      ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
  }

  @Override
  public boolean handle(ContainerCheckRequest request) {
    ContainerInfo containerInfo = request.getContainerInfo();
    Set<ContainerReplica> replicas = request.getContainerReplicas();
    if (containerInfo.getState() != HddsProtos.LifeCycleState.CLOSED) {
      // Handler is only relevant for CLOSED containers.
      return false;
    }
    List<ContainerReplica> unhealthyReplicas = replicas.stream()
        .filter(r -> !ReplicationManager
            .compareState(containerInfo.getState(), r.getState()))
        .collect(Collectors.toList());

    if (unhealthyReplicas.size() > 0) {
      handleUnhealthyReplicas(containerInfo, unhealthyReplicas);
      return true;
    }
    return false;
  }

  /**
   * Handles unhealthy container.
   * A container is inconsistent if any of the replica state doesn't
   * match the container state. We have to take appropriate action
   * based on state of the replica.
   *
   * @param container ContainerInfo
   * @param unhealthyReplicas List of ContainerReplica
   */
  private void handleUnhealthyReplicas(final ContainerInfo container,
      List<ContainerReplica> unhealthyReplicas) {
    Iterator<ContainerReplica> iterator = unhealthyReplicas.iterator();
    while (iterator.hasNext()) {
      final ContainerReplica replica = iterator.next();
      final ContainerReplicaProto.State state = replica.getState();
      if (state == ContainerReplicaProto.State.OPEN
          || state == ContainerReplicaProto.State.CLOSING) {
        replicationManager.sendCloseContainerReplicaCommand(
            container, replica.getDatanodeDetails(), true);
        iterator.remove();
      }
    }
  }
}
