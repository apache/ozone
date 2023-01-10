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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Handler to process containers which are closed, but some replicas are still
 * open or closing. This handler will send a command to the datanodes for each
 * mis-matched replica to close it.
 */
public class ClosedWithMismatchedReplicasHandler extends AbstractCheck {

  public static final Logger LOG =
      LoggerFactory.getLogger(ClosedWithMismatchedReplicasHandler.class);

  private ReplicationManager replicationManager;

  public ClosedWithMismatchedReplicasHandler(
      ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
  }

  /**
   * Handles CLOSED EC or RATIS container. If some replicas are CLOSING or
   * OPEN, this sends a force-close command for them.
   * @param request ContainerCheckRequest object representing the container
   * @return always returns true so that other handlers in the chain can fix
   * issues such as under replication
   */
  @Override
  public boolean handle(ContainerCheckRequest request) {
    ContainerInfo containerInfo = request.getContainerInfo();
    Set<ContainerReplica> replicas = request.getContainerReplicas();
    if (containerInfo.getState() != HddsProtos.LifeCycleState.CLOSED) {
      // Handler is only relevant for CLOSED containers.
      return false;
    }
    LOG.debug("Checking container {} in ClosedWithMismatchedReplicasHandler",
        containerInfo);

    // close replica if its state is OPEN or CLOSING
    for (ContainerReplica replica : replicas) {
      if (isMismatched(replica)) {
        LOG.debug("Sending close command for mismatched replica {} of " +
            "container {}.", replica, containerInfo);
        replicationManager.sendCloseContainerReplicaCommand(
            containerInfo, replica.getDatanodeDetails(), true);
      }
    }

    /*
     This handler is unique because it always returns false. This allows
     handlers further in the chain to fix issues such as under replication.
     */
    return false;
  }

  /**
   * If a CLOSED container has an OPEN or CLOSING replica, there is a state
   * mismatch.
   * @param replica replica to check for mismatch
   * @return true if the replica is in CLOSING or OPEN state, else false
   */
  private boolean isMismatched(ContainerReplica replica) {
    return replica.getState() == ContainerReplicaProto.State.OPEN ||
        replica.getState() == ContainerReplicaProto.State.CLOSING;
  }
}
