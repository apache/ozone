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

import java.util.Set;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler to process containers which are closed or quasi-closed, but some
 * replicas are still open or closing. This handler will send a command to
 * the datanodes for each mis-matched replica to close it.
 */
public class MismatchedReplicasHandler extends AbstractCheck {

  private static final Logger LOG =
      LoggerFactory.getLogger(MismatchedReplicasHandler.class);

  private final ReplicationManager replicationManager;

  public MismatchedReplicasHandler(
      ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
  }

  /**
   * Handles CLOSED EC or CLOSED/QUASI-CLOSED RATIS containers. If some
   * replicas are CLOSING or OPEN or QUASI_CLOSED, this tries to close them.
   * Force close command is sent for replicas of CLOSED containers and close
   * command is sent for replicas of QUASI-CLOSED containers (replicas of
   * quasi-closed containers should move to quasi-closed state).
   *
   * @param request ContainerCheckRequest object representing the container
   * @return always returns false so that other handlers in the chain can fix
   * issues such as under replication
   */
  @Override
  public boolean handle(ContainerCheckRequest request) {
    if (request.isReadOnly()) {
      return false;
    }

    final ContainerInfo containerInfo = request.getContainerInfo();
    final Set<ContainerReplica> replicas = request.getContainerReplicas();

    if (containerInfo.getState() != HddsProtos.LifeCycleState.CLOSED &&
        containerInfo.getState() != HddsProtos.LifeCycleState.QUASI_CLOSED) {
      // Handler is only relevant for CLOSED or QUASI-CLOSED containers.
      return false;
    }
    LOG.debug("Checking container {} in MismatchedReplicasHandler",
        containerInfo);
    // close replica if needed
    for (ContainerReplica replica : replicas) {
      ContainerReplicaProto.State replicaState = getTransitionState(containerInfo, replica);
      if (replicaState != null) {
        LOG.debug("Sending close command for mismatched replica {} of " +
            "container {}.", replica, containerInfo);
        replicationManager.sendCloseContainerReplicaCommand(
            containerInfo, replica.getDatanodeDetails(), ContainerReplicaProto.State.CLOSED.equals(replicaState));
      }
    }

    /*
     This handler is unique because it always returns false. This allows
     handlers further in the chain to fix issues such as under replication.
     */
    return false;
  }

  /**
   * Returns the final expected closed state type based on the scm container state and the replica state.
   * @param replica replica to check for mismatch and if it should be closed
   * @return null if the replica should not be closed, else CLOSED/QUASI_CLOSED based on the replica's
   * state.
   */
  private ContainerReplicaProto.State getTransitionState(ContainerInfo container,
                                                         ContainerReplica replica) {
    if (replica.getState() == ContainerReplicaProto.State.OPEN ||
        replica.getState() == ContainerReplicaProto.State.CLOSING) {
      return HddsProtos.ReplicationType.RATIS == container.getReplicationType() ?
          ContainerReplicaProto.State.QUASI_CLOSED : ContainerReplicaProto.State.CLOSED;
    }

    // a quasi closed replica of a closed container should be closed if their
    // sequence IDs match
    return container.getState() == HddsProtos.LifeCycleState.CLOSED &&
        replica.getState() == ContainerReplicaProto.State.QUASI_CLOSED &&
        container.getSequenceId() == replica.getSequenceId() ? ContainerReplicaProto.State.CLOSED
        : null;
  }
}
