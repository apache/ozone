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
import java.util.function.Predicate;
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

  public static final Logger LOG =
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

    LOG.debug("Checking container {} in MismatchedReplicasHandler", containerInfo);

    final Predicate<ContainerReplica> shouldSendClose = (r) -> shouldSendClose(containerInfo, r);

    replicas.stream().filter(shouldSendClose).forEach(r -> {
      LOG.debug("Sending close command for mismatched replica {} of container {}.", r, containerInfo);
      replicationManager.sendCloseContainerReplicaCommand(
          containerInfo, r.getDatanodeDetails(), shouldForceClose(containerInfo, r));
    });

    /*
     This handler is unique because it always returns false. This allows
     handlers further in the chain to fix issues such as under replication.
     */
    return false;
  }

  /**
   * Returns true if the replica state doesn't match the container state and the replica can be 
   * QUASI_CLOSED/CLOSED.
   *
   * This method only works for QUASI_CLOSED/CLOSED Containers.
   */
  private boolean shouldSendClose(final ContainerInfo container, final ContainerReplica replica) {
    return replica.getState() == ContainerReplicaProto.State.OPEN ||
        replica.getState() == ContainerReplicaProto.State.CLOSING ||
        (replica.getState() == ContainerReplicaProto.State.QUASI_CLOSED &&
        shouldForceClose(container, replica));
  }

  /**
   * Retruns true if the Container is CLOSED but the Replica is not, and the Sequence Id matches.
   */
  private boolean shouldForceClose(final ContainerInfo container, final ContainerReplica replica) {
    return container.getState() == HddsProtos.LifeCycleState.CLOSED &&
        container.getSequenceId() == replica.getSequenceId();
  }
}
