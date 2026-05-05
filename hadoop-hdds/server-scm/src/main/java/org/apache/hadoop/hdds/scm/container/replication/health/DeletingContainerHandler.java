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
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used in Replication Manager to handle the
 * replicas of containers in DELETING State.
 */
public class DeletingContainerHandler extends AbstractCheck {
  private final ReplicationManager replicationManager;

  private static final Logger LOG =
      LoggerFactory.getLogger(DeletingContainerHandler.class);

  public DeletingContainerHandler(ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
  }

  /**
   * If the number of replicas of the container is 0, change the state
   * of the container to Deleted, otherwise resend delete command if needed.
   * @param request ContainerCheckRequest object representing the container
   * @return false if the specified container is not in DELETING state,
   * otherwise true.
   */
  @Override
  public boolean handle(ContainerCheckRequest request) {
    ContainerInfo containerInfo = request.getContainerInfo();
    ContainerID cID = containerInfo.containerID();
    HddsProtos.LifeCycleState containerState = containerInfo.getState();

    if (containerState == HddsProtos.LifeCycleState.DELETED) {
      LOG.debug("Container {} is DELETED so returning true", containerInfo);
      return true;
    }

    if (containerState != HddsProtos.LifeCycleState.DELETING) {
      return false;
    }
    LOG.debug("Checking container {} in DeletingContainerHandler",
        containerInfo);

    if (request.isReadOnly()) {
      // The reset of this method modifies state, so we just return true if
      // the request is read only
      return true;
    }

    if (request.getContainerReplicas().isEmpty()) {
      LOG.debug("Deleting Container {} has no replicas so marking for cleanup" +
          " and returning true", containerInfo);
      replicationManager.updateContainerState(
          cID, HddsProtos.LifeCycleEvent.CLEANUP);
      return true;
    }

    Set<DatanodeDetails> pendingDelete = request.getPendingOps().stream()
        .filter(o -> o.getOpType() == ContainerReplicaOp.PendingOpType.DELETE)
        .map(ContainerReplicaOp::getTarget).collect(Collectors.toSet());
    //resend deleteCommand if needed
    request.getContainerReplicas().stream()
        .filter(r -> !pendingDelete.contains(r.getDatanodeDetails()))
        .filter(ContainerReplica::isEmpty)
        .forEach(rp -> {
          try {
            replicationManager.sendDeleteCommand(
                containerInfo, rp.getReplicaIndex(), rp.getDatanodeDetails(),
                false);
          } catch (NotLeaderException e) {
            LOG.warn("Failed to delete empty replica with index {} for " +
                    "container {} on datanode {}", rp.getReplicaIndex(),
                cID, rp.getDatanodeDetails(), e);
          }
        });
    return true;
  }
}
