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

package org.apache.hadoop.hdds.scm.container.reconciliation;

import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND;

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.reconciliation.ReconciliationEligibilityHandler.EligibilityResult;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When a reconcile container event is fired, this class checks whether the container is eligible for reconciliation
 * and sends reconcile requests to eligible datanodes with a replica of that container.
 */
public class ReconcileContainerEventHandler implements EventHandler<ContainerID> {
  public static final Logger LOG =
      LoggerFactory.getLogger(ReconcileContainerEventHandler.class);

  private final NodeManager nodeManager;
  private final ContainerManager containerManager;
  private final SCMContext scmContext;

  public ReconcileContainerEventHandler(ContainerManager containerManager, SCMContext scmContext,
      NodeManager nodeManager) {
    this.containerManager = containerManager;
    this.scmContext = scmContext;
    this.nodeManager = nodeManager;
  }

  @Override
  public void onMessage(ContainerID containerID, EventPublisher publisher) {
    if (!scmContext.isLeader()) {
      LOG.info("Skip reconciling container {} since current SCM is not leader.", containerID);
      return;
    }

    EligibilityResult result = ReconciliationEligibilityHandler.isEligibleForReconciliation(containerID,
        containerManager);
    if (!result.isOk()) {
      LOG.error("{}", result);
      return;
    }

    try {
      Set<DatanodeDetails> targets = new HashSet<>();
      Set<DatanodeDetails> peers = new HashSet<>();
      for (ContainerReplica replica : containerManager.getContainerReplicas(containerID)) {
        DatanodeDetails datanode = replica.getDatanodeDetails();
        final NodeStatus status;
        try {
          status = nodeManager.getNodeStatus(datanode);
        } catch (NodeNotFoundException ex) {
          LOG.warn("Skipping datanode {} for reconciliation of container {} since its status is unknown.",
              datanode, containerID);
          continue;
        }
        if (!status.isHealthy()) {
          continue;
        }
        // Transitioning nodes remain peers so other replicas can recover any unique data before the node leaves.
        if (!status.isDecommissioned() && !status.isInMaintenance()) {
          peers.add(datanode);
        }
        if (status.isInService()) {
          targets.add(datanode);
        }
      }

      LOG.info("Reconcile container event triggered for container {} with targets {} and peers {}",
          containerID, targets, peers);

      if (targets.isEmpty()) {
        LOG.warn("Skipping reconciliation for container {} since no eligible target datanodes are available.",
            containerID);
        return;
      }

      for (DatanodeDetails target : targets) {
        Set<DatanodeDetails> otherPeers = new HashSet<>(peers);
        otherPeers.remove(target);
        // Even with no peers, the command generates missing checksum data and triggers a container scan on the target.
        ReconcileContainerCommand command = new ReconcileContainerCommand(containerID.getId(), otherPeers);
        command.setTerm(scmContext.getTermOfLeader());
        publisher.fireEvent(DATANODE_COMMAND, new CommandForDatanode<>(target, command));
      }
    } catch (ContainerNotFoundException ex) {
      LOG.error("Failed to start reconciliation for container {}. Container not found.", containerID);
    } catch (NotLeaderException nle) {
      LOG.info("Skip reconciling container {} since current SCM is not leader.", containerID);
    }
  }
}
