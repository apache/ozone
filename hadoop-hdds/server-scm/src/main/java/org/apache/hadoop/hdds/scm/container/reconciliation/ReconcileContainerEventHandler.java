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
 * When a reconcile container event is fired, this class will check if the container is eligible for reconciliation,
 * and if so, send the reconcile request to all datanodes with a replica of that container.
 */
public class ReconcileContainerEventHandler implements EventHandler<ContainerID> {
  public static final Logger LOG =
      LoggerFactory.getLogger(ReconcileContainerEventHandler.class);

  private final NodeManager nodeManager;
  private final ContainerManager containerManager;
  private final SCMContext scmContext;

  public ReconcileContainerEventHandler(NodeManager nodeManager, ContainerManager containerManager,
      SCMContext scmContext) {
    this.nodeManager = nodeManager;
    this.containerManager = containerManager;
    this.scmContext = scmContext;
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
      // Restrict which nodes participate in reconciliation based on their status (HDDS-10714).
      // Stale, dead, decommissioned, and in-maintenance nodes are neither peers nor targets.
      // Decommissioning and entering-maintenance nodes can be peers but not targets, so their
      // data can be reconciled off before they leave the cluster. Healthy, in-service nodes are both.
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
        if (!status.isDecommissioned() && !status.isInMaintenance()) {
          peers.add(datanode);
        }
        if (status.isInService()) {
          targets.add(datanode);
        }
      }

      LOG.info("Reconcile container event triggered for container {} with targets {} and peers {}",
          containerID, targets, peers);

      for (DatanodeDetails target : targets) {
        Set<DatanodeDetails> otherPeers = new HashSet<>(peers);
        otherPeers.remove(target);
        if (otherPeers.isEmpty()) {
          // No eligible peer to reconcile against, so skip sending a command with an empty peer list.
          continue;
        }
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
