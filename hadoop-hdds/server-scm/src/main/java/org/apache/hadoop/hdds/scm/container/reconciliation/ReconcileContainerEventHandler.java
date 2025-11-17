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

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.reconciliation.ReconciliationEligibilityHandler.EligibilityResult;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
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

  private final ContainerManager containerManager;
  private final SCMContext scmContext;

  public ReconcileContainerEventHandler(ContainerManager containerManager, SCMContext scmContext) {
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
      // TODO HDDS-10714 restriction peer and target nodes based on node status.
      Set<DatanodeDetails> allReplicaNodes = containerManager.getContainerReplicas(containerID)
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toSet());

      LOG.info("Reconcile container event triggered for container {} with peers {}", containerID, allReplicaNodes);

      for (DatanodeDetails replica : allReplicaNodes) {
        Set<DatanodeDetails> otherReplicas = allReplicaNodes.stream()
            .filter(other -> !other.equals(replica))
            .collect(Collectors.toSet());
        ReconcileContainerCommand command = new ReconcileContainerCommand(containerID.getId(), otherReplicas);
        command.setTerm(scmContext.getTermOfLeader());
        publisher.fireEvent(DATANODE_COMMAND, new CommandForDatanode<>(replica, command));
      }
    } catch (ContainerNotFoundException ex) {
      LOG.error("Failed to start reconciliation for container {}. Container not found.", containerID);
    } catch (NotLeaderException nle) {
      LOG.info("Skip reconciling container {} since current SCM is not leader.", containerID);
    }
  }
}
