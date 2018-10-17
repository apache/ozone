/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import java.util.Set;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerException;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationRequest;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Dead Node event.
 */
public class DeadNodeHandler implements EventHandler<DatanodeDetails> {

  private final ContainerManager containerManager;

  private final NodeManager nodeManager;

  private static final Logger LOG =
      LoggerFactory.getLogger(DeadNodeHandler.class);

  public DeadNodeHandler(NodeManager nodeManager,
      ContainerManager containerManager) {
    this.containerManager = containerManager;
    this.nodeManager = nodeManager;
  }

  @Override
  public void onMessage(DatanodeDetails datanodeDetails,
      EventPublisher publisher) {
    nodeManager.processDeadNode(datanodeDetails.getUuid());

    // TODO: check if there are any pipeline on this node and fire close
    // pipeline event
    Set<ContainerID> ids =
        nodeManager.getContainers(datanodeDetails.getUuid());
    if (ids == null) {
      LOG.info("There's no containers in dead datanode {}, no replica will be"
          + " removed from the in-memory state.", datanodeDetails.getUuid());
      return;
    }
    LOG.info("Datanode {}  is dead. Removing replications from the in-memory" +
            " state.", datanodeDetails.getUuid());
    for (ContainerID id : ids) {
      try {
        final ContainerInfo container = containerManager.getContainer(id);
        if (!container.isOpen()) {
          final ContainerReplica replica = ContainerReplica.newBuilder()
              .setContainerID(id)
              .setDatanodeDetails(datanodeDetails)
              .build();
          try {
            containerManager.removeContainerReplica(id, replica);
            replicateIfNeeded(container, publisher);
          } catch (ContainerException ex) {
            LOG.warn("Exception while removing container replica #{} for " +
                "container #{}.", replica, container, ex);
          }
        }
      } catch (ContainerNotFoundException cnfe) {
        LOG.warn("Container Not found!", cnfe);
      }
    }
  }

  /**
   * Compare the existing replication number with the expected one.
   */
  private void replicateIfNeeded(ContainerInfo container,
      EventPublisher publisher) throws ContainerNotFoundException {
    final int existingReplicas = containerManager
        .getContainerReplicas(container.containerID()).size();
    final int expectedReplicas = container.getReplicationFactor().getNumber();
    if (existingReplicas != expectedReplicas) {
      publisher.fireEvent(SCMEvents.REPLICATE_CONTAINER,
          new ReplicationRequest(
              container.getContainerID(), existingReplicas, expectedReplicas));
    }
  }

  /**
   * Returns logger.
   * */
  // TODO: remove this.
  public static Logger getLogger() {
    return LOG;
  }
}
