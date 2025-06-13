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

package org.apache.hadoop.hdds.scm.node;

import static org.apache.hadoop.hdds.scm.events.SCMEvents.CLOSE_CONTAINER;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.container.ContainerException;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Dead Node event.
 */
public class DeadNodeHandler implements EventHandler<DatanodeDetails> {

  private final NodeManager nodeManager;
  private final PipelineManager pipelineManager;
  private final ContainerManager containerManager;
  @Nullable
  private final DeletedBlockLog deletedBlockLog;

  private static final Logger LOG =
      LoggerFactory.getLogger(DeadNodeHandler.class);

  public DeadNodeHandler(final NodeManager nodeManager,
                         final PipelineManager pipelineManager,
                         final ContainerManager containerManager) {
    this(nodeManager, pipelineManager, containerManager, null);
  }

  public DeadNodeHandler(final NodeManager nodeManager,
                         final PipelineManager pipelineManager,
                         final ContainerManager containerManager,
                         @Nullable final DeletedBlockLog deletedBlockLog) {
    this.nodeManager = nodeManager;
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;
    this.deletedBlockLog = deletedBlockLog;
  }

  @Override
  public void onMessage(final DatanodeDetails datanodeDetails,
                        final EventPublisher publisher) {

    try {

      /*
       * We should have already destroyed all the pipelines on this datanode
       * when it was marked as stale. Destroy pipeline should also have closed
       * all the containers on this datanode.
       *
       * Ideally we should not have any pipeline or OPEN containers now.
       *
       * To be on a safer side, we double check here and take appropriate
       * action.
       */
      LOG.info("A dead datanode is detected. {}", datanodeDetails);
      closeContainers(datanodeDetails, publisher);
      destroyPipelines(datanodeDetails);

      boolean isNodeInMaintenance = nodeManager.getNodeStatus(datanodeDetails).isInMaintenance();

      // Remove the container replicas associated with the dead node unless it
      // is IN_MAINTENANCE
      if (!isNodeInMaintenance) {
        removeContainerReplicas(datanodeDetails);
      }

      // Notify ReplicationManager
      if (!isNodeInMaintenance) {
        LOG.debug("Notifying ReplicationManager about dead node: {}",
            datanodeDetails);
        publisher.fireEvent(SCMEvents.REPLICATION_MANAGER_NOTIFY, datanodeDetails);
      }

      // remove commands in command queue for the DN
      final List<SCMCommand<?>> cmdList = nodeManager.getCommandQueue(
          datanodeDetails.getID());
      LOG.info("Clearing command queue of size {} for DN {}",
          cmdList.size(), datanodeDetails);

      // remove DeleteBlocksCommand associated with the dead node unless it
      // is IN_MAINTENANCE
      if (deletedBlockLog != null && !isNodeInMaintenance) {
        deletedBlockLog.onDatanodeDead(datanodeDetails.getID());
      }

      //move dead datanode out of ClusterNetworkTopology
      NetworkTopology nt = nodeManager.getClusterNetworkTopologyMap();
      if (nt.contains(datanodeDetails)) {
        nt.remove(datanodeDetails);
        //make sure after DN is removed from topology,
        //DatanodeDetails instance returned from nodeStateManager has no parent.
        Preconditions.checkState(
            nodeManager.getNode(datanodeDetails.getID())
                .getParent() == null);
      }
    } catch (NodeNotFoundException ex) {
      // This should not happen, we cannot get a dead node event for an
      // unregistered datanode!
      LOG.error("DeadNode event for a unregistered node: {}!", datanodeDetails);
    }
  }

  /**
   * Destroys all the pipelines on the given datanode if there are any.
   *
   * @param datanodeDetails DatanodeDetails
   */
  private void destroyPipelines(final DatanodeDetails datanodeDetails) {
    Optional.ofNullable(nodeManager.getPipelines(datanodeDetails))
        .ifPresent(pipelines ->
            pipelines.forEach(id -> {
              try {
                pipelineManager.closePipeline(id);
                pipelineManager.deletePipeline(id);
              } catch (PipelineNotFoundException ignore) {
                // Pipeline is not there in pipeline manager,
                // should we care?
              } catch (IOException ex) {
                LOG.warn("Exception while finalizing pipeline {}",
                    id, ex);
              }
            }));
  }

  /**
   * Sends CloseContainerCommand to all the open containers on the
   * given datanode.
   *
   * @param datanodeDetails DatanodeDetails
   * @param publisher EventPublisher
   * @throws NodeNotFoundException
   */
  private void closeContainers(final DatanodeDetails datanodeDetails,
                               final EventPublisher publisher)
      throws NodeNotFoundException {
    nodeManager.getContainers(datanodeDetails)
        .forEach(id -> {
          try {
            final ContainerInfo container = containerManager.getContainer(id);
            if (container.getState() == HddsProtos.LifeCycleState.OPEN) {
              publisher.fireEvent(CLOSE_CONTAINER, id);
            }
          } catch (ContainerNotFoundException cnfe) {
            LOG.warn("Container {} is not managed by ContainerManager.",
                id, cnfe);
          }
        });
  }

  /**
   * Removes the ContainerReplica of the dead datanode from the containers
   * which are hosted by that datanode.
   *
   * @param datanodeDetails DatanodeDetails
   * @throws NodeNotFoundException
   */
  private void removeContainerReplicas(final DatanodeDetails datanodeDetails)
      throws NodeNotFoundException {
    nodeManager.getContainers(datanodeDetails)
        .forEach(id -> {
          try {
            final ContainerInfo container = containerManager.getContainer(id);
            // Identify and remove the ContainerReplica of dead node
            containerManager.getContainerReplicas(id)
                .stream()
                .filter(r -> r.getDatanodeDetails().equals(datanodeDetails))
                .findFirst()
                .ifPresent(replica -> {
                  try {
                    containerManager.removeContainerReplica(id, replica);
                  } catch (ContainerException ex) {
                    LOG.warn("Exception while removing container replica #{} " +
                        "of container {}.", replica, container, ex);
                  }
                });
          } catch (ContainerNotFoundException cnfe) {
            LOG.warn("Container {} is not managed by ContainerManager.",
                id, cnfe);
          }
        });
  }

  protected NodeManager getNodeManager() {
    return nodeManager;
  }

}
