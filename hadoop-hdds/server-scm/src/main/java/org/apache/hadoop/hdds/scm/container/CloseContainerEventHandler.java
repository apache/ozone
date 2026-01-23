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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.lease.LeaseAlreadyExistException;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In case of a node failure, volume failure, volume out of spapce, node
 * out of space etc, CLOSE_CONTAINER will be triggered.
 * CloseContainerEventHandler is the handler for CLOSE_CONTAINER.
 * When a close container event is fired, a close command for the container
 * should be sent to all the datanodes in the pipeline and containerStateManager
 * needs to update the container state to Closing.
 */
public class CloseContainerEventHandler implements EventHandler<ContainerID> {

  private static final Logger LOG =
      LoggerFactory.getLogger(CloseContainerEventHandler.class);

  private final PipelineManager pipelineManager;
  private final ContainerManager containerManager;
  private final SCMContext scmContext;

  private final LeaseManager<Object> leaseManager;
  private final long timeout;

  public CloseContainerEventHandler(
      final PipelineManager pipelineManager,
      final ContainerManager containerManager,
      final SCMContext scmContext,
      @Nullable LeaseManager<Object> leaseManager,
      final long timeout) {
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;
    this.scmContext = scmContext;
    this.leaseManager = leaseManager;
    this.timeout = timeout;
  }

  @Override
  public void onMessage(ContainerID containerID, EventPublisher publisher) {
    if (!scmContext.isLeader()) {
      LOG.info("Skip close container {} since current SCM is not leader.",
          containerID);
      return;
    }

    try {
      LOG.info("Close container Event triggered for container : {}, " +
              "current state: {}", containerID,
              containerManager.getContainer(containerID).getState());
      // If the container is in OPEN state, FINALIZE it.
      if (containerManager.getContainer(containerID).getState()
          == LifeCycleState.OPEN) {
        containerManager.updateContainerState(
            containerID, LifeCycleEvent.FINALIZE);
      }

      // ContainerInfo has to read again after the above state change.
      final ContainerInfo container = containerManager
          .getContainer(containerID);
      // Send close command to datanodes, if the container is in CLOSING state
      if (container.getState() == LifeCycleState.CLOSING) {
        boolean force = false;
        // Any container that is not of type RATIS should be moved to CLOSED
        // immediately on the DNs. Setting force to true, avoids the container
        // going into the QUASI_CLOSED state, which is only applicable for RATIS
        // containers.
        if (container.getReplicationConfig().getReplicationType()
            != HddsProtos.ReplicationType.RATIS) {
          force = true;
        }
        SCMCommand<?> command = new CloseContainerCommand(
            containerID.getId(), container.getPipelineID(), force);
        command.setTerm(scmContext.getTermOfLeader());
        command.setEncodedToken(getContainerToken(containerID));

        if (null != leaseManager) {
          try {
            leaseManager.acquire(command, timeout, () -> triggerCloseCallback(
                publisher, container, command));
          } catch (LeaseAlreadyExistException ex) {
            LOG.debug("Close container {} in {} state already in queue.",
                containerID, container.getState());
          } catch (Exception ex) {
            LOG.error("Error while scheduling close", ex);
          }
        } else {
          // case of recon, lease manager will be null, trigger event directly
          triggerCloseCallback(publisher, container, command);
        }
      } else {
        LOG.debug("Cannot close container {}, which is in {} state.",
            containerID, container.getState());
      }
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending close container command,"
          + " since current SCM is not leader.", nle);
    } catch (IOException | InvalidStateTransitionException ex) {
      LOG.error("Failed to close the container {}.", containerID, ex);
    }
  }

  /**
   * Callback method triggered when timeout occurs at lease manager.
   * This will then send close command to DN (adding to command queue)
   * after this delay. This delay is provided to ensure the allocated blocks
   * are written successfully by the client with in the delay, and 
   * SCM in closing state will not allocate new blocks during this time.
   * 
   * @param publisher the publisher
   * @param container the container info
   * @param command the scm delete command
   * @return Void
   * @throws ContainerNotFoundException
   */
  private Void triggerCloseCallback(
      EventPublisher publisher, ContainerInfo container, SCMCommand<?> command)
      throws ContainerNotFoundException {
    getNodes(container).forEach(node ->
        publisher.fireEvent(DATANODE_COMMAND,
            new CommandForDatanode<>(node, command)));
    return null;
  }

  private String getContainerToken(ContainerID containerID) {
    if (scmContext.getScm() instanceof StorageContainerManager) {
      StorageContainerManager scm =
          (StorageContainerManager) scmContext.getScm();
      return scm.getContainerTokenGenerator().generateEncodedToken(containerID);
    }
    return ""; //Recon and unit test
  }

  /**
   * Returns the list of Datanodes where this container lives.
   *
   * @param container ContainerInfo
   * @return list of DatanodeDetails
   * @throws ContainerNotFoundException
   */
  private List<DatanodeDetails> getNodes(final ContainerInfo container)
      throws ContainerNotFoundException {
    try {
      return pipelineManager.getPipeline(container.getPipelineID()).getNodes();
    } catch (PipelineNotFoundException ex) {
      // Use container replica if the pipeline is not available.
      return containerManager.getContainerReplicas(container.containerID())
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());
    }
  }
}
