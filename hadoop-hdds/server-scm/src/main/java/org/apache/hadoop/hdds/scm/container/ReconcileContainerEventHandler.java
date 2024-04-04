package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND;

/**
 * When a reconcile container event is fired, this class will check if the container is eligible for reconciliation,
 * and if so, send the reconcile request to all datanodes with a replica of that container.
 */
public class ReconcileContainerEventHandler implements EventHandler<ContainerID> {
  public static final Logger LOG =
      LoggerFactory.getLogger(ReconcileContainerEventHandler.class);

  private ContainerManager containerManager;
  private SCMContext scmContext;

  public ReconcileContainerEventHandler(
      final ContainerManager containerManager,
      final SCMContext scmContext) {
    this.containerManager = containerManager;
    this.scmContext = scmContext;
  }

  @Override
  public void onMessage(ContainerID containerID, EventPublisher publisher) {
    if (!scmContext.isLeader()) {
      LOG.info("Skip reconciling container {} since current SCM is not leader.", containerID);
      return;
    }

    try {
      ContainerInfo container = containerManager.getContainer(containerID);
      final HddsProtos.LifeCycleState state = container.getState();
      if (state.equals(HddsProtos.LifeCycleState.OPEN)) {
        LOG.error("Cannot reconcile container in state {}.", state);
        return;
      }

      // This restriction can be removed when reconciliation for EC containers is added.
      final HddsProtos.ReplicationType repType = container.getReplicationType();
      if (repType == HddsProtos.ReplicationType.EC) {
        LOG.error("Cannot reconcile container {} with replication type {}. Reconciliation is currently only supported" +
            " for Ratis containers.", containerID, repType);
        return;
      }

      Set<DatanodeDetails> replicas = containerManager.getContainerReplicas(containerID)
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toSet());

      LOG.info("Reconcile container event triggered for container {} with peers {}", containerID, replicas);

      for (DatanodeDetails replica: replicas) {
        List<DatanodeDetails> otherReplicas = replicas.stream()
            .filter(other -> !other.equals(replica))
            .collect(Collectors.toList());
        ReconcileContainerCommand command = new ReconcileContainerCommand(containerID.getId(), otherReplicas);
        publisher.fireEvent(DATANODE_COMMAND, new CommandForDatanode<>(replica.getUuid(), command));
      }
    } catch (ContainerNotFoundException ex) {
      LOG.error("Failed to start reconciliation for container {}. Container not found.", containerID);
    }
  }
}
