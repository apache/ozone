package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND;

/**
 * SCM may trigger a reconcile container request when it sees multiple non-open containers whose hashes do not match.
 * The reconcile command can also be triggered manually from the command line.
 * This command will instruct datanodes to read blocks from their peers that also have replicas of the specified
 * container to reach an agreement on its contents.
 */
public class ReconcileContainerCommandHandler implements EventHandler<ContainerID> {

  public static final Logger LOG =
      LoggerFactory.getLogger(CloseContainerEventHandler.class);

  private final ContainerManager containerManager;
  private final SCMContext scmContext;

  public ReconcileContainerCommandHandler(ContainerManager containerManager, SCMContext scmContext) {
    this.containerManager = containerManager;
    this.scmContext = scmContext;
  }

  @Override
  public void onMessage(ContainerID containerID, EventPublisher publisher) {
    if (!scmContext.isLeader()) {
      LOG.info("Skip command to reconcile container {} since the current SCM is not the leader.",
          containerID);
      return;
    }

    try {
      List<DatanodeDetails> nodesWithReplica = containerManager.getContainerReplicas(containerID)
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());

      // TODO fail if container recon not allowed

      // Datanodes will not reconcile with themselves even if they are listed as a source.
      // Therefore, send the same source list to every datanode.
      SCMCommand<?> reconcileCommand = new ReconcileContainerCommand(containerID.getId(), nodesWithReplica);
      nodesWithReplica.forEach(node ->
          publisher.fireEvent(DATANODE_COMMAND, new CommandForDatanode<>(node.getUuid(), reconcileCommand)));
    } catch (ContainerNotFoundException ex) {
      LOG.error("Cannot send reconcile command for unknown container {}", containerID);
    }
  }
}
