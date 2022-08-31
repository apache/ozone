package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;

import java.util.Set;

import static org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.compareState;

public class OpenContainerHandler extends AbstractCheck {

  ReplicationManager replicationManager;

  public OpenContainerHandler(ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
  }

  @Override
  public boolean handle(ContainerCheckRequest request) {
    ContainerInfo containerInfo = request.getContainerInfo();
    if (containerInfo.getState() == HddsProtos.LifeCycleState.OPEN) {
      if (!isOpenContainerHealthy(
          containerInfo, request.getContainerReplicas())) {
        // This is an unhealthy open container, so we need to trigger the
        // close process on it.
        request.getReport().incrementAndSample(
            ReplicationManagerReport.HealthState.OPEN_UNHEALTHY,
            containerInfo.containerID());
        replicationManager.sendCloseContainerEvent(containerInfo.containerID());
        return true;
      }
      // For open containers we do not want to do any further processing in RM
      // so return true to stop the command chain.
      return true;
    }
    // The container is not open, so we return false to let the next handler in
    // the chain process it.
    return false;
  }

  /**
   * An open container is healthy if all its replicas are in the same state as
   * the container.
   * @param container The container to check
   * @param replicas The replicas belonging to the container
   * @return True if the container is healthy, false otherwise
   */
  private boolean isOpenContainerHealthy(
      ContainerInfo container, Set< ContainerReplica > replicas) {
    HddsProtos.LifeCycleState state = container.getState();
    return replicas.stream()
        .allMatch(r -> compareState(state, r.getState()));
  }
}
