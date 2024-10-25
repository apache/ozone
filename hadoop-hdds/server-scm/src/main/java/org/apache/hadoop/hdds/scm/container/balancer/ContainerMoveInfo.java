package org.apache.hadoop.hdds.scm.container.balancer;

/**
 * Information about moving containers.
 */
public class ContainerMoveInfo {
  private final long containerMovesScheduled;
  private final long containerMovesCompleted;
  private final long containerMovesFailed;
  private final long containerMovesTimeout;

  public ContainerMoveInfo(ContainerBalancerMetrics metrics) {
    this.containerMovesScheduled = metrics.getNumContainerMovesScheduledInLatestIteration();
    this.containerMovesCompleted = metrics.getNumContainerMovesCompletedInLatestIteration();
    this.containerMovesFailed = metrics.getNumContainerMovesFailedInLatestIteration();
    this.containerMovesTimeout = metrics.getNumContainerMovesTimeoutInLatestIteration();
  }

  public long getContainerMovesScheduled() {
    return containerMovesScheduled;
  }

  public long getContainerMovesCompleted() {
    return containerMovesCompleted;
  }

  public long getContainerMovesFailed() {
    return containerMovesFailed;
  }

  public long getContainerMovesTimeout() {
    return containerMovesTimeout;
  }
}
