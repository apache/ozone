package org.apache.hadoop.ozone.container.checksum;

import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Used to execute a container reconciliation task that has been queued from the ReplicationSupervisor.
 */
public class ReconcileContainerTask extends AbstractReplicationTask {
  private final ReconcileContainerCommand command;
  private final DNContainerOperationClient dnClient;
  private final ContainerController controller;

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconcileContainerTask.class);

  public ReconcileContainerTask(ContainerController controller,
      DNContainerOperationClient dnClient, ReconcileContainerCommand command) {
    super(command.getContainerID(), command.getDeadline(), command.getTerm());
    this.command = command;
    this.controller = controller;
    this.dnClient = dnClient;
  }

  @Override
  public void runTask() {
    long start = Time.monotonicNow();

    LOG.info("{}", this);

    try {
      controller.reconcileContainer(dnClient, command.getContainerID(), command.getPeerDatanodes());
      setStatus(Status.DONE);
      long elapsed = Time.monotonicNow() - start;
      LOG.info("{} completed in {} ms", this, elapsed);
    } catch (Exception e) {
      long elapsed = Time.monotonicNow() - start;
      setStatus(Status.FAILED);
      LOG.warn("{} failed in {} ms", this, elapsed, e);
    }
  }

  @Override
  protected Object getCommandForDebug() {
    return command.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReconcileContainerTask that = (ReconcileContainerTask) o;
    return Objects.equals(command, that.command);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getContainerId());
  }
}
