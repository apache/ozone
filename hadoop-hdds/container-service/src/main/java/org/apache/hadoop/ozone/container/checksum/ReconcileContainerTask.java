package org.apache.hadoop.ozone.container.checksum;

import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinatorTask;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ReconcileContainerTask extends AbstractReplicationTask {
  private final ReconcileContainerCommand command;
  // Used to read container checksum info from this datanode.
  private final ContainerChecksumTreeManager checksumManager;
  // TODO HDDS-10376 used to read container checksum info and blocks from other datanodes.
   private final DNContainerOperationClient dnClient;
  private final ContainerController controller;

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconcileContainerTask.class);

  public ReconcileContainerTask(ContainerChecksumTreeManager checksumManager, ContainerController controller,
                                DNContainerOperationClient dnClient, ReconcileContainerCommand command) {
    super(command.getContainerID(), command.getDeadline(), command.getTerm());
    this.checksumManager = checksumManager;
    this.command = command;
    this.controller = controller;
    this.dnClient = dnClient;
  }

  @Override
  public void runTask() {
    long start = Time.monotonicNow();

    LOG.info("{}", this);

    try {
      long elapsed = Time.monotonicNow() - start;
      controller.reconcileContainer(dnClient, checksumManager, command.getContainerID(), command.getPeerDatanodes());
      setStatus(Status.DONE);
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
    ECReconstructionCoordinatorTask that = (ECReconstructionCoordinatorTask) o;
    return getContainerId() == that.getContainerId();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getContainerId());
  }
}
