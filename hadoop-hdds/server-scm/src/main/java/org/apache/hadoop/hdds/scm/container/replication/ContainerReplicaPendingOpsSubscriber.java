package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.scm.container.ContainerID;

/**
 * A subscriber can register with ContainerReplicaPendingOps to receive
 * updates on pending ops.
 */
public interface ContainerReplicaPendingOpsSubscriber {

  /**
   * Notifies that the specified op has been completed for the specified
   * containerID. Might have completed normally or timed out.
   *
   * @param op Add or Delete op
   * @param containerID container on which the operation is being performed
   * @param timedOut true if the timed out, else false
   */
  void opCompleted(ContainerReplicaOp op, ContainerID containerID,
      boolean timedOut);
}
