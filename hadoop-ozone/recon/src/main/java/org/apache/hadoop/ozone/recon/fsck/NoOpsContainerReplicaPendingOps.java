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

package org.apache.hadoop.ozone.recon.fsck;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

/**
 * No-op implementation of ContainerReplicaPendingOps for Recon's
 * local ReplicationManager.
 *
 * <p>This stub always returns empty pending operations because Recon does not
 * send replication commands to datanodes. It only uses ReplicationManager's
 * health check logic to determine container health states.</p>
 *
 * <p> Since SCM's health determination logic (Phase 1) explicitly ignores pending operations by calling
 * isSufficientlyReplicated(false). Pending operations only affect command
 * deduplication (Phase 2), which Recon doesn't need since it doesn't enqueue
 * commands.</p>
 */
public class NoOpsContainerReplicaPendingOps extends ContainerReplicaPendingOps {

  public NoOpsContainerReplicaPendingOps(Clock clock,
      ReplicationManager.ReplicationManagerConfiguration rmConf) {
    super(clock, rmConf);
  }

  /**
   * Always returns an empty list since Recon does not track pending operations.
   * This is correct because health state determination does not depend on
   * pending operations (see RatisReplicationCheckHandler.java:212).
   *
   * @param id The ContainerID to check for pending operations
   * @return Empty list - Recon has no pending operations
   */
  @Override
  public List<ContainerReplicaOp> getPendingOps(ContainerID id) {
    return Collections.emptyList();
  }

  /**
   * No-op since Recon doesn't add pending operations.
   */
  @Override
  public void scheduleAddReplica(ContainerID containerID, DatanodeDetails target,
      int replicaIndex, SCMCommand<?> command, long deadlineEpochMillis,
      long containerSize, long scheduledEpochMillis) {
    // No-op - Recon doesn't send commands
  }

  /**
   * No-op since Recon doesn't add pending operations.
   */
  @Override
  public void scheduleDeleteReplica(ContainerID containerID, DatanodeDetails target,
      int replicaIndex, SCMCommand<?> command, long deadlineEpochMillis) {
    // No-op - Recon doesn't send commands
  }

  /**
   * No-op since Recon doesn't complete operations.
   * @return false - operation not tracked
   */
  @Override
  public boolean completeAddReplica(ContainerID containerID, DatanodeDetails target,
      int replicaIndex) {
    // No-op - Recon doesn't track command completion
    return false;
  }

  /**
   * No-op since Recon doesn't complete operations.
   * @return false - operation not tracked
   */
  @Override
  public boolean completeDeleteReplica(ContainerID containerID, DatanodeDetails target,
      int replicaIndex) {
    // No-op - Recon doesn't track command completion
    return false;
  }

  /**
   * Always returns 0 since Recon has no pending operations.
   */
  @Override
  public long getPendingOpCount(ContainerReplicaOp.PendingOpType opType) {
    return 0L;
  }

}
