/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import java.util.ArrayList;
import java.util.List;

/**
 * Class used to represent the Health States of containers.
 */
public class ContainerHealthResult {

  /**
   * All possible container health states.
   */
  public enum HealthState {
    HEALTHY,
    UNDER_REPLICATED,
    OVER_REPLICATED
  }

  private final ContainerInfo containerInfo;
  private final HealthState healthState;
  private final List<SCMCommand> commands = new ArrayList<>();

  ContainerHealthResult(ContainerInfo containerInfo, HealthState healthState) {
    this.containerInfo = containerInfo;
    this.healthState = healthState;
  }

  public HealthState getHealthState() {
    return healthState;
  }

  public void addCommand(SCMCommand command) {
    commands.add(command);
  }

  public List<SCMCommand> getCommands() {
    return commands;
  }

  public ContainerInfo getContainerInfo() {
    return containerInfo;
  }

  /**
   * Class for Healthy Container Check results.
   */
  public static class HealthyResult extends ContainerHealthResult {

    HealthyResult(ContainerInfo containerInfo) {
      super(containerInfo, HealthState.HEALTHY);
    }
  }

  /**
   * Class for UnderReplicated Container Check Results.
   */
  public static class UnderReplicatedHealthResult
      extends ContainerHealthResult {

    private final int remainingRedundancy;
    private final boolean dueToDecommission;
    private final boolean sufficientlyReplicatedAfterPending;
    private final boolean unrecoverable;

    UnderReplicatedHealthResult(ContainerInfo containerInfo,
        int remainingRedundancy, boolean dueToDecommission,
        boolean replicatedOkWithPending, boolean unrecoverable) {
      super(containerInfo, HealthState.UNDER_REPLICATED);
      this.remainingRedundancy = remainingRedundancy;
      this.dueToDecommission = dueToDecommission;
      this.sufficientlyReplicatedAfterPending = replicatedOkWithPending;
      this.unrecoverable = unrecoverable;
    }

    /**
     * How many more replicas can be lost before the the container is
     * unreadable. For containers which are under-replicated due to decommission
     * or maintenance only, the remaining redundancy will include those
     * decommissioning or maintenance replicas, as they are technically still
     * available until the datanode processes are stopped.
     * @return Count of remaining redundant replicas.
     */
    public int getRemainingRedundancy() {
      return remainingRedundancy;
    }

    /**
     * Indicates whether the under-replication is caused only by replicas
     * being decommissioned or entering maintenance. Ie, there are not replicas
     * unavailable.
     * @return True is the under-replication is caused by decommission.
     */
    public boolean underReplicatedDueToDecommission() {
      return dueToDecommission;
    }

    /**
     * Considering the pending replicas, which have been scheduled for copy or
     * reconstruction, will the container still be under-replicated when they
     * complete.
     * @return True if the under-replication is corrected by the pending
     *         replicas. False otherwise.
     */
    public boolean isSufficientlyReplicatedAfterPending() {
      return sufficientlyReplicatedAfterPending;
    }

    /**
     * Indicates whether a container has enough replicas to be read. For Ratis
     * at least one replia must be available. For EC, at least dataNum replicas
     * are needed.
     * @return True if the container has insufficient replicas available to be
     *         read, false otherwise
     */
    public boolean isUnrecoverable() {
      return unrecoverable;
    }
  }

  /**
   * Class for Over Replicated Container Health Results.
   */
  public static class OverReplicatedHealthResult extends ContainerHealthResult {

    private final int excessRedundancy;
    private final boolean sufficientlyReplicatedAfterPending;


    OverReplicatedHealthResult(ContainerInfo containerInfo,
        int excessRedundancy, boolean replicatedOkWithPending) {
      super(containerInfo, HealthState.OVER_REPLICATED);
      this.excessRedundancy = excessRedundancy;
      this.sufficientlyReplicatedAfterPending = replicatedOkWithPending;
    }

    /**
     * How many extra replicas are present for this container and need to be
     * removed. For EC, this number indicates how many indexes have more than
     * one copy. Eg index 1 could have 3 copies. Index 2 could have 2 copies.
     * The rest have 1 copy. The value returned here will be 2, indicating 2
     * indexes have excess redundancy, even though we have to remove 3 replicas.
     * For Ratis containers, the number return is simply the current replica
     * count minus the expected replica count.
     * @return The number of excess replicas.
     */
    public int getExcessRedundancy() {
      return excessRedundancy;
    }

    /**
     * Considering the pending replicas, which have been scheduled for delete,
     * will the container still be over-replicated when they complete.
     * @return True if the over-replication is corrected by the pending
     *         deletes. False otherwise.
     */
    public boolean isSufficientlyReplicatedAfterPending() {
      return sufficientlyReplicatedAfterPending;
    }
  }
}
