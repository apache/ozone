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

package org.apache.hadoop.hdds.scm.container.replication;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

/**
 * Class used to represent the Health States of containers.
 */
public class ContainerHealthResult {

  private final ContainerInfo containerInfo;
  private final HealthState healthState;
  private final List<SCMCommand<?>> commands = new ArrayList<>();

  public ContainerHealthResult(ContainerInfo containerInfo,
      HealthState healthState) {
    this.containerInfo = containerInfo;
    this.healthState = healthState;
  }

  public HealthState getHealthState() {
    return healthState;
  }

  public void addCommand(SCMCommand<?> command) {
    commands.add(command);
  }

  public List<SCMCommand<?>> getCommands() {
    return commands;
  }

  public ContainerInfo getContainerInfo() {
    return containerInfo;
  }

  /**
   * Class for Healthy Container Check results.
   */
  public static class HealthyResult extends ContainerHealthResult {

    public HealthyResult(ContainerInfo containerInfo) {
      super(containerInfo, HealthState.HEALTHY);
    }
  }

  /**
   * Class for Unhealthy container check results, where the container has some
   * issue other than over- or under-replication.
   */
  public static class UnHealthyResult extends ContainerHealthResult {

    public UnHealthyResult(ContainerInfo containerInfo) {
      super(containerInfo, HealthState.UNHEALTHY);
    }
  }

  /**
   * Class for UnderReplicated Container Check Results.
   */
  public static class UnderReplicatedHealthResult
      extends ContainerHealthResult {

    // For under-replicated containers, the best remaining redundancy we can
    // have is 3 for EC-10-4, 2 for EC-6-3, 1 for EC-3-2 and 2 for Ratis.
    // A container which is under-replicated due to decommission or maintenance
    // will have one more, ie 4, 3, 2, 3 respectively. Ideally we want to sort
    // such under-replication after all other under-replicated containers.
    // It may also make sense to allow under-replicated containers a chance to
    // retry once before processing the out-of-service under-replication.
    // Therefore, we should adjust the weighted remaining redundancy of
    // containers under-replicated only due to out-of-service to 5, so they
    // sort after an under-replicated container with 3 remaining replicas
    // (EC-10-4) and plus one retry.
    private static final int OUT_OF_SERVICE_REDUNDANCY = 5;

    private final int remainingRedundancy;
    private final boolean dueToOutOfService;
    private final boolean sufficientlyReplicatedAfterPending;
    private final boolean unrecoverable;
    private boolean isMissing = false;
    private boolean hasHealthyReplicas;
    private boolean hasUnReplicatedOfflineIndexes = false;
    private boolean offlineIndexesOkAfterPending = false;
    private int requeueCount = 0;
    private boolean hasVulnerableUnhealthy = false;

    public UnderReplicatedHealthResult(ContainerInfo containerInfo,
        int remainingRedundancy, boolean dueToOutOfService,
        boolean replicatedOkWithPending, boolean unrecoverable) {
      this(containerInfo, remainingRedundancy, dueToOutOfService,
          replicatedOkWithPending, unrecoverable, HealthState.UNDER_REPLICATED);
    }

    protected UnderReplicatedHealthResult(ContainerInfo containerInfo,
        int remainingRedundancy, boolean dueToOutOfService,
        boolean replicatedOkWithPending, boolean unrecoverable,
        HealthState healthState) {
      super(containerInfo, healthState);
      this.remainingRedundancy = remainingRedundancy;
      this.dueToOutOfService = dueToOutOfService;
      this.sufficientlyReplicatedAfterPending = replicatedOkWithPending;
      this.unrecoverable = unrecoverable;
    }

    /**
     * How many more replicas can be lost before the container is
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
     * The weightedRedundancy, is the remaining redundancy + the requeue count.
     * When this value is used for ordering in a priority queue it ensures the
     * priority is reduced each time it is requeued, to prevent it from blocking
     * other containers from being processed.
     * Additionally, so that decommission and maintenance replicas are not
     * ordered ahead of under-replicated replicas, a redundancy of
     * DECOMMISSION_REDUNDANCY is used for the decommission redundancy rather
     * than its real redundancy.
     * @return The weightedRedundancy of this result.
     */
    public int getWeightedRedundancy() {
      int result = requeueCount;
      if (dueToOutOfService) {
        result += OUT_OF_SERVICE_REDUNDANCY;
      } else {
        result += getRemainingRedundancy();
      }
      return result;
    }

    /**
     * If there is an attempt to process this under-replicated result, and it
     * fails and has to be requeued, this method should be called to increment
     * the requeue count to ensure the result is not placed back at the head
     * of the queue.
     */
    public void incrementRequeueCount() {
      ++requeueCount;
    }

    public int getRequeueCount() {
      return requeueCount;
    }

    /**
     * Indicates whether the under-replication is caused only by replicas
     * being decommissioned or entering maintenance. Ie, there are not replicas
     * unavailable.
     * @return True is the under-replication is caused by decommission.
     */
    public boolean underReplicatedDueToOutOfService() {
      return dueToOutOfService;
    }

    /**
     * Considering the pending replicas, which have been scheduled for copy or
     * reconstruction, will the container still be under-replicated when they
     * complete.
     * @return True if the under-replication is corrected by the pending
     *         replicas. False otherwise.
     */
    public boolean isReplicatedOkAfterPending() {
      return sufficientlyReplicatedAfterPending;
    }

    /**
     * Indicates whether a container has enough replicas to be read. For Ratis
     * at least one healthy replia must be available. For EC, at least
     * dataNum healthy replicas are needed.
     * @return True if the container has insufficient replicas available to be
     *         read, false otherwise
     */
    public boolean isUnrecoverable() {
      return unrecoverable;
    }

    /**
     * Pass true if a container has some indexes which are only on nodes
     * which are DECOMMISSIONING or ENTERING_MAINTENANCE. These containers may
     * need to be processed even if they are unrecoverable.
     * @param val pass true if the container has indexes on nodes going offline
     *            or false otherwise.
     */
    public void setHasUnReplicatedOfflineIndexes(boolean val) {
      hasUnReplicatedOfflineIndexes = val;
    }

    /**
     * Indicates whether a container has some indexes which are only on nodes
     * which are DECOMMISSIONING or ENTERING_MAINTENANCE. These containers may
     * need to be processed even if they are unrecoverable.
     * @return True if the container has some decommission or maintenance only
     *         indexes.
     */
    public boolean hasUnreplicatedOfflineIndexes() {
      return hasUnReplicatedOfflineIndexes;
    }

    /**
     * Pass true if a container has some indexes which are only on nodes which
     * are DECOMMISSIONING or ENTERING_MAINTENANCE, but the container has a
     * pending add to correct the under replication caused by decommission, but
     * it has not completed yet.
     * @param val Pass True if the container has a pending add to correct the
     *            under replication caused by decommission. False otherwise.
     */
    public void setOfflineIndexesOkAfterPending(boolean val) {
      offlineIndexesOkAfterPending = val;
    }

    /**
     * Returns true if a container has under-replication caused by offline
     * indexes, but it is corrected by a pending add.
     */
    public boolean offlineIndexesOkAfterPending() {
      return offlineIndexesOkAfterPending;
    }

    public boolean hasHealthyReplicas() {
      return hasHealthyReplicas;
    }

    public void setHasHealthyReplicas(boolean hasHealthyReplicas) {
      this.hasHealthyReplicas = hasHealthyReplicas;
    }

    public void setIsMissing(boolean isMissing) {
      this.isMissing = isMissing;
    }

    public boolean isMissing() {
      return isMissing;
    }

    public void setHasVulnerableUnhealthy(boolean hasVulnerableUnhealthy) {
      this.hasVulnerableUnhealthy = hasVulnerableUnhealthy;
    }

    public boolean hasVulnerableUnhealthy() {
      return hasVulnerableUnhealthy;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("UnderReplicatedHealthResult{")
          .append("containerId=").append(getContainerInfo().getContainerID())
          .append(", state=").append(getHealthState())
          .append(", remainingRedundancy=").append(remainingRedundancy);
      if (dueToOutOfService) {
        sb.append(" +dueToOutOfService");
      }
      if (sufficientlyReplicatedAfterPending) {
        sb.append(" +sufficientlyReplicatedAfterPending");
      }
      if (unrecoverable) {
        sb.append(" +unrecoverable");
      }
      if (isMissing) {
        sb.append(" +missing");
      }
      if (hasHealthyReplicas) {
        sb.append(" +hasHealthyReplicas");
      }
      if (hasUnReplicatedOfflineIndexes) {
        sb.append(" +hasUnReplicatedOfflineIndexes");
      }
      if (requeueCount > 0) {
        sb.append(" requeued:").append(requeueCount);
      }
      return sb.append('}').toString();
    }
  }

  /**
   * Class to represent a container healthy state which is mis-Replicated. This
   * means the container is neither over- nor under-replicated, but its replicas
   * don't meet the requirements of the container placement policy. Eg the
   * containers are not spread across enough racks.
   */
  public static class MisReplicatedHealthResult
      extends UnderReplicatedHealthResult {

    /**
     * In UnderReplicatedHealthState, DECOMMISSION_REDUNDANCY is defined as
     * 5 so that containers which are really under-replicated get fixed as a
     * priority over decommissioning hosts. We have defined that a container
     * can only be mis-replicated if it is not over- or under-replicated. Fixing
     * mis-replication is arguably less important than completing decommission.
     * To avoid a lot of mis-replicated containers blocking decommission, we
     * set the redundancy of mis-replicated containers to 6, so they sort after
     * under/over-replicated and decommissioning replicas in the under
     * replication queue.
     */
    private static final int MIS_REP_REDUNDANCY = 6;
    private final String misReplicatedReason;

    public MisReplicatedHealthResult(ContainerInfo containerInfo,
        boolean replicatedOkAfterPending, String misReplicatedReason) {
      super(containerInfo, MIS_REP_REDUNDANCY, false,
          replicatedOkAfterPending, false,
          HealthState.MIS_REPLICATED);
      this.misReplicatedReason = misReplicatedReason;
    }

    public String getMisReplicatedReason() {
      return misReplicatedReason;
    }
  }

  /**
   * Class for Over Replicated Container Health Results.
   */
  public static class OverReplicatedHealthResult extends ContainerHealthResult {

    private final int excessRedundancy;
    private final boolean sufficientlyReplicatedAfterPending;
    private boolean hasMismatchedReplicas;
    private boolean isSafelyOverReplicated;

    public OverReplicatedHealthResult(ContainerInfo containerInfo,
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
    public boolean isReplicatedOkAfterPending() {
      return sufficientlyReplicatedAfterPending;
    }

    public boolean hasMismatchedReplicas() {
      return hasMismatchedReplicas;
    }

    public void setHasMismatchedReplicas(boolean hasMismatchedReplicas) {
      this.hasMismatchedReplicas = hasMismatchedReplicas;
    }

    public boolean isSafelyOverReplicated() {
      return isSafelyOverReplicated;
    }

    public void setIsSafelyOverReplicated(boolean isSafelyOverReplicated) {
      this.isSafelyOverReplicated = isSafelyOverReplicated;
    }
  }

  /**
   * All possible container health states.
   */
  public enum HealthState {
    HEALTHY,
    UNHEALTHY,
    UNDER_REPLICATED,
    OVER_REPLICATED,
    MIS_REPLICATED
  }
}
