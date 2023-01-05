/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;

/**
 * Immutable object that is created with a set of ContainerReplica objects and
 * the number of in flight replica add and deletes, the container replication
 * factor and the min count which must be available for maintenance. This
 * information can be used to determine if the container is over or under
 * replicated and also how many additional replicas need created or removed.
 */
public class RatisContainerReplicaCount implements ContainerReplicaCount {

  private int healthyCount;
  private int decommissionCount;
  private int maintenanceCount;
  private final int inFlightAdd;
  private final int inFlightDel;
  private final int repFactor;
  private final int minHealthyForMaintenance;
  private final ContainerInfo container;
  private final Set<ContainerReplica> replica;

  public RatisContainerReplicaCount(ContainerInfo container,
                               Set<ContainerReplica> replica, int inFlightAdd,
                               int inFlightDelete, int replicationFactor,
                               int minHealthyForMaintenance) {
    this.healthyCount = 0;
    this.decommissionCount = 0;
    this.maintenanceCount = 0;
    this.inFlightAdd = inFlightAdd;
    this.inFlightDel = inFlightDelete;
    this.repFactor = replicationFactor;
    this.replica = replica;
    this.minHealthyForMaintenance
        = Math.min(this.repFactor, minHealthyForMaintenance);
    this.container = container;

    for (ContainerReplica cr : this.replica) {
      HddsProtos.NodeOperationalState state =
          cr.getDatanodeDetails().getPersistedOpState();
      if (state == DECOMMISSIONED || state == DECOMMISSIONING) {
        decommissionCount++;
      } else if (state == IN_MAINTENANCE || state == ENTERING_MAINTENANCE) {
        maintenanceCount++;
      } else {
        healthyCount++;
      }
    }
  }

  public int getHealthyCount() {
    return healthyCount;
  }

  @Override
  public int getDecommissionCount() {
    return decommissionCount;
  }

  @Override
  public int getMaintenanceCount() {
    return maintenanceCount;
  }

  public int getReplicationFactor() {
    return repFactor;
  }

  @Override
  public ContainerInfo getContainer() {
    return container;
  }

  @Override
  public Set<ContainerReplica> getReplicas() {
    return replica;
  }

  @Override
  public String toString() {
    return "Container State: " + container.getState() +
        " Replica Count: " + replica.size() +
        " Healthy Count: " + healthyCount +
        " Decommission Count: " + decommissionCount +
        " Maintenance Count: " + maintenanceCount +
        " inFlightAdd Count: " + inFlightAdd +
        " inFightDel Count: " + inFlightDel +
        " ReplicationFactor: " + repFactor +
        " minMaintenance Count: " + minHealthyForMaintenance;
  }

  /**
   * Calculates the delta of replicas which need to be created or removed
   * to ensure the container is correctly replicated when considered inflight
   * adds and deletes.
   *
   * When considering inflight operations, it is assumed any operation will
   * fail. However, to consider the worst case and avoid data loss, we always
   * assume a delete will succeed and and add will fail. In this way, we will
   * avoid scheduling too many deletes which could result in dataloss.
   *
   * Decisions around over-replication are made only on healthy replicas,
   * ignoring any in maintenance and also any inflight adds. InFlight adds are
   * ignored, as they may not complete, so if we have:
   *
   *     H, H, H, IN_FLIGHT_ADD
   *
   * And then schedule a delete, we could end up under-replicated (add fails,
   * delete completes). It is better to let the inflight operations complete
   * and then deal with any further over or under replication.
   *
   * For maintenance replicas, assuming replication factor 3, and minHealthy
   * 2, it is possible for all 3 hosts to be put into maintenance, leaving the
   * following (H = healthy, M = maintenance):
   *
   *     H, H, M, M, M
   *
   * Even though we are tracking 5 replicas, this is not over replicated as we
   * ignore the maintenance copies. Later, the replicas could look like:
   *
   *     H, H, H, H, M
   *
   * At this stage, the container is over replicated by 1, so one replica can be
   * removed.
   *
   * For containers which have replication factor healthy replica, we ignore any
   * inflight add or deletes, as they may fail. Instead, wait for them to
   * complete and then deal with any excess or deficit.
   *
   * For under replicated containers we do consider inflight add and delete to
   * avoid scheduling more adds than needed. There is additional logic around
   * containers with maintenance replica to ensure minHealthyForMaintenance
   * replia are maintained.
   *
   * @return Delta of replicas needed. Negative indicates over replication and
   *         containers should be removed. Positive indicates over replication
   *         and zero indicates the containers has replicationFactor healthy
   *         replica
   */
  public int additionalReplicaNeeded() {
    int delta = missingReplicas();

    if (delta < 0) {
      // Over replicated, so may need to remove a container. Do not consider
      // inFlightAdds, as they may fail, but do consider inFlightDel which
      // will reduce the over-replication if it completes.
      // Note this could make the delta positive if there are too many in flight
      // deletes, which will result in an additional being scheduled.
      return delta + inFlightDel;
    } else {
      // May be under or perfectly replicated.
      // We must consider in flight add and delete when calculating the new
      // containers needed, but we bound the lower limit at zero to allow
      // inflight operations to complete before handling any potential over
      // replication
      return Math.max(0, delta - inFlightAdd + inFlightDel);
    }
  }

  /**
   * Returns the count of replicas which need to be created or removed to
   * ensure the container is perfectly replicate. Inflight operations are not
   * considered here, but the logic to determine the missing or excess counts
   * for maintenance is present.
   *
   * Decisions around over-replication are made only on healthy replicas,
   * ignoring any in maintenance. For example, if we have:
   *
   *     H, H, H, M, M
   *
   * This will not be consider over replicated until one of the Maintenance
   * replicas moves to Healthy.
   *
   * If the container is perfectly replicated, zero will be return.
   *
   * If it is under replicated a positive value will be returned, indicating
   * how many replicas must be added.
   *
   * If it is over replicated a negative value will be returned, indicating now
   * many replicas to remove.
   *
   * @return Zero if the container is perfectly replicated, a positive value
   *         for under replicated and a negative value for over replicated.
   */
  private int missingReplicas() {
    int delta = repFactor - healthyCount;

    if (delta < 0) {
      // Over replicated, so may need to remove a container.
      return delta;
    } else if (delta > 0) {
      // May be under-replicated, depending on maintenance.
      delta = Math.max(0, delta - maintenanceCount);
      int neededHealthy =
          Math.max(0, minHealthyForMaintenance - healthyCount);
      delta = Math.max(neededHealthy, delta);
      return delta;
    } else { // delta == 0
      // We have exactly the number of healthy replicas needed.
      return delta;
    }
  }

  /**
   * Return true if the container is sufficiently replicated. Decommissioning
   * and Decommissioned containers are ignored in this check, assuming they will
   * eventually be removed from the cluster.
   * This check ignores inflight additions, as those replicas have not yet been
   * created and the create could fail for some reason.
   * The check does consider inflight deletes as there may be 3 healthy replicas
   * now, but once the delete completes it will reduce to 2.
   * We also assume a replica in Maintenance state cannot be removed, so the
   * pending delete would affect only the healthy replica count.
   *
   * @return True if the container is sufficiently replicated and False
   *         otherwise.
   */
  @Override
  public boolean isSufficientlyReplicated() {
    return isSufficientlyReplicated(false);
  }

  /**
   * For Ratis, this method is the same as isSufficientlyReplicated.
   * @param datanode Not used in this implementation
   * @return True if the container is sufficiently replicated and False
   *         otherwise.
   */
  @Override
  public boolean isSufficientlyReplicatedForOffline(DatanodeDetails datanode) {
    return isSufficientlyReplicated();
  }

  /**
   * Return true if the container is sufficiently replicated. Decommissioning
   * and Decommissioned containers are ignored in this check, assuming they will
   * eventually be removed from the cluster.
   * This check ignores inflight additions, if includePendingAdd is false,
   * otherwise it will assume they complete ok.
   *
   * @return True if the container is sufficiently replicated and False
   *         otherwise.
   */
  public boolean isSufficientlyReplicated(boolean includePendingAdd) {
    // Positive for under-rep, negative for over-rep
    int delta = redundancyDelta(true, includePendingAdd);
    return delta <= 0;
  }

  /**
   * Return true if the container is over replicated. Decommission and
   * maintenance containers are ignored for this check.
   * The check ignores inflight additions, as they may fail, but it does
   * consider inflight deletes, as they would reduce the over replication when
   * they complete.
   *
   * @return True if the container is over replicated, false otherwise.
   */
  @Override
  public boolean isOverReplicated() {
    return isOverReplicated(true);
  }

  /**
   * Return true if the container is over replicated. Decommission and
   * maintenance containers are ignored for this check.
   * The check ignores inflight additions, as they may fail, but it does
   * consider inflight deletes if includePendingDelete is true.
   *
   * @return True if the container is over replicated, false otherwise.
   */
  public boolean isOverReplicated(boolean includePendingDelete) {
    return getExcessRedundancy(includePendingDelete) > 0;
  }

  /**
   * @return Return Excess Redundancy replica nums.
   */
  public int getExcessRedundancy(boolean includePendingDelete) {
    int excessRedundancy = redundancyDelta(includePendingDelete, false);
    if (excessRedundancy >= 0) {
      // either perfectly replicated or under replicated
      return 0;
    }
    return -excessRedundancy;
  }

  /**
   * Return the delta from the expected number of replicas, optionally
   * considering inflight add and deletes.
   * @param includePendingDelete
   * @param includePendingAdd
   * @return zero if perfectly replicated, a negative value for over replication
   *         and a positive value for under replication. The magnitude of the
   *         return value indicates how many replias the container is over or
   *         under replicated by.
   */
  private int redundancyDelta(boolean includePendingDelete,
      boolean includePendingAdd) {
    int excessRedundancy = missingReplicas();
    if (includePendingDelete) {
      excessRedundancy += inFlightDel;
    }
    if (includePendingAdd) {
      excessRedundancy -= inFlightAdd;
    }
    return excessRedundancy;
  }

  /**
   * How many more replicas can be lost before the container is
   * unreadable, assuming any infligh deletes will complete. For containers
   * which are under-replicated due to decommission
   * or maintenance only, the remaining redundancy will include those
   * decommissioning or maintenance replicas, as they are technically still
   * available until the datanode processes are stopped.
   * @return Count of remaining redundant replicas.
   */
  public int getRemainingRedundancy() {
    return Math.max(0,
        healthyCount + decommissionCount + maintenanceCount - inFlightDel - 1);
  }

  /**
   * Returns true is there are no replicas of the container available, ie the
   * set of container replicas has zero entries.
   *
   * @return true if there are no replicas, false otherwise.
   */
  @Override
  public boolean isUnrecoverable() {
    return getReplicas().isEmpty();
  }
}
