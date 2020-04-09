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
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

/**
 * Immutable object that is created with a set of ContainerReplica objects and
 * the number of in flight replica add and deletes, the container replication
 * factor and the min count which must be available for maintenance. This
 * information can be used to determine if the container is over or under
 * replicated and also how many additional replicas need created or removed.
 */
public class ContainerReplicaCount {

  private int healthyCount = 0;
  private int decommissionCount = 0;
  private int maintenanceCount = 0;
  private int inFlightAdd = 0;
  private int inFlightDel = 0;
  private int repFactor;
  private int minHealthyForMaintenance;
  private ContainerInfo container;
  private Set<ContainerReplica> replica;

  public ContainerReplicaCount(ContainerInfo container,
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

  public int getDecommissionCount() {
    return decommissionCount;
  }

  public int getMaintenanceCount() {
    return maintenanceCount;
  }

  public int getReplicationFactor() {
    return repFactor;
  }

  public ContainerInfo getContainer() {
    return container;
  }

  public Set<ContainerReplica> getReplica() {
    return replica;
  }

  @Override
  public String toString() {
    return "Container State: " +container.getState()+
        " Replica Count: "+replica.size()+
        " Healthy Count: "+healthyCount+
        " Decommission Count: "+decommissionCount+
        " Maintenance Count: "+maintenanceCount+
        " inFlightAdd Count: "+inFlightAdd+
        " inFightDel Count: "+inFlightDel+
        " ReplicationFactor: "+repFactor+
        " minMaintenance Count: "+minHealthyForMaintenance;
  }

  /**
   * Calculates the the delta of replicas which need to be created or removed
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
  public boolean isSufficientlyReplicated() {
    return missingReplicas() + inFlightDel <= 0;
  }

  /**
   * Return true is the container is over replicated. Decommission and
   * maintenance containers are ignored for this check.
   * The check ignores inflight additions, as they may fail, but it does
   * consider inflight deletes, as they would reduce the over replication when
   * they complete.
   *
   * @return True if the container is over replicated, false otherwise.
   */
  public boolean isOverReplicated() {
    return missingReplicas() + inFlightDel < 0;
  }

  /**
   * Returns true if the container is healthy, meaning all replica which are not
   * in a decommission or maintenance state are in the same state as the
   * container and in QUASI_CLOSED or in CLOSED state.
   *
   * @return true if the container is healthy, false otherwise
   */
  public boolean isHealthy() {
    return (container.getState() == HddsProtos.LifeCycleState.CLOSED
        || container.getState() == HddsProtos.LifeCycleState.QUASI_CLOSED)
        && replica.stream()
        .filter(r -> r.getDatanodeDetails().getPersistedOpState() == IN_SERVICE)
        .allMatch(r -> ReplicationManager.compareState(
            container.getState(), r.getState()));
  }
}
