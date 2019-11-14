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

import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import java.util.Set;

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
  private Set<ContainerReplica> replica;

  public ContainerReplicaCount(Set<ContainerReplica> replica, int inFlightAdd,
                               int inFlightDelete, int replicationFactor,
                               int minHealthyForMaintenance) {
    this.healthyCount = 0;
    this.decommissionCount = 0;
    this.maintenanceCount = 0;
    this.inFlightAdd = inFlightAdd;
    this.inFlightDel = inFlightDelete;
    this.repFactor = replicationFactor;
    this.minHealthyForMaintenance = minHealthyForMaintenance;
    this.replica = replica;

    for (ContainerReplica cr : this.replica) {
      ContainerReplicaProto.State state = cr.getState();
      if (state == ContainerReplicaProto.State.DECOMMISSIONED) {
        decommissionCount++;
      } else if (state == ContainerReplicaProto.State.MAINTENANCE) {
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

  public Set<ContainerReplica> getReplica() {
    return replica;
  }

  @Override
  public String toString() {
    return "Replica Count: "+replica.size()+
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
   * to ensure the container is correctly replicated.
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
   * replia are maintained/
   *
   * @return Delta of replicas needed. Negative indicates over replication and
   *         containers should be removed. Positive indicates over replication
   *         and zero indicates the containers has replicationFactor healthy
   *         replica
   */
  public int additionalReplicaNeeded() {
    int delta = repFactor - healthyCount;

    if (delta < 0) {
      // Over replicated, so may need to remove a container. Do not consider
      // inFlightAdds, as they may fail, but do consider inFlightDel which
      // will reduce the over-replication if it completes.
      // Note this could make the delta positive if there are too many in flight
      // deletes, which will result in an additional being scheduled.
      return delta + inFlightDel;
    } else if (delta > 0) {
      // May be under-replicated, depending on maintenance. When a container is
      // under-replicated, we must consider in flight add and delete when
      // calculating the new containers needed.
      delta = Math.max(0, delta - maintenanceCount);
      // Check we have enough healthy replicas
      minHealthyForMaintenance = Math.min(repFactor, minHealthyForMaintenance);
      int neededHealthy =
          Math.max(0, minHealthyForMaintenance - healthyCount);
      delta = Math.max(neededHealthy, delta);
      return delta - inFlightAdd + inFlightDel;
    } else { // delta == 0
      // We have exactly the number of healthy replicas needed, but there may
      // be inflight add or delete. Some of these may fail, but we want to
      // avoid scheduling needless extra replicas. Therefore enforce a lower
      // bound of 0 on the delta, but include the in flight requests in the
      // calculation.
      return Math.max(0, delta + inFlightDel - inFlightAdd);
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
    return (healthyCount + maintenanceCount - inFlightDel) >= repFactor
        && healthyCount - inFlightDel
        >= Math.min(repFactor, minHealthyForMaintenance);
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
    return healthyCount - inFlightDel > repFactor;
  }
}