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


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

/**
 * Class to count the replicas in a quasi-closed stuck container.
 */
public class QuasiClosedStuckReplicaCount {

  private final Map<UUID, Set<ContainerReplica>> replicasByOrigin = new HashMap<>();
  private final Map<UUID, Set<ContainerReplica>> inServiceReplicasByOrigin = new HashMap<>();
  private final Map<UUID, Set<ContainerReplica>> maintenanceReplicasByOrigin = new HashMap<>();
  private boolean hasOutOfServiceReplicas = false;
  private int minHealthyForMaintenance;

  public QuasiClosedStuckReplicaCount(Set<ContainerReplica> replicas, int minHealthyForMaintenance) {
    this.minHealthyForMaintenance = minHealthyForMaintenance;
    for (ContainerReplica r : replicas) {
      replicasByOrigin.computeIfAbsent(r.getOriginDatanodeId(), k -> new HashSet<>()).add(r);
      HddsProtos.NodeOperationalState opState = r.getDatanodeDetails().getPersistedOpState();
      if (opState == HddsProtos.NodeOperationalState.IN_SERVICE) {
        inServiceReplicasByOrigin.computeIfAbsent(r.getOriginDatanodeId(), k -> new HashSet<>()).add(r);
      } else if (opState == HddsProtos.NodeOperationalState.IN_MAINTENANCE
          || opState == HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE) {
        maintenanceReplicasByOrigin.computeIfAbsent(r.getOriginDatanodeId(), k -> new HashSet<>()).add(r);
        hasOutOfServiceReplicas = true;
      } else {
        hasOutOfServiceReplicas = true;
      }
    }
  }

  public boolean hasOutOfServiceReplicas() {
    return hasOutOfServiceReplicas;
  }

  public boolean isUnderReplicated() {
    // If there is only a single origin, we expect 3 copies, otherwise we expect 2 copies of each origin
    if (replicasByOrigin.size() == 1) {
      UUID origin = replicasByOrigin.keySet().iterator().next();
      Set<ContainerReplica> inService = inServiceReplicasByOrigin.get(origin);
      if (inService == null) {
        return true;
      }
      if (inService.size() >= 3) {
        return false;
      }
      // If it is less than 3, we need to check for maintenance copies.
      Set<ContainerReplica> maintenance = maintenanceReplicasByOrigin.get(origin);
      int maintenanceCount = maintenance == null ? 0 : maintenance.size();

      return inService.size() < minHealthyForMaintenance || inService.size() + maintenanceCount < 3;
    }

    // If there are multiple origins, we expect 2 copies of each origin
    // For maintenance, we expect 1 copy of each origin and ignore the minHealthyForMaintenance parameter
    for (UUID origin : replicasByOrigin.keySet()) {
      Set<ContainerReplica> replicas = inServiceReplicasByOrigin.get(origin);
      if (replicas == null) {
        return true;
      }
      Set<ContainerReplica> maintenance = maintenanceReplicasByOrigin.get(origin);
      int maintenanceCount = maintenance == null ? 0 : maintenance.size();
      if (replicas.size() + maintenanceCount < 2) {
        return true;
      }
    }
    // If we have 2 copies of each origin, we are not under-replicated
    return false;
  }

  /**
   * Returns True is the container is over-replicated. This means that if we have a single origin, there are more than
   * 3 copies. If we have multiple origins, there are more than 2 copies of each origin.
   * The over replication check ignore maintenance replicas. The container may become over replicated when maintenance
   * ends.
   *
   * @return True if the container is over-replicated, otherwise false
   */
  public boolean isOverReplicated() {
    // If there is only a single origin, we expect 3 copies, otherwise we expect 2 copies of each origin
    if (replicasByOrigin.size() == 1) {
      UUID origin = replicasByOrigin.keySet().iterator().next();
      Set<ContainerReplica> inService = inServiceReplicasByOrigin.get(origin);
      return inService != null && inService.size() > 3;
    }

    // If there are multiple origins, we expect 2 copies of each origin
    for (UUID origin : replicasByOrigin.keySet()) {
      Set<ContainerReplica> replicas = inServiceReplicasByOrigin.get(origin);
      if (replicas != null && replicas.size() > 2) {
        return true;
      }
    }
    // If we have 2 copies or less of each origin, we are not over-replicated
    return false;
  }

}
