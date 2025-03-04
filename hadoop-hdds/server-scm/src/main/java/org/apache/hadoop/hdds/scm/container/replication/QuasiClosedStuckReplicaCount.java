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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
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
  private boolean hasHealthyReplicas = false;

  public QuasiClosedStuckReplicaCount(Set<ContainerReplica> replicas, int minHealthyForMaintenance) {
    this.minHealthyForMaintenance = minHealthyForMaintenance;
    for (ContainerReplica r : replicas) {
      if (r.getState() != StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.UNHEALTHY) {
        hasHealthyReplicas = true;
      }
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

  public boolean hasHealthyReplicas() {
    return hasHealthyReplicas;
  }

  public boolean isUnderReplicated() {
    return !getUnderReplicatedReplicas().isEmpty();
  }

  public List<UnderReplicatedOrigin> getUnderReplicatedReplicas() {
    List<UnderReplicatedOrigin> underReplicatedOrigins = new ArrayList<>();

    if (replicasByOrigin.size() == 1) {
      UUID origin = replicasByOrigin.keySet().iterator().next();
      Set<ContainerReplica> inService = inServiceReplicasByOrigin.get(origin);
      if (inService == null) {
        inService = Collections.emptySet();
      }
      Set<ContainerReplica> maintenance = maintenanceReplicasByOrigin.get(origin);
      int maintenanceCount = maintenance == null ? 0 : maintenance.size();

      if (maintenanceCount > 0) {
        if (inService.size() < minHealthyForMaintenance) {
          int additionalReplicas = minHealthyForMaintenance - inService.size();
          underReplicatedOrigins.add(new UnderReplicatedOrigin(replicasByOrigin.get(origin), additionalReplicas));
        }
      } else {
        if (inService.size() < 3) {
          int additionalReplicas = 3 - inService.size();
          underReplicatedOrigins.add(new UnderReplicatedOrigin(replicasByOrigin.get(origin), additionalReplicas));
        }
      }
      return underReplicatedOrigins;
    }

    // If there are multiple origins, we expect 2 copies of each origin
    // For maintenance, we expect 1 copy of each origin and ignore the minHealthyForMaintenance parameter
    for (UUID origin : replicasByOrigin.keySet()) {
      Set<ContainerReplica> inService = inServiceReplicasByOrigin.get(origin);
      if (inService == null) {
        inService = Collections.emptySet();
      }
      Set<ContainerReplica> maintenance = maintenanceReplicasByOrigin.get(origin);
      int maintenanceCount = maintenance == null ? 0 : maintenance.size();

      if (inService.size() < 2) {
        if (maintenanceCount > 0) {
          if (inService.isEmpty()) {
            // We need 1 copy online for maintenance
            underReplicatedOrigins.add(new UnderReplicatedOrigin(replicasByOrigin.get(origin), 1));
          }
        } else {
          underReplicatedOrigins.add(new UnderReplicatedOrigin(replicasByOrigin.get(origin), 2 - inService.size()));
        }
      }
    }
    return underReplicatedOrigins;
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

  /**
   * Class to represent the origin of under replicated replicas and the number of additional replicas required.
   */
  public static class UnderReplicatedOrigin {

    private final Set<ContainerReplica> sources;
    private final int additionalRequired;

    public UnderReplicatedOrigin(Set<ContainerReplica> sources, int additionalRequired) {
      this.sources = sources;
      this.additionalRequired = additionalRequired;
    }

    public Set<ContainerReplica> getSources() {
      return sources;
    }

    public int getAdditionalRequired() {
      return additionalRequired;
    }

  }

}
