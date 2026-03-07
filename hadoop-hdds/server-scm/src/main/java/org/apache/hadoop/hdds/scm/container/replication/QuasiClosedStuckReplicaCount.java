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

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.UNHEALTHY;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

/**
 * Class to count the replicas in a quasi-closed stuck container.
 *
 * <p>Origins are ranked by their highest healthy BCSID (sequenceId). The origins with the
 * highest BCSID receive {@code bestOriginCopies} replicas, while all other origins receive
 * {@code otherOriginCopies}. If multiple origins share the same highest BCSID they are all treated
 * as "best". For a single-origin container, {@code bestOriginCopies} is always used.
 *
 */
public class QuasiClosedStuckReplicaCount {

  private final Map<DatanodeID, Set<ContainerReplica>> replicasByOrigin = new HashMap<>();
  private final Map<DatanodeID, Set<ContainerReplica>> inServiceReplicasByOrigin = new HashMap<>();
  private final Map<DatanodeID, Set<ContainerReplica>> maintenanceReplicasByOrigin = new HashMap<>();
  private final int minHealthyForMaintenance;
  private final boolean hasHealthyReplicas;
  private final boolean hasOutOfServiceReplicas;
  private final int bestOriginCopies;
  private final int otherOriginCopies;
  private final Set<DatanodeID> bestOrigins;

  public QuasiClosedStuckReplicaCount(Set<ContainerReplica> replicas, int minHealthyForMaintenance,
      int bestOriginCopies, int otherOriginCopies) {
    this.minHealthyForMaintenance = minHealthyForMaintenance;
    this.bestOriginCopies = bestOriginCopies;
    this.otherOriginCopies = otherOriginCopies;

    boolean hasHealthy = false;
    boolean hasOutOfService = false;
    for (ContainerReplica r : replicas) {
      if (r.getState() != UNHEALTHY) {
        hasHealthy = true;
      }
      replicasByOrigin.computeIfAbsent(r.getOriginDatanodeId(), k -> new HashSet<>()).add(r);
      HddsProtos.NodeOperationalState opState = r.getDatanodeDetails().getPersistedOpState();
      if (opState == HddsProtos.NodeOperationalState.IN_SERVICE) {
        inServiceReplicasByOrigin.computeIfAbsent(r.getOriginDatanodeId(), k -> new HashSet<>()).add(r);
      } else if (opState == HddsProtos.NodeOperationalState.IN_MAINTENANCE
          || opState == HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE) {
        maintenanceReplicasByOrigin.computeIfAbsent(r.getOriginDatanodeId(), k -> new HashSet<>()).add(r);
        hasOutOfService = true;
      } else {
        hasOutOfService = true;
      }
    }

    this.hasHealthyReplicas = hasHealthy;
    this.hasOutOfServiceReplicas = hasOutOfService;
    this.bestOrigins = computeBestOrigins();
  }

  /**
   * Identifies the best origins whose maximum BCSID among healthy replicas equals the cluster-wide maximum healthy
   * BCSID. Origins with only UNHEALTHY replicas are excluded. If all replicas are UNHEALTHY the returned set is empty.
   */
  private Set<DatanodeID> computeBestOrigins() {
    long maxBcsid = Long.MIN_VALUE;

    for (Map.Entry<DatanodeID, Set<ContainerReplica>> entry : replicasByOrigin.entrySet()) {
      for (ContainerReplica r : entry.getValue()) {
        if (r.getState() != UNHEALTHY && r.getSequenceId() != null && r.getSequenceId() > maxBcsid) {
          maxBcsid = r.getSequenceId();
        }
      }
    }

    if (maxBcsid == Long.MIN_VALUE) {
      return Collections.emptySet();
    }

    final long highestBcsid = maxBcsid;
    Set<DatanodeID> best = new HashSet<>();
    for (Map.Entry<DatanodeID, Set<ContainerReplica>> entry : replicasByOrigin.entrySet()) {
      boolean hasBestBcsid = entry.getValue().stream()
          .anyMatch(r -> r.getState() != UNHEALTHY && r.getSequenceId() != null
              && r.getSequenceId() == highestBcsid);
      if (hasBestBcsid) {
        best.add(entry.getKey());
      }
    }
    return best;
  }

  public int availableOrigins() {
    return replicasByOrigin.size();
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

  private Set<ContainerReplica> getInService(DatanodeID origin) {
    final Set<ContainerReplica> set = inServiceReplicasByOrigin.get(origin);
    return set == null ? Collections.emptySet() : set;
  }

  private int getMaintenanceCount(DatanodeID origin) {
    final Set<ContainerReplica> maintenance = maintenanceReplicasByOrigin.get(origin);
    return maintenance == null ? 0 : maintenance.size();
  }

  private int targetCopiesForOrigin(DatanodeID origin) {
    return bestOrigins.contains(origin) ? bestOriginCopies : otherOriginCopies;
  }

  public List<MisReplicatedOrigin> getUnderReplicatedReplicas() {
    List<MisReplicatedOrigin> misReplicatedOrigins = new ArrayList<>();

    if (replicasByOrigin.size() == 1) {
      final Map.Entry<DatanodeID, Set<ContainerReplica>> entry = replicasByOrigin.entrySet().iterator().next();
      final Set<ContainerReplica> inService = getInService(entry.getKey());
      final int maintenanceCount = getMaintenanceCount(entry.getKey());

      if (maintenanceCount > 0) {
        if (inService.size() < minHealthyForMaintenance) {
          int additionalReplicas = minHealthyForMaintenance - inService.size();
          misReplicatedOrigins.add(new MisReplicatedOrigin(entry.getValue(), additionalReplicas));
        }
      } else {
        if (inService.size() < bestOriginCopies) {
          int additionalReplicas = bestOriginCopies - inService.size();
          misReplicatedOrigins.add(new MisReplicatedOrigin(entry.getValue(), additionalReplicas));
        }
      }
      return misReplicatedOrigins;
    }

    // Multiple origins: the best origins target bestOriginCopies; all others target otherOriginCopies.
    // For maintenance, we expect 1 copy of each origin online regardless of best/other designation.
    for (Map.Entry<DatanodeID, Set<ContainerReplica>> entry : replicasByOrigin.entrySet()) {
      final Set<ContainerReplica> inService = getInService(entry.getKey());
      final int maintenanceCount = getMaintenanceCount(entry.getKey());
      final int target = targetCopiesForOrigin(entry.getKey());

      if (inService.size() < target) {
        if (maintenanceCount > 0) {
          if (inService.isEmpty()) {
            // We need 1 copy online for maintenance
            misReplicatedOrigins.add(new MisReplicatedOrigin(entry.getValue(), 1));
          }
        } else {
          misReplicatedOrigins.add(new MisReplicatedOrigin(entry.getValue(), target - inService.size()));
        }
      }
    }
    return misReplicatedOrigins;
  }

  /**
   * Returns True is the container is over-replicated. This means that if we have a single origin, there are more than
   * bestOrigin copies. If we have multiple origins, there are more than target copies of each origin.
   * The over replication check ignore maintenance replicas. The container may become over replicated when maintenance
   * ends.
   *
   * @return True if the container is over-replicated, otherwise false
   */
  public boolean isOverReplicated() {
    return !getOverReplicatedOrigins().isEmpty();
  }

  public List<MisReplicatedOrigin> getOverReplicatedOrigins() {
    // If there is only a single origin, we expect bestOriginCopies copies.
    if (replicasByOrigin.size() == 1) {
      final DatanodeID origin = replicasByOrigin.keySet().iterator().next();
      final Set<ContainerReplica> inService = getInService(origin);
      if (inService.size() > bestOriginCopies) {
        return Collections.singletonList(new MisReplicatedOrigin(inService, inService.size() - bestOriginCopies));
      }
      return Collections.emptyList();
    }

    List<MisReplicatedOrigin> overReplicatedOrigins = new ArrayList<>();
    for (DatanodeID origin : replicasByOrigin.keySet()) {
      final Set<ContainerReplica> replicas = getInService(origin);
      final int target = targetCopiesForOrigin(origin);
      if (replicas.size() > target) {
        overReplicatedOrigins.add(new MisReplicatedOrigin(replicas, replicas.size() - target));
      }
    }
    return overReplicatedOrigins;
  }

  /**
   * Class to represent the origin of under replicated replicas and the number of additional replicas required.
   */
  public static class MisReplicatedOrigin {

    private final Set<ContainerReplica> sources;
    private final int replicaDelta;

    public MisReplicatedOrigin(Set<ContainerReplica> sources, int replicaDelta) {
      this.sources = sources;
      this.replicaDelta = replicaDelta;
    }

    public Set<ContainerReplica> getSources() {
      return sources;
    }

    public int getReplicaDelta() {
      return replicaDelta;
    }
  }

}
