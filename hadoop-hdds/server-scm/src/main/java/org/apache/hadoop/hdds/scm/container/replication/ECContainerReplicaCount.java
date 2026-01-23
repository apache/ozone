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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.node.NodeManager;

/**
 * This class provides a set of methods to test for over / under replication of
 * EC containers, taking into account decommission / maintenance nodes,
 * pending replications, pending deletes and the existing replicas.
 *
 * The intention for this class, is to wrap the logic used to detect over and
 * under replication to allow other areas to easily check the status of a
 * container.
 *
 * For calculating under replication:
 *
 *   * Assume that decommission replicas are already lost, as they
 *     will eventually go away.
 *   * Any pending deletes are treated as if they have deleted
 *   * Pending adds are ignored as they may fail to create.
 *   * Unhealthy replicas contribute to under replication - an index with
 *   only unhealthy and no closed replicas is considered under replicated
 *
 * Similar for over replication:
 *
 *   * Assume decommissioned replicas are already lost.
 *   * Pending delete replicas will complete
 *   * Pending adds are ignored as they may not complete.
 *   * Maintenance copies are not considered until they are back to IN_SERVICE
 *   * Having unhealthy replicas is not considered over replication
 */

public class ECContainerReplicaCount implements ContainerReplicaCount {

  private final ContainerInfo containerInfo;
  private final ECReplicationConfig repConfig;
  private final List<Integer> pendingAdd;
  private final List<Integer> pendingDelete;
  private final int remainingMaintenanceRedundancy;
  private final Map<Integer, Integer> healthyIndexes = new HashMap<>();
  private final Map<Integer, Integer> unHealthyIndexes = new HashMap<>();
  private final Map<Integer, Integer> decommissionIndexes = new HashMap<>();
  private final Map<Integer, Integer> maintenanceIndexes = new HashMap<>();
  private final Set<DatanodeDetails> unhealthyReplicaDNs;
  private final List<ContainerReplica> replicas;

  public ECContainerReplicaCount(ContainerInfo containerInfo,
      Set<ContainerReplica> replicas,
      List<ContainerReplicaOp> replicaPendingOps,
      int remainingMaintenanceRedundancy) {
    this.containerInfo = containerInfo;
    // Iterate replicas in deterministic order to avoid potential data loss
    // on delete.
    // See https://issues.apache.org/jira/browse/HDDS-4589.
    // N.B., sort replicas by (containerID, datanodeDetails).
    this.replicas = replicas.stream()
        .sorted(Comparator.comparingLong(ContainerReplica::hashCode))
        .collect(Collectors.toList());
    this.repConfig = (ECReplicationConfig)containerInfo.getReplicationConfig();
    this.pendingAdd = new ArrayList<>();
    this.pendingDelete = new ArrayList<>();
    this.remainingMaintenanceRedundancy
        = Math.min(repConfig.getParity(), remainingMaintenanceRedundancy);

    unhealthyReplicaDNs = new HashSet<>();
    for (ContainerReplica r : replicas) {
      if (r.getState() == ContainerReplicaProto.State.UNHEALTHY) {
        unhealthyReplicaDNs.add(r.getDatanodeDetails());
      }
    }

    for (ContainerReplicaOp op : replicaPendingOps) {
      processPendingOp(op);
    }

    for (ContainerReplica replica : replicas) {
      /*
      Remove UNHEALTHY replicas because they are unavailable. They could be a
      reason for under replication but should not be a reason for over
      replication.

      For example, consider the following set of replicas for an EC 3-2
      container:
      Replica Index 1: Closed
      Replica Index 2: Closed
      Replica Index 3: Closed, Unhealthy (2 replicas for this index)
      Replica Index 4: Unhealthy
      Replica Index 5: Closed

      This is a case of under replication because index 4 is unavailable. Index
      3 is not considered over replicated because its second copy is unhealthy.
      */
      if (replica.getState() == ContainerReplicaProto.State.UNHEALTHY) {
        int val = unHealthyIndexes
            .getOrDefault(replica.getReplicaIndex(), 0);
        unHealthyIndexes.put(replica.getReplicaIndex(), val + 1);
        continue;
      }
      HddsProtos.NodeOperationalState state =
          replica.getDatanodeDetails().getPersistedOpState();
      int index = replica.getReplicaIndex();
      ensureIndexWithinBounds(index, "replicaSet");
      if (state == DECOMMISSIONED || state == DECOMMISSIONING) {
        int val = decommissionIndexes.getOrDefault(index, 0);
        decommissionIndexes.put(index, val + 1);
      } else if (state == IN_MAINTENANCE || state == ENTERING_MAINTENANCE) {
        int val = maintenanceIndexes.getOrDefault(index, 0);
        maintenanceIndexes.put(index, val + 1);
      } else {
        int val = healthyIndexes.getOrDefault(index, 0);
        healthyIndexes.put(index, val + 1);
      }
    }
    // Remove the pending delete replicas from the healthy set as we assume they
    // will eventually be removed and reduce the count for this replica. If the
    // count goes to zero, remove it from the map.
    for (Integer i : pendingDelete) {
      adjustHealthyCountWithPendingDelete(i);
    }
  }

  @Override
  public ContainerInfo getContainer() {
    return containerInfo;
  }

  @Override
  public List<ContainerReplica> getReplicas() {
    return replicas;
  }

  @Override
  public int getDecommissionCount() {
    return decommissionIndexes.size();
  }

  @Override
  public int getMaintenanceCount() {
    return maintenanceIndexes.size();
  }

  /**
   * Given a ContainerReplicaOp, check its index is within the expected
   * bounds and then add it to the relevant list.
   */
  private void processPendingOp(ContainerReplicaOp op) {
    ensureIndexWithinBounds(op.getReplicaIndex(), "pending" + op.getOpType());
    if (op.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
      pendingAdd.add(op.getReplicaIndex());
    } else if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
      if (!unhealthyReplicaDNs.contains(op.getTarget())) {
        // We ignore unhealthy replicas later in this method, so we also
        // need to ignore pending deletes on those unhealthy replicas,
        // otherwise the pending delete will decrement the healthy count and
        // make the container appear under-replicated when it is not.
        pendingDelete.add(op.getReplicaIndex());
      }
    }
  }

  /**
   * Remove the pending delete replicas from the healthy set as we assume they
   * will eventually be removed and reduce the count for this replica. If the
   * count goes to zero, remove it from the map.
   * @param index The replica Index which is pending delete.
   */
  private void adjustHealthyCountWithPendingDelete(int index) {
    Integer count = healthyIndexes.get(index);
    if (count != null) {
      count = count - 1;
      if (count < 1) {
        healthyIndexes.remove(index);
      } else {
        healthyIndexes.put(index, count);
      }
    }
  }

  /**
   * Add a pending op to the object. This allows the same
   * ECContainerReplicaCount object to be used for multiple processing stages
   * where commands are created at each state. The addition of a new pending
   * op could influence what further replicas are needed in subsequent stages.
   * @param op The ContainerReplicaOp to add.
   */
  public void addPendingOp(ContainerReplicaOp op) {
    processPendingOp(op);
    if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
      adjustHealthyCountWithPendingDelete(op.getReplicaIndex());
    }
  }

  /**
   * Get a set containing all decommissioning indexes, or an empty set if none
   * are decommissioning. Note it is possible for an index to be
   * decommissioning, healthy and in maintenance, if there are multiple copies
   * of it.
   * @return Set of indexes in decommission
   */
  public Set<Integer> decommissioningIndexes() {
    return decommissionIndexes.keySet();
  }

  /**
   * Get a set containing all decommissioning only indexes, or an empty set if
   * none are decommissioning.
   * @param includePendingAdd - removes the indexes from
   *                         decommissioningOnlyIndexes if we already scheduled
   *                         for reconstruction before.
   * @return Set of indexes in decommission only.
   */
  public Set<Integer> decommissioningOnlyIndexes(boolean includePendingAdd) {
    Set<Integer> decommissioningOnlyIndexes = new HashSet<>();
    for (Integer i : decommissionIndexes.keySet()) {
      if (!healthyIndexes.containsKey(i)) {
        decommissioningOnlyIndexes.add(i);
      }
    }
    // Now we have a list of decommissionIndexes. Remove any pending add as they
    // should eventually recover.
    if (includePendingAdd) {
      for (Integer i : pendingAdd) {
        decommissioningOnlyIndexes.remove(i);
      }
    }
    return decommissioningOnlyIndexes;
  }

  /**
   * Get a set containing all maintenance indexes, or an empty set if none are
   * in maintenance. Note it is possible for an index to be
   * decommissioning, healthy and in maintenance, if there are multiple copies
   * of it.
   * @return Set of indexes in maintenance
   */
  public Set<Integer> maintenanceIndexes() {
    return maintenanceIndexes.keySet();
  }

  /**
   * Gets a set containing all maintenance only indexes. These are replicas
   * that are only on maintenance nodes, without any copies on healthy nodes.
   *
   * @param includePendingAdd true if any indexes that are pending an add
   * {@link ContainerReplicaOp.PendingOpType#ADD} should be excluded
   * @return set containing maintenance only indexes or empty set if none are
   * in maintenance
   */
  public Set<Integer> maintenanceOnlyIndexes(boolean includePendingAdd) {
    Set<Integer> maintenanceOnlyIndexes = new HashSet<>();
    for (Integer i : maintenanceIndexes.keySet()) {
      if (!healthyIndexes.containsKey(i)) {
        maintenanceOnlyIndexes.add(i);
      }
    }

    // Now we have a set of maintenance only indexes. Remove any pending add
    // as they should eventually recover.
    if (includePendingAdd) {
      for (Integer i : pendingAdd) {
        maintenanceOnlyIndexes.remove(i);
      }
    }
    return maintenanceOnlyIndexes;
  }

  /**
   * Return true if there are insufficient replicas to recover this container.
   * Ie, less than EC Datanum containers are present.
   * @return True if the container cannot be recovered, false otherwise.
   */
  @Override
  public boolean isUnrecoverable() {
    Set<Integer> distinct = healthyReplicas();
    return distinct.size() < repConfig.getData();
  }

  /**
   * Return true if there are insufficient replicas to recover this container
   * when unhealthy replicas are included.
   * @return True if the container is missing, false otherwise.
   */
  public boolean isMissing() {
    Set<Integer> distinct = healthyReplicas();
    distinct.addAll(unHealthyIndexes.keySet());
    return distinct.size() < repConfig.getData();
  }

  private Set<Integer> healthyReplicas() {
    Set<Integer> distinct = new HashSet<>();
    distinct.addAll(healthyIndexes.keySet());
    distinct.addAll(decommissionIndexes.keySet());
    distinct.addAll(maintenanceIndexes.keySet());
    return distinct;
  }

  /**
   * Returns an unsorted list of indexes which need additional copies to
   * ensure the container is sufficiently replicated. These missing indexes will
   * not be on maintenance nodes, or decommission nodes.
   * Replicas pending delete are assumed to be removed.
   * If includePendingAdd is true, any replicas pending add
   * are assume to be created and omitted them from the returned list. If it is
   * true, we assume the pendingAdd will complete, giving a view of the
   * potential future state of the container.
   * This list can be used to determine which replicas must be recovered via an
   * EC reconstuction, rather than making copies of maintenance / decommission
   * replicas
   * @param includePendingAdd  If true, treat pending add containers as if they
   *                           have completed successfully.
   * @return List of missing indexes which have no online copy.
   */
  public List<Integer> unavailableIndexes(boolean includePendingAdd) {
    if (isSufficientlyReplicated(false)) {
      return Collections.emptyList();
    }
    Set<Integer> missing = new HashSet<>();
    for (int i = 1; i <= repConfig.getRequiredNodes(); i++) {
      if (!healthyIndexes.containsKey(i)) {
        missing.add(i);
      }
    }
    // Now we have a list of missing. Remove any pending add as they should
    // eventually recover.
    if (includePendingAdd) {
      for (Integer i : pendingAdd) {
        missing.remove(i);
      }
    }
    // Remove any maintenance copies, as they are still available. What remains
    // is the set of indexes we have no copy of, and hence must get re-created
    for (Integer i : maintenanceIndexes.keySet()) {
      missing.remove(i);
    }
    // Remove any decommission copies, as they are still available
    for (Integer i : decommissionIndexes.keySet()) {
      missing.remove(i);
    }
    return new ArrayList<>(missing);
  }

  /**
   * Get the number of additional replicas needed to make the container
   * sufficiently replicated for maintenance. For EC-3-2, if there is a
   * remainingMaintenanceRedundancy of 1, and two replicas in maintenance,
   * this will return 1, indicating one of the maintenance replicas must be
   * copied to an in-service node to meet the redundancy guarantee.
   */
  public int additionalMaintenanceCopiesNeeded(boolean includePendingAdd) {
    Set<Integer> maintenanceOnly = maintenanceOnlyIndexes(includePendingAdd);
    return Math.max(0, maintenanceOnly.size() - getMaxMaintenance());
  }

  /**
   * If any index has more than one copy that is not in maintenance or
   * decommission, then the container is over replicated. If the
   * includePendingDeletes flag is false we ignore replicas pending delete.
   * If it is true, we assume inflight deletes have been removed, giving
   * a view of the future state of the container if they complete successfully.
   * Pending add are always ignored as they may fail to create.
   * Note it is possible for a container to be both over and under replicated
   * as it could have multiple copies of 1 index, but zero copies of another
   * index.
   * @param includePendingDelete If true, treat replicas pending delete as if
   *                             they have deleted successfully.
   * @return True if overReplicated, false otherwise.
   */
  public boolean isOverReplicated(boolean includePendingDelete) {
    final Map<Integer, Integer> availableIndexes
        = getHealthyWithDelete(includePendingDelete);
    for (Integer count : availableIndexes.values()) {
      if (count > 1) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isOverReplicated() {
    return isOverReplicated(false);
  }

  /**
   * Return an unsorted list of any replica indexes which have more than one
   * replica and are therefore over-replicated. Maintenance replicas are ignored
   * as if we have excess including maintenance, it may be due to replication
   * which was needed to ensure sufficient redundancy for maintenance.
   * Pending adds are ignored as they may fail to complete.
   * If the includePendingDeletes flag is false we ignore replicas pending
   * delete. If it is true, we assume inflight deletes have been removed, giving
   * a view of the future state of the container if they complete successfully.
   * Pending deletes are assumed to complete and any indexes returned from here
   * will have the pending deletes already removed.
   * @param includePendingDelete If true, treat replicas pending delete as if
   *    *                        they have deleted successfully.
   * @return List of indexes which are over-replicated.
   */
  public List<Integer> overReplicatedIndexes(boolean includePendingDelete) {
    final Map<Integer, Integer> availableIndexes =
        getHealthyWithDelete(includePendingDelete);
    List<Integer> indexes = new ArrayList<>();
    for (Map.Entry<Integer, Integer> entry : availableIndexes.entrySet()) {
      if (entry.getValue() > 1) {
        indexes.add(entry.getKey());
      }
    }
    return indexes;
  }

  private Map<Integer, Integer> getHealthyWithDelete(boolean includeDelete) {
    final Map<Integer, Integer> availableIndexes;
    if (includeDelete) {
      // Deletes are already removed from the healthy list so just use the
      // healthy list
      availableIndexes = Collections.unmodifiableMap(healthyIndexes);
    } else {
      availableIndexes = new HashMap<>(healthyIndexes);
      pendingDelete.forEach(k -> availableIndexes.merge(k, 1, Integer::sum));
    }
    return availableIndexes;
  }

  /**
   * The container is sufficiently replicated if the healthy indexes minus any
   * pending deletes give a complete set of container indexes. If not, we must
   * also check the maintenance indexes - the container is still sufficiently
   * replicated if the complete set is made up of healthy + maintenance and
   * there is still sufficient maintenance redundancy.
   * If the includePendingAdd flag is set to true, this method treats replicas
   * pending add as if they have completed and hence shows the potential future
   * state of the container assuming they all complete.
   * @param includePendingAdd If true, treat pending add containers as if they
   *                          have completed successfully.
   * @return True if sufficiently replicated, false otherwise.
   */
  public boolean isSufficientlyReplicated(boolean includePendingAdd) {
    final Map<Integer, Integer> onlineIndexes;
    if (includePendingAdd) {
      onlineIndexes = new HashMap<>(healthyIndexes);
      pendingAdd.forEach(k -> onlineIndexes.merge(k, 1, Integer::sum));
    } else {
      onlineIndexes = Collections.unmodifiableMap(healthyIndexes);
    }

    if (hasFullSetOfIndexes(onlineIndexes)) {
      return true;
    }
    // Check if the maintenance copies give a full set and also that we do not
    // have too many in maintenance
    Map<Integer, Integer> healthy = new HashMap<>(onlineIndexes);
    maintenanceIndexes.forEach((k, v) -> healthy.merge(k, v, Integer::sum));
    return hasFullSetOfIndexes(healthy) && onlineIndexes.size()
        >= repConfig.getData() + remainingMaintenanceRedundancy;
  }

  /**
   * If we are checking a container for sufficient replication for "offline",
   * ie decommission or maintenance, then it is not really a requirement that
   * all replicas for the container are present. Instead, we can ensure the
   * replica on the node going offline has a copy elsewhere on another
   * IN_SERVICE node, and if so that replica is sufficiently replicated.
   * @param datanode The datanode being checked to go offline.
   * @param nodeManager not used in this implementation
   * @return True if the container is sufficiently replicated or if this replica
   *         on the passed node is present elsewhere on an IN_SERVICE node.
   */
  @Override
  public boolean isSufficientlyReplicatedForOffline(DatanodeDetails datanode,
      NodeManager nodeManager) {
    boolean sufficientlyReplicated = isSufficientlyReplicated(false);
    if (sufficientlyReplicated) {
      return true;
    }
    // If it is not sufficiently replicated (ie the container has all replicas)
    // then we need to check if the replica that is on this node is available
    // on another ONLINE node, ie in the healthy set. This means we avoid
    // blocking decommission or maintenance caused by un-recoverable EC
    // containers.
    if (datanode.getPersistedOpState() == IN_SERVICE) {
      // The node passed into this method must be a node going offline, so it
      // cannot be IN_SERVICE. If an IN_SERVICE mode is passed, just return
      // false.
      return false;
    }
    ContainerReplica thisReplica = null;
    for (ContainerReplica r : replicas) {
      if (r.getDatanodeDetails().equals(datanode)) {
        thisReplica = r;
        break;
      }
    }
    if (thisReplica == null) {
      // From the set of replicas, none are on the passed datanode.
      // This should not happen in practice but if it does we cannot indicate
      // the container is sufficiently replicated.
      return false;
    }
    return healthyIndexes.containsKey(thisReplica.getReplicaIndex());
  }

  @Override
  public boolean isHealthyEnoughForOffline() {
    return isHealthy();
  }

  @Override
  public boolean isSufficientlyReplicated() {
    return isSufficientlyReplicated(false);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Container State: ").append(containerInfo.getState())
        .append(", Replicas: (Count: ").append(replicas.size());
    if (!healthyIndexes.isEmpty()) {
      sb.append(", Healthy: ").append(healthyIndexes.size());
    }
    if (!unhealthyReplicaDNs.isEmpty()) {
      sb.append(", Unhealthy: ").append(unhealthyReplicaDNs.size());
    }
    if (!decommissionIndexes.isEmpty()) {
      sb.append(", Decommission: ").append(decommissionIndexes.size());
    }
    if (!maintenanceIndexes.isEmpty()) {
      sb.append(", Maintenance: ").append(maintenanceIndexes.size());
    }
    if (!pendingAdd.isEmpty()) {
      sb.append(", PendingAdd: ").append(pendingAdd.size());
    }
    if (!pendingDelete.isEmpty()) {
      sb.append(", PendingDelete: ").append(pendingDelete.size());
    }
    sb.append(')')
        .append(", ReplicationConfig: ").append(repConfig)
        .append(", RemainingMaintenanceRedundancy: ")
        .append(remainingMaintenanceRedundancy);
    return sb.toString();
  }

  /**
   * Check if there is an entry in the map for all expected replica indexes,
   * and also that the count against each index is greater than zero.
   * @param indexSet A map representing the replica index and count of the
   *                 replicas for that index.
   * @return True if there is a full set of indexes, false otherwise.
   */
  private boolean hasFullSetOfIndexes(Map<Integer, Integer> indexSet) {
    return indexSet.size() == repConfig.getRequiredNodes();
  }

  /**
   * Returns the maximum number of replicas that are allowed to be only on a
   * maintenance node, with no other copies on in-service nodes.
   */
  private int getMaxMaintenance() {
    return Math.max(0, repConfig.getParity() - remainingMaintenanceRedundancy);
  }

  /**
   * Validate to ensure that the replia index is between 1 and the max expected
   * replica index for the replication config, eg 5 for 3-2, 9 for 6-3 etc.
   * @param index The replica index to check.
   * @throws IllegalArgumentException if the index is out of bounds.
   */
  private void ensureIndexWithinBounds(Integer index, String setName) {
    if (index < 1 || index > repConfig.getRequiredNodes()) {
      throw new IllegalArgumentException("Replica Index in " + setName
          + " for containerID " + containerInfo.getContainerID()
          + "must be between 1 and " + repConfig.getRequiredNodes()
          + ". But the given index is: " + index);
    }
  }
}
