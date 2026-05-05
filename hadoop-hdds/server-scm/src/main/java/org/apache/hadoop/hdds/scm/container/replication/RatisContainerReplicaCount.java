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
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.compareState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.OverReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult.UnderReplicatedHealthResult;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;

/**
 * Immutable object that is created with a set of ContainerReplica objects and
 * the number of in flight replica add and deletes, the container replication
 * factor and the min count which must be available for maintenance. This
 * information can be used to determine if the container is over or under
 * replicated and also how many additional replicas need created or removed.
 */
public class RatisContainerReplicaCount implements ContainerReplicaCount {

  private int healthyReplicaCount;
  private int unhealthyReplicaCount;
  private int misMatchedReplicaCount;
  private int matchingReplicaCount;
  private int decommissionCount;
  private int maintenanceCount;
  private int unhealthyDecommissionCount;
  private int unhealthyMaintenanceCount;
  private int inFlightAdd;
  private int inFlightDel;
  private final int repFactor;
  private final int minHealthyForMaintenance;
  private final ContainerInfo container;
  private final List<ContainerReplica> replicas;
  private boolean considerUnhealthy = false;

  public RatisContainerReplicaCount(ContainerInfo container,
                               Set<ContainerReplica> replicas,
                                    int inFlightAdd,
                               int inFlightDelete, int replicationFactor,
                               int minHealthyForMaintenance) {
    this.inFlightAdd = inFlightAdd;
    this.inFlightDel = inFlightDelete;
    this.repFactor = replicationFactor;
    // Iterate replicas in deterministic order to avoid potential data loss
    // on delete.
    // See https://issues.apache.org/jira/browse/HDDS-4589.
    // N.B., sort replicas by (containerID, datanodeDetails).
    this.replicas = replicas.stream()
        .sorted(Comparator.comparingLong(ContainerReplica::hashCode))
        .collect(Collectors.toList());
    this.minHealthyForMaintenance
        = Math.min(this.repFactor, minHealthyForMaintenance);
    this.container = container;

    countReplicas();
  }

  public RatisContainerReplicaCount(ContainerInfo containerInfo,
      Set<ContainerReplica> replicas,
      List<ContainerReplicaOp> replicaPendingOps,
      int minHealthyForMaintenance, boolean considerUnhealthy) {
    // Iterate replicas in deterministic order to avoid potential data loss
    // on delete.
    // See https://issues.apache.org/jira/browse/HDDS-4589.
    // N.B., sort replicas by (containerID, datanodeDetails).
    this.replicas = replicas.stream()
        .sorted(Comparator.comparingLong(ContainerReplica::hashCode))
        .collect(Collectors.toList());

    this.container = containerInfo;
    this.repFactor = containerInfo.getReplicationFactor().getNumber();
    this.minHealthyForMaintenance
        = Math.min(this.repFactor, minHealthyForMaintenance);
    this.considerUnhealthy = considerUnhealthy;

    // collect DNs that have UNHEALTHY replicas
    Set<DatanodeDetails> unhealthyReplicaDNs = new HashSet<>();
    for (ContainerReplica r : replicas) {
      if (r.getState() == ContainerReplicaProto.State.UNHEALTHY) {
        unhealthyReplicaDNs.add(r.getDatanodeDetails());
      }
    }

    // count pending adds and deletes
    for (ContainerReplicaOp op : replicaPendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
        inFlightAdd++;
      } else if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        if (!unhealthyReplicaDNs.contains(op.getTarget()) ||
            considerUnhealthy) {
          /*
          We don't count UNHEALTHY replicas when considering sufficient
          replication, so we also need to ignore pending deletes on
          those unhealthy replicas, otherwise the pending delete will
          decrement the healthy count and make the container appear
          under-replicated when it is not.
           */
          inFlightDel++;
        }
      }
    }

    countReplicas();
  }

  private void countReplicas() {
    for (ContainerReplica cr : replicas) {
      HddsProtos.NodeOperationalState state =
          cr.getDatanodeDetails().getPersistedOpState();

      /*
       * When there is Quasi Closed Replica with incorrect sequence id
       * for a Closed container, it's treated as unhealthy.
       */
      boolean unhealthy =
          cr.getState() == ContainerReplicaProto.State.UNHEALTHY ||
              (cr.getState() == ContainerReplicaProto.State.QUASI_CLOSED &&
                  container.getState() == HddsProtos.LifeCycleState.CLOSED &&
                  container.getSequenceId() != cr.getSequenceId());

      if (state == DECOMMISSIONED || state == DECOMMISSIONING) {
        if (unhealthy) {
          unhealthyDecommissionCount++;
        } else {
          decommissionCount++;
        }
      } else if (state == IN_MAINTENANCE || state == ENTERING_MAINTENANCE) {
        if (unhealthy) {
          unhealthyMaintenanceCount++;
        } else {
          maintenanceCount++;
        }
      } else if (unhealthy) {
        unhealthyReplicaCount++;
      } else {
        healthyReplicaCount++;
        if (compareState(container.getState(), cr.getState())) {
          matchingReplicaCount++;
        } else {
          misMatchedReplicaCount++;
        }
      }
    }
  }

  /**
   * Healthy replica count = Number of replicas that match the container's
   * state + Number of replicas that don't match the container's state but
   * their state is not UNHEALTHY (also called mismatched replicas).
   * For example, consider a CLOSED container with the following replicas:
   * {CLOSED, CLOSING, OPEN, UNHEALTHY}
   * In this case, healthy replica count equals 3. Calculation:
   * 1 CLOSED -&gt; 1 matching replica.
   * 1 OPEN, 1 CLOSING -&gt; 2 mismatched replicas.
   * 1 UNHEALTHY -&gt; 1 unhealthy replica. Not counted as healthy.
   * Total healthy replicas = 3 = 1 matching + 2 mismatched replicas
   */
  public int getHealthyReplicaCount() {
    return healthyReplicaCount + decommissionCount + maintenanceCount;
  }

  public int getUnhealthyReplicaCount() {
    return unhealthyReplicaCount + unhealthyDecommissionCount + unhealthyMaintenanceCount;
  }

  public int getMisMatchedReplicaCount() {
    return misMatchedReplicaCount;
  }

  public int getMatchingReplicaCount() {
    return matchingReplicaCount;
  }

  private int getAvailableReplicas() {
    int available = healthyReplicaCount;
    if (considerUnhealthy) {
      available += unhealthyReplicaCount;
    }
    return available;
  }

  @Override
  public int getDecommissionCount() {
    return considerUnhealthy
        ? decommissionCount + unhealthyDecommissionCount
        : decommissionCount;
  }

  @Override
  public int getMaintenanceCount() {
    return considerUnhealthy
        ? maintenanceCount + unhealthyMaintenanceCount
        : maintenanceCount;
  }

  public int getReplicationFactor() {
    return repFactor;
  }

  @Override
  public ContainerInfo getContainer() {
    return container;
  }

  @Override
  public List<ContainerReplica> getReplicas() {
    return new ArrayList<>(replicas);
  }

  @Override
  public String toString() {
    String result = "Container State: " + container.getState() +
        " Replica Count: " + replicas.size() +
        " Healthy (I/D/M): " + healthyReplicaCount +
            "/" + decommissionCount + "/" + maintenanceCount +
        " Unhealthy (I/D/M): " + unhealthyReplicaCount +
            "/" + unhealthyDecommissionCount + "/" + unhealthyMaintenanceCount +
        " inFlightAdd: " + inFlightAdd +
        " inFightDel: " + inFlightDel +
        " ReplicationFactor: " + repFactor +
        " minMaintenance: " + minHealthyForMaintenance;
    if (considerUnhealthy) {
      result += " +considerUnhealthy";
    }
    return result;
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
    int availableReplicas = getAvailableReplicas();
    int delta = repFactor - availableReplicas;

    if (delta < 0) {
      // Over replicated, so may need to remove a container.
      return delta;
    } else if (delta > 0) {
      // May be under-replicated, depending on maintenance.
      delta = Math.max(0, delta - getMaintenanceCount());
      int neededHealthy =
          Math.max(0, minHealthyForMaintenance - availableReplicas);
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
   * @param nodeManager not used in this implementation
   * @return True if the container is sufficiently replicated and False
   *         otherwise.
   */
  @Override
  public boolean isSufficientlyReplicatedForOffline(DatanodeDetails datanode,
      NodeManager nodeManager) {
    return isSufficientlyReplicated();
  }

  /**
   * Checks if all replicas (except UNHEALTHY) on in-service nodes are in the
   * same health state as the container. This is similar to what
   * {@link ContainerReplicaCount#isHealthy()} does. The difference is in how
   * both methods treat UNHEALTHY replicas.
   * <p>
   * This method is the interface between the decommissioning flow and
   * Replication Manager. Callers can use it to check whether replicas of a
   * container are in the same state as the container before a datanode is
   * taken offline.
   * </p>
   * <p>
   * Note that this method's purpose is to only compare the replica state with
   * the container state. It does not check if the container has sufficient
   * number of replicas - that is the job of {@link ContainerReplicaCount
   * #isSufficientlyReplicatedForOffline(DatanodeDetails, NodeManager)}.
   * @return true if the container is healthy enough, which is determined by
   * various checks
   * </p>
   */
  @Override
  public boolean isHealthyEnoughForOffline() {
    long countInService = getReplicas().stream()
        .filter(r -> r.getDatanodeDetails().getPersistedOpState() == IN_SERVICE)
        .count();
    if (countInService == 0) {
      /*
      Having no in-service nodes is unexpected and SCM shouldn't allow this
      to happen in the first place. Return false here just to be safe.
      */
      return false;
    }

    HddsProtos.LifeCycleState containerState = getContainer().getState();
    return (containerState == HddsProtos.LifeCycleState.CLOSED
        || containerState == HddsProtos.LifeCycleState.QUASI_CLOSED)
        && getReplicas().stream()
        .filter(r -> r.getDatanodeDetails().getPersistedOpState() == IN_SERVICE)
        .filter(r -> r.getState() !=
            ContainerReplicaProto.State.UNHEALTHY)
        .allMatch(r -> ReplicationManager.compareState(
            containerState, r.getState()));
  }

  /**
   * QUASI_CLOSED containers that have a mix of healthy and UNHEALTHY
   * replicas require special treatment. If the UNHEALTHY replicas have the
   * correct sequence ID and have unique origins, then we need to save at least
   * one copy of each such UNHEALTHY replicas. This method finds such UNHEALTHY
   * replicas.
   *
   * @param nodeStatusFn a function used to check the {@link NodeStatus} of a node,
   * accepting a {@link DatanodeDetails} and returning {@link NodeStatus}
   * @return List of UNHEALTHY replicas with the greatest Sequence ID that
   * need to be replicated to other nodes. Empty list if this container is not
   * QUASI_CLOSED, doesn't have a mix of healthy and UNHEALTHY replicas, or
   * if there are no replicas that need to be saved.
   */
  public List<ContainerReplica> getVulnerableUnhealthyReplicas(Function<DatanodeDetails, NodeStatus> nodeStatusFn) {
    if (container.getState() != HddsProtos.LifeCycleState.QUASI_CLOSED) {
      // this method is only relevant for QUASI_CLOSED containers
      return Collections.emptyList();
    }

    boolean foundHealthy = false;
    List<ContainerReplica> unhealthyReplicas = new ArrayList<>();
    for (ContainerReplica replica : replicas) {
      if (replica.getState() != ContainerReplicaProto.State.UNHEALTHY) {
        foundHealthy = true;
      }

      if (replica.getSequenceId() == container.getSequenceId()) {
        if (replica.getState() == ContainerReplicaProto.State.UNHEALTHY && !replica.isEmpty()) {
          unhealthyReplicas.add(replica);
        }
      }
    }
    if (!foundHealthy) {
      // this method is only relevant when there's a mix of healthy and
      // unhealthy replicas
      return Collections.emptyList();
    }

    unhealthyReplicas.removeIf(
        replica -> {
          NodeStatus status = nodeStatusFn.apply(replica.getDatanodeDetails());
          return status == null || !status.isHealthy();
        });
    replicas.removeIf(
        replica -> {
          NodeStatus status = nodeStatusFn.apply(replica.getDatanodeDetails());
          return status == null || !status.isHealthy();
        });
    /*
    At this point, the list of unhealthyReplicas contains all UNHEALTHY non-empty
    replicas with the greatest Sequence ID that are on healthy Datanodes.
    Note that this also includes multiple copies of the same UNHEALTHY
    replica, that is, replicas with the same Origin ID. We need to consider
    the fact that replicas can be uniquely unhealthy. That is, 2 UNHEALTHY
    replicas with different Origin ID need not be exact copies of each other.

    Replicas that don't have at least one instance (multiple instances of a
    replica will have the same Origin ID) on an IN_SERVICE node are
    vulnerable and need to be saved.
     */
    // TODO should we also consider pending deletes?
    final Set<DatanodeID> originsOfInServiceReplicas = new HashSet<>();
    for (ContainerReplica replica : replicas) {
      if (replica.getDatanodeDetails().getPersistedOpState()
          .equals(IN_SERVICE) && replica.getSequenceId().equals(container.getSequenceId())) {
        originsOfInServiceReplicas.add(replica.getOriginDatanodeId());
      }
    }
    unhealthyReplicas.removeIf(replica -> originsOfInServiceReplicas.contains(
        replica.getOriginDatanodeId()));
    return unhealthyReplicas;
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
   * @return true if the container is under replicated, false otherwise
   */
  public boolean isUnderReplicated() {
    return !isSufficientlyReplicated();
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
   * A container is safely over replicated if:
   * 1. It is over replicated.
   * 2. Has at least replication factor number of matching replicas.
   */
  public boolean isSafelyOverReplicated() {
    if (!isOverReplicated(true)) {
      return false;
    }

    return getMatchingReplicaCount() >= repFactor;
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
   * Checks whether insufficient replication is because of some replicas
   * being on datanodes that were decommissioned or are in maintenance.
   *
   * @return true if there is insufficient replication and it's because of
   * decommissioning.
   */
  boolean insufficientDueToOutOfService() {
    int delta = redundancyDelta(true, false);
    return 0 < delta && delta <= getDecommissionCount() + getMaintenanceCount();
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
    int availableReplicas = getAvailableReplicas()
        + getDecommissionCount() + getMaintenanceCount();

    return Math.max(0, availableReplicas - inFlightDel - 1);
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

  public UnderReplicatedHealthResult toUnderHealthResult() {
    UnderReplicatedHealthResult result = new UnderReplicatedHealthResult(
        getContainer(),
        getRemainingRedundancy(),
        insufficientDueToOutOfService(),
        isSufficientlyReplicated(true),
        isUnrecoverable());
    result.setHasHealthyReplicas(getHealthyReplicaCount() > 0);
    return result;
  }

  public OverReplicatedHealthResult toOverHealthResult() {
    OverReplicatedHealthResult result = new OverReplicatedHealthResult(
        getContainer(),
        getExcessRedundancy(false),
        !isOverReplicated(true));
    result.setHasMismatchedReplicas(getMisMatchedReplicaCount() > 0);
    // FIXME not used in RatisReplicationCheckHandler: OK?
    result.setIsSafelyOverReplicated(isSafelyOverReplicated());
    return result;

  }

}
