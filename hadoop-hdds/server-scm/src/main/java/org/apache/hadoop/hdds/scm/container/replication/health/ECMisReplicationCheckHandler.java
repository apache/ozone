package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;

/**
 * This class checks if an EC container is mis replicated. The container should
 * not be over or under replicated, and should not have an excess of UNHEALTHY
 * replicas.
 */
public class ECMisReplicationCheckHandler extends AbstractCheck {
  static final Logger LOG =
      LoggerFactory.getLogger(ECMisReplicationCheckHandler.class);

  private final PlacementPolicy placementPolicy;

  public ECMisReplicationCheckHandler(PlacementPolicy placementPolicy) {
    this.placementPolicy = placementPolicy;
  }

  @Override
  public boolean handle(ContainerCheckRequest request) {
    if (request.getContainerInfo().getReplicationType() != EC) {
      // This handler is only for EC containers.
      return false;
    }
    ReplicationManagerReport report = request.getReport();
    ContainerInfo container = request.getContainerInfo();
    ContainerID containerID = container.containerID();

    ContainerHealthResult health = checkMisReplication(request);
    if (health.getHealthState() ==
        ContainerHealthResult.HealthState.MIS_REPLICATED) {
      report.incrementAndSample(
          ReplicationManagerReport.HealthState.MIS_REPLICATED, containerID);
      ContainerHealthResult.MisReplicatedHealthResult misRepHealth
          = ((ContainerHealthResult.MisReplicatedHealthResult) health);
      if (!misRepHealth.isReplicatedOkAfterPending()) {
        request.getReplicationQueue().enqueue(misRepHealth);
      }
      LOG.debug("Container {} is Mis Replicated. isReplicatedOkAfterPending "
              + "is [{}]. Reason for mis replication is [{}].", container,
          misRepHealth.isReplicatedOkAfterPending(),
          misRepHealth.getMisReplicatedReason());
      return true;
    }

    return false;
  }

  ContainerHealthResult checkMisReplication(ContainerCheckRequest request) {
    ContainerInfo container = request.getContainerInfo();
    Set<ContainerReplica> replicas = request.getContainerReplicas();

    ContainerPlacementStatus placement = getPlacementStatus(replicas,
        container.getReplicationConfig().getRequiredNodes(),
        Collections.emptyList());
    if (!placement.isPolicySatisfied()) {
      ContainerPlacementStatus placementAfterPending = getPlacementStatus(
          replicas, container.getReplicationConfig().getRequiredNodes(),
          request.getPendingOps());
      return new ContainerHealthResult.MisReplicatedHealthResult(
          container, placementAfterPending.isPolicySatisfied(),
          placementAfterPending.misReplicatedReason());
    }

    return new ContainerHealthResult.HealthyResult(container);
  }

  /**
   * Given a set of ContainerReplica, transform it to a list of DatanodeDetails
   * and then check if the list meets the container placement policy.
   * @param replicas List of containerReplica
   * @param replicationFactor Expected Replication Factor of the container
   * @return ContainerPlacementStatus indicating if the policy is met or not
   */
  private ContainerPlacementStatus getPlacementStatus(
      Set<ContainerReplica> replicas, int replicationFactor,
      List<ContainerReplicaOp> pendingOps) {

    Set<DatanodeDetails> replicaDns = replicas
        .stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toSet());

    for (ContainerReplicaOp op : pendingOps) {
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.ADD) {
        replicaDns.add(op.getTarget());
      }
      if (op.getOpType() == ContainerReplicaOp.PendingOpType.DELETE) {
        replicaDns.remove(op.getTarget());
      }
    }

    return placementPolicy.validateContainerPlacement(
        new ArrayList<>(replicaDns), replicationFactor);
  }

}
