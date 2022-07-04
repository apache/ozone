/*
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
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class holds some common methods that will be shared among
 * different kinds of implementation of OverReplicationHandler.
 * */
public abstract class AbstractOverReplicationHandler
    implements UnhealthyReplicationHandler {
  private final PlacementPolicy placementPolicy;

  protected AbstractOverReplicationHandler(PlacementPolicy placementPolicy) {
    this.placementPolicy = placementPolicy;
  }

  /**
   * Identify a new set of datanode(s) to delete the container
   * and form the SCM commands to send it to DN.
   *
   * @param replicas - Set of available container replicas.
   * @param pendingOps - Inflight replications and deletion ops.
   * @param result - Health check result.
   * @param remainingMaintenanceRedundancy - represents that how many nodes go
   *                                      into maintenance.
   * @return Returns the key value pair of destination dn where the command gets
   * executed and the command itself.
   */
  public abstract Map<DatanodeDetails, SCMCommand<?>> processAndCreateCommands(
      Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      ContainerHealthResult result, int remainingMaintenanceRedundancy);

  /**
   * Identify whether the placement status is actually equal for a
   * replica set after removing those filtered replicas.
   *
   * @param replicas the oringianl set of replicas
   * @param replicationFactor the criteria which replicas should be removed.
   * @param replica the replica to be removed
   */
  public boolean isPlacementStatusActuallyEqualAfterRemove(
      final Set<ContainerReplica> replicas,
      final ContainerReplica replica,
      final int replicationFactor) {
    ContainerPlacementStatus currentCPS =
        getPlacementStatus(replicas, replicationFactor);
    replicas.remove(replica);
    ContainerPlacementStatus newCPS =
        getPlacementStatus(replicas, replicationFactor);
    replicas.add(replica);
    return isPlacementStatusActuallyEqual(currentCPS, newCPS);
  }

  /**
   * Given a set of ContainerReplica, transform it to a list of DatanodeDetails
   * and then check if the list meets the container placement policy.
   * @param replicas List of containerReplica
   * @param replicationFactor Expected Replication Factor of the containe
   * @return ContainerPlacementStatus indicating if the policy is met or not
   */
  private ContainerPlacementStatus getPlacementStatus(
      Set<ContainerReplica> replicas, int replicationFactor) {
    List<DatanodeDetails> replicaDns = replicas.stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());
    return placementPolicy.validateContainerPlacement(
        replicaDns, replicationFactor);
  }

  /**
   * whether the given two ContainerPlacementStatus are actually equal.
   *
   * @param cps1 ContainerPlacementStatus
   * @param cps2 ContainerPlacementStatus
   */
  private boolean isPlacementStatusActuallyEqual(
      ContainerPlacementStatus cps1,
      ContainerPlacementStatus cps2) {
    return (!cps1.isPolicySatisfied() &&
        cps1.actualPlacementCount() == cps2.actualPlacementCount()) ||
        cps1.isPolicySatisfied() && cps2.isPolicySatisfied();
  }
}
