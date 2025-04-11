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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

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
   * Identify whether the placement status is actually equal for a
   * replica set after removing those filtered replicas.
   *
   * @param replicas the oringianl set of replicas
   * @param replicationFactor the criteria which replicas should be removed.
   * @param replica the replica to be removed
   */
  public boolean isPlacementStatusActuallyEqualAfterRemove(
      ContainerPlacementStatus currentCPS,
      final Set<ContainerReplica> replicas,
      final ContainerReplica replica,
      final int replicationFactor) {
    replicas.remove(replica);
    ContainerPlacementStatus newCPS =
        getPlacementStatus(replicas, replicationFactor);
    replicas.add(replica);
    return isPlacementStatusActuallyEqual(currentCPS, newCPS);
  }

  /**
   * Allow the placement policy to indicate which replicas can be removed for
   * an over replicated container, so that the placement policy is not violated
   * by removing them.
   */
  protected Set<ContainerReplica> selectReplicasToRemove(
      Set<ContainerReplica> replicas, int expectedCountPerUniqueReplica) {
    return placementPolicy.replicasToRemoveToFixOverreplication(
        replicas, expectedCountPerUniqueReplica);
  }

  /**
   * Given a set of ContainerReplica, transform it to a list of DatanodeDetails
   * and then check if the list meets the container placement policy.
   * @param replicas List of containerReplica
   * @param replicationFactor Expected Replication Factor of the containe
   * @return ContainerPlacementStatus indicating if the policy is met or not
   */
  protected ContainerPlacementStatus getPlacementStatus(
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
