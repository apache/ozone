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

package org.apache.hadoop.hdds.scm.container.reconciliation;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

/**
 * Determines whether a container is eligible for reconciliation based on its state, replica states, replication
 * type, and replication factor.
 */
public final class ReconciliationEligibilityHandler {
  public static final Set<HddsProtos.LifeCycleState> ELIGIBLE_CONTAINER_STATES =
      EnumSet.of(HddsProtos.LifeCycleState.CLOSED, HddsProtos.LifeCycleState.QUASI_CLOSED);
  public static final Set<State> ELIGIBLE_REPLICA_STATES =
      EnumSet.of(State.CLOSED, State.QUASI_CLOSED, State.UNHEALTHY);

  /**
   * Utility class only.
   */
  private ReconciliationEligibilityHandler() { }

  public static EligibilityResult isEligibleForReconciliation(
      ContainerID containerID, ContainerManager containerManager) {
    ContainerInfo container;
    Set<ContainerReplica> replicas;
    try {
      container = containerManager.getContainer(containerID);
      replicas = containerManager.getContainerReplicas(containerID);
    } catch (ContainerNotFoundException ex) {
      return new EligibilityResult(Result.CONTAINER_NOT_FOUND,
          String.format("Container %s not found for reconciliation.", containerID));
    }

    if (!ELIGIBLE_CONTAINER_STATES.contains(container.getState())) {
      return new EligibilityResult(Result.INELIGIBLE_CONTAINER_STATE,
          String.format("Cannot reconcile container %d in state %s.", container.getContainerID(),
              container.getState()));
    }

    if (replicas.isEmpty()) {
      return new EligibilityResult(Result.NO_REPLICAS_FOUND,
          String.format("Cannot reconcile container %d because no replicas could be found.",
              container.getContainerID()));
    }

    boolean replicasValid = replicas.stream()
        .map(ContainerReplica::getState)
        .allMatch(ELIGIBLE_REPLICA_STATES::contains);
    if (!replicasValid) {
      return new EligibilityResult(Result.INELIGIBLE_REPLICA_STATES,
          String.format("Cannot reconcile container %s in state %s with replica states: %s", containerID,
              container.getState(), replicas.stream()
                  .map(r -> r.getState().toString())
                  .collect(Collectors.joining(", "))));
    }

    // Reconcile on EC containers is not yet implemented.
    ReplicationConfig repConfig = container.getReplicationConfig();
    if (repConfig.getReplicationType() != HddsProtos.ReplicationType.RATIS) {
      return new EligibilityResult(Result.INELIGIBLE_REPLICATION_TYPE,
          String.format("Cannot reconcile container %s with replication type %s. Reconciliation is currently only " +
              "supported for Ratis containers.", containerID, repConfig.getReplicationType()));
    }

    // Reconciliation requires multiple replicas to reconcile.
    int requiredNodes = repConfig.getRequiredNodes();
    if (requiredNodes <= 1) {
      return new EligibilityResult(Result.NOT_ENOUGH_REQUIRED_NODES,
          String.format("Cannot reconcile container %s with %d required nodes. Reconciliation is only supported for " +
              "containers with more than 1 required node.", containerID, repConfig.getRequiredNodes()));
    }

    return new EligibilityResult(Result.OK, "Container %s is eligible for reconciliation." + containerID);
  }

  /**
   * Defines the reasons a container may not be eligible for reconciliation.
   */
  public enum Result {
    OK,
    CONTAINER_NOT_FOUND,
    INELIGIBLE_CONTAINER_STATE,
    INELIGIBLE_REPLICA_STATES,
    INELIGIBLE_REPLICATION_TYPE,
    NOT_ENOUGH_REQUIRED_NODES,
    NO_REPLICAS_FOUND
  }

  /**
   * Provides a status and message indicating whether a container is eligible for reconciliation.
   */
  public static final class EligibilityResult {
    private final Result result;
    private final String message;

    private EligibilityResult(Result result, String message) {
      this.result = result;
      this.message = message;
    }

    public Result getResult() {
      return result;
    }

    public boolean isOk() {
      return result == Result.OK;
    }

    @Override
    public String toString() {
      return message;
    }
  }
}
