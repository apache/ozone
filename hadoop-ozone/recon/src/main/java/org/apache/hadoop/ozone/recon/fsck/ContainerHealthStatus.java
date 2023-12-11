/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.fsck;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class which encapsulates all the information required to determine if a
 * container and its replicas are correctly replicated and placed.
 */

public class ContainerHealthStatus {

  private ContainerInfo container;
  private int replicaDelta;
  private Set<ContainerReplica> healthyReplicas;
  private ContainerPlacementStatus placementStatus;
  private ReconContainerMetadataManager reconContainerMetadataManager;
  private int numReplicas;
  private long numKeys;

  ContainerHealthStatus(ContainerInfo container,
                        Set<ContainerReplica> healthyReplicas,
                        PlacementPolicy placementPolicy,
                        ReconContainerMetadataManager
                            reconContainerMetadataManager) {
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.container = container;
    int repFactor = container.getReplicationConfig().getRequiredNodes();
    this.healthyReplicas = healthyReplicas
        .stream()
        .filter(r -> !r.getState()
            .equals((ContainerReplicaProto.State.UNHEALTHY)))
        .collect(Collectors.toSet());
    this.replicaDelta = repFactor - this.healthyReplicas.size();
    this.placementStatus = getPlacementStatus(placementPolicy, repFactor);
    this.numReplicas = healthyReplicas.size();
    this.numKeys = getContainerKeyCount(container.getContainerID());
  }

  public long getContainerID() {
    return this.container.getContainerID();
  }

  public ContainerInfo getContainer() {
    return this.container;
  }

  public int getReplicationFactor() {
    return container.getReplicationConfig().getRequiredNodes();
  }

  public boolean isHealthy() {
    return replicaDelta == 0 && !isMisReplicated();
  }

  public boolean isDeleted() {
    return container.getState() == HddsProtos.LifeCycleState.DELETED ||
        container.getState() == HddsProtos.LifeCycleState.DELETING;
  }

  public boolean isOverReplicated() {
    return replicaDelta < 0;
  }

  public boolean isUnderReplicated() {
    return !isMissing() && replicaDelta > 0;
  }

  public int replicaDelta() {
    return replicaDelta;
  }

  public int getReplicaCount() {
    return healthyReplicas.size();
  }

  public boolean isMisReplicated() {
    return !isMissing() && !placementStatus.isPolicySatisfied();
  }

  public int misReplicatedDelta() {
    return placementStatus.misReplicationCount();
  }

  public int expectedPlacementCount() {
    return placementStatus.expectedPlacementCount();
  }

  public int actualPlacementCount() {
    return placementStatus.actualPlacementCount();
  }

  public String misReplicatedReason() {
    return placementStatus.misReplicatedReason();
  }

  public boolean isMissing() {
    return numReplicas == 0;
  }

  public boolean isEmpty() {
    return numKeys == 0;
  }

  private ContainerPlacementStatus getPlacementStatus(
      PlacementPolicy policy, int repFactor) {
    List<DatanodeDetails> dns = healthyReplicas.stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());
    return policy.validateContainerPlacement(dns, repFactor);
  }

  private long getContainerKeyCount(long containerID) {
    try {
      return reconContainerMetadataManager.getKeyCountForContainer(
          containerID);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public long getNumKeys() {
    return numKeys;
  }
}
