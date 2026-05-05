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

package org.apache.hadoop.ozone.recon.fsck;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.ECContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.RatisContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;

/**
 * Class which encapsulates all the information required to determine if a
 * container and its replicas are correctly replicated and placed.
 */

public class ContainerHealthStatus {

  private final ContainerInfo container;
  private final int replicaDelta;
  private final Set<ContainerReplica> replicas;
  private final Set<ContainerReplica> healthyReplicas;
  private final Set<ContainerReplica> healthyAvailReplicas;
  private final ContainerPlacementStatus placementStatus;
  private final ReconContainerMetadataManager reconContainerMetadataManager;
  private final int numReplicas;
  private final long numKeys;
  private final ContainerReplicaCount containerReplicaCount;

  ContainerHealthStatus(ContainerInfo container,
                        Set<ContainerReplica> replicas,
                        PlacementPolicy placementPolicy,
                        ReconContainerMetadataManager
                            reconContainerMetadataManager,
                        OzoneConfiguration conf) {
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.container = container;
    int repFactor = container.getReplicationConfig().getRequiredNodes();
    this.replicas = replicas;
    this.healthyReplicas = replicas
        .stream()
        .filter(r -> !r.getState()
            .equals((ContainerReplicaProto.State.UNHEALTHY)))
        .collect(Collectors.toSet());
    this.healthyAvailReplicas = replicas
        .stream()
        // Filter unhealthy replicas and
        // replicas belonging to out-of-service nodes.
        .filter(r ->
            (!r.getDatanodeDetails().isDecommissioned() &&
             !r.getDatanodeDetails().isMaintenance() &&
             !r.getState().equals(ContainerReplicaProto.State.UNHEALTHY)))
        .collect(Collectors.toSet());
    this.replicaDelta = repFactor - this.healthyAvailReplicas.size();
    this.placementStatus = getPlacementStatus(placementPolicy, repFactor);
    this.numReplicas = replicas.size();
    this.numKeys = getContainerKeyCount(container.getContainerID());

    this.containerReplicaCount =
        getContainerReplicaCountInstance(conf, replicas);
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
    return containerReplicaCount.isHealthy();
  }

  public boolean isSufficientlyReplicated() {
    return containerReplicaCount.isSufficientlyReplicated();
  }

  public boolean isHealthilyReplicated() {
    return replicaDelta == 0 && !isMisReplicated();
  }

  public boolean isDeleted() {
    return container.getState() == HddsProtos.LifeCycleState.DELETED ||
        container.getState() == HddsProtos.LifeCycleState.DELETING;
  }

  public boolean isOverReplicated() {
    return containerReplicaCount.isOverReplicated();
  }

  public boolean isUnderReplicated() {
    return !isMissing() && !containerReplicaCount.isSufficientlyReplicated();
  }

  public int replicaDelta() {
    return replicaDelta;
  }

  public int getReplicaCount() {
    return healthyAvailReplicas.size();
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

  public boolean areChecksumsMismatched() {
    return !replicas.isEmpty() && replicas.stream()
            .map(ContainerReplica::getChecksums)
            .distinct()
            .count() != 1;
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

  private ContainerReplicaCount getContainerReplicaCountInstance(
      OzoneConfiguration conf, Set<ContainerReplica> containerReplicas) {
    ReplicationManager.ReplicationManagerConfiguration rmConf = conf.getObject(
        ReplicationManager.ReplicationManagerConfiguration.class);
    boolean isEC = container.getReplicationConfig()
                       .getReplicationType() == HddsProtos.ReplicationType.EC;
    return isEC ?
               new ECContainerReplicaCount(container,
                   containerReplicas, new ArrayList<>(),
                   rmConf.getMaintenanceRemainingRedundancy()) :
               // This class ignores unhealthy replicas,
               // therefore set 'considerUnhealthy' to false.
               new RatisContainerReplicaCount(container,
                   containerReplicas, new ArrayList<>(),
                   rmConf.getMaintenanceReplicaMinimum(), false);
  }
}
