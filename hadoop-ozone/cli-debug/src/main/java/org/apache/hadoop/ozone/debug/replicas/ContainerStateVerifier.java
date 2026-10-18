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

package org.apache.hadoop.ozone.debug.replicas;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

/**
 * Verifies the state of a replica from the DN.
 * [OPEN, CLOSING, QUASI_CLOSED, CLOSED] are considered good states.
 */
public class ContainerStateVerifier implements ReplicaVerifier {
  private static final String CHECK_TYPE = "containerState";
  private static final long DEFAULT_CONTAINER_CACHE_SIZE = 1000000;
  private final ContainerOperationClient containerOperationClient;
  private final XceiverClientManager xceiverClientManager;
  // cache for information about the container from SCM
  private final Cache<Long, ContainerInformation> containerCache;

  private static final Set<ContainerDataProto.State> GOOD_REPLICA_STATES =
      EnumSet.of(
          ContainerDataProto.State.OPEN,
          ContainerDataProto.State.CLOSING,
          ContainerDataProto.State.QUASI_CLOSED,
          ContainerDataProto.State.CLOSED
      );

  private static final Set<HddsProtos.LifeCycleState> GOOD_CONTAINER_STATES =
      EnumSet.of(
          HddsProtos.LifeCycleState.OPEN,
          HddsProtos.LifeCycleState.CLOSING,
          HddsProtos.LifeCycleState.QUASI_CLOSED,
          HddsProtos.LifeCycleState.CLOSED
      );

  public ContainerStateVerifier(OzoneConfiguration conf, long containerCacheSize) throws IOException {
    containerOperationClient = new ContainerOperationClient(conf);
    xceiverClientManager = containerOperationClient.getXceiverClientManager();

    if (containerCacheSize < 1) {
      System.err.println("Invalid cache size provided: " + containerCacheSize +
              ". Falling back to default: " + DEFAULT_CONTAINER_CACHE_SIZE);
      containerCacheSize = DEFAULT_CONTAINER_CACHE_SIZE;
    }
    containerCache = CacheBuilder.newBuilder()
        .maximumSize(containerCacheSize)
        .build();
  }

  @Override
  public String getType() {
    return CHECK_TYPE;
  }

  @Override
  public BlockVerificationResult verifyBlock(DatanodeDetails datanode, OmKeyLocationInfo keyLocation) {
    try {
      StringBuilder replicaCheckMsg = new StringBuilder().append("Replica state is ");
      boolean pass = false;

      long containerID = keyLocation.getContainerID();
      ContainerInformation containerInformation = fetchContainerInformationFromSCM(containerID);
      ContainerDataProto containerData = fetchContainerDataFromDatanode(datanode, containerID,
          keyLocation, containerInformation.getEncodedToken());

      if (containerData == null) {
        return BlockVerificationResult.failIncomplete("No container data returned from DN.");
      }
      ContainerDataProto.State state = containerData.getState();
      replicaCheckMsg.append(state.name());
      boolean replicaStateGood = areContainerAndReplicasInGoodState(state, containerInformation.getContainerState());
      replicaCheckMsg.append(", Container state in SCM is ").append(containerInformation.getContainerState());

      String replicationStatus = containerInformation.getReplicationStatus();
      replicaCheckMsg.append(", ").append(replicationStatus);

      // Replication status check evaluates container-level health by counting healthy replicas
      // across all datanodes. Therefore, when a container is UNDER_REPLICATED or OVER_REPLICATED,
      // this information should be reflected in all replica outputs, not just the unhealthy ones.
      if (replicationStatus.startsWith(ContainerHealthResult.HealthState.HEALTHY.name())
          && replicaStateGood) {
        pass = true;
      }

      if (pass) {
        return BlockVerificationResult.pass();
      } else {
        return BlockVerificationResult.failCheck(replicaCheckMsg.toString());
      }
    } catch (IOException e) {
      if (e.getMessage().contains("ContainerID") && e.getMessage().contains("does not exist")) {
        // if container "does not exist", mark it as failed instead of incomplete
        return BlockVerificationResult.failCheck(e.getMessage());
      }
      return BlockVerificationResult.failIncomplete(e.getMessage());
    }
  }

  private boolean areContainerAndReplicasInGoodState(ContainerDataProto.State replicaState,
      HddsProtos.LifeCycleState containerState) {
    return GOOD_REPLICA_STATES.contains(replicaState) &&
        GOOD_CONTAINER_STATES.contains(containerState);
  }

  private ContainerDataProto fetchContainerDataFromDatanode(DatanodeDetails dn, long containerId,
                                                            OmKeyLocationInfo keyLocation,
                                                            String encodedToken)
      throws IOException {
    XceiverClientSpi client = null;
    ReadContainerResponseProto response;
    try {
      Pipeline pipeline = keyLocation.getPipeline().copyForReadFromNode(dn);

      client = xceiverClientManager.acquireClientForReadData(pipeline);
      response = ContainerProtocolCalls
          .readContainer(client, containerId, encodedToken);
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client, false);
      }
    }

    if (!response.hasContainerData()) {
      return null;
    }
    return response.getContainerData();
  }

  private ContainerInformation fetchContainerInformationFromSCM(long containerId)
      throws IOException {
    ContainerInformation cachedData = containerCache.getIfPresent(containerId);
    if (cachedData != null) {
      return cachedData;
    }
    // Cache miss - fetch container info, token, and compute replication status
    ContainerInfo containerInfo = containerOperationClient.getContainer(containerId);
    String encodeToken = containerOperationClient.getEncodedContainerToken(containerId);
    String replicationStatus = computeReplicationStatus(containerId, containerInfo);
    cachedData = new ContainerInformation(containerInfo.getState(), encodeToken, replicationStatus);
    containerCache.put(containerId, cachedData);
    return cachedData;
  }

  private String computeReplicationStatus(long containerId, ContainerInfo containerInfo) {
    try {
      List<ContainerReplicaInfo> replicaInfos =
          containerOperationClient.getContainerReplicas(containerId);

      if (replicaInfos.isEmpty()) {
        return ContainerHealthResult.HealthState.UNDER_REPLICATED
            + ": no replicas found";
      }

      int requiredNodes =
          containerInfo.getReplicationConfig().getRequiredNodes();
      int healthyReplicas = 0;

      for (ContainerReplicaInfo replicaInfo : replicaInfos) {
        if (!"UNHEALTHY".equals(replicaInfo.getState())) {
          healthyReplicas++;
        }
      }

      if (healthyReplicas == requiredNodes) {
        return ContainerHealthResult.HealthState.HEALTHY.toString();
      }

      ContainerHealthResult.HealthState status =
          healthyReplicas < requiredNodes
              ? ContainerHealthResult.HealthState.UNDER_REPLICATED
              : ContainerHealthResult.HealthState.OVER_REPLICATED;

      return String.format("%s: %d/%d healthy replicas",
          status, healthyReplicas, requiredNodes);
    } catch (Exception e) {
      return "REPLICATION_CHECK_FAILED: " + e.getMessage();
    }
  }

  /** Information from SCM about the container needed for each replica. */
  private static class ContainerInformation {
    private final HddsProtos.LifeCycleState state;
    private final String encodedToken;
    private final String replicationStatus;

    ContainerInformation(HddsProtos.LifeCycleState lifeState, String token, String replicationStatus) {
      this.state = lifeState;
      this.encodedToken = token;
      this.replicationStatus = replicationStatus;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ContainerInformation)) {
        return false;
      }
      ContainerInformation key = (ContainerInformation) o;
      return Objects.equals(state, key.state) &&
          Objects.equals(encodedToken, key.encodedToken) &&
          Objects.equals(replicationStatus, key.replicationStatus);
    }

    @Override
    public int hashCode() {
      return Objects.hash(state, encodedToken, replicationStatus);
    }

    public HddsProtos.LifeCycleState getContainerState() {
      return state;
    }

    public String getEncodedToken() {
      return encodedToken;
    }

    public String getReplicationStatus() {
      return replicationStatus;
    }
  }

}
