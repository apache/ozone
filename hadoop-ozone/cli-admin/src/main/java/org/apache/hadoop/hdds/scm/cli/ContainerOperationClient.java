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

package org.apache.hadoop.hdds.scm.cli;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocolScm;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartContainerBalancerResponseProto;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerListResult;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.x509.certificate.client.CACertificateProvider;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides the client-facing APIs of container operations.
 */
public class ContainerOperationClient implements ScmClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerOperationClient.class);
  private final long containerSizeB;
  private final HddsProtos.ReplicationFactor replicationFactor;
  private final HddsProtos.ReplicationType replicationType;
  private final StorageContainerLocationProtocol
      storageContainerLocationClient;
  private final SecretKeyProtocolScm secretKeyClient;
  private final boolean containerTokenEnabled;
  private final OzoneConfiguration configuration;
  private XceiverClientManager xceiverClientManager;
  private int maxCountOfContainerList;

  public synchronized XceiverClientManager getXceiverClientManager()
      throws IOException {
    if (this.xceiverClientManager == null) {
      this.xceiverClientManager = newXCeiverClientManager(configuration);
    }
    return xceiverClientManager;
  }

  public ContainerOperationClient(OzoneConfiguration conf) throws IOException {
    this.configuration = conf;
    storageContainerLocationClient = newContainerRpcClient(conf);
    secretKeyClient = newSecretKeyClient(conf);
    containerSizeB = (int) conf.getStorageSize(OZONE_SCM_CONTAINER_SIZE,
        OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    boolean useRatis = conf.getBoolean(
        ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_KEY,
        ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_DEFAULT);
    if (useRatis) {
      replicationFactor = HddsProtos.ReplicationFactor.THREE;
      replicationType = HddsProtos.ReplicationType.RATIS;
    } else {
      replicationFactor = HddsProtos.ReplicationFactor.ONE;
      replicationType = HddsProtos.ReplicationType.STAND_ALONE;
    }
    containerTokenEnabled = conf.getBoolean(HDDS_CONTAINER_TOKEN_ENABLED,
        HDDS_CONTAINER_TOKEN_ENABLED_DEFAULT);
    maxCountOfContainerList = conf
        .getInt(ScmConfigKeys.OZONE_SCM_CONTAINER_LIST_MAX_COUNT,
            ScmConfigKeys.OZONE_SCM_CONTAINER_LIST_MAX_COUNT_DEFAULT);
  }

  private XceiverClientManager newXCeiverClientManager(ConfigurationSource conf)
      throws IOException {
    XceiverClientManager manager;
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      CACertificateProvider caCerts = () -> HAUtils.buildCAX509List(conf);
      manager = new XceiverClientManager(conf,
          conf.getObject(XceiverClientManager.ScmClientConfig.class),
          new ClientTrustManager(caCerts, null));
    } else {
      manager = new XceiverClientManager(conf);
    }
    return manager;
  }

  public static StorageContainerLocationProtocol newContainerRpcClient(
      ConfigurationSource configSource) {
    return HAUtils.getScmContainerClient(configSource);
  }

  public static SecretKeyProtocolScm newSecretKeyClient(
      ConfigurationSource configSource) throws IOException {
    return HddsServerUtil.getSecretKeyClientForSCM(configSource);
  }

  @Override
  public ContainerWithPipeline createContainer(String owner)
      throws IOException {
    XceiverClientSpi client = null;
    XceiverClientManager clientManager = getXceiverClientManager();
    try {
      ContainerWithPipeline containerWithPipeline =
          storageContainerLocationClient.
              allocateContainer(replicationType, replicationFactor, owner);

      Pipeline pipeline = containerWithPipeline.getPipeline();
      client = clientManager.acquireClient(pipeline);

      Preconditions.checkState(
          pipeline.isOpen(),
          "Unexpected state=%s for pipeline=%s, expected state=%s",
          pipeline.getPipelineState(), pipeline.getId(),
          Pipeline.PipelineState.OPEN);
      createContainer(client,
          containerWithPipeline.getContainerInfo().getContainerID());
      return containerWithPipeline;
    } finally {
      if (client != null) {
        clientManager.releaseClient(client, false);
      }
    }
  }

  /**
   * Create a container over pipeline specified by the SCM.
   *
   * @param client      - Client to communicate with Datanodes.
   * @param containerId - Container ID.
   * @throws IOException
   */
  public void createContainer(XceiverClientSpi client,
      long containerId) throws IOException {
    String encodedToken = getEncodedContainerToken(containerId);

    ContainerProtocolCalls.createContainer(client, containerId, encodedToken);

    // Let us log this info after we let SCM know that we have completed the
    // creation state.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created container {} machines {}", containerId,
          client.getPipeline().getNodes());
    }
  }

  public String getEncodedContainerToken(long containerId) throws IOException {
    if (!containerTokenEnabled) {
      return "";
    }
    ContainerID containerID = ContainerID.valueOf(containerId);
    return storageContainerLocationClient.getContainerToken(containerID)
        .encodeToUrlString();
  }

  @Override
  public ContainerWithPipeline createContainer(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, String owner) throws IOException {
    ReplicationConfig replicationConfig =
        ReplicationConfig.fromProtoTypeAndFactor(replicationType, factor);
    return createContainer(replicationConfig, owner);
  }

  @Override
  public ContainerWithPipeline createContainer(ReplicationConfig replicationConfig, String owner) throws IOException {
    XceiverClientSpi client = null;
    XceiverClientManager clientManager = getXceiverClientManager();
    try {
      ContainerWithPipeline containerWithPipeline =
          storageContainerLocationClient.allocateContainer(replicationConfig, owner);
      Pipeline pipeline = containerWithPipeline.getPipeline();
      // connect to pipeline leader and allocate container on leader datanode.
      client = clientManager.acquireClient(pipeline);
      createContainer(client,
          containerWithPipeline.getContainerInfo().getContainerID());
      return containerWithPipeline;
    } finally {
      if (client != null) {
        clientManager.releaseClient(client, false);
      }
    }
  }

  @Override
  public Map<String, List<ContainerID>> getContainersOnDecomNode(DatanodeDetails dn) throws IOException {
    return storageContainerLocationClient.getContainersOnDecomNode(dn);
  }

  @Override
  public List<HddsProtos.Node> queryNode(
      HddsProtos.NodeOperationalState opState,
      HddsProtos.NodeState nodeState,
      HddsProtos.QueryScope queryScope, String poolName)
      throws IOException {
    return storageContainerLocationClient.queryNode(opState, nodeState,
        queryScope, poolName, ClientVersion.CURRENT.serialize());
  }

  @Override
  public HddsProtos.Node queryNode(UUID uuid) throws IOException {
    return storageContainerLocationClient.queryNode(uuid);
  }

  @Override
  public List<DatanodeAdminError> decommissionNodes(List<String> hosts, boolean force)
      throws IOException {
    return storageContainerLocationClient.decommissionNodes(hosts, force);
  }

  @Override
  public List<DatanodeAdminError> recommissionNodes(List<String> hosts)
      throws IOException {
    return storageContainerLocationClient.recommissionNodes(hosts);
  }

  @Override
  public List<DatanodeAdminError> startMaintenanceNodes(List<String> hosts,
      int endHours, boolean force) throws IOException {
    return storageContainerLocationClient.startMaintenanceNodes(
        hosts, endHours, force);
  }

  @Override
  public Pipeline createReplicationPipeline(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, HddsProtos.NodePool nodePool)
      throws IOException {
    return storageContainerLocationClient.createReplicationPipeline(type,
        factor, nodePool);
  }

  @Override
  public List<Pipeline> listPipelines() throws IOException {
    return storageContainerLocationClient.listPipelines();
  }

  @Override
  public Pipeline getPipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    return storageContainerLocationClient.getPipeline(pipelineID);
  }

  @Override
  public void activatePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    storageContainerLocationClient.activatePipeline(pipelineID);
  }

  @Override
  public void deactivatePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    storageContainerLocationClient.deactivatePipeline(pipelineID);
  }

  @Override
  public void closePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    storageContainerLocationClient.closePipeline(pipelineID);
  }

  @Override
  public void close() {
    try {
      if (xceiverClientManager != null) {
        xceiverClientManager.close();
      }
      if (storageContainerLocationClient != null) {
        storageContainerLocationClient.close();
      }
    } catch (Exception ex) {
      LOG.error("Can't close " + this.getClass().getSimpleName(), ex);
    }
  }

  @Override
  public void deleteContainer(long containerId, Pipeline pipeline,
      boolean force) throws IOException {
    XceiverClientSpi client = null;
    XceiverClientManager clientManager = getXceiverClientManager();
    try {
      String encodedToken = getEncodedContainerToken(containerId);

      client = clientManager.acquireClient(pipeline);
      ContainerProtocolCalls
          .deleteContainer(client, containerId, force, encodedToken);
      storageContainerLocationClient
          .deleteContainer(containerId);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleted container {}, machines: {} ", containerId,
            pipeline.getNodes());
      }
    } finally {
      if (client != null) {
        clientManager.releaseClient(client, false);
      }
    }
  }

  @Override
  public void deleteContainer(long containerID, boolean force)
      throws IOException {
    ContainerWithPipeline info = getContainerWithPipeline(containerID);
    deleteContainer(containerID, info.getPipeline(), force);
  }

  @Override
  public ContainerListResult listContainer(long startContainerID,
      int count) throws IOException {
    if (count > maxCountOfContainerList) {
      LOG.warn("Attempting to list {} containers. However, this exceeds" +
          " the cluster's current limit of {}. The results will be capped at the" +
          " maximum allowed count.", count, maxCountOfContainerList);
      count = maxCountOfContainerList;
    }
    return storageContainerLocationClient.listContainer(
        startContainerID, count);
  }

  @Override
  public ContainerListResult listContainer(long startContainerID,
      int count, HddsProtos.LifeCycleState state,
      HddsProtos.ReplicationType repType,
      ReplicationConfig replicationConfig) throws IOException {
    if (count > maxCountOfContainerList) {
      LOG.warn("Attempting to list {} containers. However, this exceeds" +
          " the cluster's current limit of {}. The results will be capped at the" +
          " maximum allowed count.", count, maxCountOfContainerList);
      count = maxCountOfContainerList;
    }
    return storageContainerLocationClient.listContainer(
        startContainerID, count, state, repType, replicationConfig);
  }

  @Override
  public ContainerDataProto readContainer(long containerID,
      Pipeline pipeline) throws IOException {
    XceiverClientManager clientManager = getXceiverClientManager();
    String encodedToken = getEncodedContainerToken(containerID);
    XceiverClientSpi client = null;
    try {
      client = clientManager.acquireClientForReadData(pipeline);
      ReadContainerResponseProto response = ContainerProtocolCalls
          .readContainer(client, containerID, encodedToken);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Read container {}, machines: {} ", containerID,
            pipeline.getNodes());
      }
      return response.getContainerData();
    } finally {
      if (client != null) {
        clientManager.releaseClient(client, false);
      }
    }
  }

  public Map<DatanodeDetails, ReadContainerResponseProto> readContainerFromAllNodes(long containerID, Pipeline pipeline)
      throws IOException, InterruptedException {
    XceiverClientManager clientManager = getXceiverClientManager();
    String encodedToken = getEncodedContainerToken(containerID);
    XceiverClientSpi client = null;
    try {
      client = clientManager.acquireClientForReadData(pipeline);
      Map<DatanodeDetails, ReadContainerResponseProto> responses =
          ContainerProtocolCalls.readContainerFromAllNodes(client, containerID,
              encodedToken);
      return responses;
    } finally {
      if (client != null) {
        clientManager.releaseClient(client, false);
      }
    }
  }

  @Override
  public ContainerDataProto readContainer(long containerID) throws IOException {
    ContainerWithPipeline info = getContainerWithPipeline(containerID);
    return readContainer(containerID, info.getPipeline());
  }

  @Override
  public ContainerInfo getContainer(long containerId) throws
      IOException {
    return storageContainerLocationClient.getContainer(containerId);
  }

  @Override
  public ContainerWithPipeline getContainerWithPipeline(long containerId)
      throws IOException {
    return storageContainerLocationClient.getContainerWithPipeline(containerId);
  }

  @Override
  public List<ContainerReplicaInfo> getContainerReplicas(long containerId) throws IOException {
    List<HddsProtos.SCMContainerReplicaProto> protos =
        storageContainerLocationClient.getContainerReplicas(containerId,
            ClientVersion.CURRENT.serialize());
    List<ContainerReplicaInfo> replicas = new ArrayList<>();
    for (HddsProtos.SCMContainerReplicaProto p : protos) {
      replicas.add(ContainerReplicaInfo.fromProto(p));
    }
    return replicas;
  }

  @Override
  public void closeContainer(long containerId)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Close container {}", containerId);
    }
    storageContainerLocationClient.closeContainer(containerId);
  }

  @Override
  public long getContainerSize(long containerID) throws IOException {
    // TODO : Fix this, it currently returns the capacity
    // but not the current usage.
    return containerSizeB;
  }

  @Override
  public boolean inSafeMode() throws IOException {
    return storageContainerLocationClient.inSafeMode();
  }

  @Override
  public Map<String, Pair<Boolean, String>> getSafeModeRuleStatuses()
      throws IOException {
    return storageContainerLocationClient.getSafeModeRuleStatuses();
  }

  @Override
  public boolean forceExitSafeMode() throws IOException {
    return storageContainerLocationClient.forceExitSafeMode();
  }

  @Override
  public void startReplicationManager() throws IOException {
    storageContainerLocationClient.startReplicationManager();
  }

  @Override
  public void stopReplicationManager() throws IOException {
    storageContainerLocationClient.stopReplicationManager();
  }

  @Override
  public boolean getReplicationManagerStatus() throws IOException {
    return storageContainerLocationClient.getReplicationManagerStatus();
  }

  @Override
  public ReplicationManagerReport getReplicationManagerReport()
      throws IOException {
    return storageContainerLocationClient.getReplicationManagerReport();
  }

  @Override
  public StartContainerBalancerResponseProto startContainerBalancer(
      Optional<Double> threshold, Optional<Integer> iterations,
      Optional<Integer> maxDatanodesPercentageToInvolvePerIteration,
      Optional<Long> maxSizeToMovePerIterationInGB,
      Optional<Long> maxSizeEnteringTargetInGB,
      Optional<Long> maxSizeLeavingSourceInGB,
      Optional<Integer> balancingInterval,
      Optional<Integer> moveTimeout,
      Optional<Integer> moveReplicationTimeout,
      Optional<Boolean> networkTopologyEnable,
      Optional<String> includeNodes,
      Optional<String> excludeNodes) throws IOException {
    return storageContainerLocationClient.startContainerBalancer(threshold,
        iterations, maxDatanodesPercentageToInvolvePerIteration,
        maxSizeToMovePerIterationInGB, maxSizeEnteringTargetInGB,
        maxSizeLeavingSourceInGB, balancingInterval, moveTimeout,
        moveReplicationTimeout, networkTopologyEnable, includeNodes,
        excludeNodes);
  }

  @Override
  public void stopContainerBalancer() throws IOException {
    storageContainerLocationClient.stopContainerBalancer();
  }

  @Override
  public boolean getContainerBalancerStatus() throws IOException {
    return storageContainerLocationClient.getContainerBalancerStatus();
  }

  @Override
  public ContainerBalancerStatusInfoResponseProto getContainerBalancerStatusInfo() throws IOException {
    return storageContainerLocationClient.getContainerBalancerStatusInfo();
  }

  @Override
  public List<String> getScmRoles() throws IOException {
    return storageContainerLocationClient.getScmInfo().getPeerRoles();
  }

  @Override
  public boolean rotateSecretKeys(boolean force) throws IOException {
    return secretKeyClient.checkAndRotate(force);
  }

  @Override
  public void transferLeadership(String newLeaderId) throws IOException {
    storageContainerLocationClient.transferLeadership(newLeaderId);
  }

  @Override
  public DeletedBlocksTransactionSummary getDeletedBlockSummary() throws IOException {
    return storageContainerLocationClient.getDeletedBlockSummary();
  }

  @Override
  public List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
      String address, String uuid) throws IOException {
    return storageContainerLocationClient.getDatanodeUsageInfo(address,
        uuid, ClientVersion.CURRENT.serialize());
  }

  @Override
  public List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
      boolean mostUsed, int count) throws IOException {
    return storageContainerLocationClient.getDatanodeUsageInfo(mostUsed, count,
        ClientVersion.CURRENT.serialize());
  }

  @Override
  public StatusAndMessages finalizeScmUpgrade(String upgradeClientID)
      throws IOException {
    return storageContainerLocationClient.finalizeScmUpgrade(upgradeClientID);
  }

  @Override
  public StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean force, boolean readonly)
      throws IOException {
    return storageContainerLocationClient.queryUpgradeFinalizationProgress(
        upgradeClientID, force, readonly);
  }

  @Override
  public DecommissionScmResponseProto decommissionScm(
      String scmId)
      throws IOException {
    return storageContainerLocationClient.decommissionScm(scmId);
  }

  @Override
  public String getMetrics(String query) throws IOException {
    return storageContainerLocationClient.getMetrics(query);
  }

  @Override
  public void reconcileContainer(long id) throws IOException {
    storageContainerLocationClient.reconcileContainer(id);
  }
}
