/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartContainerBalancerResponseProto;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT;

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
  private final boolean containerTokenEnabled;
  private final OzoneConfiguration configuration;
  private XceiverClientManager xceiverClientManager;

  public synchronized XceiverClientManager getXceiverClientManager()
      throws IOException {
    if (this.xceiverClientManager == null) {
      this.xceiverClientManager = newXCeiverClientManager(configuration);
    }
    return xceiverClientManager;
  }

  public ContainerOperationClient(OzoneConfiguration conf) {
    this.configuration = conf;
    storageContainerLocationClient = newContainerRpcClient(conf);
    containerSizeB = (int) conf.getStorageSize(OZONE_SCM_CONTAINER_SIZE,
        OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    boolean useRatis = conf.getBoolean(
        ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY,
        ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
    if (useRatis) {
      replicationFactor = HddsProtos.ReplicationFactor.THREE;
      replicationType = HddsProtos.ReplicationType.RATIS;
    } else {
      replicationFactor = HddsProtos.ReplicationFactor.ONE;
      replicationType = HddsProtos.ReplicationType.STAND_ALONE;
    }
    containerTokenEnabled = conf.getBoolean(HDDS_CONTAINER_TOKEN_ENABLED,
        HDDS_CONTAINER_TOKEN_ENABLED_DEFAULT);
  }

  private XceiverClientManager newXCeiverClientManager(ConfigurationSource conf)
      throws IOException {
    XceiverClientManager manager;
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      List<X509Certificate> caCertificates =
          HAUtils.buildCAX509List(null, conf);
      manager = new XceiverClientManager(conf,
          conf.getObject(XceiverClientManager.ScmClientConfig.class),
          caCertificates);
    } else {
      manager = new XceiverClientManager(conf);
    }
    return manager;
  }

  public static StorageContainerLocationProtocol newContainerRpcClient(
      ConfigurationSource configSource) {
    return HAUtils.getScmContainerClient(configSource);
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

  private String getEncodedContainerToken(long containerId) throws IOException {
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
    XceiverClientSpi client = null;
    XceiverClientManager clientManager = getXceiverClientManager();
    try {
      // allocate container on SCM.
      ContainerWithPipeline containerWithPipeline =
          storageContainerLocationClient.allocateContainer(type, factor,
              owner);
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
  public List<HddsProtos.Node> queryNode(
      HddsProtos.NodeOperationalState opState,
      HddsProtos.NodeState nodeState,
      HddsProtos.QueryScope queryScope, String poolName)
      throws IOException {
    return storageContainerLocationClient.queryNode(opState, nodeState,
        queryScope, poolName, ClientVersion.CURRENT_VERSION);
  }

  @Override
  public List<DatanodeAdminError> decommissionNodes(List<String> hosts)
      throws IOException {
    return storageContainerLocationClient.decommissionNodes(hosts);
  }

  @Override
  public List<DatanodeAdminError> recommissionNodes(List<String> hosts)
      throws IOException {
    return storageContainerLocationClient.recommissionNodes(hosts);
  }

  @Override
  public List<DatanodeAdminError> startMaintenanceNodes(List<String> hosts,
      int endHours) throws IOException {
    return storageContainerLocationClient.startMaintenanceNodes(
        hosts, endHours);
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
  public List<ContainerInfo> listContainer(long startContainerID,
      int count) throws IOException {
    return storageContainerLocationClient.listContainer(
        startContainerID, count);
  }

  @Override
  public List<ContainerInfo> listContainer(long startContainerID,
      int count, HddsProtos.LifeCycleState state,
      HddsProtos.ReplicationType repType,
      ReplicationConfig replicationConfig) throws IOException {
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
  public List<ContainerReplicaInfo>
      getContainerReplicas(long containerId) throws IOException {
    List<HddsProtos.SCMContainerReplicaProto> protos =
        storageContainerLocationClient.getContainerReplicas(containerId,
            ClientVersion.CURRENT_VERSION);
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
      Optional<Long> maxSizeLeavingSourceInGB)
      throws IOException {
    return storageContainerLocationClient.startContainerBalancer(threshold,
        iterations, maxDatanodesPercentageToInvolvePerIteration,
        maxSizeToMovePerIterationInGB, maxSizeEnteringTargetInGB,
        maxSizeLeavingSourceInGB);
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
  public List<String> getScmRatisRoles() throws IOException {
    return storageContainerLocationClient.getScmInfo().getRatisPeerRoles();
  }

  @Override
  public int resetDeletedBlockRetryCount(List<Long> txIDs) throws IOException {
    return storageContainerLocationClient.resetDeletedBlockRetryCount(txIDs);
  }

  @Override
  public List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
      String ipaddress, String uuid) throws IOException {
    return storageContainerLocationClient.getDatanodeUsageInfo(ipaddress,
        uuid, ClientVersion.CURRENT_VERSION);
  }

  @Override
  public List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
      boolean mostUsed, int count) throws IOException {
    return storageContainerLocationClient.getDatanodeUsageInfo(mostUsed, count,
        ClientVersion.CURRENT_VERSION);
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
}
