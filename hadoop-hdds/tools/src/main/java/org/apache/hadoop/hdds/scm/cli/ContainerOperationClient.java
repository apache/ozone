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

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;
import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForClients;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClient;
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

  public XceiverClientManager getXceiverClientManager() {
    return xceiverClientManager;
  }

  private final XceiverClientManager xceiverClientManager;

  public ContainerOperationClient(OzoneConfiguration conf) throws IOException {
    storageContainerLocationClient = newContainerRpcClient(conf);
    this.xceiverClientManager = newXCeiverClientManager(conf);
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
  }

  @VisibleForTesting
  public StorageContainerLocationProtocol getStorageContainerLocationProtocol() {
    return storageContainerLocationClient;
  }

  private XceiverClientManager newXCeiverClientManager(ConfigurationSource conf)
      throws IOException {
    XceiverClientManager manager;
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      SecurityConfig securityConfig = new SecurityConfig(conf);
      SCMSecurityProtocol scmSecurityProtocolClient = getScmSecurityClient(
          (OzoneConfiguration) securityConfig.getConfiguration());
      String caCertificate =
          scmSecurityProtocolClient.getCACertificate();
      manager = new XceiverClientManager(conf,
          conf.getObject(XceiverClientManager.ScmClientConfig.class),
          caCertificate);
    } else {
      manager = new XceiverClientManager(conf);
    }
    return manager;
  }

  public static StorageContainerLocationProtocol newContainerRpcClient(
      ConfigurationSource configSource) throws IOException {

    Class<StorageContainerLocationProtocolPB> protocol =
        StorageContainerLocationProtocolPB.class;
    Configuration conf =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(configSource);
    RPC.setProtocolEngine(conf, protocol, ProtobufRpcEngine.class);
    long version = RPC.getProtocolVersion(protocol);
    InetSocketAddress scmAddress = getScmAddressForClients(configSource);
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    SocketFactory socketFactory = NetUtils.getDefaultSocketFactory(conf);
    int rpcTimeOut = Client.getRpcTimeout(conf);

    StorageContainerLocationProtocolPB rpcProxy =
        RPC.getProxy(protocol, version, scmAddress, user, conf,
            socketFactory, rpcTimeOut);

    StorageContainerLocationProtocolClientSideTranslatorPB client =
        new StorageContainerLocationProtocolClientSideTranslatorPB(rpcProxy);
    return TracingUtil.createProxy(
        client, StorageContainerLocationProtocol.class, configSource);
  }

  @Override
  public ContainerWithPipeline createContainer(String owner)
      throws IOException {
    XceiverClientSpi client = null;
    try {
      ContainerWithPipeline containerWithPipeline =
          storageContainerLocationClient.
              allocateContainer(replicationType, replicationFactor, owner);

      Pipeline pipeline = containerWithPipeline.getPipeline();
      client = xceiverClientManager.acquireClient(pipeline);

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
        xceiverClientManager.releaseClient(client, false);
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
    ContainerProtocolCalls.createContainer(client, containerId, null);

    // Let us log this info after we let SCM know that we have completed the
    // creation state.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created container " + containerId
          + " machines:" + client.getPipeline().getNodes());
    }
  }

  /**
   * Creates a pipeline over the machines choosen by the SCM.
   *
   * @param client   - Client
   * @param pipeline - pipeline to be createdon Datanodes.
   * @throws IOException
   */
  private void createPipeline(XceiverClientSpi client, Pipeline pipeline)
      throws IOException {

    Preconditions.checkNotNull(pipeline.getId(), "Pipeline " +
        "name cannot be null when client create flag is set.");

    // Pipeline creation is a three step process.
    //
    // 1. Notify SCM that this client is doing a create pipeline on
    // datanodes.
    //
    // 2. Talk to Datanodes to create the pipeline.
    //
    // 3. update SCM that pipeline creation was successful.

    // TODO: this has not been fully implemented on server side
    // SCMClientProtocolServer#notifyObjectStageChange
    // TODO: when implement the pipeline state machine, change
    // the pipeline name (string) to pipeline id (long)
    //storageContainerLocationClient.notifyObjectStageChange(
    //    ObjectStageChangeRequestProto.Type.pipeline,
    //    pipeline.getPipelineName(),
    //    ObjectStageChangeRequestProto.Op.create,
    //    ObjectStageChangeRequestProto.Stage.begin);

    // client.createPipeline();
    // TODO: Use PipelineManager to createPipeline

    //storageContainerLocationClient.notifyObjectStageChange(
    //    ObjectStageChangeRequestProto.Type.pipeline,
    //    pipeline.getPipelineName(),
    //    ObjectStageChangeRequestProto.Op.create,
    //    ObjectStageChangeRequestProto.Stage.complete);

    // TODO : Should we change the state on the client side ??
    // That makes sense, but it is not needed for the client to work.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Pipeline creation successful. Pipeline: {}",
          pipeline);
    }
  }

  @Override
  public ContainerWithPipeline createContainer(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, String owner) throws IOException {
    XceiverClientSpi client = null;
    try {
      // allocate container on SCM.
      ContainerWithPipeline containerWithPipeline =
          storageContainerLocationClient.allocateContainer(type, factor,
              owner);
      Pipeline pipeline = containerWithPipeline.getPipeline();
      // connect to pipeline leader and allocate container on leader datanode.
      client = xceiverClientManager.acquireClient(pipeline);
      createContainer(client,
          containerWithPipeline.getContainerInfo().getContainerID());
      return containerWithPipeline;
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client, false);
      }
    }
  }

  /**
   * Returns a set of Nodes that meet a query criteria.
   *
   * @param opState - The operational state we want the node to have
   *                eg IN_SERVICE, DECOMMISSIONED, etc
   * @param nodeState - The health we want the node to have, eg HEALTHY, STALE,
   *                  etc
   * @param queryScope - Query scope - Cluster or pool.
   * @param poolName - if it is pool, a pool name is required.
   * @return A set of nodes that meet the requested criteria.
   * @throws IOException
   */
  @Override
  public List<HddsProtos.Node> queryNode(
      HddsProtos.NodeOperationalState opState,
      HddsProtos.NodeState nodeState,
      HddsProtos.QueryScope queryScope, String poolName)
      throws IOException {
    return storageContainerLocationClient.queryNode(opState, nodeState,
        queryScope, poolName);
  }

  @Override
  public void decommissionNodes(List<String> hosts) throws IOException {
    storageContainerLocationClient.decommissionNodes(hosts);
  }

  @Override
  public void recommissionNodes(List<String> hosts) throws IOException {
    storageContainerLocationClient.recommissionNodes(hosts);
  }

  @Override
  public void startMaintenanceNodes(List<String> hosts, int endHours)
      throws IOException {
    storageContainerLocationClient.startMaintenanceNodes(hosts, endHours);
  }

  /**
   * Creates a specified replication pipeline.
   */
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
      xceiverClientManager.close();
    } catch (Exception ex) {
      LOG.error("Can't close " + this.getClass().getSimpleName(), ex);
    }
  }

  /**
   * Deletes an existing container.
   *
   * @param containerId - ID of the container.
   * @param pipeline    - Pipeline that represents the container.
   * @param force       - true to forcibly delete the container.
   * @throws IOException
   */
  @Override
  public void deleteContainer(long containerId, Pipeline pipeline,
      boolean force) throws IOException {
    XceiverClientSpi client = null;
    try {
      client = xceiverClientManager.acquireClient(pipeline);
      ContainerProtocolCalls
          .deleteContainer(client, containerId, force, null);
      storageContainerLocationClient
          .deleteContainer(containerId);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleted container {}, machines: {} ", containerId,
            pipeline.getNodes());
      }
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client, false);
      }
    }
  }

  /**
   * Delete the container, this will release any resource it uses.
   *
   * @param containerID - containerID.
   * @param force       - True to forcibly delete the container.
   * @throws IOException
   */
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

  /**
   * Get meta data from an existing container.
   *
   * @param containerID - ID of the container.
   * @param pipeline    - Pipeline where the container is located.
   * @return ContainerInfo
   * @throws IOException
   */
  @Override
  public ContainerDataProto readContainer(long containerID,
      Pipeline pipeline) throws IOException {
    XceiverClientSpi client = null;
    try {
      client = xceiverClientManager.acquireClientForReadData(pipeline);
      ReadContainerResponseProto response =
          ContainerProtocolCalls.readContainer(client, containerID, null);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Read container {}, machines: {} ", containerID,
            pipeline.getNodes());
      }
      return response.getContainerData();
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client, false);
      }
    }
  }

  /**
   * Get meta data from an existing container.
   *
   * @param containerID - ID of the container.
   * @return ContainerInfo - a message of protobuf which has basic info
   * of a container.
   * @throws IOException
   */
  @Override
  public ContainerDataProto readContainer(long containerID) throws IOException {
    ContainerWithPipeline info = getContainerWithPipeline(containerID);
    return readContainer(containerID, info.getPipeline());
  }

  /**
   * Given an id, return the pipeline associated with the container.
   *
   * @param containerId - String Container ID
   * @return Pipeline of the existing container, corresponding to the given id.
   * @throws IOException
   */
  @Override
  public ContainerInfo getContainer(long containerId) throws
      IOException {
    return storageContainerLocationClient.getContainer(containerId);
  }

  /**
   * Gets a container by Name -- Throws if the container does not exist.
   *
   * @param containerId - Container ID
   * @return ContainerWithPipeline
   * @throws IOException
   */
  @Override
  public ContainerWithPipeline getContainerWithPipeline(long containerId)
      throws IOException {
    return storageContainerLocationClient.getContainerWithPipeline(containerId);
  }

  /**
   * Close a container.
   *
   * @throws IOException
   */
  @Override
  public void closeContainer(long containerId)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Close container {}", containerId);
    }
    storageContainerLocationClient.closeContainer(containerId);
  }

  /**
   * Get the the current usage information.
   *
   * @param containerID - ID of the container.
   * @return the size of the given container.
   * @throws IOException
   */
  @Override
  public long getContainerSize(long containerID) throws IOException {
    // TODO : Fix this, it currently returns the capacity
    // but not the current usage.
    return containerSizeB;
  }

  /**
   * Check if SCM is in safe mode.
   *
   * @return Returns true if SCM is in safe mode else returns false.
   * @throws IOException
   */
  public boolean inSafeMode() throws IOException {
    return storageContainerLocationClient.inSafeMode();
  }

  public Map<String, Pair<Boolean, String>> getSafeModeRuleStatuses()
      throws IOException {
    return storageContainerLocationClient.getSafeModeRuleStatuses();
  }

  /**
   * Force SCM out of safe mode.
   *
   * @return returns true if operation is successful.
   * @throws IOException
   */
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

}
