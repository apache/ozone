/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ProtocolMessageEnum;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.audit.SCMAction;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.thirdparty.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StorageContainerLocationProtocolService.newReflectiveBlockingService;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.startRpcServer;
import static org.apache.hadoop.hdds.server.ServerUtils.getRemoteUserName;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;

/**
 * The RPC server that listens to requests from clients.
 */
public class SCMClientProtocolServer implements
    StorageContainerLocationProtocol, Auditor {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMClientProtocolServer.class);
  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.SCMLOGGER);
  private final RPC.Server clientRpcServer;
  private final InetSocketAddress clientRpcAddress;
  private final StorageContainerManager scm;
  private final ProtocolMessageMetrics<ProtocolMessageEnum> protocolMetrics;

  public SCMClientProtocolServer(OzoneConfiguration conf,
      StorageContainerManager scm) throws IOException {
    this.scm = scm;
    final int handlerCount =
        conf.getInt(OZONE_SCM_HANDLER_COUNT_KEY,
            OZONE_SCM_HANDLER_COUNT_DEFAULT);
    RPC.setProtocolEngine(conf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);

    protocolMetrics = ProtocolMessageMetrics
        .create("ScmContainerLocationProtocol",
            "SCM ContainerLocation protocol metrics",
            StorageContainerLocationProtocolProtos.Type.values());

    // SCM Container Service RPC
    BlockingService storageProtoPbService =
        newReflectiveBlockingService(
            new StorageContainerLocationProtocolServerSideTranslatorPB(this,
                scm,
                protocolMetrics));

    final InetSocketAddress scmAddress =
        scm.getScmNodeDetails().getClientProtocolServerAddress();
    clientRpcServer =
        startRpcServer(
            conf,
            scmAddress,
            StorageContainerLocationProtocolPB.class,
            storageProtoPbService,
            handlerCount);
    clientRpcAddress =
        updateRPCListenAddress(conf,
            scm.getScmNodeDetails().getClientProtocolServerAddressKey(),
            scmAddress, clientRpcServer);
    if (conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      clientRpcServer.refreshServiceAcl(conf, SCMPolicyProvider.getInstance());
    }
  }

  public RPC.Server getClientRpcServer() {
    return clientRpcServer;
  }

  public InetSocketAddress getClientRpcAddress() {
    return clientRpcAddress;
  }

  public void start() {
    protocolMetrics.register();
    LOG.info(
        StorageContainerManager.buildRpcServerStartMessage(
            "RPC server for Client ", getClientRpcAddress()));
    getClientRpcServer().start();
  }

  public void stop() {
    protocolMetrics.unregister();
    try {
      LOG.info("Stopping the RPC server for Client Protocol");
      getClientRpcServer().stop();
    } catch (Exception ex) {
      LOG.error("Client Protocol RPC stop failed.", ex);
    }
    IOUtils.cleanupWithLogger(LOG, scm.getScmNodeManager());
  }

  public void join() throws InterruptedException {
    LOG.trace("Join RPC server for Client Protocol");
    getClientRpcServer().join();
  }

  public UserGroupInformation getRemoteUser() {
    return Server.getRemoteUser();
  }
  @Override
  public ContainerWithPipeline allocateContainer(HddsProtos.ReplicationType
      replicationType, HddsProtos.ReplicationFactor factor,
      String owner) throws IOException {
    if (scm.getScmContext().isInSafeMode()) {
      throw new SCMException("SafeModePrecheck failed for allocateContainer",
          ResultCodes.SAFE_MODE_EXCEPTION);
    }
    getScm().checkAdminAccess(getRemoteUser());

    final ContainerInfo container = scm.getContainerManager()
        .allocateContainer(
            ReplicationConfig.fromTypeAndFactor(replicationType, factor),
            owner);
    final Pipeline pipeline = scm.getPipelineManager()
        .getPipeline(container.getPipelineID());
    return new ContainerWithPipeline(container, pipeline);
  }

  @Override
  public ContainerInfo getContainer(long containerID) throws IOException {
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", String.valueOf(containerID));
    getScm().checkAdminAccess(getRemoteUser());
    try {
      return scm.getContainerManager()
          .getContainer(ContainerID.valueOf(containerID));
    } catch (IOException ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.GET_CONTAINER, auditMap, ex)
      );
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.GET_CONTAINER, auditMap)
        );
      }
    }

  }

  private ContainerWithPipeline getContainerWithPipelineCommon(
      long containerID) throws IOException {
    final ContainerID cid = ContainerID.valueOf(containerID);
    final ContainerInfo container = scm.getContainerManager()
        .getContainer(cid);

    if (scm.getScmContext().isInSafeMode()) {
      if (container.isOpen()) {
        if (!hasRequiredReplicas(container)) {
          throw new SCMException("Open container " + containerID + " doesn't"
              + " have enough replicas to service this operation in "
              + "Safe mode.", ResultCodes.SAFE_MODE_EXCEPTION);
        }
      }
    }

    Pipeline pipeline;
    try {
      pipeline = container.isOpen() ? scm.getPipelineManager()
          .getPipeline(container.getPipelineID()) : null;
    } catch (PipelineNotFoundException ex) {
      // The pipeline is destroyed.
      pipeline = null;
    }

    if (pipeline == null) {
      pipeline = scm.getPipelineManager().createPipeline(
          new StandaloneReplicationConfig(container.getReplicationFactor()),
          scm.getContainerManager()
              .getContainerReplicas(cid).stream()
              .map(ContainerReplica::getDatanodeDetails)
              .collect(Collectors.toList()));
    }

    return new ContainerWithPipeline(container, pipeline);
  }

  @Override
  public ContainerWithPipeline getContainerWithPipeline(long containerID)
      throws IOException {
    getScm().checkAdminAccess(null);

    try {
      ContainerWithPipeline cp = getContainerWithPipelineCommon(containerID);
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.GET_CONTAINER_WITH_PIPELINE,
          Collections.singletonMap("containerID",
          ContainerID.valueOf(containerID).toString())));
      return cp;
    } catch (IOException ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_CONTAINER_WITH_PIPELINE,
          Collections.singletonMap("containerID",
              ContainerID.valueOf(containerID).toString()), ex));
      throw ex;
    }
  }

  @Override
  public List<ContainerWithPipeline> getContainerWithPipelineBatch(
      List<Long> containerIDs) throws IOException {
    getScm().checkAdminAccess(null);

    List<ContainerWithPipeline> cpList = new ArrayList<>();

    StringBuilder strContainerIDs = new StringBuilder();
    for (Long containerID : containerIDs) {
      try {
        ContainerWithPipeline cp = getContainerWithPipelineCommon(containerID);
        cpList.add(cp);
        strContainerIDs.append(ContainerID.valueOf(containerID).toString());
        strContainerIDs.append(",");
      } catch (IOException ex) {
        AUDIT.logReadFailure(buildAuditMessageForFailure(
            SCMAction.GET_CONTAINER_WITH_PIPELINE_BATCH,
            Collections.singletonMap("containerID",
                ContainerID.valueOf(containerID).toString()), ex));
        throw ex;
      }
    }


    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
        SCMAction.GET_CONTAINER_WITH_PIPELINE_BATCH,
        Collections.singletonMap("containerIDs", strContainerIDs.toString())));

    return cpList;
  }

  @Override
  public List<ContainerWithPipeline> getExistContainerWithPipelinesInBatch(
      List<Long> containerIDs) {
    List<ContainerWithPipeline> cpList = new ArrayList<>();
    for (Long containerID : containerIDs) {
      try {
        ContainerWithPipeline cp = getContainerWithPipelineCommon(containerID);
        cpList.add(cp);
      } catch (IOException ex) {
        //not found , just go ahead
      }
    }
    return cpList;
  }

  /**
   * Check if container reported replicas are equal or greater than required
   * replication factor.
   */
  private boolean hasRequiredReplicas(ContainerInfo contInfo) {
    try{
      return getScm().getContainerManager()
          .getContainerReplicas(contInfo.containerID())
          .size() >= contInfo.getReplicationFactor().getNumber();
    } catch (ContainerNotFoundException ex) {
      // getContainerReplicas throws exception if no replica's exist for given
      // container.
      return false;
    }
  }

  /**
   * Lists a range of containers and get their info.
   *
   * @param startContainerID start containerID.
   * @param count count must be {@literal >} 0.
   *
   * @return a list of pipeline.
   * @throws IOException
   */
  @Override
  public List<ContainerInfo> listContainer(long startContainerID,
      int count) throws IOException {
    return listContainer(startContainerID, count, null);
  }

  /**
   * Lists a range of containers and get their info.
   *
   * @param startContainerID start containerID.
   * @param count count must be {@literal >} 0.
   * @param state Container with this state will be returned.
   *
   * @return a list of pipeline.
   * @throws IOException
   */
  @Override
  public List<ContainerInfo> listContainer(long startContainerID,
      int count, HddsProtos.LifeCycleState state) throws IOException {
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("startContainerID", String.valueOf(startContainerID));
    auditMap.put("count", String.valueOf(count));
    if (state != null) {
      auditMap.put("state", state.name());
    }
    try {
      final ContainerID containerId = ContainerID.valueOf(startContainerID);
      if(null == state) {
        return scm.getContainerManager().getContainers(containerId, count);
      }
      return scm.getContainerManager().getContainers(state).stream()
          .filter(info -> info.containerID().getId() >= startContainerID)
          .sorted().limit(count).collect(Collectors.toList());
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.LIST_CONTAINER, auditMap, ex));
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.LIST_CONTAINER, auditMap));
      }
    }
  }

  @Override
  public void deleteContainer(long containerID) throws IOException {
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", String.valueOf(containerID));
    UserGroupInformation remoteUser = getRemoteUser();
    auditMap.put("remoteUser", remoteUser.getUserName());
    try {
      getScm().checkAdminAccess(remoteUser);
      scm.getContainerManager().deleteContainer(
          ContainerID.valueOf(containerID));
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(SCMAction.DELETE_CONTAINER, auditMap, ex)
      );
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(SCMAction.DELETE_CONTAINER, auditMap)
        );
      }
    }
  }

  @Override
  public List<HddsProtos.Node> queryNode(
      HddsProtos.NodeOperationalState opState, HddsProtos.NodeState state,
      HddsProtos.QueryScope queryScope, String poolName, int clientVersion)
      throws IOException {

    if (queryScope == HddsProtos.QueryScope.POOL) {
      throw new IllegalArgumentException("Not Supported yet");
    }

    List<HddsProtos.Node> result = new ArrayList<>();
    for (DatanodeDetails node : queryNode(opState, state)) {
      try {
        NodeStatus ns = scm.getScmNodeManager().getNodeStatus(node);
        result.add(HddsProtos.Node.newBuilder()
            .setNodeID(node.toProto(clientVersion))
            .addNodeStates(ns.getHealth())
            .addNodeOperationalStates(ns.getOperationalState())
            .build());
      } catch (NodeNotFoundException e) {
        throw new IOException(
            "An unexpected error occurred querying the NodeStatus", e);
      }
    }
    return result;
  }

  @Override
  public List<DatanodeAdminError> decommissionNodes(List<String> nodes)
      throws IOException {
    try {
      getScm().checkAdminAccess(getRemoteUser());
      return scm.getScmDecommissionManager().decommissionNodes(nodes);
    } catch (Exception ex) {
      LOG.error("Failed to decommission nodes", ex);
      throw ex;
    }
  }

  @Override
  public List<DatanodeAdminError> recommissionNodes(List<String> nodes)
      throws IOException {
    try {
      getScm().checkAdminAccess(getRemoteUser());
      return scm.getScmDecommissionManager().recommissionNodes(nodes);
    } catch (Exception ex) {
      LOG.error("Failed to recommission nodes", ex);
      throw ex;
    }
  }

  @Override
  public List<DatanodeAdminError> startMaintenanceNodes(List<String> nodes,
      int endInHours) throws IOException {
    try {
      getScm().checkAdminAccess(getRemoteUser());
      return scm.getScmDecommissionManager()
          .startMaintenanceNodes(nodes, endInHours);
    } catch (Exception ex) {
      LOG.error("Failed to place nodes into maintenance mode", ex);
      throw ex;
    }
  }

  @Override
  public void closeContainer(long containerID) throws IOException {
    final UserGroupInformation remoteUser = getRemoteUser();
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", String.valueOf(containerID));
    auditMap.put("remoteUser", remoteUser.getUserName());
    try {
      scm.checkAdminAccess(remoteUser);
      final ContainerID cid = ContainerID.valueOf(containerID);
      final HddsProtos.LifeCycleState state = scm.getContainerManager()
          .getContainer(cid).getState();
      if (!state.equals(HddsProtos.LifeCycleState.OPEN)) {
        throw new SCMException("Cannot close a " + state + " container.",
            ResultCodes.UNEXPECTED_CONTAINER_STATE);
      }
      scm.getEventQueue().fireEvent(SCMEvents.CLOSE_CONTAINER,
          ContainerID.valueOf(containerID));
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.CLOSE_CONTAINER, auditMap));
    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.CLOSE_CONTAINER, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public Pipeline createReplicationPipeline(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, HddsProtos.NodePool nodePool)
      throws IOException {
    Pipeline result = scm.getPipelineManager()
        .createPipeline(ReplicationConfig.fromTypeAndFactor(type, factor));
    AUDIT.logWriteSuccess(
        buildAuditMessageForSuccess(SCMAction.CREATE_PIPELINE, null));
    return result;
  }

  @Override
  public List<Pipeline> listPipelines() {
    AUDIT.logReadSuccess(
        buildAuditMessageForSuccess(SCMAction.LIST_PIPELINE, null));
    return scm.getPipelineManager().getPipelines();
  }

  @Override
  public Pipeline getPipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    return scm.getPipelineManager().getPipeline(
        PipelineID.getFromProtobuf(pipelineID));
  }

  @Override
  public void activatePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
        SCMAction.ACTIVATE_PIPELINE, null));
    scm.getPipelineManager().activatePipeline(
        PipelineID.getFromProtobuf(pipelineID));
  }

  @Override
  public void deactivatePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    getScm().checkAdminAccess(getRemoteUser());
    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
        SCMAction.DEACTIVATE_PIPELINE, null));
    scm.getPipelineManager().deactivatePipeline(
        PipelineID.getFromProtobuf(pipelineID));
  }

  @Override
  public void closePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    getScm().checkAdminAccess(getRemoteUser());
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("pipelineID", pipelineID.getId());
    PipelineManager pipelineManager = scm.getPipelineManager();
    Pipeline pipeline =
        pipelineManager.getPipeline(PipelineID.getFromProtobuf(pipelineID));
    pipelineManager.closePipeline(pipeline, true);
    AUDIT.logWriteSuccess(
        buildAuditMessageForSuccess(SCMAction.CLOSE_PIPELINE, null)
    );
  }

  @Override
  public ScmInfo getScmInfo() throws IOException {
    boolean auditSuccess = true;
    try{
      ScmInfo.Builder builder =
          new ScmInfo.Builder()
              .setClusterId(scm.getScmStorageConfig().getClusterID())
              .setScmId(scm.getScmStorageConfig().getScmId());
      if (scm.getScmHAManager().getRatisServer() != null) {
        builder.setRatisPeerRoles(
            scm.getScmHAManager().getRatisServer().getRatisRoles());
      } else {
        // In case, there is no ratis, there is no ratis role.
        // This will just print the hostname with ratis port as the default
        // behaviour.
        String adddress = scm.getSCMHANodeDetails().getLocalNodeDetails()
            .getRatisHostPortStr();
        builder.setRatisPeerRoles(Arrays.asList(adddress));
      }
      return builder.build();
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.GET_SCM_INFO, null, ex)
      );
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.GET_SCM_INFO, null)
        );
      }
    }
  }

  /**
   * Check if SCM is in safe mode.
   *
   * @return Returns true if SCM is in safe mode else returns false.
   * @throws IOException
   */
  @Override
  public boolean inSafeMode() throws IOException {
    AUDIT.logReadSuccess(
        buildAuditMessageForSuccess(SCMAction.IN_SAFE_MODE, null)
    );
    return scm.isInSafeMode();
  }

  @Override
  public Map<String, Pair<Boolean, String>> getSafeModeRuleStatuses()
      throws IOException {
    return scm.getRuleStatus();
  }

  /**
   * Force SCM out of Safe mode.
   *
   * @return returns true if operation is successful.
   * @throws IOException
   */
  @Override
  public boolean forceExitSafeMode() throws IOException {
    getScm().checkAdminAccess(getRemoteUser());
    AUDIT.logWriteSuccess(
        buildAuditMessageForSuccess(SCMAction.FORCE_EXIT_SAFE_MODE, null)
    );
    return scm.exitSafeMode();
  }

  @Override
  public void startReplicationManager() throws IOException {
    getScm().checkAdminAccess(getRemoteUser());
    AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
        SCMAction.START_REPLICATION_MANAGER, null));
    scm.getReplicationManager().start();
  }

  @Override
  public void stopReplicationManager() throws IOException {
    getScm().checkAdminAccess(getRemoteUser());
    AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
        SCMAction.STOP_REPLICATION_MANAGER, null));
    scm.getReplicationManager().stop();
  }

  @Override
  public boolean getReplicationManagerStatus() {
    AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
        SCMAction.GET_REPLICATION_MANAGER_STATUS, null));
    return scm.getReplicationManager().isRunning();
  }

  /**
   * Get Datanode usage info such as capacity, SCMUsed, and remaining by ip
   * or uuid.
   *
   * @param ipaddress Datanode Address String
   * @param uuid Datanode UUID String
   * @return List of DatanodeUsageInfoProto. Each element contains usage info
   * such as capacity, SCMUsed, and remaining space.
   * @throws IOException if admin authentication fails
   */
  @Override
  public List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
      String ipaddress, String uuid) throws IOException {

    // check admin authorisation
    try {
      getScm().checkAdminAccess(getRemoteUser());
    } catch (IOException e) {
      LOG.error("Authorisation failed", e);
      throw e;
    }

    // get datanodes by ip or uuid
    List<DatanodeDetails> nodes = new ArrayList<>();
    if (!Strings.isNullOrEmpty(uuid)) {
      nodes.add(scm.getScmNodeManager().getNodeByUuid(uuid));
    } else if (!Strings.isNullOrEmpty(ipaddress)) {
      nodes = scm.getScmNodeManager().getNodesByAddress(ipaddress);
    } else {
      throw new IOException(
          "Could not get datanode with the specified parameters."
      );
    }

    // get datanode usage info
    List<HddsProtos.DatanodeUsageInfoProto> infoList = new ArrayList<>();
    for (DatanodeDetails node : nodes) {
      infoList.add(getUsageInfoFromDatanodeDetails(node));
    }

    return infoList;
  }

  /**
   * Get usage details for a specific DatanodeDetails node.
   *
   * @param node DatanodeDetails
   * @return Usage info such as capacity, SCMUsed, and remaining space.
   */
  private HddsProtos.DatanodeUsageInfoProto getUsageInfoFromDatanodeDetails(
      DatanodeDetails node) {
    SCMNodeStat stat = scm.getScmNodeManager().getNodeStat(node).get();

    long capacity = stat.getCapacity().get();
    long used = stat.getScmUsed().get();
    long remaining = stat.getRemaining().get();

    return HddsProtos.DatanodeUsageInfoProto.newBuilder()
        .setCapacity(capacity)
        .setUsed(used)
        .setRemaining(remaining)
        .setNode(node.toProto(node.getCurrentVersion()))
        .build();
  }

  /**
   * Get a sorted list of most or least used DatanodeUsageInfo containing
   * healthy, in-service nodes.
   *
   * @param mostUsed true if most used, false if least used
   * @param count number of nodes to get; must be an integer greater than zero
   * @return List of DatanodeUsageInfoProto. Each element contains usage info
   * such as capacity, SCMUsed, and remaining space.
   * @throws IOException if admin authentication fails
   * @throws IllegalArgumentException if count is not an integer greater than
   * zero
   */
  @Override
  public List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
      boolean mostUsed, int count) throws IOException, IllegalArgumentException{

    // check admin authorisation
    try {
      getScm().checkAdminAccess(getRemoteUser());
    } catch (IOException e) {
      LOG.error("Authorisation failed", e);
      throw e;
    }

    if (count < 1) {
      throw new IllegalArgumentException("The specified parameter count must " +
          "be an integer greater than zero.");
    }

    List<DatanodeUsageInfo> datanodeUsageInfoList =
        scm.getScmNodeManager().getMostOrLeastUsedDatanodes(mostUsed);

    // if count is greater than the size of list containing healthy,
    // in-service nodes, just set count to that size
    if (count > datanodeUsageInfoList.size()) {
      count = datanodeUsageInfoList.size();
    }

    // return count number of DatanodeUsageInfoProto
    return datanodeUsageInfoList.stream()
        .map(DatanodeUsageInfo::toProto)
        .limit(count)
        .collect(Collectors.toList());
  }

  @Override
  public Token<?> getContainerToken(ContainerID containerID)
      throws IOException {
    UserGroupInformation remoteUser = getRemoteUser();
    getScm().checkAdminAccess(remoteUser);

    return scm.getContainerTokenGenerator()
        .generateToken(remoteUser.getUserName(), containerID);
  }

  /**
   * Queries a list of Node that match a set of statuses.
   *
   * <p>For example, if the nodeStatuses is HEALTHY and RAFT_MEMBER, then
   * this call will return all
   * healthy nodes which members in Raft pipeline.
   *
   * <p>Right now we don't support operations, so we assume it is an AND
   * operation between the
   * operators.
   *
   * @param opState - NodeOperational State
   * @param state - NodeState.
   * @return List of Datanodes.
   */
  public List<DatanodeDetails> queryNode(
      HddsProtos.NodeOperationalState opState, HddsProtos.NodeState state) {
    return new ArrayList<>(queryNodeState(opState, state));
  }

  @VisibleForTesting
  public StorageContainerManager getScm() {
    return scm;
  }

  /**
   * Set safe mode status based on .
   */
  public boolean getSafeModeStatus() {
    return scm.getScmContext().isInSafeMode();
  }


  /**
   * Query the System for Nodes.
   *
   * @params opState - The node operational state
   * @param nodeState - NodeState that we are interested in matching.
   * @return Set of Datanodes that match the NodeState.
   */
  private Set<DatanodeDetails> queryNodeState(
      HddsProtos.NodeOperationalState opState, HddsProtos.NodeState nodeState) {
    Set<DatanodeDetails> returnSet = new TreeSet<>();
    List<DatanodeDetails> tmp = scm.getScmNodeManager()
        .getNodes(opState, nodeState);
    if ((tmp != null) && (tmp.size() > 0)) {
      returnSet.addAll(tmp);
    }
    return returnSet;
  }

  @Override
  public AuditMessage buildAuditMessageForSuccess(
      AuditAction op, Map<String, String> auditMap) {

    return new AuditMessage.Builder()
        .setUser(getRemoteUserName())
        .atIp(Server.getRemoteAddress())
        .forOperation(op)
        .withParams(auditMap)
        .withResult(AuditEventStatus.SUCCESS)
        .build();
  }

  @Override
  public AuditMessage buildAuditMessageForFailure(AuditAction op, Map<String,
      String> auditMap, Throwable throwable) {

    return new AuditMessage.Builder()
        .setUser(getRemoteUserName())
        .atIp(Server.getRemoteAddress())
        .forOperation(op)
        .withParams(auditMap)
        .withResult(AuditEventStatus.FAILURE)
        .withException(throwable)
        .build();
  }

  @Override
  public void close() throws IOException {
    stop();
  }
}
