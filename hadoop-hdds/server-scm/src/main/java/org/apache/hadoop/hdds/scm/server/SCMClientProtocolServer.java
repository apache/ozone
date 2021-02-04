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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmOps;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.safemode.SafeModePrecheck;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
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

import com.google.protobuf.ProtocolMessageEnum;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StorageContainerLocationProtocolService.newReflectiveBlockingService;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.startRpcServer;
import static org.apache.hadoop.hdds.server.ServerUtils.getRemoteUserName;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RPC server that listens to requests from clients.
 */
public class SCMClientProtocolServer implements
    StorageContainerLocationProtocol, Auditor,
    EventHandler<SafeModeStatus> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMClientProtocolServer.class);
  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.SCMLOGGER);
  private final RPC.Server clientRpcServer;
  private final InetSocketAddress clientRpcAddress;
  private final StorageContainerManager scm;
  private final OzoneConfiguration conf;
  private SafeModePrecheck safeModePrecheck;
  private final ProtocolMessageMetrics<ProtocolMessageEnum> protocolMetrics;

  public SCMClientProtocolServer(OzoneConfiguration conf,
      StorageContainerManager scm) throws IOException {
    this.scm = scm;
    this.conf = conf;
    safeModePrecheck = new SafeModePrecheck(conf);
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
                protocolMetrics));

    final InetSocketAddress scmAddress = HddsServerUtil
        .getScmClientBindAddress(conf);
    clientRpcServer =
        startRpcServer(
            conf,
            scmAddress,
            StorageContainerLocationProtocolPB.class,
            storageProtoPbService,
            handlerCount);
    clientRpcAddress =
        updateRPCListenAddress(conf, OZONE_SCM_CLIENT_ADDRESS_KEY,
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

  @VisibleForTesting
  public String getRpcRemoteUsername() {
    return getRemoteUserName();
  }

  @Override
  public ContainerWithPipeline allocateContainer(HddsProtos.ReplicationType
      replicationType, HddsProtos.ReplicationFactor factor,
      String owner) throws IOException {
    ScmUtils.preCheck(ScmOps.allocateContainer, safeModePrecheck);
    getScm().checkAdminAccess(getRpcRemoteUsername());

    final ContainerInfo container = scm.getContainerManager()
        .allocateContainer(replicationType, factor, owner);
    final Pipeline pipeline = scm.getPipelineManager()
        .getPipeline(container.getPipelineID());
    return new ContainerWithPipeline(container, pipeline);
  }

  @Override
  public ContainerInfo getContainer(long containerID) throws IOException {
    String remoteUser = getRpcRemoteUsername();
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", String.valueOf(containerID));
    getScm().checkAdminAccess(remoteUser);
    try {
      return scm.getContainerManager()
          .getContainer(ContainerID.valueof(containerID));
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
    final ContainerID cid = ContainerID.valueof(containerID);
    final ContainerInfo container = scm.getContainerManager()
        .getContainer(cid);

    if (safeModePrecheck.isInSafeMode()) {
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
          HddsProtos.ReplicationType.STAND_ALONE,
          container.getReplicationFactor(),
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
          ContainerID.valueof(containerID).toString())));
      return cp;
    } catch (IOException ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_CONTAINER_WITH_PIPELINE,
          Collections.singletonMap("containerID",
              ContainerID.valueof(containerID).toString()), ex));
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
        strContainerIDs.append(ContainerID.valueof(containerID).toString());
        strContainerIDs.append(",");
      } catch (IOException ex) {
        AUDIT.logReadFailure(buildAuditMessageForFailure(
            SCMAction.GET_CONTAINER_WITH_PIPELINE_BATCH,
            Collections.singletonMap("containerID",
                ContainerID.valueof(containerID).toString()), ex));
        throw ex;
      }
    }


    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
        SCMAction.GET_CONTAINER_WITH_PIPELINE_BATCH,
        Collections.singletonMap("containerIDs", strContainerIDs.toString())));

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

  @Override
  public List<ContainerInfo> listContainer(long startContainerID,
      int count) throws IOException {
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("startContainerID", String.valueOf(startContainerID));
    auditMap.put("count", String.valueOf(count));
    try {
      // To allow startcontainerId to take the value "0",
      // "null" is assigned, so that its handled in the
      // scm.getContainerManager().listContainer method
      final ContainerID containerId = startContainerID != 0 ? ContainerID
          .valueof(startContainerID) : null;
      return scm.getContainerManager().
          listContainer(containerId, count);
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
    String remoteUser = getRpcRemoteUsername();
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", String.valueOf(containerID));
    auditMap.put("remoteUser", remoteUser);
    try {
      getScm().checkAdminAccess(remoteUser);
      scm.getContainerManager().deleteContainer(
          ContainerID.valueof(containerID));
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
  public void decommissionNodes(List<String> nodes) throws IOException {
    String remoteUser = getRpcRemoteUsername();
    try {
      getScm().checkAdminAccess(remoteUser);
      scm.getScmDecommissionManager().decommissionNodes(nodes);
    } catch (Exception ex) {
      LOG.error("Failed to decommission nodes", ex);
      throw ex;
    }
  }

  @Override
  public void recommissionNodes(List<String> nodes) throws IOException {
    String remoteUser = getRpcRemoteUsername();
    try {
      getScm().checkAdminAccess(remoteUser);
      scm.getScmDecommissionManager().recommissionNodes(nodes);
    } catch (Exception ex) {
      LOG.error("Failed to recommission nodes", ex);
      throw ex;
    }
  }

  @Override
  public void startMaintenanceNodes(List<String> nodes, int endInHours)
      throws IOException {
    String remoteUser = getRpcRemoteUsername();
    try {
      getScm().checkAdminAccess(remoteUser);
      scm.getScmDecommissionManager().startMaintenanceNodes(nodes, endInHours);
    } catch (Exception ex) {
      LOG.error("Failed to place nodes into maintenance mode", ex);
      throw ex;
    }
  }

  @Override
  public void closeContainer(long containerID) throws IOException {
    final String remoteUser = getRpcRemoteUsername();
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", String.valueOf(containerID));
    auditMap.put("remoteUser", remoteUser);
    try {
      scm.checkAdminAccess(remoteUser);
      final ContainerID cid = ContainerID.valueof(containerID);
      final HddsProtos.LifeCycleState state = scm.getContainerManager()
          .getContainer(cid).getState();
      if (!state.equals(HddsProtos.LifeCycleState.OPEN)) {
        throw new SCMException("Cannot close a " + state + " container.",
            ResultCodes.UNEXPECTED_CONTAINER_STATE);
      }
      scm.getEventQueue().fireEvent(SCMEvents.CLOSE_CONTAINER,
          ContainerID.valueof(containerID));
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
    Pipeline result = scm.getPipelineManager().createPipeline(type, factor);
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
    String remoteUser = getRemoteUserName();
    getScm().checkAdminAccess(remoteUser);
    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
        SCMAction.DEACTIVATE_PIPELINE, null));
    scm.getPipelineManager().deactivatePipeline(
        PipelineID.getFromProtobuf(pipelineID));
  }

  @Override
  public void closePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    String remoteUser = getRemoteUserName();
    getScm().checkAdminAccess(remoteUser);
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("pipelineID", pipelineID.getId());
    PipelineManager pipelineManager = scm.getPipelineManager();
    Pipeline pipeline =
        pipelineManager.getPipeline(PipelineID.getFromProtobuf(pipelineID));
    pipelineManager.finalizeAndDestroyPipeline(pipeline, true);
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
    String remoteUser = getRemoteUserName();
    getScm().checkAdminAccess(remoteUser);
    AUDIT.logWriteSuccess(
        buildAuditMessageForSuccess(SCMAction.FORCE_EXIT_SAFE_MODE, null)
    );
    return scm.exitSafeMode();
  }

  @Override
  public void startReplicationManager() throws IOException {
    String remoteUser = getRemoteUserName();
    getScm().checkAdminAccess(remoteUser);
    AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
        SCMAction.START_REPLICATION_MANAGER, null));
    scm.getReplicationManager().start();
  }

  @Override
  public void stopReplicationManager() throws IOException {
    String remoteUser = getRemoteUserName();
    getScm().checkAdminAccess(remoteUser);
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
    return safeModePrecheck.isInSafeMode();
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

  @Override
  public void onMessage(SafeModeStatus status,
      EventPublisher publisher) {
    safeModePrecheck.setInSafeMode(status.isInSafeMode());
  }
}
