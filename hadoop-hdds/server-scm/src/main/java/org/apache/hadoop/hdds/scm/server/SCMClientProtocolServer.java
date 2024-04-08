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
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ProtocolMessageEnum;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.ReconfigureProtocolService;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionInfo;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto.Builder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartContainerBalancerResponseProto;
import org.apache.hadoop.hdds.protocolPB.ReconfigureProtocolPB;
import org.apache.hadoop.hdds.protocolPB.ReconfigureProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.common.helpers.DeletedBlocksTransactionInfoWrapper;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancer;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.apache.hadoop.hdds.scm.container.balancer.IllegalContainerBalancerStateException;
import org.apache.hadoop.hdds.scm.container.balancer.InvalidContainerBalancerConfigurationException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.FetchMetrics;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.audit.SCMAction;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StorageContainerLocationProtocolService.newReflectiveBlockingService;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.ScmUtils.checkIfCertSignRequestAllowed;
import static org.apache.hadoop.hdds.scm.ha.HASecurityUtils.createSCMRatisTLSConfig;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.startRpcServer;
import static org.apache.hadoop.hdds.server.ServerUtils.getRemoteUserName;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getRemoteUser;

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
  private final OzoneConfiguration config;
  private final ProtocolMessageMetrics<ProtocolMessageEnum> protocolMetrics;

  public SCMClientProtocolServer(OzoneConfiguration conf,
      StorageContainerManager scm,
      ReconfigurationHandler reconfigurationHandler) throws IOException {
    this.scm = scm;
    this.config = conf;
    final int handlerCount = conf.getInt(OZONE_SCM_CLIENT_HANDLER_COUNT_KEY,
        OZONE_SCM_HANDLER_COUNT_KEY, OZONE_SCM_HANDLER_COUNT_DEFAULT,
            LOG::info);
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

    // Add reconfigureProtocolService.
    ReconfigureProtocolServerSideTranslatorPB reconfigureServerProtocol
        = new ReconfigureProtocolServerSideTranslatorPB(reconfigurationHandler);
    BlockingService reconfigureService =
        ReconfigureProtocolService.newReflectiveBlockingService(
            reconfigureServerProtocol);
    HddsServerUtil.addPBProtocol(conf, ReconfigureProtocolPB.class,
        reconfigureService, clientRpcServer);

    clientRpcAddress =
        updateRPCListenAddress(conf,
            scm.getScmNodeDetails().getClientProtocolServerAddressKey(),
            scmAddress, clientRpcServer);
    if (conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      clientRpcServer.refreshServiceAcl(conf, SCMPolicyProvider.getInstance());
    }
    HddsServerUtil.addSuppressedLoggingExceptions(clientRpcServer);
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

  @Override
  public ContainerWithPipeline allocateContainer(HddsProtos.ReplicationType
      replicationType, HddsProtos.ReplicationFactor factor,
      String owner) throws IOException {
    if (scm.getScmContext().isInSafeMode()) {
      throw new SCMException("SafeModePrecheck failed for allocateContainer",
          ResultCodes.SAFE_MODE_EXCEPTION);
    }
    getScm().checkAdminAccess(getRemoteUser(), false);
    final ContainerInfo container = scm.getContainerManager()
        .allocateContainer(
            ReplicationConfig.fromProtoTypeAndFactor(replicationType, factor),
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
    getScm().checkAdminAccess(getRemoteUser(), true);
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
      if (auditSuccess) {
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
      pipeline = scm.getPipelineManager().createPipelineForRead(
          container.getReplicationConfig(),
          scm.getContainerManager().getContainerReplicas(cid));
    }

    return new ContainerWithPipeline(container, pipeline);
  }

  @Override
  public ContainerWithPipeline getContainerWithPipeline(long containerID)
      throws IOException {
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
  public List<HddsProtos.SCMContainerReplicaProto> getContainerReplicas(
      long containerId, int clientVersion) throws IOException {
    List<HddsProtos.SCMContainerReplicaProto> results = new ArrayList<>();

    Set<ContainerReplica> replicas = getScm().getContainerManager()
        .getContainerReplicas(ContainerID.valueOf(containerId));
    for (ContainerReplica r : replicas) {
      results.add(
          HddsProtos.SCMContainerReplicaProto.newBuilder()
              .setContainerID(containerId)
              .setState(r.getState().toString())
              .setDatanodeDetails(r.getDatanodeDetails().toProto(clientVersion))
              .setBytesUsed(r.getBytesUsed())
              .setPlaceOfBirth(r.getOriginDatanodeId().toString())
              .setKeyCount(r.getKeyCount())
              .setSequenceID(r.getSequenceId())
              .setReplicaIndex(r.getReplicaIndex()).build()
      );
    }
    return results;
  }

  @Override
  public List<ContainerWithPipeline> getContainerWithPipelineBatch(
      Iterable<? extends Long> containerIDs) throws IOException {
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
        LOG.error("Container with common pipeline not found: {}", ex);
      }
    }
    return cpList;
  }

  /**
   * Check if container reported replicas are equal or greater than required
   * replication factor.
   */
  private boolean hasRequiredReplicas(ContainerInfo contInfo) {
    try {
      return getScm().getContainerManager()
          .getContainerReplicas(contInfo.containerID())
          .size() >= contInfo.getReplicationConfig().getRequiredNodes();
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
    return listContainer(startContainerID, count, null, null, null);
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
    return listContainer(startContainerID, count, state, null, null);
  }

  /**
   * Lists a range of containers and get their info.
   *
   * @param startContainerID start containerID.
   * @param count count must be {@literal >} 0.
   * @param state Container with this state will be returned.
   * @param factor Container factor.
   * @return a list of pipeline.
   * @throws IOException
   */
  @Override
  @Deprecated
  public List<ContainerInfo> listContainer(long startContainerID,
      int count, HddsProtos.LifeCycleState state,
      HddsProtos.ReplicationFactor factor) throws IOException {
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("startContainerID", String.valueOf(startContainerID));
    auditMap.put("count", String.valueOf(count));
    if (state != null) {
      auditMap.put("state", state.name());
    }
    if (factor != null) {
      auditMap.put("factor", factor.name());
    }
    try {
      final ContainerID containerId = ContainerID.valueOf(startContainerID);
      if (state != null) {
        if (factor != null) {
          Stream<ContainerInfo> containerInfoStream =
              scm.getContainerManager().getContainers(state).stream()
                .filter(info -> info.containerID().getId() >= startContainerID)
                //Filtering EC replication type as EC will not have factor.
                .filter(info -> info
                    .getReplicationType() != HddsProtos.ReplicationType.EC)
                .filter(info -> (info.getReplicationFactor() == factor))
                .sorted();
          return count == -1 ? containerInfoStream.collect(Collectors.toList()) :
              containerInfoStream.limit(count).collect(Collectors.toList());
        } else {
          Stream<ContainerInfo> containerInfoStream =
              scm.getContainerManager().getContainers(state).stream()
                  .filter(info -> info.containerID().getId() >= startContainerID)
                  .sorted();
          return count == -1 ? containerInfoStream.collect(Collectors.toList()) :
              containerInfoStream.limit(count).collect(Collectors.toList());
        }
      } else {
        if (factor != null) {
          Stream<ContainerInfo> containerInfoStream =
              scm.getContainerManager().getContainers().stream()
                  .filter(info -> info.containerID().getId() >= startContainerID)
                  //Filtering EC replication type as EC will not have factor.
                  .filter(info -> info
                      .getReplicationType() != HddsProtos.ReplicationType.EC)
                  .filter(info -> info.getReplicationFactor() == factor)
                  .sorted();
          return count == -1 ? containerInfoStream.collect(Collectors.toList())
              : containerInfoStream.limit(count).collect(Collectors.toList());
        } else {
          return scm.getContainerManager().getContainers(containerId, count);
        }
      }
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.LIST_CONTAINER, auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.LIST_CONTAINER, auditMap));
      }
    }
  }

  /**
   * Lists a range of containers and get their info.
   *
   * @param startContainerID start containerID.
   * @param count count must be {@literal >} 0.
   * @param state Container with this state will be returned.
   * @param repConfig Replication Config for the container.
   * @return a list of pipeline.
   * @throws IOException
   */
  @Override
  public List<ContainerInfo> listContainer(long startContainerID,
      int count, HddsProtos.LifeCycleState state,
      HddsProtos.ReplicationType replicationType,
      ReplicationConfig repConfig) throws IOException {
    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("startContainerID", String.valueOf(startContainerID));
    auditMap.put("count", String.valueOf(count));
    if (state != null) {
      auditMap.put("state", state.name());
    }
    if (replicationType != null) {
      auditMap.put("replicationType", replicationType.toString());
    }
    if (repConfig != null) {
      auditMap.put("replicationConfig", repConfig.toString());
    }
    try {
      final ContainerID containerId = ContainerID.valueOf(startContainerID);
      if (state == null && replicationType == null && repConfig == null) {
        // Not filters, so just return everything
        return scm.getContainerManager().getContainers(containerId, count);
      }

      List<ContainerInfo> containerList;
      if (state != null) {
        containerList = scm.getContainerManager().getContainers(state);
      } else {
        containerList = scm.getContainerManager().getContainers();
      }

      Stream<ContainerInfo> containerStream = containerList.stream()
          .filter(info -> info.containerID().getId() >= startContainerID);
      // If we have repConfig filter by it, as it includes repType too.
      // Otherwise, we may have a filter just for repType, eg all EC containers
      // without filtering on their replication scheme
      if (repConfig != null) {
        containerStream = containerStream
            .filter(info -> info.getReplicationConfig().equals(repConfig));
      } else if (replicationType != null) {
        containerStream = containerStream
            .filter(info -> info.getReplicationType() == replicationType);
      }
      Stream<ContainerInfo> containerInfoStream = containerStream.sorted();
      return count == -1 ? containerInfoStream.collect(Collectors.toList()) :
          containerInfoStream.limit(count).collect(Collectors.toList());
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.LIST_CONTAINER, auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.LIST_CONTAINER, auditMap));
      }
    }
  }

  @Override
  public void deleteContainer(long containerID) throws IOException {
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", String.valueOf(containerID));
    UserGroupInformation remoteUser = getRemoteUser();
    auditMap.put("remoteUser", remoteUser.getUserName());
    try {
      getScm().checkAdminAccess(remoteUser, false);
      scm.getContainerManager().deleteContainer(
          ContainerID.valueOf(containerID));
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.DELETE_CONTAINER, auditMap));
    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.DELETE_CONTAINER, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public Map<String, List<ContainerID>> getContainersOnDecomNode(DatanodeDetails dn) throws IOException {
    try {
      return scm.getScmDecommissionManager().getContainersPendingReplication(dn);
    } catch (NodeNotFoundException e) {
      throw new IOException("Failed to get containers list. Unable to find required node", e);
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
  public HddsProtos.Node queryNode(UUID uuid)
      throws IOException {
    HddsProtos.Node result = null;
    try {
      DatanodeDetails node = scm.getScmNodeManager().getNodeByUuid(uuid);
      if (node != null) {
        NodeStatus ns = scm.getScmNodeManager().getNodeStatus(node);
        result = HddsProtos.Node.newBuilder()
            .setNodeID(node.getProtoBufMessage())
            .addNodeStates(ns.getHealth())
            .addNodeOperationalStates(ns.getOperationalState())
            .build();
      }
    } catch (NodeNotFoundException e) {
      throw new IOException(
          "An unexpected error occurred querying the NodeStatus", e);
    }
    return result;
  }

  @Override
  public List<DatanodeAdminError> decommissionNodes(List<String> nodes, boolean force)
      throws IOException {
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      return scm.getScmDecommissionManager().decommissionNodes(nodes, force);
    } catch (Exception ex) {
      LOG.error("Failed to decommission nodes", ex);
      throw ex;
    }
  }

  @Override
  public List<DatanodeAdminError> recommissionNodes(List<String> nodes)
      throws IOException {
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
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
      getScm().checkAdminAccess(getRemoteUser(), false);
      return scm.getScmDecommissionManager()
          .startMaintenanceNodes(nodes, endInHours);
    } catch (Exception ex) {
      LOG.error("Failed to place nodes into maintenance mode", ex);
      throw ex;
    }
  }

  @Override
  public void closeContainer(long containerID) throws IOException {
    getScm().checkAdminAccess(getRemoteUser(), false);
    final UserGroupInformation remoteUser = getRemoteUser();
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", String.valueOf(containerID));
    auditMap.put("remoteUser", remoteUser.getUserName());
    try {
      scm.checkAdminAccess(remoteUser, false);
      final ContainerID cid = ContainerID.valueOf(containerID);
      final HddsProtos.LifeCycleState state = scm.getContainerManager()
          .getContainer(cid).getState();
      if (!state.equals(HddsProtos.LifeCycleState.OPEN)) {
        ResultCodes resultCode = ResultCodes.UNEXPECTED_CONTAINER_STATE;
        if (state.equals(HddsProtos.LifeCycleState.CLOSED)) {
          resultCode = ResultCodes.CONTAINER_ALREADY_CLOSED;
        }
        if (state.equals(HddsProtos.LifeCycleState.CLOSING)) {
          resultCode = ResultCodes.CONTAINER_ALREADY_CLOSING;
        }
        throw new SCMException("Cannot close a " + state + " container.",
            resultCode);
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
    getScm().checkAdminAccess(getRemoteUser(), false);
    Map<String, String> auditMap = Maps.newHashMap();
    if (type != null) {
      auditMap.put("replicationType", type.toString());
    }
    if (factor != null) {
      auditMap.put("replicationFactor", factor.toString());
    }
    if (nodePool != null && !nodePool.getNodesList().isEmpty()) {
      List<String> nodeIpAddresses = new ArrayList<>();
      for (HddsProtos.Node node : nodePool.getNodesList()) {
        nodeIpAddresses.add(node.getNodeID().getIpAddress());
      }
      auditMap.put("nodePool", String.join(", ", nodeIpAddresses));
    }
    try {
      Pipeline result = scm.getPipelineManager().createPipeline(
          ReplicationConfig.fromProtoTypeAndFactor(type, factor));
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.CREATE_PIPELINE, auditMap));
      return result;
    } catch (SCMException e) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.CREATE_PIPELINE, auditMap, e));
      throw e;
    }
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
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("pipelineID", pipelineID.getId());
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      scm.getPipelineManager().activatePipeline(
          PipelineID.getFromProtobuf(pipelineID));
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.ACTIVATE_PIPELINE, auditMap));
    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.ACTIVATE_PIPELINE, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public void deactivatePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("pipelineID", pipelineID.getId());
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      scm.getPipelineManager().deactivatePipeline(
          PipelineID.getFromProtobuf(pipelineID));
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.DEACTIVATE_PIPELINE, auditMap));
    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.DEACTIVATE_PIPELINE, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public void closePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    Map<String, String> auditMap = Maps.newHashMap();
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      auditMap.put("pipelineID", pipelineID.getId());
      PipelineManager pipelineManager = scm.getPipelineManager();
      Pipeline pipeline =
          pipelineManager.getPipeline(PipelineID.getFromProtobuf(pipelineID));
      pipelineManager.closePipeline(pipeline, true);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.CLOSE_PIPELINE, auditMap));
    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.CLOSE_PIPELINE, auditMap, ex));
    }
  }

  @Override
  public ScmInfo getScmInfo() {
    boolean auditSuccess = true;
    try {
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
        String address = scm.getSCMHANodeDetails().getLocalNodeDetails()
            .getRatisHostPortStr();
        builder.setRatisPeerRoles(Arrays.asList(address));
      }
      return builder.build();
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.GET_SCM_INFO, null, ex)
      );
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.GET_SCM_INFO, null)
        );
      }
    }
  }

  @Override
  public void transferLeadership(String newLeaderId)
      throws IOException {
    getScm().checkAdminAccess(getRemoteUser(), false);
    if (!SCMHAUtils.isSCMHAEnabled(getScm().getConfiguration())) {
      throw new SCMException("SCM HA not enabled.", ResultCodes.INTERNAL_ERROR);
    }

    checkIfCertSignRequestAllowed(scm.getRootCARotationManager(),
        false, config, "transferLeadership");

    boolean auditSuccess = true;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("newLeaderId", newLeaderId);
    try {
      SCMRatisServer scmRatisServer = scm.getScmHAManager().getRatisServer();
      RaftGroup group = scmRatisServer.getDivision().getGroup();
      RaftPeerId targetPeerId;
      if (newLeaderId.isEmpty()) {
        RaftPeer curLeader = ((SCMRatisServerImpl) scm.getScmHAManager()
            .getRatisServer()).getLeader();
        targetPeerId = group.getPeers()
            .stream()
            .filter(a -> !a.equals(curLeader)).findFirst()
            .map(RaftPeer::getId)
            .orElseThrow(() -> new IOException("Cannot" +
                " find a new leader to transfer leadership."));
      } else {
        targetPeerId = RaftPeerId.valueOf(newLeaderId);
      }
      final GrpcTlsConfig tlsConfig =
          createSCMRatisTLSConfig(new SecurityConfig(scm.getConfiguration()),
          scm.getScmCertificateClient());

      RatisHelper.transferRatisLeadership(scm.getConfiguration(), group,
          targetPeerId, tlsConfig);
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.TRANSFER_LEADERSHIP, auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(buildAuditMessageForSuccess(
            SCMAction.TRANSFER_LEADERSHIP, auditMap));
      }
    }
  }

  public List<DeletedBlocksTransactionInfo> getFailedDeletedBlockTxn(int count,
      long startTxId) throws IOException {
    List<DeletedBlocksTransactionInfo> result;
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("count", String.valueOf(count));
    auditMap.put("startTxId", String.valueOf(startTxId));
    try {
      result = scm.getScmBlockManager().getDeletedBlockLog()
          .getFailedTransactions(count, startTxId).stream()
          .map(DeletedBlocksTransactionInfoWrapper::fromTxn)
          .collect(Collectors.toList());
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.GET_FAILED_DELETED_BLOCKS_TRANSACTION, auditMap));
      return result;
    } catch (IOException ex) {
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(
              SCMAction.GET_FAILED_DELETED_BLOCKS_TRANSACTION, auditMap, ex)
      );
      throw ex;
    }
  }

  @Override
  public int resetDeletedBlockRetryCount(List<Long> txIDs) throws IOException {
    Map<String, String> auditMap = Maps.newHashMap();
    getScm().checkAdminAccess(getRemoteUser(), false);
    try {
      int count = scm.getScmBlockManager().getDeletedBlockLog().
          resetCount(txIDs);
      auditMap.put("txIDs", txIDs.toString());
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.RESET_DELETED_BLOCK_RETRY_COUNT, auditMap));
      return count;
    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.RESET_DELETED_BLOCK_RETRY_COUNT, auditMap, ex));
      throw ex;
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
    getScm().checkAdminAccess(getRemoteUser(), false);
    AUDIT.logWriteSuccess(
        buildAuditMessageForSuccess(SCMAction.FORCE_EXIT_SAFE_MODE, null)
    );
    return scm.exitSafeMode();
  }

  @Override
  public void startReplicationManager() throws IOException {
    getScm().checkAdminAccess(getRemoteUser(), false);
    AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
        SCMAction.START_REPLICATION_MANAGER, null));
    scm.getReplicationManager().start();
  }

  @Override
  public void stopReplicationManager() throws IOException {
    getScm().checkAdminAccess(getRemoteUser(), false);
    AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
        SCMAction.STOP_REPLICATION_MANAGER, null));
    scm.getReplicationManager().stop();
  }

  @Override
  public boolean getReplicationManagerStatus() {
    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
        SCMAction.GET_REPLICATION_MANAGER_STATUS, null));
    return scm.getReplicationManager().isRunning();
  }

  @Override
  public ReplicationManagerReport getReplicationManagerReport() {
    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
        SCMAction.GET_REPLICATION_MANAGER_REPORT, null));
    return scm.getReplicationManager().getContainerReport();
  }

  @Override
  public StatusAndMessages finalizeScmUpgrade(String upgradeClientID) throws
      IOException {
    // check admin authorization
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
    } catch (IOException e) {
      LOG.error("Authorization failed for finalize scm upgrade", e);
      throw e;
    }
    // TODO HDDS-6762: Return to the client once the FINALIZATION_STARTED
    //  checkpoint has been crossed and continue finalizing asynchronously.
    return scm.getFinalizationManager().finalizeUpgrade(upgradeClientID);
  }

  @Override
  public StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean force, boolean readonly)
      throws IOException {
    if (!readonly) {
      // check admin authorization
      try {
        getScm().checkAdminAccess(getRemoteUser(), true);
      } catch (IOException e) {
        LOG.error("Authorization failed for query scm upgrade finalization " +
            "progress", e);
        throw e;
      }
    }

    return scm.getFinalizationManager()
        .queryUpgradeFinalizationProgress(upgradeClientID, force, readonly);
  }

  @Override
  public StartContainerBalancerResponseProto startContainerBalancer(
      Optional<Double> threshold, Optional<Integer> iterations,
      Optional<Integer> maxDatanodesPercentageToInvolvePerIteration,
      Optional<Long> maxSizeToMovePerIterationInGB,
      Optional<Long> maxSizeEnteringTarget,
      Optional<Long> maxSizeLeavingSource,
      Optional<Integer> balancingInterval,
      Optional<Integer> moveTimeout,
      Optional<Integer> moveReplicationTimeout,
      Optional<Boolean> networkTopologyEnable,
      Optional<String> includeNodes,
      Optional<String> excludeNodes) throws IOException {
    getScm().checkAdminAccess(getRemoteUser(), false);
    ContainerBalancerConfiguration cbc =
        scm.getConfiguration().getObject(ContainerBalancerConfiguration.class);
    Map<String, String> auditMap = Maps.newHashMap();
    try {
      if (threshold.isPresent()) {
        double tsd = threshold.get();
        auditMap.put("threshold", String.valueOf(tsd));
        if (tsd < 0.0D || tsd >= 100.0D) {
          throw new IOException("Threshold should be specified in the range [0.0, 100.0).");
        }
        cbc.setThreshold(tsd);
      }

      if (maxSizeToMovePerIterationInGB.isPresent()) {
        long mstm = maxSizeToMovePerIterationInGB.get();
        auditMap.put("maxSizeToMovePerIterationInGB", String.valueOf(mstm));
        if (mstm <= 0) {
          throw new IOException("Max Size To Move Per Iteration In GB must be positive.");
        }
        cbc.setMaxSizeToMovePerIteration(mstm * OzoneConsts.GB);
      }

      if (maxDatanodesPercentageToInvolvePerIteration.isPresent()) {
        int mdti = maxDatanodesPercentageToInvolvePerIteration.get();
        auditMap.put("maxDatanodesPercentageToInvolvePerIteration",
            String.valueOf(mdti));
        if (mdti < 0 || mdti > 100) {
          throw new IOException("Max Datanodes Percentage To Involve Per Iteration" +
                  "should be specified in the range [0, 100]");
        }
        cbc.setMaxDatanodesPercentageToInvolvePerIteration(mdti);
      }

      if (iterations.isPresent()) {
        int i = iterations.get();
        auditMap.put("iterations", String.valueOf(i));
        if (i < -1 || i == 0) {
          throw new IOException("Number of Iterations must be positive or" +
              " -1 (for running container balancer infinitely).");
        }
        cbc.setIterations(i);
      }

      if (maxSizeEnteringTarget.isPresent()) {
        long mset = maxSizeEnteringTarget.get();
        auditMap.put("maxSizeEnteringTarget", String.valueOf(mset));
        if (mset <= 0) {
          throw new IOException("Max Size Entering Target must be " +
              "greater than zero.");
        }
        cbc.setMaxSizeEnteringTarget(mset * OzoneConsts.GB);
      }

      if (maxSizeLeavingSource.isPresent()) {
        long msls = maxSizeLeavingSource.get();
        auditMap.put("maxSizeLeavingSource", String.valueOf(msls));
        if (msls <= 0) {
          throw new IOException("Max Size Leaving Source must be " +
              "greater than zero.");
        }
        cbc.setMaxSizeLeavingSource(msls * OzoneConsts.GB);
      }

      if (balancingInterval.isPresent()) {
        int bi = balancingInterval.get();
        auditMap.put("balancingInterval", String.valueOf(bi));
        if (bi <= 0) {
          throw new IOException("Balancing Interval must be greater than zero.");
        }
        cbc.setBalancingInterval(Duration.ofMinutes(bi));
      }

      if (moveTimeout.isPresent()) {
        int mt = moveTimeout.get();
        auditMap.put("moveTimeout", String.valueOf(mt));
        if (mt <= 0) {
          throw new IOException("Move Timeout must be greater than zero.");
        }
        cbc.setMoveTimeout(Duration.ofMinutes(mt));
      }

      if (moveReplicationTimeout.isPresent()) {
        int mrt = moveReplicationTimeout.get();
        auditMap.put("moveReplicationTimeout", String.valueOf(mrt));
        if (mrt <= 0) {
          throw new IOException("Move Replication Timeout must be greater than zero.");
        }
        cbc.setMoveReplicationTimeout(Duration.ofMinutes(mrt));
      }

      if (networkTopologyEnable.isPresent()) {
        Boolean nt = networkTopologyEnable.get();
        auditMap.put("networkTopologyEnable", String.valueOf(nt));
        cbc.setNetworkTopologyEnable(nt);
      }

      if (includeNodes.isPresent()) {
        String in = includeNodes.get();
        auditMap.put("includeNodes", (in));
        cbc.setIncludeNodes(in);
      }

      if (excludeNodes.isPresent()) {
        String ex = excludeNodes.get();
        auditMap.put("excludeNodes", (ex));
        cbc.setExcludeNodes(ex);
      }

      ContainerBalancer containerBalancer = scm.getContainerBalancer();
      containerBalancer.startBalancer(cbc);

      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.START_CONTAINER_BALANCER, auditMap));
      return StartContainerBalancerResponseProto.newBuilder()
           .setStart(true)
           .build();
    } catch (IllegalContainerBalancerStateException | IOException |
        InvalidContainerBalancerConfigurationException e) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.START_CONTAINER_BALANCER, auditMap, e));
      return StartContainerBalancerResponseProto.newBuilder()
          .setStart(false)
          .setMessage(e.getMessage())
          .build();
    }
  }

  @Override
  public void stopContainerBalancer() throws IOException {
    getScm().checkAdminAccess(getRemoteUser(), false);
    try {
      scm.getContainerBalancer().stopBalancer();
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.STOP_CONTAINER_BALANCER, null));
    } catch (IllegalContainerBalancerStateException e) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.STOP_CONTAINER_BALANCER, null, e));
    }
  }

  @Override
  public boolean getContainerBalancerStatus() {
    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
        SCMAction.GET_CONTAINER_BALANCER_STATUS, null));
    return scm.getContainerBalancer().isBalancerRunning();
  }

  /**
   * Get Datanode usage info such as capacity, SCMUsed, and remaining by ip
   * or hostname or uuid.
   *
   * @param address Datanode Address String
   * @param uuid Datanode UUID String
   * @return List of DatanodeUsageInfoProto. Each element contains usage info
   * such as capacity, SCMUsed, and remaining space.
   * @throws IOException if admin authentication fails
   */
  @Override
  public List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
      String address, String uuid, int clientVersion) throws IOException {

    // check admin authorisation
    try {
      getScm().checkAdminAccess(getRemoteUser(), true);
    } catch (IOException e) {
      LOG.error("Authorization failed", e);
      throw e;
    }

    // get datanodes by ip or uuid
    List<DatanodeDetails> nodes = new ArrayList<>();
    if (!Strings.isNullOrEmpty(uuid)) {
      nodes.add(scm.getScmNodeManager().getNodeByUuid(uuid));
    } else if (!Strings.isNullOrEmpty(address)) {
      nodes = scm.getScmNodeManager().getNodesByAddress(address);
    } else {
      throw new IOException(
          "Could not get datanode with the specified parameters."
      );
    }

    // get datanode usage info
    List<HddsProtos.DatanodeUsageInfoProto> infoList = new ArrayList<>();
    for (DatanodeDetails node : nodes) {
      infoList.add(getUsageInfoFromDatanodeDetails(node, clientVersion));
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
      DatanodeDetails node, int clientVersion) {
    DatanodeUsageInfo usageInfo = scm.getScmNodeManager().getUsageInfo(node);
    return usageInfo.toProto(clientVersion);
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
      boolean mostUsed, int count, int clientVersion)
      throws IOException, IllegalArgumentException {

    // check admin authorisation
    try {
      getScm().checkAdminAccess(getRemoteUser(), true);
    } catch (IOException e) {
      LOG.error("Authorization failed", e);
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
        .map(each -> each.toProto(clientVersion))
        .limit(count)
        .collect(Collectors.toList());
  }

  @Override
  public Token<?> getContainerToken(ContainerID containerID)
      throws IOException {
    UserGroupInformation remoteUser = getRemoteUser();
    getScm().checkAdminAccess(remoteUser, true);

    return scm.getContainerTokenGenerator()
        .generateToken(remoteUser.getUserName(), containerID);
  }

  @Override
  public long getContainerCount() throws IOException {
    return scm.getContainerManager().getContainers().size();
  }

  @Override
  public long getContainerCount(HddsProtos.LifeCycleState state)
      throws IOException {
    return scm.getContainerManager().getContainers(state).size();
  }

  @Override
  public List<ContainerInfo> getListOfContainers(
      long startContainerID, int count, HddsProtos.LifeCycleState state)
      throws IOException {
    return scm.getContainerManager().getContainers(
        ContainerID.valueOf(startContainerID), count, state);
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

  @Override
  public DecommissionScmResponseProto decommissionScm(
      String scmId) {
    Builder decommissionScmResponseBuilder =
        DecommissionScmResponseProto.newBuilder();

    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      decommissionScmResponseBuilder
          .setSuccess(scm.removePeerFromHARing(scmId));
    } catch (IOException ex) {
      decommissionScmResponseBuilder
          .setSuccess(false)
          .setErrorMsg(ex.getMessage());
    }
    return decommissionScmResponseBuilder.build();
  }

  @Override
  public String getMetrics(String query) throws IOException {
    FetchMetrics fetchMetrics = new FetchMetrics();
    return fetchMetrics.getMetrics(query);
  }
}
