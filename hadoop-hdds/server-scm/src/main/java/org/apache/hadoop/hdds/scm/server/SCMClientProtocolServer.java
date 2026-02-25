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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StorageContainerLocationProtocolService.newReflectiveBlockingService;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_READ_THREADPOOL_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_READ_THREADPOOL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.ScmUtils.checkIfCertSignRequestAllowed;
import static org.apache.hadoop.hdds.scm.ha.HASecurityUtils.createSCMRatisTLSConfig;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.startRpcServer;
import static org.apache.hadoop.hdds.server.ServerUtils.getRemoteUserName;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getRemoteUser;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.protobuf.BlockingService;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.ReconfigureProtocolProtos.ReconfigureProtocolService;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto.Builder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartContainerBalancerResponseProto;
import org.apache.hadoop.hdds.protocolPB.ReconfigureProtocolPB;
import org.apache.hadoop.hdds.protocolPB.ReconfigureProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.FetchMetrics;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerListResult;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancer;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerStatusInfo;
import org.apache.hadoop.hdds.scm.container.balancer.IllegalContainerBalancerStateException;
import org.apache.hadoop.hdds.scm.container.balancer.InvalidContainerBalancerConfigurationException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.reconciliation.ReconciliationEligibilityHandler;
import org.apache.hadoop.hdds.scm.container.reconciliation.ReconciliationEligibilityHandler.EligibilityResult;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
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
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.audit.SCMAction;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final ProtocolMessageMetrics<StorageContainerLocationProtocolProtos.Type> protocolMetrics;

  public SCMClientProtocolServer(OzoneConfiguration conf,
      StorageContainerManager scm,
      ReconfigurationHandler reconfigurationHandler) throws IOException {
    this.scm = scm;
    this.config = conf;
    final int handlerCount = conf.getInt(OZONE_SCM_CLIENT_HANDLER_COUNT_KEY,
        OZONE_SCM_HANDLER_COUNT_KEY, OZONE_SCM_HANDLER_COUNT_DEFAULT,
            LOG::info);
    final int readThreads = conf.getInt(OZONE_SCM_CLIENT_READ_THREADPOOL_KEY,
        OZONE_SCM_CLIENT_READ_THREADPOOL_DEFAULT);
    RPC.setProtocolEngine(conf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);

    protocolMetrics = ProtocolMessageMetrics
        .create("ScmContainerLocationProtocol",
            "SCM ContainerLocation protocol metrics",
            StorageContainerLocationProtocolProtos.Type.class);

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
            handlerCount,
            readThreads);

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
    if (conf.getBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
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
    ReplicationConfig replicationConfig =
        ReplicationConfig.fromProtoTypeAndFactor(replicationType, factor);
    return allocateContainer(replicationConfig, owner);
  }

  @Override
  public ContainerWithPipeline allocateContainer(ReplicationConfig replicationConfig, String owner) throws IOException {
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("replicationType", String.valueOf(replicationConfig.getReplicationType()));
    auditMap.put("replication", String.valueOf(replicationConfig.getReplication()));
    auditMap.put("owner", String.valueOf(owner));

    try {
      if (scm.getScmContext().isInSafeMode()) {
        throw new SCMException("SafeModePrecheck failed for allocateContainer",
            ResultCodes.SAFE_MODE_EXCEPTION);
      }
      getScm().checkAdminAccess(getRemoteUser(), false);
      final ContainerInfo container = scm.getContainerManager()
          .allocateContainer(replicationConfig, owner);
      final Pipeline pipeline = scm.getPipelineManager()
          .getPipeline(container.getPipelineID());
      ContainerWithPipeline cp = new ContainerWithPipeline(container, pipeline);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.ALLOCATE_CONTAINER, auditMap)
      );
      return cp;
    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.ALLOCATE_CONTAINER, auditMap, ex)
      );
      throw ex;
    }
  }

  @Override
  public ContainerInfo getContainer(long containerID) throws IOException {
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", String.valueOf(containerID));
    try {
      getScm().checkAdminAccess(getRemoteUser(), true);
      ContainerInfo info = scm.getContainerManager()
          .getContainer(ContainerID.valueOf(containerID));
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.GET_CONTAINER, auditMap)
      );
      return info;
    } catch (IOException ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_CONTAINER, auditMap, ex)
      );
      throw ex;
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
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", ContainerID.valueOf(containerID).toString());
    try {
      ContainerWithPipeline cp = getContainerWithPipelineCommon(containerID);
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.GET_CONTAINER_WITH_PIPELINE, auditMap));
      return cp;
    } catch (IOException ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_CONTAINER_WITH_PIPELINE, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public List<HddsProtos.SCMContainerReplicaProto> getContainerReplicas(
      long containerId, int clientVersion) throws IOException {
    List<HddsProtos.SCMContainerReplicaProto> results = new ArrayList<>();
    Map<String, String> auditMap = new HashMap<>();
    auditMap.put("containerId", String.valueOf(containerId));
    auditMap.put("clientVersion", String.valueOf(clientVersion));

    try {
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
                .setReplicaIndex(r.getReplicaIndex())
                .setDataChecksum(r.getDataChecksum())
                .build()
        );
      }
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.GET_CONTAINER_WITH_PIPELINE_BATCH,
          auditMap));
      return results;
    } catch (Exception ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_CONTAINER_REPLICAS,
          auditMap, ex));
      throw ex;
    }
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
        strContainerIDs.append(',');
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
   * @return a list of containers capped by max count allowed
   * in "ozone.scm.container.list.max.count" and total number of containers.
   * @throws IOException
   */
  @Override
  public ContainerListResult listContainer(long startContainerID,
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
   * @return a list of containers capped by max count allowed
   * in "ozone.scm.container.list.max.count" and total number of containers.
   * @throws IOException
   */
  @Override
  public ContainerListResult listContainer(long startContainerID,
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
   * @return a list of containers capped by max count allowed
   * in "ozone.scm.container.list.max.count" and total number of containers.
   * @throws IOException
   */
  @Override
  @Deprecated
  public ContainerListResult listContainer(long startContainerID,
      int count, HddsProtos.LifeCycleState state,
      HddsProtos.ReplicationFactor factor) throws IOException {
    return listContainerInternal(startContainerID, count, state, factor, null, null);
  }

  private ContainerListResult listContainerInternal(long startContainerID, int count,
      HddsProtos.LifeCycleState state,
      HddsProtos.ReplicationFactor factor,
      HddsProtos.ReplicationType replicationType,
      ReplicationConfig repConfig) throws IOException {
    boolean auditSuccess = true;
    Map<String, String> auditMap = buildAuditMap(startContainerID, count, state, factor, replicationType, repConfig);

    try {
      Stream<ContainerInfo> containerStream =
          buildContainerStream(factor, replicationType, repConfig, getBaseContainerStream(state));
      List<ContainerInfo> containerInfos =
          containerStream.filter(info -> info.containerID().getId() >= startContainerID)
              .sorted().collect(Collectors.toList());
      List<ContainerInfo> limitedContainers =
          containerInfos.stream().limit(count).collect(Collectors.toList());
      long totalCount = (long) containerInfos.size();
      return new ContainerListResult(limitedContainers, totalCount);
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

  private Stream<ContainerInfo> buildContainerStream(HddsProtos.ReplicationFactor factor,
      HddsProtos.ReplicationType replicationType,
      ReplicationConfig repConfig,
      Stream<ContainerInfo> containerStream) {
    if (factor != null) {
      containerStream = containerStream.filter(info -> info.getReplicationType() != HddsProtos.ReplicationType.EC)
          .filter(info -> info.getReplicationFactor() == factor);
    } else if (repConfig != null) {
      // If we have repConfig filter by it, as it includes repType too.
      // Otherwise, we may have a filter just for repType, eg all EC containers
      // without filtering on their replication scheme
      containerStream = containerStream
          .filter(info -> info.getReplicationConfig().equals(repConfig));
    } else if (replicationType != null) {
      containerStream = containerStream.filter(info -> info.getReplicationType() == replicationType);
    }
    return containerStream;
  }

  private Stream<ContainerInfo> getBaseContainerStream(HddsProtos.LifeCycleState state) {
    if (state != null) {
      return scm.getContainerManager().getContainers(state).stream();
    } else {
      return scm.getContainerManager().getContainers().stream();
    }
  }

  private Map<String, String> buildAuditMap(long startContainerID, int count,
      HddsProtos.LifeCycleState state,
      HddsProtos.ReplicationFactor factor,
      HddsProtos.ReplicationType replicationType,
      ReplicationConfig repConfig) {
    Map<String, String> auditMap = new HashMap<>();
    auditMap.put("startContainerID", String.valueOf(startContainerID));
    auditMap.put("count", String.valueOf(count));
    if (state != null) {
      auditMap.put("state", state.name());
    }
    if (factor != null) {
      auditMap.put("factor", factor.name());
    }
    if (replicationType != null) {
      auditMap.put("replicationType", replicationType.toString());
    }
    if (repConfig != null) {
      auditMap.put("replicationConfig", repConfig.toString());
    }

    return auditMap;
  }

  /**
   * Lists a range of containers and get their info.
   *
   * @param startContainerID start containerID.
   * @param count count must be {@literal >} 0.
   * @param state Container with this state will be returned.
   * @param repConfig Replication Config for the container.
   * @return a list of containers capped by max count allowed
   * in "ozone.scm.container.list.max.count" and total number of containers.
   * @throws IOException
   */
  @Override
  public ContainerListResult listContainer(long startContainerID,
      int count, HddsProtos.LifeCycleState state,
      HddsProtos.ReplicationType replicationType,
      ReplicationConfig repConfig) throws IOException {
    return listContainerInternal(startContainerID, count, state, null, replicationType, repConfig);
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
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("datanodeDetails", String.valueOf(dn));
    try {
      Map<String, List<ContainerID>> result =  scm.getScmDecommissionManager().getContainersPendingReplication(dn);
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(SCMAction.GET_CONTAINERS_ON_DECOM_NODE, auditMap));
      return result;
    } catch (NodeNotFoundException e) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(SCMAction.GET_CONTAINERS_ON_DECOM_NODE, auditMap, e));
      throw new IOException("Failed to get containers list. Unable to find required node", e);
    }
  }

  @Override
  public List<HddsProtos.Node> queryNode(
      HddsProtos.NodeOperationalState opState, HddsProtos.NodeState state,
      HddsProtos.QueryScope queryScope, String poolName, int clientVersion)
      throws IOException {
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("opState", String.valueOf(opState));
    auditMap.put("state", String.valueOf(state));
    auditMap.put("queryScope", String.valueOf(queryScope));
    auditMap.put("poolName", poolName);
    auditMap.put("clientVersion", String.valueOf(clientVersion));

    if (queryScope == HddsProtos.QueryScope.POOL) {
      IllegalArgumentException ex =  new IllegalArgumentException("Not Supported yet");
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.QUERY_NODE, auditMap, ex));
      throw ex;
    }
    try {
      List<HddsProtos.Node> result = new ArrayList<>();
      for (DatanodeDetails node : queryNode(opState, state)) {
        NodeStatus ns = scm.getScmNodeManager().getNodeStatus(node);
        DatanodeInfo datanodeInfo = scm.getScmNodeManager().getDatanodeInfo(node);
        HddsProtos.Node.Builder nodeBuilder = HddsProtos.Node.newBuilder()
            .setNodeID(node.toProto(clientVersion))
            .addNodeStates(ns.getHealth())
            .addNodeOperationalStates(ns.getOperationalState());
        
        if (datanodeInfo != null) {
          nodeBuilder.setTotalVolumeCount(datanodeInfo.getStorageReports().size());
          nodeBuilder.setHealthyVolumeCount(datanodeInfo.getHealthyVolumeCount());
        }
        result.add(nodeBuilder.build());
      }
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.QUERY_NODE, auditMap));
      return result;
    } catch (NodeNotFoundException e) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.QUERY_NODE, auditMap, e));
      throw new IOException("An unexpected error occurred querying the NodeStatus", e);
    }
  }

  @Override
  public HddsProtos.Node queryNode(UUID uuid)
      throws IOException {
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("uuid", String.valueOf(uuid));
    HddsProtos.Node result = null;
    try {
      DatanodeDetails node = scm.getScmNodeManager().getNode(DatanodeID.of(uuid));
      if (node != null) {
        NodeStatus ns = scm.getScmNodeManager().getNodeStatus(node);
        DatanodeInfo datanodeInfo = scm.getScmNodeManager().getDatanodeInfo(node);
        HddsProtos.Node.Builder nodeBuilder = HddsProtos.Node.newBuilder()
            .setNodeID(node.getProtoBufMessage())
            .addNodeStates(ns.getHealth())
            .addNodeOperationalStates(ns.getOperationalState());

        if (datanodeInfo != null) {
          nodeBuilder.setTotalVolumeCount(datanodeInfo.getStorageReports().size());
          nodeBuilder.setHealthyVolumeCount(datanodeInfo.getHealthyVolumeCount());
        }
        result = nodeBuilder.build();
      }
    } catch (NodeNotFoundException e) {
      IOException ex = new IOException(
          "An unexpected error occurred querying the NodeStatus", e);
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.QUERY_NODE, auditMap, ex));
      throw ex;
    }
    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
        SCMAction.QUERY_NODE, auditMap));
    return result;
  }

  @Override
  public List<DatanodeAdminError> decommissionNodes(List<String> nodes, boolean force)
      throws IOException {
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("nodes", String.valueOf(nodes));
    auditMap.put("force", String.valueOf(force));

    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      List<DatanodeAdminError> result =  scm.getScmDecommissionManager().decommissionNodes(nodes, force);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.DECOMMISSION_NODES, auditMap));
      return result;
    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.DECOMMISSION_NODES, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public List<DatanodeAdminError> recommissionNodes(List<String> nodes)
      throws IOException {
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("nodes", String.valueOf(nodes));
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      List<DatanodeAdminError> result = scm.getScmDecommissionManager().recommissionNodes(nodes);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.DECOMMISSION_NODES, auditMap));
      return result;
    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.DECOMMISSION_NODES, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public List<DatanodeAdminError> startMaintenanceNodes(List<String> nodes,
      int endInHours, boolean force) throws IOException {
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("nodes", String.valueOf(nodes));
    auditMap.put("endInHours", String.valueOf(endInHours));
    auditMap.put("force", String.valueOf(force));
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      List<DatanodeAdminError> result =  scm.getScmDecommissionManager()
          .startMaintenanceNodes(nodes, endInHours, force);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.START_MAINTENANCE_NODES, auditMap));
      return result;
    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.START_MAINTENANCE_NODES, auditMap, ex));
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
      getScm().checkAdminAccess(getRemoteUser(), false);
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
  public List<Pipeline> listPipelines() throws IOException {
    try {
      List<Pipeline> pipelines = scm.getPipelineManager().getPipelines();
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.LIST_PIPELINE, null));
      return pipelines;
    } catch (Exception ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.LIST_PIPELINE, null, ex));
      throw ex;
    }
  }

  @Override
  public Pipeline getPipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("pipelineID", pipelineID.getId());
    try {
      Pipeline pipeline = scm.getPipelineManager().getPipeline(PipelineID.getFromProtobuf(pipelineID));
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
              SCMAction.GET_PIPELINE, auditMap));
      return pipeline;
    } catch (Exception ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_PIPELINE, auditMap, ex));
      throw ex;
    }
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
      pipelineManager.closePipeline(pipeline.getId());
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.CLOSE_PIPELINE, auditMap));
    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.CLOSE_PIPELINE, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public ScmInfo getScmInfo() {
    try {
      ScmInfo.Builder builder =
          new ScmInfo.Builder()
              .setClusterId(scm.getScmStorageConfig().getClusterID())
              .setScmId(scm.getScmStorageConfig().getScmId())
              .setPeerRoles(scm.getScmHAManager().getRatisServer().getRatisRoles());
      ScmInfo info = builder.build();
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
            SCMAction.GET_SCM_INFO, null));
      return info;
    } catch (Exception ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_SCM_INFO, null, ex)
      );
      throw ex;
    }
  }

  @Override
  public void transferLeadership(String newLeaderId)
      throws IOException {

    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("newLeaderId", newLeaderId);
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      checkIfCertSignRequestAllowed(scm.getRootCARotationManager(),
          false, config, "transferLeadership");
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
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.TRANSFER_LEADERSHIP, auditMap, ex));
      throw ex;
    }
    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
            SCMAction.TRANSFER_LEADERSHIP, auditMap));
  }

  @Deprecated
  @Override
  public List<DeletedBlocksTransactionInfo> getFailedDeletedBlockTxn(int count,
      long startTxId) throws IOException {
    return Collections.emptyList();
  }

  @Deprecated
  @Override
  public int resetDeletedBlockRetryCount(List<Long> txIDs) throws IOException {
    return 0;
  }

  @Nullable
  @Override
  public DeletedBlocksTransactionSummary getDeletedBlockSummary() {
    final Map<String, String> auditMap = Maps.newHashMap();
    try {
      DeletedBlocksTransactionSummary summary =
          scm.getScmBlockManager().getDeletedBlockLog().getTransactionSummary();
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.GET_DELETED_BLOCK_SUMMARY, auditMap));
      return summary;
    } catch (Exception ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_DELETED_BLOCK_SUMMARY, auditMap, ex));
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
    try {
      Map<String, Pair<Boolean, String>> result = scm.getRuleStatus();
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.GET_SAFE_MODE_RULE_STATUSES, null));
      return result;
    } catch (Exception ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_SAFE_MODE_RULE_STATUSES, null, ex));
      throw ex;
    }
  }

  /**
   * Force SCM out of Safe mode.
   *
   * @return returns true if operation is successful.
   * @throws IOException
   */
  @Override
  public boolean forceExitSafeMode() throws IOException {
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      boolean result = scm.exitSafeMode();
      AUDIT.logWriteSuccess(
          buildAuditMessageForSuccess(SCMAction.FORCE_EXIT_SAFE_MODE, null)
      );
      return result;

    } catch (Exception ex) {
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(SCMAction.FORCE_EXIT_SAFE_MODE, null, ex)
      );
      throw ex;
    }
  }

  @Override
  public void startReplicationManager() throws IOException {
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      scm.getReplicationManager().start();
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.START_REPLICATION_MANAGER, null));

    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.START_REPLICATION_MANAGER, null, ex));
      throw ex;
    }
  }

  @Override
  public void stopReplicationManager() throws IOException {
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.STOP_REPLICATION_MANAGER, null));
      scm.getReplicationManager().stop();
    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.STOP_REPLICATION_MANAGER, null, ex));
      throw ex;
    }
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
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("upgradeClientID", upgradeClientID);
    try {
      // check admin authorization
      getScm().checkAdminAccess(getRemoteUser(), false);
      // TODO HDDS-6762: Return to the client once the FINALIZATION_STARTED
      //  checkpoint has been crossed and continue finalizing asynchronously.
      StatusAndMessages result = scm.getFinalizationManager().finalizeUpgrade(upgradeClientID);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.FINALIZE_SCM_UPGRADE, auditMap));
      return result;
    } catch (Exception ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.FINALIZE_SCM_UPGRADE, auditMap, ex));
      throw ex;
    }

  }

  @Override
  public StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean force, boolean readonly)
      throws IOException {
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("upgradeClientID", upgradeClientID);
    auditMap.put("force", String.valueOf(force));
    auditMap.put("readonly", String.valueOf(readonly));

    try {
      // check admin authorization
      if (!readonly) {
        getScm().checkAdminAccess(getRemoteUser(), true);
      }
      StatusAndMessages result = scm.getFinalizationManager()
          .queryUpgradeFinalizationProgress(upgradeClientID, force, readonly);
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.QUERY_UPGRADE_FINALIZATION_PROGRESS, auditMap));
      return result;
    } catch (IOException ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.QUERY_UPGRADE_FINALIZATION_PROGRESS, auditMap, ex));
      throw ex;
    }
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
      Optional<String> excludeNodes,
      Optional<String> excludeContainers) throws IOException {
    Map<String, String> auditMap = Maps.newHashMap();
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      ContainerBalancerConfiguration cbc =
          scm.getConfiguration().getObject(ContainerBalancerConfiguration.class);
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

      if (excludeContainers.isPresent()) {
        String ec = excludeContainers.get();
        auditMap.put("excludeContainers", (ec));
        cbc.setExcludeContainers(ec);
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
    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      scm.getContainerBalancer().stopBalancer();
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.STOP_CONTAINER_BALANCER, null));
    } catch (IllegalContainerBalancerStateException e) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.STOP_CONTAINER_BALANCER, null, e));
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public boolean getContainerBalancerStatus() {
    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
        SCMAction.GET_CONTAINER_BALANCER_STATUS, null));
    return scm.getContainerBalancer().isBalancerRunning();
  }

  @Override
  public ContainerBalancerStatusInfoResponseProto getContainerBalancerStatusInfo() throws IOException {
    AUDIT.logReadSuccess(buildAuditMessageForSuccess(
        SCMAction.GET_CONTAINER_BALANCER_STATUS_INFO, null));
    ContainerBalancerStatusInfo balancerStatusInfo = scm.getContainerBalancer().getBalancerStatusInfo();
    if (balancerStatusInfo == null) {
      return ContainerBalancerStatusInfoResponseProto
          .newBuilder()
          .setIsRunning(false)
          .build();
    } else {

      return ContainerBalancerStatusInfoResponseProto
          .newBuilder()
          .setIsRunning(true)
          .setContainerBalancerStatusInfo(balancerStatusInfo.toProto())
          .build();
    }
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

    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("address", address);
    auditMap.put("uuid", uuid);
    auditMap.put("clientVersion", String.valueOf(clientVersion));

    try {
      // check admin authorisation
      getScm().checkAdminAccess(getRemoteUser(), true);
      // get datanodes by ip or uuid
      List<DatanodeDetails> nodes = new ArrayList<>();
      if (!Strings.isNullOrEmpty(uuid)) {
        nodes.add(scm.getScmNodeManager().getNode(DatanodeID.fromUuidString(uuid)));
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

      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.GET_DATANODE_USAGE_INFO, auditMap));

      return infoList;
    } catch (Exception ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(SCMAction.GET_DATANODE_USAGE_INFO, auditMap, ex));
      throw ex;
    }
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

    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("mostUsed", String.valueOf(mostUsed));
    auditMap.put("count", String.valueOf(count));
    auditMap.put("clientVersion", String.valueOf(clientVersion));

    try {
      // check admin authorisation
      getScm().checkAdminAccess(getRemoteUser(), true);
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
      List<HddsProtos.DatanodeUsageInfoProto> result =  datanodeUsageInfoList.stream()
          .map(each -> each.toProto(clientVersion))
          .limit(count)
          .collect(Collectors.toList());

      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.GET_DATANODE_USAGE_INFO, auditMap));
      return result;
    } catch (Exception ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_DATANODE_USAGE_INFO, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public Token<?> getContainerToken(ContainerID containerID)
      throws IOException {
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", String.valueOf(containerID));
    try {
      UserGroupInformation remoteUser = getRemoteUser();
      getScm().checkAdminAccess(getRemoteUser(), true);

      Token<?> token = scm.getContainerTokenGenerator()
          .generateToken(remoteUser.getUserName(), containerID);
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.GET_CONTAINER_TOKEN, auditMap));
      return token;
    } catch (Exception ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_CONTAINER_TOKEN, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public long getContainerCount() throws IOException {
    try {
      long count = scm.getContainerManager().getContainers().size();
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.GET_CONTAINER_COUNT, null));
      return count;
    } catch (Exception ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_CONTAINER_COUNT, null, ex));
      throw ex;
    }
  }

  @Override
  public long getContainerCount(HddsProtos.LifeCycleState state)
      throws IOException {
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("state", String.valueOf(state));

    try {
      long count = scm.getContainerManager().getContainers(state).size();
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.GET_CONTAINER_COUNT, auditMap));
      return count;
    } catch (Exception ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_CONTAINER_COUNT, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public List<ContainerInfo> getListOfContainers(
      long startContainerID, int count, HddsProtos.LifeCycleState state)
      throws IOException {

    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("startContainerID", String.valueOf(startContainerID));
    auditMap.put("count", String.valueOf(count));
    auditMap.put("state", String.valueOf(state));
    try {
      List<ContainerInfo> results = scm.getContainerManager().getContainers(
          ContainerID.valueOf(startContainerID), count, state);
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.LIST_CONTAINER, auditMap));
      return results;
    } catch (Exception ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.LIST_CONTAINER, auditMap, ex));
      throw ex;
    }
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
    if ((tmp != null) && (!tmp.isEmpty())) {
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
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("scmId", scmId);
    Builder decommissionScmResponseBuilder =
        DecommissionScmResponseProto.newBuilder();

    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      decommissionScmResponseBuilder
          .setSuccess(scm.removePeerFromHARing(scmId));
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
          SCMAction.DECOMMISSION_SCM, auditMap));
    } catch (IOException ex) {
      decommissionScmResponseBuilder
          .setSuccess(false)
          .setErrorMsg(ex.getMessage() == null ? StringUtils.stringifyException(ex) : ex.getMessage());
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.DECOMMISSION_SCM, auditMap, ex));
    }
    return decommissionScmResponseBuilder.build();
  }

  @Override
  public String getMetrics(String query) throws IOException {
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("query", query);
    try {
      FetchMetrics fetchMetrics = new FetchMetrics();
      String metrics = fetchMetrics.getMetrics(query);
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(
          SCMAction.GET_METRICS, auditMap));
      return metrics;
    } catch (Exception ex) {
      AUDIT.logReadFailure(buildAuditMessageForFailure(
          SCMAction.GET_METRICS, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public void reconcileContainer(long longContainerID) throws IOException {
    ContainerID containerID = ContainerID.valueOf(longContainerID);
    getScm().checkAdminAccess(getRemoteUser(), false);
    final UserGroupInformation remoteUser = getRemoteUser();
    final Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("containerID", containerID.toString());
    auditMap.put("remoteUser", remoteUser.getUserName());

    try {
      EligibilityResult result = ReconciliationEligibilityHandler.isEligibleForReconciliation(containerID,
          getScm().getContainerManager());
      if (!result.isOk()) {
        switch (result.getResult()) {
        case OK:
          break;
        case CONTAINER_NOT_FOUND:
          throw new ContainerNotFoundException(result.toString());
        case INELIGIBLE_CONTAINER_STATE:
          throw new SCMException(result.toString(), ResultCodes.UNEXPECTED_CONTAINER_STATE);
        case INELIGIBLE_REPLICA_STATES:
        case INELIGIBLE_REPLICATION_TYPE:
        case NOT_ENOUGH_REQUIRED_NODES:
        case NO_REPLICAS_FOUND:
          throw new SCMException(result.toString(), ResultCodes.UNSUPPORTED_OPERATION);
        default:
          throw new SCMException("Unknown reconciliation eligibility result " + result, ResultCodes.INTERNAL_ERROR);
        }
      }

      scm.getEventQueue().fireEvent(SCMEvents.RECONCILE_CONTAINER, containerID);
      AUDIT.logWriteSuccess(buildAuditMessageForSuccess(SCMAction.RECONCILE_CONTAINER, auditMap));
    } catch (SCMException ex) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(SCMAction.RECONCILE_CONTAINER, auditMap, ex));
      throw ex;
    }
  }

  @Override
  public void setAckMissingContainer(long longContainerID, boolean acknowledge) throws IOException {
    ContainerID containerID = ContainerID.valueOf(longContainerID);
    final Map<String, String> auditMap = new HashMap<>();
    auditMap.put("containerID", containerID.toString());
    auditMap.put("acknowledge", String.valueOf(acknowledge));

    try {
      getScm().checkAdminAccess(getRemoteUser(), false);
      ContainerInfo containerInfo = scm.getContainerManager().getContainer(containerID);
      
      if (acknowledge) {
        // Validation for setting ACK_MISSING
        Set<ContainerReplica> replicas = scm.getContainerManager().getContainerReplicas(containerID);
        if (replicas != null && !replicas.isEmpty()) {
          throw new IOException("Container " + longContainerID + " has " + replicas.size() +
              " replicas and cannot be acknowledged as missing");
        }
        if (containerInfo.getNumberOfKeys() == 0) {
          throw new IOException("Container " + longContainerID + " is empty (0 keys) and cannot be acknowledged.");
        }
        // Set to ACK_MISSING
        containerInfo.setHealthState(ContainerHealthState.ACK_MISSING);
        AUDIT.logWriteSuccess(buildAuditMessageForSuccess(SCMAction.ACKNOWLEDGE_MISSING_CONTAINER, auditMap));
      } else {
        containerInfo.setHealthState(null);
        AUDIT.logWriteSuccess(buildAuditMessageForSuccess(SCMAction.UNACKNOWLEDGE_MISSING_CONTAINER, auditMap));
      }
      scm.getContainerManager().updateContainerInfo(containerID, containerInfo.getProtobuf());
    } catch (IOException ex) {
      SCMAction action = acknowledge ?
          SCMAction.ACKNOWLEDGE_MISSING_CONTAINER : SCMAction.UNACKNOWLEDGE_MISSING_CONTAINER;
      AUDIT.logWriteFailure(buildAuditMessageForFailure(action, auditMap, ex));
      throw ex;
    }
  }
}
