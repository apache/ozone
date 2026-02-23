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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_READ_THREADPOOL_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_READ_THREADPOOL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.IO_EXCEPTION;
import static org.apache.hadoop.hdds.scm.net.NetConstants.NODE_COST_DEFAULT;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.getPerfMetrics;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.startRpcServer;
import static org.apache.hadoop.hdds.server.ServerUtils.getRemoteUserName;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getRemoteUser;

import com.google.common.collect.Maps;
import com.google.protobuf.BlockingService;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.DeleteBlockResult;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMPerformanceMetrics;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.audit.SCMAction;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.ozone.common.DeletedBlock;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCM block protocol is the protocol used by Namenode and OzoneManager to get
 * blocks from the SCM.
 */
public class SCMBlockProtocolServer implements
    ScmBlockLocationProtocol, Auditor {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMBlockProtocolServer.class);

  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.SCMLOGGER);

  private final StorageContainerManager scm;
  private final RPC.Server blockRpcServer;
  private final InetSocketAddress blockRpcAddress;
  private final ProtocolMessageMetrics<ScmBlockLocationProtocolProtos.Type>
      protocolMessageMetrics;
  private final SCMPerformanceMetrics perfMetrics;

  /**
   * The RPC server that listens to requests from block service clients.
   */
  public SCMBlockProtocolServer(OzoneConfiguration conf,
      StorageContainerManager scm) throws IOException {
    this.scm = scm;
    this.perfMetrics = getPerfMetrics();
    final int handlerCount = conf.getInt(OZONE_SCM_BLOCK_HANDLER_COUNT_KEY,
        OZONE_SCM_HANDLER_COUNT_KEY, OZONE_SCM_HANDLER_COUNT_DEFAULT,
            LOG::info);
    final int readThreads = conf.getInt(OZONE_SCM_BLOCK_READ_THREADPOOL_KEY,
        OZONE_SCM_BLOCK_READ_THREADPOOL_DEFAULT);

    RPC.setProtocolEngine(conf, ScmBlockLocationProtocolPB.class,
        ProtobufRpcEngine.class);

    protocolMessageMetrics =
        ProtocolMessageMetrics.create(
            "ScmBlockLocationProtocol",
            "SCM Block location protocol counters",
            ScmBlockLocationProtocolProtos.Type.class);

    // SCM Block Service RPC.
    BlockingService blockProtoPbService =
        ScmBlockLocationProtocolProtos.ScmBlockLocationProtocolService
            .newReflectiveBlockingService(
                new ScmBlockLocationProtocolServerSideTranslatorPB(this, scm,
                    protocolMessageMetrics));

    final InetSocketAddress scmBlockAddress =
        scm.getScmNodeDetails().getBlockProtocolServerAddress();
    blockRpcServer =
        startRpcServer(
            conf,
            scmBlockAddress,
            ScmBlockLocationProtocolPB.class,
            blockProtoPbService,
            handlerCount,
            readThreads);
    blockRpcAddress =
        updateRPCListenAddress(
            conf, scm.getScmNodeDetails().getBlockProtocolServerAddressKey(),
            scmBlockAddress, blockRpcServer);
    if (conf.getBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      blockRpcServer.refreshServiceAcl(conf, SCMPolicyProvider.getInstance());
    }
    HddsServerUtil.addSuppressedLoggingExceptions(blockRpcServer);
  }

  public RPC.Server getBlockRpcServer() {
    return blockRpcServer;
  }

  public InetSocketAddress getBlockRpcAddress() {
    return blockRpcAddress;
  }

  public void start() {
    protocolMessageMetrics.register();
    LOG.info(
        StorageContainerManager.buildRpcServerStartMessage(
            "RPC server for Block Protocol", getBlockRpcAddress()));
    getBlockRpcServer().start();
  }

  public void stop() {
    try {
      protocolMessageMetrics.unregister();
      LOG.info("Stopping the RPC server for Block Protocol");
      getBlockRpcServer().stop();
    } catch (Exception ex) {
      LOG.error("Block Protocol RPC stop failed.", ex);
    }
    IOUtils.cleanupWithLogger(LOG, scm.getScmNodeManager());
  }

  public void join() throws InterruptedException {
    LOG.trace("Join RPC server for Block Protocol");
    getBlockRpcServer().join();
  }

  @Override
  public List<AllocatedBlock> allocateBlock(
      long size, int num,
      ReplicationConfig replicationConfig,
      String owner, ExcludeList excludeList,
      String clientMachine,
      StorageType storageType
  ) throws IOException {
    long startNanos = Time.monotonicNowNanos();
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("size", String.valueOf(size));
    auditMap.put("num", String.valueOf(num));
    auditMap.put("replication", replicationConfig.toString());
    auditMap.put("owner", owner);
    auditMap.put("client", clientMachine);
    auditMap.put("storageType", String.valueOf(storageType));
    List<AllocatedBlock> blocks = new ArrayList<>(num);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Allocating {} blocks of size {}, with {}",
          num, size, excludeList);
    }
    try {
      for (int i = 0; i < num; i++) {
        AllocatedBlock block = scm.getScmBlockManager()
            .allocateBlock(size, replicationConfig, owner, excludeList,
                storageType);
        if (block != null) {
          // Sort the datanodes if client machine is specified
          final Node client = getClientNode(clientMachine);
          if (client != null) {
            final List<DatanodeDetails> nodes = block.getPipeline().getNodes();
            final List<DatanodeDetails> sorted = scm.getClusterMap()
                .sortByDistanceCost(client, nodes, nodes.size());
            if (!Objects.equals(sorted, block.getPipeline().getNodesInOrder())) {
              block = block.toBuilder()
                  .setPipeline(block.getPipeline().copyWithNodesInOrder(sorted))
                  .build();
            }
          }
          blocks.add(block);
        }
      }

      auditMap.put("allocated", String.valueOf(blocks.size()));
      String blockIDs = blocks.stream().limit(10)
          .map(block -> block.getBlockID().toString())
          .collect(Collectors.joining(", ", "[", "]"));
      auditMap.put("sampleBlocks", blockIDs);

      if (blocks.size() < num) {
        AUDIT.logWriteFailure(buildAuditMessageForFailure(
            SCMAction.ALLOCATE_BLOCK, auditMap, null)
        );
        perfMetrics.updateAllocateBlockFailureLatencyNs(startNanos);
      } else {
        AUDIT.logWriteSuccess(buildAuditMessageForSuccess(
            SCMAction.ALLOCATE_BLOCK, auditMap));
        perfMetrics.updateAllocateBlockSuccessLatencyNs(startNanos);
      }

      return blocks;
    } catch (TimeoutException ex) {
      perfMetrics.updateAllocateBlockFailureLatencyNs(startNanos);
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.ALLOCATE_BLOCK, auditMap, ex));
      throw new IOException(ex);
    } catch (Exception ex) {
      perfMetrics.updateAllocateBlockFailureLatencyNs(startNanos);
      AUDIT.logWriteFailure(buildAuditMessageForFailure(
          SCMAction.ALLOCATE_BLOCK, auditMap, ex));
      throw ex;
    }
  }

  /**
   * Delete blocks for a set of object keys.
   *
   * @param keyBlocksInfoList list of block keys with object keys to delete.
   * @return deletion results.
   */
  @Override
  public List<DeleteBlockGroupResult> deleteKeyBlocks(
      List<BlockGroup> keyBlocksInfoList) throws IOException {
    long totalBlocks = 0;
    for (BlockGroup bg : keyBlocksInfoList) {
      totalBlocks += bg.getDeletedBlocks().size();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("SCM is informed by OM to delete {} keys. Total blocks to deleted {}.",
          keyBlocksInfoList.size(), totalBlocks);
    }
    List<DeleteBlockGroupResult> results = new ArrayList<>();
    Map<String, String> auditMap = Maps.newHashMap();
    ScmBlockLocationProtocolProtos.DeleteScmBlockResult.Result resultCode;
    Exception e = null;
    long startNanos = Time.monotonicNowNanos();
    try {
      scm.getScmBlockManager().deleteBlocks(keyBlocksInfoList);
      perfMetrics.updateDeleteKeySuccessBlocks(totalBlocks);
      perfMetrics.updateDeleteKeySuccessStats(keyBlocksInfoList.size(), startNanos);
      resultCode = ScmBlockLocationProtocolProtos.
          DeleteScmBlockResult.Result.success;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Total number of blocks ACK by SCM in this cycle: " + totalBlocks);
      }
    } catch (IOException ioe) {
      e = ioe;
      perfMetrics.updateDeleteKeyFailedBlocks(totalBlocks);
      perfMetrics.updateDeleteKeyFailureStats(keyBlocksInfoList.size(), startNanos);
      LOG.warn("Fail to delete {} keys", keyBlocksInfoList.size(), ioe);
      switch (ioe instanceof SCMException ? ((SCMException) ioe).getResult() :
          IO_EXCEPTION) {
      case SAFE_MODE_EXCEPTION:
        resultCode =
            ScmBlockLocationProtocolProtos.DeleteScmBlockResult.Result.safeMode;
        break;
      case FAILED_TO_FIND_BLOCK:
        resultCode =
            ScmBlockLocationProtocolProtos.DeleteScmBlockResult.Result.
                errorNotFound;
        break;
      default:
        resultCode =
            ScmBlockLocationProtocolProtos.DeleteScmBlockResult.Result.
                unknownFailure;
      }
    }
    for (BlockGroup bg : keyBlocksInfoList) {
      List<DeleteBlockResult> blockResult = new ArrayList<>();
      for (DeletedBlock b : bg.getDeletedBlocks()) {
        blockResult.add(new DeleteBlockResult(b.getBlockID(), resultCode));
      }
      results.add(new DeleteBlockGroupResult(bg.getGroupID(), blockResult));
    }
    auditMap.put("KeyBlockToDelete", keyBlocksInfoList.toString());
    if (e == null) {
      AUDIT.logWriteSuccess(
          buildAuditMessageForSuccess(SCMAction.DELETE_KEY_BLOCK, auditMap));
    } else {
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(SCMAction.DELETE_KEY_BLOCK, auditMap, e));
    }
    return results;
  }

  @Override
  public ScmInfo getScmInfo() throws IOException {
    boolean auditSuccess = true;
    try {
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
      if (auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.GET_SCM_INFO, null)
        );
      }
    }
  }

  @Override
  public boolean addSCM(AddSCMRequest request) throws IOException {
    scm.checkAdminAccess(getRemoteUser(), false);
    LOG.debug("Adding SCM {} addr {} cluster id {}",
        request.getScmId(), request.getRatisAddr(), request.getClusterId());

    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("scmId", String.valueOf(request.getScmId()));
    auditMap.put("cluster", String.valueOf(request.getClusterId()));
    auditMap.put("addr", String.valueOf(request.getRatisAddr()));
    boolean auditSuccess = true;
    try {
      return scm.getScmHAManager().addSCM(request);
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(SCMAction.ADD_SCM, auditMap, ex)
      );
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(SCMAction.ADD_SCM, auditMap)
        );
      }
    }
  }

  @Override
  public List<DatanodeDetails> sortDatanodes(List<String> nodes,
      String clientMachine) {
    boolean auditSuccess = true;
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put("client", clientMachine);
    auditMap.put("nodes", String.valueOf(nodes));
    try {
      NodeManager nodeManager = scm.getScmNodeManager();
      final Node client = getClientNode(clientMachine);
      List<DatanodeDetails> nodeList = new ArrayList<>();
      nodes.forEach(uuid -> {
        DatanodeDetails node = nodeManager.getNode(DatanodeID.fromUuidString(uuid));
        if (node != null) {
          nodeList.add(node);
        }
      });
      return scm.getClusterMap()
          .sortByDistanceCost(client, nodeList, nodeList.size());
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.SORT_DATANODE, auditMap, ex)
      );
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.SORT_DATANODE, auditMap)
        );
      }
    }
  }

  private Node getClientNode(String clientMachine) {
    if (StringUtils.isEmpty(clientMachine)) {
      return null;
    }
    List<DatanodeDetails> datanodes = scm.getScmNodeManager()
        .getNodesByAddress(clientMachine);
    return !datanodes.isEmpty() ? datanodes.get(0) :
        getOtherNode(clientMachine);
  }

  private Node getOtherNode(String clientMachine) {
    try {
      String clientLocation = scm.resolveNodeLocation(clientMachine);
      if (clientLocation != null) {
        Node rack = scm.getClusterMap().getNode(clientLocation);
        if (rack instanceof InnerNode) {
          return new NodeImpl(clientMachine, clientLocation,
              (InnerNode) rack, rack.getLevel() + 1,
              NODE_COST_DEFAULT);
        }
      }
    } catch (Exception e) {
      LOG.info("Could not resolve client {}: {}",
          clientMachine, e.getMessage());
    }
    return null;
  }

  @Override
  public InnerNode getNetworkTopology() {
    return (InnerNode) scm.getClusterMap().getNode(ROOT);
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

  public SCMPerformanceMetrics getMetrics() {
    return perfMetrics;
  }
}
