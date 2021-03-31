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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.DeleteBlockResult;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
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
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocolServerSideTranslatorPB;

import com.google.common.collect.Maps;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ProtocolMessageEnum;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.IO_EXCEPTION;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager.startRpcServer;
import static org.apache.hadoop.hdds.server.ServerUtils.getRemoteUserName;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;
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
  private final OzoneConfiguration conf;
  private final RPC.Server blockRpcServer;
  private final InetSocketAddress blockRpcAddress;
  private final ProtocolMessageMetrics<ProtocolMessageEnum>
      protocolMessageMetrics;

  /**
   * The RPC server that listens to requests from block service clients.
   */
  public SCMBlockProtocolServer(OzoneConfiguration conf,
      StorageContainerManager scm) throws IOException {
    this.scm = scm;
    this.conf = conf;
    final int handlerCount =
        conf.getInt(OZONE_SCM_HANDLER_COUNT_KEY,
            OZONE_SCM_HANDLER_COUNT_DEFAULT);

    RPC.setProtocolEngine(conf, ScmBlockLocationProtocolPB.class,
        ProtobufRpcEngine.class);

    protocolMessageMetrics =
        ProtocolMessageMetrics.create(
            "ScmBlockLocationProtocol",
            "SCM Block location protocol counters",
            ScmBlockLocationProtocolProtos.Type.values());

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
            handlerCount);
    blockRpcAddress =
        updateRPCListenAddress(
            conf, scm.getScmNodeDetails().getBlockProtocolServerAddressKey(),
            scmBlockAddress, blockRpcServer);
    if (conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      blockRpcServer.refreshServiceAcl(conf, SCMPolicyProvider.getInstance());
    }
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
  public List<AllocatedBlock> allocateBlock(long size, int num,
      HddsProtos.ReplicationType type, HddsProtos.ReplicationFactor factor,
      String owner, ExcludeList excludeList) throws IOException {
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("size", String.valueOf(size));
    auditMap.put("num", String.valueOf(num));
    auditMap.put("type", type.name());
    auditMap.put("factor", factor.name());
    auditMap.put("owner", owner);
    List<AllocatedBlock> blocks = new ArrayList<>(num);
    boolean auditSuccess = true;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Allocating {} blocks of size {}, with {}",
          num, size, excludeList);
    }
    try {
      for (int i = 0; i < num; i++) {
        AllocatedBlock block = scm.getScmBlockManager()
            .allocateBlock(size, type, factor, owner, excludeList);
        if (block != null) {
          blocks.add(block);
        }
      }
      return blocks;
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(SCMAction.ALLOCATE_BLOCK, auditMap, ex)
      );
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(SCMAction.ALLOCATE_BLOCK, auditMap)
        );
      }
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("SCM is informed by OM to delete {} blocks",
          keyBlocksInfoList.size());
    }
    List<DeleteBlockGroupResult> results = new ArrayList<>();
    Map<String, String> auditMap = Maps.newHashMap();
    ScmBlockLocationProtocolProtos.DeleteScmBlockResult.Result resultCode;
    Exception e = null;
    try {
      scm.getScmBlockManager().deleteBlocks(keyBlocksInfoList);
      resultCode = ScmBlockLocationProtocolProtos.
          DeleteScmBlockResult.Result.success;
    } catch (IOException ioe) {
      e = ioe;
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
      auditMap.put("KeyBlockToDelete", bg.toString());
      List<DeleteBlockResult> blockResult = new ArrayList<>();
      for (BlockID b : bg.getBlockIDList()) {
        blockResult.add(new DeleteBlockResult(b, resultCode));
      }
      results.add(new DeleteBlockGroupResult(bg.getGroupID(), blockResult));
    }
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

  @Override
  public boolean addSCM(AddSCMRequest request) throws IOException {
    LOG.debug("Adding SCM {} addr {} cluster id {}",
        request.getScmId(), request.getRatisAddr(), request.getClusterId());

    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("scmId", String.valueOf(request.getScmId()));
    auditMap.put("cluster", String.valueOf(request.getClusterId()));
    auditMap.put("addr", String.valueOf(request.getRatisAddr()));
    boolean auditSuccess = true;
    try{
      return scm.getScmHAManager().addSCM(request);
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.ADD_SCM, auditMap, ex)
      );
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.ADD_SCM, auditMap)
        );
      }
    }
  }

  @Override
  public List<DatanodeDetails> sortDatanodes(List<String> nodes,
      String clientMachine) throws IOException {
    boolean auditSuccess = true;
    try{
      NodeManager nodeManager = scm.getScmNodeManager();
      Node client = null;
      List<DatanodeDetails> possibleClients =
          nodeManager.getNodesByAddress(clientMachine);
      if (possibleClients.size()>0){
        client = possibleClients.get(0);
      }
      List<Node> nodeList = new ArrayList();
      nodes.stream().forEach(uuid -> {
        DatanodeDetails node = nodeManager.getNodeByUuid(uuid);
        if (node != null) {
          nodeList.add(node);
        }
      });
      List<? extends Node> sortedNodeList = scm.getClusterMap()
          .sortByDistanceCost(client, nodeList, nodes.size());
      List<DatanodeDetails> ret = new ArrayList<>();
      sortedNodeList.stream().forEach(node -> ret.add((DatanodeDetails)node));
      return ret;
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.SORT_DATANODE, null, ex)
      );
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.SORT_DATANODE, null)
        );
      }
    }
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
