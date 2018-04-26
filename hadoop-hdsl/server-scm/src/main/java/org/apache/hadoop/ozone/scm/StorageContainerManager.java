/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.scm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.BlockingService;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdsl.HdslUtils;
import org.apache.hadoop.hdsl.protocol.DatanodeDetails;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.hdsl.server.ServiceRuntimeInfoImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.common.StorageInfo;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos.NodeState;
import org.apache.hadoop.hdsl.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.ReportState;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMNodeAddressList;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMNodeReport;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredCmdResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMReregisterCmdResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SendContainerReportProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCmdType;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto.DeleteBlockTransactionResult;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKResponseProto;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerLocationProtocolProtos.ObjectStageChangeRequestProto;
import org.apache.hadoop.ozone.protocolPB.ScmBlockLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.scm.block.BlockManager;
import org.apache.hadoop.ozone.scm.block.BlockManagerImpl;
import org.apache.hadoop.ozone.scm.container.ContainerMapping;
import org.apache.hadoop.ozone.scm.container.Mapping;
import org.apache.hadoop.ozone.scm.container.placement.metrics.ContainerStat;
import org.apache.hadoop.ozone.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.ozone.scm.exceptions.SCMException;
import org.apache.hadoop.scm.ScmInfo;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.ozone.scm.node.SCMNodeManager;
import org.apache.hadoop.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.scm.container.common.helpers.DeleteBlockResult;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.ozone.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.ozone.scm.HdslServerUtil
    .getScmBlockClientBindAddress;
import static org.apache.hadoop.ozone.scm.HdslServerUtil
    .getScmClientBindAddress;
import static org.apache.hadoop.ozone.scm.HdslServerUtil
    .getScmDataNodeBindAddress;
import static org.apache.hadoop.hdsl.server.ServerUtils
    .updateRPCListenAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdsl.protocol.proto
    .ScmBlockLocationProtocolProtos.DeleteScmBlockResult.Result;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_MB;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * StorageContainerManager is the main entry point for the service that provides
 * information about which SCM nodes host containers.
 *
 * DataNodes report to StorageContainerManager using heartbeat
 * messages. SCM allocates containers and returns a pipeline.
 *
 * A client once it gets a pipeline (a list of datanodes) will connect to the
 * datanodes and create a container, which then can be used to store data.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "CBLOCK", "OZONE", "HBASE"})
public class StorageContainerManager extends ServiceRuntimeInfoImpl
    implements StorageContainerDatanodeProtocol,
    StorageContainerLocationProtocol, ScmBlockLocationProtocol, SCMMXBean {

  private static final Logger LOG =
      LoggerFactory.getLogger(StorageContainerManager.class);

  /**
   *  Startup options.
   */
  public enum StartupOption {
    INIT("-init"),
    CLUSTERID("-clusterid"),
    GENCLUSTERID("-genclusterid"),
    REGULAR("-regular"),
    HELP("-help");

    private final String name;
    private String clusterId = null;

    public void setClusterId(String cid) {
      if(cid != null && !cid.isEmpty()) {
        clusterId = cid;
      }
    }

    public String getClusterId() {
      return clusterId;
    }

    StartupOption(String arg) {
      this.name = arg;
    }

    public String getName() {
      return name;
    }
  }

  /**
   * NodeManager and container Managers for SCM.
   */
  private final NodeManager scmNodeManager;
  private final Mapping scmContainerManager;
  private final BlockManager scmBlockManager;
  private final SCMStorage scmStorage;

  /** The RPC server that listens to requests from DataNodes. */
  private final RPC.Server datanodeRpcServer;
  private final InetSocketAddress datanodeRpcAddress;

  /** The RPC server that listens to requests from clients. */
  private final RPC.Server clientRpcServer;
  private final InetSocketAddress clientRpcAddress;

  /** The RPC server that listens to requests from block service clients. */
  private final RPC.Server blockRpcServer;
  private final InetSocketAddress blockRpcAddress;

  private final StorageContainerManagerHttpServer httpServer;

  /** SCM mxbean. */
  private ObjectName scmInfoBeanName;

  /** SCM super user. */
  private final String scmUsername;
  private final Collection<String> scmAdminUsernames;

  /** SCM metrics. */
  private static SCMMetrics metrics;
  /** Key = DatanodeUuid, value = ContainerStat. */
  private Cache<String, ContainerStat> containerReportCache;


  private static final String USAGE =
      "Usage: \n oz scm [genericOptions] "
          + "[ " + StartupOption.INIT.getName() + " [ "
          + StartupOption.CLUSTERID.getName() + " <cid> ] ]\n "
          + "oz scm [genericOptions] [ "
          + StartupOption.GENCLUSTERID.getName() + " ]\n " +
          "oz scm [ "
          + StartupOption.HELP.getName() + " ]\n";
  /**
   * Creates a new StorageContainerManager.  Configuration will be updated with
   * information on the actual listening addresses used for RPC servers.
   *
   * @param conf configuration
   */
  private StorageContainerManager(OzoneConfiguration conf)
      throws IOException {

    final int handlerCount = conf.getInt(
        OZONE_SCM_HANDLER_COUNT_KEY, OZONE_SCM_HANDLER_COUNT_DEFAULT);
    final int cacheSize = conf.getInt(OZONE_SCM_DB_CACHE_SIZE_MB,
        OZONE_SCM_DB_CACHE_SIZE_DEFAULT);

    StorageContainerManager.initMetrics();
    initContainerReportCache(conf);

    scmStorage = new SCMStorage(conf);
    if (scmStorage.getState() != StorageState.INITIALIZED) {
      throw new SCMException("SCM not initialized.",
          ResultCodes.SCM_NOT_INITIALIZED);
    }
    scmNodeManager = new SCMNodeManager(conf, scmStorage.getClusterID(), this);
    scmContainerManager = new ContainerMapping(conf, scmNodeManager, cacheSize);
    scmBlockManager = new BlockManagerImpl(conf, scmNodeManager,
        scmContainerManager, cacheSize);

    scmAdminUsernames = conf.getTrimmedStringCollection(
        OzoneConfigKeys.OZONE_ADMINISTRATORS);
    scmUsername = UserGroupInformation.getCurrentUser().getUserName();
    if (!scmAdminUsernames.contains(scmUsername)) {
      scmAdminUsernames.add(scmUsername);
    }

    RPC.setProtocolEngine(conf, StorageContainerDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    RPC.setProtocolEngine(conf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    RPC.setProtocolEngine(conf, ScmBlockLocationProtocolPB.class,
        ProtobufRpcEngine.class);

    BlockingService dnProtoPbService = StorageContainerDatanodeProtocolProtos.
        StorageContainerDatanodeProtocolService.newReflectiveBlockingService(
        new StorageContainerDatanodeProtocolServerSideTranslatorPB(this));

    final InetSocketAddress datanodeRpcAddr =
        getScmDataNodeBindAddress(conf);
    datanodeRpcServer = startRpcServer(conf, datanodeRpcAddr,
        StorageContainerDatanodeProtocolPB.class, dnProtoPbService,
        handlerCount);
    datanodeRpcAddress = updateRPCListenAddress(conf,
        OZONE_SCM_DATANODE_ADDRESS_KEY, datanodeRpcAddr, datanodeRpcServer);

    // SCM Container Service RPC
    BlockingService storageProtoPbService =
        StorageContainerLocationProtocolProtos
            .StorageContainerLocationProtocolService
            .newReflectiveBlockingService(
            new StorageContainerLocationProtocolServerSideTranslatorPB(this));

    final InetSocketAddress scmAddress =
        getScmClientBindAddress(conf);
    clientRpcServer = startRpcServer(conf, scmAddress,
        StorageContainerLocationProtocolPB.class, storageProtoPbService,
        handlerCount);
    clientRpcAddress = updateRPCListenAddress(conf,
        OZONE_SCM_CLIENT_ADDRESS_KEY, scmAddress, clientRpcServer);

    // SCM Block Service RPC
    BlockingService blockProtoPbService =
        ScmBlockLocationProtocolProtos
            .ScmBlockLocationProtocolService
            .newReflectiveBlockingService(
            new ScmBlockLocationProtocolServerSideTranslatorPB(this));

    final InetSocketAddress scmBlockAddress =
        getScmBlockClientBindAddress(conf);
    blockRpcServer = startRpcServer(conf, scmBlockAddress,
        ScmBlockLocationProtocolPB.class, blockProtoPbService,
        handlerCount);
    blockRpcAddress = updateRPCListenAddress(conf,
        OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, scmBlockAddress, blockRpcServer);

    httpServer = new StorageContainerManagerHttpServer(conf);

    registerMXBean();
  }

  /**
   * Initialize container reports cache that sent from datanodes.
   *
   * @param conf
   */
  private void initContainerReportCache(OzoneConfiguration conf) {
    containerReportCache = CacheBuilder.newBuilder()
        .expireAfterAccess(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
        .maximumSize(Integer.MAX_VALUE)
        .removalListener(new RemovalListener<String, ContainerStat>() {
          @Override
          public void onRemoval(
              RemovalNotification<String, ContainerStat> removalNotification) {
            synchronized (containerReportCache) {
              ContainerStat stat = removalNotification.getValue();
              // remove invalid container report
              metrics.decrContainerStat(stat);
              LOG.debug(
                  "Remove expired container stat entry for datanode: {}.",
                  removalNotification.getKey());
            }
          }
        }).build();
  }

  /**
   * Builds a message for logging startup information about an RPC server.
   *
   * @param description RPC server description
   * @param addr RPC server listening address
   * @return server startup message
   */
  private static String buildRpcServerStartMessage(String description,
      InetSocketAddress addr) {
    return addr != null ? String.format("%s is listening at %s",
        description, addr.toString()) :
        String.format("%s not started", description);
  }

  /**
   * Starts an RPC server, if configured.
   *
   * @param conf configuration
   * @param addr configured address of RPC server
   * @param protocol RPC protocol provided by RPC server
   * @param instance RPC protocol implementation instance
   * @param handlerCount RPC server handler count
   *
   * @return RPC server
   * @throws IOException if there is an I/O error while creating RPC server
   */
  private static RPC.Server startRpcServer(OzoneConfiguration conf,
      InetSocketAddress addr, Class<?> protocol, BlockingService instance,
      int handlerCount)
      throws IOException {
    RPC.Server rpcServer = new RPC.Builder(conf)
        .setProtocol(protocol)
        .setInstance(instance)
        .setBindAddress(addr.getHostString())
        .setPort(addr.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(null)
        .build();

    DFSUtil.addPBProtocol(conf, protocol, instance, rpcServer);
    return rpcServer;
  }

  private void registerMXBean() {
    Map<String, String> jmxProperties = new HashMap<>();
    jmxProperties.put("component", "ServerRuntime");
    this.scmInfoBeanName =
        MBeans.register("StorageContainerManager",
            "StorageContainerManagerInfo",
            jmxProperties,
            this);
  }

  private void unregisterMXBean() {
    if(this.scmInfoBeanName != null) {
      MBeans.unregister(this.scmInfoBeanName);
      this.scmInfoBeanName = null;
    }
  }

  /**
   * Main entry point for starting StorageContainerManager.
   *
   * @param argv arguments
   * @throws IOException if startup fails due to I/O error
   */
  public static void main(String[] argv) throws IOException {
    if (DFSUtil.parseHelpArgument(argv, USAGE,
        System.out, true)) {
      System.exit(0);
    }
    try {
      OzoneConfiguration conf = new OzoneConfiguration();
      GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);
      if (!hParser.isParseSuccessful()) {
        System.err.println("USAGE: " + USAGE + "\n");
        hParser.printGenericCommandUsage(System.err);
        System.exit(1);
      }
      StringUtils.startupShutdownMessage(StorageContainerManager.class,
          argv, LOG);
      StorageContainerManager scm = createSCM(hParser.getRemainingArgs(), conf);
      if (scm != null) {
        scm.start();
        scm.join();
      }
    } catch (Throwable t) {
      LOG.error("Failed to start the StorageContainerManager.", t);
      terminate(1, t);
    }
  }

  private static void printUsage(PrintStream out) {
    out.println(USAGE + "\n");
  }

  public static StorageContainerManager createSCM(String[] argv,
      OzoneConfiguration conf) throws IOException {
    if (!HdslUtils.isHdslEnabled(conf)) {
      System.err.println("SCM cannot be started in secure mode or when " +
          OZONE_ENABLED + " is set to false");
      System.exit(1);
    }
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage(System.err);
      terminate(1);
      return null;
    }
    switch (startOpt) {
    case INIT:
      terminate(scmInit(conf) ? 0 : 1);
      return null;
    case GENCLUSTERID:
      System.out.println("Generating new cluster id:");
      System.out.println(StorageInfo.newClusterID());
      terminate(0);
      return null;
    case HELP:
      printUsage(System.err);
      terminate(0);
      return null;
    default:
      return new StorageContainerManager(conf);
    }
  }

  /**
   * Routine to set up the Version info for StorageContainerManager.
   *
   * @param conf OzoneConfiguration
   * @return true if SCM initialization is successful, false otherwise.
   * @throws IOException if init fails due to I/O error
   */
  public static boolean scmInit(OzoneConfiguration conf) throws IOException {
    SCMStorage scmStorage = new SCMStorage(conf);
    StorageState state = scmStorage.getState();
    if (state != StorageState.INITIALIZED) {
      try {
        String clusterId = StartupOption.INIT.getClusterId();
        if (clusterId != null && !clusterId.isEmpty()) {
          scmStorage.setClusterId(clusterId);
        }
        scmStorage.initialize();
        System.out.println("SCM initialization succeeded." +
            "Current cluster id for sd=" + scmStorage.getStorageDir() + ";cid="
                + scmStorage.getClusterID());
        return true;
      } catch (IOException ioe) {
        LOG.error("Could not initialize SCM version file", ioe);
        return false;
      }
    } else {
      System.out.println("SCM already initialized. Reusing existing" +
          " cluster id for sd=" + scmStorage.getStorageDir() + ";cid="
              + scmStorage.getClusterID());
      return true;
    }
  }

  private static StartupOption parseArguments(String[] args) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.HELP;
    if (argsLen == 0) {
      startOpt = StartupOption.REGULAR;
    }
    for (int i = 0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.INIT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.INIT;
        if (argsLen > 3) {
          return null;
        }
        for (i = i + 1; i < argsLen; i++) {
          if (args[i].equalsIgnoreCase(StartupOption.CLUSTERID.getName())) {
            i++;
            if (i < argsLen && !args[i].isEmpty()) {
              startOpt.setClusterId(args[i]);
            } else {
              // if no cluster id specified or is empty string, return null
              LOG.error("Must specify a valid cluster ID after the "
                  + StartupOption.CLUSTERID.getName() + " flag");
              return null;
            }
          } else {
            return null;
          }
        }
      } else if (StartupOption.GENCLUSTERID.getName().equalsIgnoreCase(cmd)) {
        if (argsLen > 1) {
          return null;
        }
        startOpt = StartupOption.GENCLUSTERID;
      }
    }
    return startOpt;
  }

  /**
   * Returns a SCMCommandRepose from the SCM Command.
   * @param cmd - Cmd
   * @return SCMCommandResponseProto
   * @throws InvalidProtocolBufferException
   */
  @VisibleForTesting
  public SCMCommandResponseProto getCommandResponse(SCMCommand cmd,
      final String datanodID)
      throws IOException {
    SCMCmdType type = cmd.getType();
    SCMCommandResponseProto.Builder builder =
        SCMCommandResponseProto.newBuilder()
        .setDatanodeUUID(datanodID);
    switch (type) {
    case registeredCommand:
      return builder.setCmdType(SCMCmdType.registeredCommand)
          .setRegisteredProto(
              SCMRegisteredCmdResponseProto.getDefaultInstance())
          .build();
    case versionCommand:
      return builder.setCmdType(SCMCmdType.versionCommand)
          .setVersionProto(SCMVersionResponseProto.getDefaultInstance())
          .build();
    case sendContainerReport:
      return builder.setCmdType(SCMCmdType.sendContainerReport)
          .setSendReport(SendContainerReportProto.getDefaultInstance())
          .build();
    case reregisterCommand:
      return builder.setCmdType(SCMCmdType.reregisterCommand)
          .setReregisterProto(SCMReregisterCmdResponseProto
              .getDefaultInstance())
          .build();
    case deleteBlocksCommand:
      // Once SCM sends out the deletion message, increment the count.
      // this is done here instead of when SCM receives the ACK, because
      // DN might not be able to response the ACK for sometime. In case
      // it times out, SCM needs to re-send the message some more times.
      List<Long> txs = ((DeleteBlocksCommand) cmd).blocksTobeDeleted()
          .stream().map(tx -> tx.getTxID()).collect(Collectors.toList());
      this.getScmBlockManager().getDeletedBlockLog().incrementCount(txs);
      return builder.setCmdType(SCMCmdType.deleteBlocksCommand)
          .setDeleteBlocksProto(((DeleteBlocksCommand) cmd).getProto())
          .build();
    case closeContainerCommand:
      return builder.setCmdType(SCMCmdType.closeContainerCommand)
          .setCloseContainerProto(((CloseContainerCommand)cmd).getProto())
          .build();
    default:
      throw new IllegalArgumentException("Not implemented");
    }
  }

  @VisibleForTesting
  public static SCMRegisteredCmdResponseProto getRegisteredResponse(
      SCMCommand cmd, SCMNodeAddressList addressList) {
    Preconditions.checkState(cmd.getClass() == RegisteredCommand.class);
    RegisteredCommand rCmd = (RegisteredCommand) cmd;
    SCMCmdType type = cmd.getType();
    if (type != SCMCmdType.registeredCommand) {
      throw new IllegalArgumentException("Registered command is not well " +
          "formed. Internal Error.");
    }
    return SCMRegisteredCmdResponseProto.newBuilder()
        //TODO : Fix this later when we have multiple SCM support.
        //.setAddressList(addressList)
        .setErrorCode(rCmd.getError())
        .setClusterID(rCmd.getClusterID())
        .setDatanodeUUID(rCmd.getDatanodeUUID()).build();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Pipeline getContainer(String containerName) throws IOException {
    checkAdminAccess();
    return scmContainerManager.getContainer(containerName).getPipeline();
  }

  @VisibleForTesting
  public ContainerInfo getContainerInfo(String containerName)
      throws IOException {
    return scmContainerManager.getContainer(containerName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ContainerInfo> listContainer(String startName,
      String prefixName, int count) throws IOException {
    return scmContainerManager.listContainer(startName, prefixName, count);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteContainer(String containerName) throws IOException {
    checkAdminAccess();
    scmContainerManager.deleteContainer(containerName);
  }

  /**
   * Queries a list of Node Statuses.
   *
   * @param nodeStatuses
   * @param queryScope
   * @param poolName @return List of Datanodes.
   */
  @Override
  public HdslProtos.NodePool queryNode(EnumSet<NodeState> nodeStatuses,
      HdslProtos.QueryScope queryScope, String poolName) throws IOException {

    if (queryScope == HdslProtos.QueryScope.POOL) {
      throw new IllegalArgumentException("Not Supported yet");
    }

    List<DatanodeDetails> datanodes = queryNode(nodeStatuses);
    HdslProtos.NodePool.Builder poolBuilder =
        HdslProtos.NodePool.newBuilder();

    for (DatanodeDetails datanode : datanodes) {
      HdslProtos.Node node = HdslProtos.Node.newBuilder()
          .setNodeID(datanode.getProtoBufMessage())
          .addAllNodeStates(nodeStatuses)
          .build();
      poolBuilder.addNodes(node);
    }

    return poolBuilder.build();
  }

  /**
   * Notify from client when begin/finish operation for container/pipeline
   * objects on datanodes.
   * @param type
   * @param name
   * @param op
   * @param stage
   */
  @Override
  public void notifyObjectStageChange(
      ObjectStageChangeRequestProto.Type type, String name,
      ObjectStageChangeRequestProto.Op op,
      ObjectStageChangeRequestProto.Stage stage) throws IOException {

    LOG.info("Object type {} name {} op {} new stage {}",
        type, name, op, stage);
    if (type == ObjectStageChangeRequestProto.Type.container) {
      if (op == ObjectStageChangeRequestProto.Op.create) {
        if (stage == ObjectStageChangeRequestProto.Stage.begin) {
          scmContainerManager.updateContainerState(name,
              HdslProtos.LifeCycleEvent.CREATE);
        } else {
          scmContainerManager.updateContainerState(name,
              HdslProtos.LifeCycleEvent.CREATED);
        }
      } else if (op == ObjectStageChangeRequestProto.Op.close) {
        if (stage == ObjectStageChangeRequestProto.Stage.begin) {
          scmContainerManager.updateContainerState(name,
              HdslProtos.LifeCycleEvent.FINALIZE);
        } else {
          scmContainerManager.updateContainerState(name,
              HdslProtos.LifeCycleEvent.CLOSE);
        }
      }
    } //else if (type == ObjectStageChangeRequestProto.Type.pipeline) {
    // TODO: pipeline state update will be addressed in future patch.
    //}
  }

  /**
   * Creates a replication pipeline of a specified type.
   */
  @Override
  public Pipeline createReplicationPipeline(
      HdslProtos.ReplicationType replicationType,
      HdslProtos.ReplicationFactor factor,
      HdslProtos.NodePool nodePool)
      throws IOException {
     // TODO: will be addressed in future patch.
    return null;
  }

  /**
   * Queries a list of Node that match a set of statuses.
   * <p>
   * For example, if the nodeStatuses is HEALTHY and RAFT_MEMBER,
   * then this call will return all healthy nodes which members in
   * Raft pipeline.
   * <p>
   * Right now we don't support operations, so we assume it is an AND operation
   * between the operators.
   *
   * @param nodeStatuses - A set of NodeStates.
   * @return List of Datanodes.
   */

  public List<DatanodeDetails> queryNode(EnumSet<NodeState> nodeStatuses) {
    Preconditions.checkNotNull(nodeStatuses, "Node Query set cannot be null");
    Preconditions.checkState(nodeStatuses.size() > 0, "No valid arguments " +
        "in the query set");
    List<DatanodeDetails> resultList = new LinkedList<>();
    Set<DatanodeDetails> currentSet = new TreeSet<>();

    for (NodeState nodeState : nodeStatuses) {
      Set<DatanodeDetails> nextSet = queryNodeState(nodeState);
      if ((nextSet == null) || (nextSet.size() == 0)) {
        // Right now we only support AND operation. So intersect with
        // any empty set is null.
        return resultList;
      }
      // First time we have to add all the elements, next time we have to
      // do an intersection operation on the set.
      if (currentSet.size() == 0) {
        currentSet.addAll(nextSet);
      } else {
        currentSet.retainAll(nextSet);
      }
    }

    resultList.addAll(currentSet);
    return resultList;
  }

  /**
   * Query the System for Nodes.
   *
   * @param nodeState - NodeState that we are interested in matching.
   * @return Set of Datanodes that match the NodeState.
   */
  private Set<DatanodeDetails> queryNodeState(NodeState nodeState) {
    if (nodeState == NodeState.RAFT_MEMBER ||
        nodeState == NodeState.FREE_NODE) {
      throw new IllegalStateException("Not implemented yet");
    }
    Set<DatanodeDetails> returnSet = new TreeSet<>();
    List<DatanodeDetails> tmp = getScmNodeManager().getNodes(nodeState);
    if ((tmp != null) && (tmp.size() > 0)) {
      returnSet.addAll(tmp);
    }
    return returnSet;
  }

  /**
   * Asks SCM where a container should be allocated. SCM responds with the set
   * of datanodes that should be used creating this container.
   *
   * @param containerName - Name of the container.
   * @param replicationFactor - replication factor.
   * @return pipeline
   * @throws IOException
   */
  @Override
  public Pipeline allocateContainer(HdslProtos.ReplicationType replicationType,
      HdslProtos.ReplicationFactor replicationFactor, String containerName,
      String owner) throws IOException {

    checkAdminAccess();
    return scmContainerManager
        .allocateContainer(replicationType, replicationFactor, containerName,
            owner).getPipeline();
  }

  /**
   * Returns listening address of StorageLocation Protocol RPC server.
   *
   * @return listen address of StorageLocation RPC server
   */
  @VisibleForTesting
  public InetSocketAddress getClientRpcAddress() {
    return clientRpcAddress;
  }

  @Override
  public String getClientRpcPort() {
    InetSocketAddress addr = getClientRpcAddress();
    return addr == null ? "0" : Integer.toString(addr.getPort());
  }

  /**
   * Returns listening address of StorageDatanode Protocol RPC server.
   *
   * @return Address where datanode are communicating.
   */
  public InetSocketAddress getDatanodeRpcAddress() {
    return datanodeRpcAddress;
  }

  @Override
  public String getDatanodeRpcPort() {
    InetSocketAddress addr = getDatanodeRpcAddress();
    return addr == null ? "0" : Integer.toString(addr.getPort());
  }

  /**
   * Start service.
   */
  public void start() throws IOException {
    LOG.info(buildRpcServerStartMessage(
        "StorageContainerLocationProtocol RPC server", clientRpcAddress));
    DefaultMetricsSystem.initialize("StorageContainerManager");
    clientRpcServer.start();
    LOG.info(buildRpcServerStartMessage(
        "ScmBlockLocationProtocol RPC server", blockRpcAddress));
    blockRpcServer.start();
    LOG.info(buildRpcServerStartMessage("RPC server for DataNodes",
        datanodeRpcAddress));
    datanodeRpcServer.start();
    httpServer.start();
    scmBlockManager.start();

    setStartTime();

  }

  /**
   * Stop service.
   */
  public void stop() {
    try {
      LOG.info("Stopping block service RPC server");
      blockRpcServer.stop();
    } catch (Exception ex) {
      LOG.error("Storage Container Manager blockRpcServer stop failed.", ex);
    }

    try {
      LOG.info("Stopping the StorageContainerLocationProtocol RPC server");
      clientRpcServer.stop();
    } catch (Exception ex) {
      LOG.error("Storage Container Manager clientRpcServer stop failed.", ex);
    }

    try {
      LOG.info("Stopping the RPC server for DataNodes");
      datanodeRpcServer.stop();
    } catch (Exception ex) {
      LOG.error("Storage Container Manager datanodeRpcServer stop failed.", ex);
    }

    try {
      LOG.info("Stopping Storage Container Manager HTTP server.");
      httpServer.stop();
    } catch (Exception ex) {
      LOG.error("Storage Container Manager HTTP server stop failed.", ex);
    }

    try {
      LOG.info("Stopping Block Manager Service.");
      scmBlockManager.stop();
    } catch (Exception ex) {
      LOG.error("SCM block manager service stop failed.", ex);
    }

    if (containerReportCache != null) {
      containerReportCache.invalidateAll();
      containerReportCache.cleanUp();
    }

    if (metrics != null) {
      metrics.unRegister();
    }

    unregisterMXBean();
    IOUtils.cleanupWithLogger(LOG, scmContainerManager);
    IOUtils.cleanupWithLogger(LOG, scmNodeManager);
  }

  /**
   * Wait until service has completed shutdown.
   */
  public void join() {
    try {
      blockRpcServer.join();
      clientRpcServer.join();
      datanodeRpcServer.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted during StorageContainerManager join.");
    }
  }

  /**
   * Returns SCM version.
   *
   * @return Version info.
   */
  @Override
  public SCMVersionResponseProto getVersion(
      SCMVersionRequestProto versionRequest) throws IOException {
    return getScmNodeManager().getVersion(versionRequest).getProtobufMessage();
  }

  /**
   * Used by data node to send a Heartbeat.
   *
   * @param datanodeDetails - Datanode Details.
   * @param nodeReport - Node Report
   * @param reportState - Container report ready info.
   * @return - SCMHeartbeatResponseProto
   * @throws IOException
   */
  @Override
  public SCMHeartbeatResponseProto sendHeartbeat(
      DatanodeDetailsProto datanodeDetails, SCMNodeReport nodeReport,
      ReportState reportState) throws IOException {
    List<SCMCommand> commands =
        getScmNodeManager().sendHeartbeat(datanodeDetails, nodeReport,
            reportState);
    List<SCMCommandResponseProto> cmdResponses = new LinkedList<>();
    for (SCMCommand cmd : commands) {
      cmdResponses.add(getCommandResponse(cmd, datanodeDetails.getUuid()
          .toString()));
    }
    return SCMHeartbeatResponseProto.newBuilder().addAllCommands(cmdResponses)
        .build();
  }

  /**
   * Register Datanode.
   *
   * @param datanodeDetails - DatanodID.
   * @param scmAddresses - List of SCMs this datanode is configured to
   * communicate.
   * @return SCM Command.
   */
  @Override
  public StorageContainerDatanodeProtocolProtos.SCMRegisteredCmdResponseProto
      register(DatanodeDetailsProto datanodeDetails, String[] scmAddresses) {
    // TODO : Return the list of Nodes that forms the SCM HA.
    return getRegisteredResponse(
        scmNodeManager.register(datanodeDetails), null);
  }

  /**
   * Send a container report.
   *
   * @param reports -- Container report
   * @return HeartbeatRespose.nullcommand.
   * @throws IOException
   */
  @Override
  public ContainerReportsResponseProto sendContainerReport(
      ContainerReportsRequestProto reports) throws IOException {
    updateContainerReportMetrics(reports);

    // should we process container reports async?
    scmContainerManager.processContainerReports(reports);
    return ContainerReportsResponseProto.newBuilder().build();
  }

  private void updateContainerReportMetrics(
      ContainerReportsRequestProto reports) {
    ContainerStat newStat = null;
    // TODO: We should update the logic once incremental container report
    // type is supported.
    if (reports
        .getType() == ContainerReportsRequestProto.reportType.fullReport) {
      newStat = new ContainerStat();
      for (StorageContainerDatanodeProtocolProtos.ContainerInfo info : reports
          .getReportsList()) {
        newStat.add(new ContainerStat(info.getSize(), info.getUsed(),
            info.getKeyCount(), info.getReadBytes(), info.getWriteBytes(),
            info.getReadCount(), info.getWriteCount()));
      }

      // update container metrics
      metrics.setLastContainerStat(newStat);
    }

    // Update container stat entry, this will trigger a removal operation if it
    // exists in cache.
    synchronized (containerReportCache) {
      String datanodeUuid = reports.getDatanodeDetails().getUuid();
      if (datanodeUuid != null && newStat != null) {
        containerReportCache.put(datanodeUuid, newStat);
        // update global view container metrics
        metrics.incrContainerStat(newStat);
      }
    }
  }

  /**
   * Handles the block deletion ACKs sent by datanodes. Once ACKs recieved,
   * SCM considers the blocks are deleted and update the metadata in SCM DB.
   *
   * @param acks
   * @return
   * @throws IOException
   */
  @Override
  public ContainerBlocksDeletionACKResponseProto sendContainerBlocksDeletionACK(
      ContainerBlocksDeletionACKProto acks) throws IOException {
    if (acks.getResultsCount() > 0) {
      List<DeleteBlockTransactionResult> resultList = acks.getResultsList();
      for (DeleteBlockTransactionResult result : resultList) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got block deletion ACK from datanode, TXIDs={}, "
                  + "success={}", result.getTxID(), result.getSuccess());
        }
        if (result.getSuccess()) {
          LOG.debug("Purging TXID={} from block deletion log",
              result.getTxID());
          this.getScmBlockManager().getDeletedBlockLog()
              .commitTransactions(Collections.singletonList(result.getTxID()));
        } else {
          LOG.warn("Got failed ACK for TXID={}, prepare to resend the "
              + "TX in next interval", result.getTxID());
        }
      }
    }
    return ContainerBlocksDeletionACKResponseProto.newBuilder()
        .getDefaultInstanceForType();
  }

  /**
   * Returns the Number of Datanodes that are communicating with SCM.
   *
   * @param nodestate Healthy, Dead etc.
   * @return int -- count
   */
  public int getNodeCount(NodeState nodestate) {
    return scmNodeManager.getNodeCount(nodestate);
  }

  /**
   * Returns SCM container manager.
   */
  @VisibleForTesting
  public Mapping getScmContainerManager() {
    return scmContainerManager;
  }

  /**
   * Returns node manager.
   * @return - Node Manager
   */
  @VisibleForTesting
  public NodeManager getScmNodeManager() {
    return scmNodeManager;
  }

  @VisibleForTesting
  public BlockManager getScmBlockManager() {
    return scmBlockManager;
  }

  /**
   * Get block locations.
   * @param keys batch of block keys to retrieve.
   * @return set of allocated blocks.
   * @throws IOException
   */
  @Override
  public Set<AllocatedBlock> getBlockLocations(final Set<String> keys)
      throws IOException {
    Set<AllocatedBlock> locatedBlocks = new HashSet<>();
    for (String key: keys) {
      Pipeline pipeline = scmBlockManager.getBlock(key);
      AllocatedBlock block = new AllocatedBlock.Builder()
          .setKey(key)
          .setPipeline(pipeline).build();
      locatedBlocks.add(block);
    }
    return locatedBlocks;
  }

  /**
   * Asks SCM where a block should be allocated. SCM responds with the set of
   * datanodes that should be used creating this block.
   *
   * @param size - size of the block.
   * @param type - Replication type.
   * @param factor
   * @return allocated block accessing info (key, pipeline).
   * @throws IOException
   */
  @Override
  public AllocatedBlock allocateBlock(long size,
      HdslProtos.ReplicationType type, HdslProtos.ReplicationFactor factor,
      String owner) throws IOException {
    return scmBlockManager.allocateBlock(size, type, factor, owner);
  }

  /**
   * Get the clusterId and SCM Id from the version file in SCM.
   */
  @Override
  public ScmInfo getScmInfo() throws IOException {
    ScmInfo.Builder builder = new ScmInfo.Builder()
        .setClusterId(scmStorage.getClusterID())
        .setScmId(scmStorage.getScmId());
    return builder.build();
  }
  /**
   * Delete blocks for a set of object keys.
   *
   * @param keyBlocksInfoList list of block keys with object keys to delete.
   * @return deletion results.
   */
  public List<DeleteBlockGroupResult> deleteKeyBlocks(
      List<BlockGroup> keyBlocksInfoList) throws IOException {
    LOG.info("SCM is informed by KSM to delete {} blocks",
        keyBlocksInfoList.size());
    List<DeleteBlockGroupResult> results = new ArrayList<>();
    for (BlockGroup keyBlocks : keyBlocksInfoList) {
      Result resultCode;
      try {
        // We delete blocks in an atomic operation to prevent getting
        // into state like only a partial of blocks are deleted,
        // which will leave key in an inconsistent state.
        scmBlockManager.deleteBlocks(keyBlocks.getBlockIDList());
        resultCode = Result.success;
      } catch (SCMException scmEx) {
        LOG.warn("Fail to delete block: {}", keyBlocks.getGroupID(), scmEx);
        switch (scmEx.getResult()) {
        case CHILL_MODE_EXCEPTION:
          resultCode = Result.chillMode;
          break;
        case FAILED_TO_FIND_BLOCK:
          resultCode = Result.errorNotFound;
          break;
        default:
          resultCode = Result.unknownFailure;
        }
      } catch (IOException ex) {
        LOG.warn("Fail to delete blocks for object key: {}",
            keyBlocks.getGroupID(), ex);
        resultCode = Result.unknownFailure;
      }
      List<DeleteBlockResult> blockResultList = new ArrayList<>();
      for (String blockKey : keyBlocks.getBlockIDList()) {
        blockResultList.add(new DeleteBlockResult(blockKey, resultCode));
      }
      results.add(new DeleteBlockGroupResult(keyBlocks.getGroupID(),
          blockResultList));
    }
    return results;
  }

  @VisibleForTesting
  public String getPpcRemoteUsername() {
    UserGroupInformation user = ProtobufRpcEngine.Server.getRemoteUser();
    return user == null ? null : user.getUserName();
  }

  private void checkAdminAccess() throws IOException {
    String remoteUser = getPpcRemoteUsername();
    if(remoteUser != null) {
      if (!scmAdminUsernames.contains(remoteUser)) {
        throw new IOException(
            "Access denied for user " + remoteUser
                + ". Superuser privilege is required.");
      }
    }
  }

  /**
   * Initialize SCM metrics.
   */
  public static void initMetrics() {
    metrics = SCMMetrics.create();
  }

  /**
   * Return SCM metrics instance.
   */
  public static SCMMetrics getMetrics() {
    return metrics == null ? SCMMetrics.create() : metrics;
  }

  /**
   * Invalidate container stat entry for given datanode.
   *
   * @param datanodeUuid
   */
  public void removeContainerReport(String datanodeUuid) {
    synchronized (containerReportCache) {
      containerReportCache.invalidate(datanodeUuid);
    }
  }

  /**
   * Get container stat of specified datanode.
   *
   * @param datanodeUuid
   * @return
   */
  public ContainerStat getContainerReport(String datanodeUuid) {
    ContainerStat stat = null;
    synchronized (containerReportCache) {
      stat = containerReportCache.getIfPresent(datanodeUuid);
    }

    return stat;
  }

  /**
   * Returns a view of the container stat entries. Modifications made to the
   * map will directly affect the cache.
   *
   * @return
   */
  public ConcurrentMap<String, ContainerStat> getContainerReportCache() {
    return containerReportCache.asMap();
  }

  @Override
  public Map<String, String> getContainerReport() {
    Map<String, String> id2StatMap = new HashMap<>();
    synchronized (containerReportCache) {
      ConcurrentMap<String, ContainerStat> map = containerReportCache.asMap();
      for (Map.Entry<String, ContainerStat> entry : map.entrySet()) {
        id2StatMap.put(entry.getKey(), entry.getValue().toJsonString());
      }
    }

    return id2StatMap;
  }
}
