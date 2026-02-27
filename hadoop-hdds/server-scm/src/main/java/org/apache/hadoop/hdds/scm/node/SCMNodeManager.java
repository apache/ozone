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

package org.apache.hadoop.hdds.scm.node;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.HTTP;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.HTTPS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.scm.SCMCommonPlacementPolicy.hasEnoughSpace;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.management.ObjectName;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto.ErrorCode;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.VersionInfo;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.states.NodeAlreadyExistsException;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.FinalizeNewLayoutVersionCommand;
import org.apache.hadoop.ozone.protocol.commands.RefreshVolumeUsageCommand;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeOperationalStateCommand;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains information about the Datanodes on SCM side.
 * <p>
 * Heartbeats under SCM is very simple compared to HDFS heartbeatManager.
 * <p>
 * The getNode(byState) functions make copy of node maps and then creates a list
 * based on that. It should be assumed that these get functions always report
 * *stale* information. For example, getting the deadNodeCount followed by
 * getNodes(DEAD) could very well produce totally different count. Also
 * getNodeCount(HEALTHY) + getNodeCount(DEAD) + getNodeCode(STALE), is not
 * guaranteed to add up to the total nodes that we know off. Please treat all
 * get functions in this file as a snap-shot of information that is inconsistent
 * as soon as you read it.
 */
public class SCMNodeManager implements NodeManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMNodeManager.class);

  private final NodeStateManager nodeStateManager;
  private final VersionInfo version;
  private final CommandQueue commandQueue;
  private final SCMNodeMetrics metrics;
  // Node manager MXBean
  private ObjectName nmInfoBean;
  private final SCMStorageConfig scmStorageConfig;
  private final NetworkTopology clusterMap;
  private final Function<String, String> nodeResolver;
  private final boolean useHostname;
  private final Map<String, Set<DatanodeID>> dnsToDnIdMap = new ConcurrentHashMap<>();
  private final int numPipelinesPerMetadataVolume;
  private final int datanodePipelineLimit;
  private final HDDSLayoutVersionManager scmLayoutVersionManager;
  private final EventPublisher scmNodeEventPublisher;
  private final SCMContext scmContext;
  private final Map<SCMCommandProto.Type,
      BiConsumer<DatanodeDetails, SCMCommand<?>>> sendCommandNotifyMap;
  private final NonWritableNodeFilter nonWritableNodeFilter;
  private final int numContainerPerVolume;

  /**
   * Lock used to synchronize some operation in Node manager to ensure a
   * consistent view of the node state.
   */
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private static final String OPESTATE = "OPSTATE";
  private static final String COMSTATE = "COMSTATE";
  private static final String LASTHEARTBEAT = "LASTHEARTBEAT";
  private static final String USEDSPACEPERCENT = "USEDSPACEPERCENT";
  private static final String TOTALCAPACITY = "CAPACITY";
  private static final String DNUUID = "UUID";
  private static final String VERSION = "VERSION";

  /**
   * Constructs SCM machine Manager.
   */
  public SCMNodeManager(
      OzoneConfiguration conf,
      SCMStorageConfig scmStorageConfig,
      EventPublisher eventPublisher,
      NetworkTopology networkTopology,
      SCMContext scmContext,
      HDDSLayoutVersionManager layoutVersionManager) {
    this(conf, scmStorageConfig, eventPublisher, networkTopology, scmContext,
        layoutVersionManager, hostname -> null);
  }

  public SCMNodeManager(
      OzoneConfiguration conf,
      SCMStorageConfig scmStorageConfig,
      EventPublisher eventPublisher,
      NetworkTopology networkTopology,
      SCMContext scmContext,
      HDDSLayoutVersionManager layoutVersionManager,
      Function<String, String> nodeResolver) {
    this.scmNodeEventPublisher = eventPublisher;
    this.nodeStateManager = new NodeStateManager(conf, eventPublisher,
        layoutVersionManager, scmContext);
    this.version = VersionInfo.getLatestVersion();
    this.commandQueue = new CommandQueue();
    this.scmStorageConfig = scmStorageConfig;
    this.scmLayoutVersionManager = layoutVersionManager;
    LOG.info("Entering startup safe mode.");
    registerMXBean();
    this.metrics = SCMNodeMetrics.create(this);
    this.clusterMap = networkTopology;
    this.nodeResolver = nodeResolver;
    this.useHostname = conf.getBoolean(
        HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME,
        HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    this.numPipelinesPerMetadataVolume =
        conf.getInt(ScmConfigKeys.OZONE_SCM_PIPELINE_PER_METADATA_VOLUME,
            ScmConfigKeys.OZONE_SCM_PIPELINE_PER_METADATA_VOLUME_DEFAULT);
    this.datanodePipelineLimit = conf.getInt(
        ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT,
        ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT);
    this.numContainerPerVolume = conf.getInt(
        ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
        ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT_DEFAULT);
    this.scmContext = scmContext;
    this.sendCommandNotifyMap = new HashMap<>();
    this.nonWritableNodeFilter = new NonWritableNodeFilter(conf);
  }

  @Override
  public void registerSendCommandNotify(SCMCommandProto.Type type,
      BiConsumer<DatanodeDetails, SCMCommand<?>> scmCommand) {
    this.sendCommandNotifyMap.put(type, scmCommand);
  }

  private void registerMXBean() {
    this.nmInfoBean = MBeans.register("SCMNodeManager",
        "SCMNodeManagerInfo", this);
  }

  private void unregisterMXBean() {
    if (this.nmInfoBean != null) {
      MBeans.unregister(this.nmInfoBean);
      this.nmInfoBean = null;
    }
  }

  protected NodeStateManager getNodeStateManager() {
    return nodeStateManager;
  }

  /**
   * Returns all datanode that are in the given state. This function works by
   * taking a snapshot of the current collection and then returning the list
   * from that collection. This means that real map might have changed by the
   * time we return this list.
   *
   * @return List of Datanodes that are known to SCM in the requested state.
   */
  @Override
  public List<DatanodeDetails> getNodes(NodeStatus nodeStatus) {
    return nodeStateManager.getNodes(nodeStatus);
  }

  /**
   * Returns all datanode that are in the given states. Passing null for one of
   * of the states acts like a wildcard for that state. This function works by
   * taking a snapshot of the current collection and then returning the list
   * from that collection. This means that real map might have changed by the
   * time we return this list.
   *
   * @param opState The operational state of the node
   * @param health The health of the node
   * @return List of Datanodes that are known to SCM in the requested states.
   */
  @Override
  public List<DatanodeDetails> getNodes(
      NodeOperationalState opState, NodeState health) {
    return nodeStateManager.getNodes(opState, health)
        .stream()
        .map(node -> (DatanodeDetails)node).collect(Collectors.toList());
  }

  @Override
  public List<DatanodeInfo> getAllNodes() {
    return nodeStateManager.getAllNodes();
  }

  @Override
  public int getAllNodeCount() {
    return nodeStateManager.getAllNodeCount();
  }

  /**
   * Returns the Number of Datanodes by State they are in.
   *
   * @return count
   */
  @Override
  public int getNodeCount(NodeStatus nodeStatus) {
    return nodeStateManager.getNodeCount(nodeStatus);
  }

  /**
   * Returns the Number of Datanodes by State they are in. Passing null for
   * either of the states acts like a wildcard for that state.
   *
   * @param nodeOpState - The Operational State of the node
   * @param health - The health of the node
   * @return count
   */
  @Override
  public int getNodeCount(NodeOperationalState nodeOpState, NodeState health) {
    return nodeStateManager.getNodeCount(nodeOpState, health);
  }

  /**
   * Returns the node status of a specific node.
   *
   * @param datanodeDetails Datanode Details
   * @return NodeStatus for the node
   */
  @Override
  public NodeStatus getNodeStatus(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException {
    return nodeStateManager.getNodeStatus(datanodeDetails);
  }

  /**
   * Set the operation state of a node.
   * @param datanodeDetails The datanode to set the new state for
   * @param newState The new operational state for the node
   */
  @Override
  public void setNodeOperationalState(DatanodeDetails datanodeDetails,
      NodeOperationalState newState) throws NodeNotFoundException {
    setNodeOperationalState(datanodeDetails, newState, 0);
  }

  /**
   * Set the operation state of a node.
   * @param datanodeDetails The datanode to set the new state for
   * @param newState The new operational state for the node
   * @param opStateExpiryEpocSec Seconds from the epoch when the operational
   *                             state should end. Zero indicates the state
   *                             never end.
   */
  @Override
  public void setNodeOperationalState(DatanodeDetails datanodeDetails,
      NodeOperationalState newState, long opStateExpiryEpocSec)
      throws NodeNotFoundException {
    nodeStateManager.setNodeOperationalState(
        datanodeDetails, newState, opStateExpiryEpocSec);
  }

  /**
   * Closes this stream and releases any system resources associated with it. If
   * the stream is already closed then invoking this method has no effect.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    unregisterMXBean();
    metrics.unRegister();
    nodeStateManager.close();
  }

  /**
   * Gets the version info from SCM.
   *
   * @param versionRequest - version Request.
   * @return - returns SCM version info and other required information needed by
   * datanode.
   */
  @Override
  public VersionResponse getVersion(SCMVersionRequestProto versionRequest) {
    return VersionResponse.newBuilder()
        .setVersion(this.version.getVersion())
        .addValue(OzoneConsts.SCM_ID,
            this.scmStorageConfig.getScmId())
        .addValue(OzoneConsts.CLUSTER_ID, this.scmStorageConfig.getClusterID())
        .build();
  }

  @Override
  public RegisteredCommand register(
      DatanodeDetails datanodeDetails, NodeReportProto nodeReport,
      PipelineReportsProto pipelineReportsProto) {
    return register(datanodeDetails, nodeReport, pipelineReportsProto,
        LayoutVersionProto.newBuilder()
            .setMetadataLayoutVersion(
                scmLayoutVersionManager.getMetadataLayoutVersion())
            .setSoftwareLayoutVersion(
                scmLayoutVersionManager.getSoftwareLayoutVersion())
            .build());
  }

  /**
   * Register the node if the node finds that it is not registered with any
   * SCM.
   *
   * @param datanodeDetails - Send datanodeDetails with Node info.
   *                        This function generates and assigns new datanode ID
   *                        for the datanode. This allows SCM to be run
   *                        independent
   *                        of Namenode if required.
   * @param nodeReport      NodeReport.
   * @return SCMRegisteredResponseProto
   */
  @Override
  public RegisteredCommand register(
      DatanodeDetails datanodeDetails, NodeReportProto nodeReport,
      PipelineReportsProto pipelineReportsProto,
      LayoutVersionProto layoutInfo) {
    if (layoutInfo.getSoftwareLayoutVersion() !=
        scmLayoutVersionManager.getSoftwareLayoutVersion()) {
      return RegisteredCommand.newBuilder()
          .setErrorCode(ErrorCode.errorNodeNotPermitted)
          .setDatanode(datanodeDetails)
          .setClusterID(this.scmStorageConfig.getClusterID())
          .build();
    }

    InetAddress dnAddress = Server.getRemoteIp();
    if (dnAddress != null) {
      // Mostly called inside an RPC, update ip
      if (!useHostname) {
        datanodeDetails.setHostName(dnAddress.getHostName());
      }
      datanodeDetails.setIpAddress(dnAddress.getHostAddress());
    }

    final String ipAddress = datanodeDetails.getIpAddress();
    final String hostName = datanodeDetails.getHostName();
    datanodeDetails.setNetworkName(datanodeDetails.getUuidString());
    String networkLocation = nodeResolver.apply(
        useHostname ? hostName : ipAddress);
    if (networkLocation != null) {
      datanodeDetails.setNetworkLocation(networkLocation);
    }

    final DatanodeID dnId = datanodeDetails.getID();
    if (!isNodeRegistered(datanodeDetails)) {
      try {
        clusterMap.add(datanodeDetails);
        nodeStateManager.addNode(datanodeDetails, layoutInfo);
        // Check that datanode in nodeStateManager has topology parent set
        DatanodeDetails dn = nodeStateManager.getNode(datanodeDetails);
        Preconditions.checkState(dn.getParent() != null);
        addToDnsToDnIdMap(dnId, ipAddress, hostName);
        // Updating Node Report, as registration is successful
        processNodeReport(datanodeDetails, nodeReport);
        LOG.info("Registered datanode: {}", datanodeDetails.toDebugString());
        scmNodeEventPublisher.fireEvent(SCMEvents.NEW_NODE, datanodeDetails);
      } catch (NodeAlreadyExistsException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Datanode is already registered: {}",
              datanodeDetails);
        }
      } catch (NodeNotFoundException e) {
        LOG.error("Cannot find datanode {} from nodeStateManager",
            datanodeDetails);
      }
    } else {
      // Update datanode if it is registered but the ip or hostname changes
      try {
        final DatanodeInfo oldNode = nodeStateManager.getNode(datanodeDetails);
        if (updateDnsToDnIdMap(oldNode.getHostName(), oldNode.getIpAddress(),
            hostName, ipAddress, dnId)) {
          LOG.info("Updating datanode from {} to {}", oldNode, datanodeDetails);
          clusterMap.update(oldNode, datanodeDetails);
          nodeStateManager.updateNode(datanodeDetails, layoutInfo);
          DatanodeDetails dn = nodeStateManager.getNode(datanodeDetails);
          Preconditions.checkState(dn.getParent() != null);
          processNodeReport(datanodeDetails, nodeReport);
          LOG.info("Updated datanode to: {}", dn);
          scmNodeEventPublisher.fireEvent(SCMEvents.NODE_ADDRESS_UPDATE, dn);
        } else if (isVersionChange(oldNode.getVersion(), datanodeDetails.getVersion())) {
          LOG.info("Update the version for registered datanode {}, " +
              "oldVersion = {}, newVersion = {}.",
              datanodeDetails, oldNode.getVersion(), datanodeDetails.getVersion());
          nodeStateManager.updateNode(datanodeDetails, layoutInfo);
        }
      } catch (NodeNotFoundException e) {
        LOG.error("Cannot find datanode {} from nodeStateManager",
                datanodeDetails);
      }
    }

    return RegisteredCommand.newBuilder().setErrorCode(ErrorCode.success)
        .setDatanode(datanodeDetails)
        .setClusterID(this.scmStorageConfig.getClusterID())
        .build();
  }

  /**
   * Add an entry to the dnsToUuidMap, which maps hostname / IP to the DNs
   * running on that host. As each address can have many DNs running on it,
   * and each host can have multiple addresses,
   * this is a many to many mapping.
   *
   * @param datanodeID the DataNodeID of the registered node.
   * @param addresses hostname and/or IP of the node
   */
  private synchronized void addToDnsToDnIdMap(DatanodeID datanodeID, String... addresses) {
    for (String addr : addresses) {
      if (!Strings.isNullOrEmpty(addr)) {
        dnsToDnIdMap.computeIfAbsent(addr, k -> ConcurrentHashMap.newKeySet())
            .add(datanodeID);
      }
    }
  }

  private synchronized void removeFromDnsToDnIdMap(DatanodeID datanodeID, String address) {
    if (address != null) {
      Set<DatanodeID> dnSet = dnsToDnIdMap.get(address);
      if (dnSet != null && dnSet.remove(datanodeID) && dnSet.isEmpty()) {
        dnsToDnIdMap.remove(address);
      }
    }
  }

  private boolean updateDnsToDnIdMap(
      String oldHostName, String oldIpAddress,
      String newHostName, String newIpAddress,
      DatanodeID datanodeID) {
    final boolean ipChanged = !Objects.equals(oldIpAddress, newIpAddress);
    final boolean hostNameChanged = !Objects.equals(oldHostName, newHostName);
    if (ipChanged || hostNameChanged) {
      synchronized (this) {
        if (ipChanged) {
          removeFromDnsToDnIdMap(datanodeID, oldIpAddress);
          addToDnsToDnIdMap(datanodeID, newIpAddress);
        }
        if (hostNameChanged) {
          removeFromDnsToDnIdMap(datanodeID, oldHostName);
          addToDnsToDnIdMap(datanodeID, newHostName);
        }
      }
    }
    return ipChanged || hostNameChanged;
  }

  /**
   * Check if the version has been updated.
   *
   * @param oldVersion datanode oldVersion
   * @param newVersion datanode newVersion
   * @return true means replacement is needed, while false means replacement is not needed.
   */
  private boolean isVersionChange(String oldVersion, String newVersion) {
    final boolean versionChanged = !Objects.equals(oldVersion, newVersion);
    return versionChanged;
  }

  /**
   * Send heartbeat to indicate the datanode is alive and doing well.
   *
   * @param datanodeDetails - DatanodeDetailsProto.
   * @return SCMheartbeat response.
   */
  @Override
  public List<SCMCommand<?>> processHeartbeat(DatanodeDetails datanodeDetails,
                                           CommandQueueReportProto queueReport) {
    Objects.requireNonNull(datanodeDetails, "Heartbeat is missing " +
        "DatanodeDetails.");
    try {
      nodeStateManager.updateLastHeartbeatTime(datanodeDetails);
      metrics.incNumHBProcessed();
      updateDatanodeOpState(datanodeDetails);
    } catch (NodeNotFoundException e) {
      metrics.incNumHBProcessingFailed();
      LOG.error("SCM trying to process heartbeat from an " +
          "unregistered node {}. Ignoring the heartbeat.", datanodeDetails);
    }
    writeLock().lock();
    try {
      Map<SCMCommandProto.Type, Integer> summary =
          commandQueue.getDatanodeCommandSummary(datanodeDetails.getID());
      List<SCMCommand<?>> commands =
          commandQueue.getCommand(datanodeDetails.getID());

      // Update the SCMCommand of deleteBlocksCommand Status
      for (SCMCommand<?> command : commands) {
        if (sendCommandNotifyMap.get(command.getType()) != null) {
          sendCommandNotifyMap.get(command.getType())
              .accept(datanodeDetails, command);
        }
      }

      if (queueReport != null) {
        processNodeCommandQueueReport(datanodeDetails, queueReport, summary);
      }
      return commands;
    } finally {
      writeLock().unlock();
    }
  }

  boolean opStateDiffers(DatanodeDetails dnDetails, NodeStatus nodeStatus) {
    return nodeStatus.getOperationalState() != dnDetails.getPersistedOpState()
        || nodeStatus.getOpStateExpiryEpochSeconds()
        != dnDetails.getPersistedOpStateExpiryEpochSec();
  }

  /**
   * This method should only be called when processing the heartbeat.
   *
   * On leader SCM, for a registered node, the information stored in SCM is
   * the source of truth. If the operational state or expiry reported in the
   * datanode heartbeat do not match those store in SCM, queue a command to
   * update the state persisted on the datanode. Additionally, ensure the
   * datanodeDetails stored in SCM match those reported in the heartbeat.
   *
   * On follower SCM, datanode notifies follower SCM its latest operational
   * state or expiry via heartbeat. If the operational state or expiry
   * reported in the datanode heartbeat do not match those stored in SCM,
   * just update the state in follower SCM accordingly.
   *
   * @param reportedDn The DatanodeDetails taken from the node heartbeat.
   * @throws NodeNotFoundException
   */
  protected void updateDatanodeOpState(DatanodeDetails reportedDn)
      throws NodeNotFoundException {
    NodeStatus scmStatus = getNodeStatus(reportedDn);
    if (opStateDiffers(reportedDn, scmStatus)) {
      if (scmContext.isLeader()) {
        LOG.info("Scheduling a command to update the operationalState " +
                "persisted on {} as the reported value ({}, {}) does not " +
                "match the value stored in SCM ({}, {})",
            reportedDn,
            reportedDn.getPersistedOpState(),
            reportedDn.getPersistedOpStateExpiryEpochSec(),
            scmStatus.getOperationalState(),
            scmStatus.getOpStateExpiryEpochSeconds());

        try {
          SCMCommand<?> command = new SetNodeOperationalStateCommand(
              Time.monotonicNow(),
              scmStatus.getOperationalState(),
              scmStatus.getOpStateExpiryEpochSeconds());
          command.setTerm(scmContext.getTermOfLeader());
          addDatanodeCommand(reportedDn.getID(), command);
        } catch (NotLeaderException nle) {
          LOG.warn("Skip sending SetNodeOperationalStateCommand,"
              + " since current SCM is not leader.", nle);
          return;
        }
      } else {
        LOG.info("Update the operationalState saved in follower SCM " +
                "for {} as the reported value ({}, {}) does not " +
                "match the value stored in SCM ({}, {})",
            reportedDn,
            reportedDn.getPersistedOpState(),
            reportedDn.getPersistedOpStateExpiryEpochSec(),
            scmStatus.getOperationalState(),
            scmStatus.getOpStateExpiryEpochSeconds());

        setNodeOperationalState(reportedDn, reportedDn.getPersistedOpState(),
            reportedDn.getPersistedOpStateExpiryEpochSec());
      }
    }
    DatanodeDetails scmDnd = nodeStateManager.getNode(reportedDn);
    NodeOperationalState oldPersistedOpState = scmDnd.getPersistedOpState();
    NodeOperationalState newPersistedOpState = reportedDn.getPersistedOpState();

    scmDnd.setPersistedOpStateExpiryEpochSec(
        reportedDn.getPersistedOpStateExpiryEpochSec());
    scmDnd.setPersistedOpState(newPersistedOpState);

    maybeNotifyReplicationManager(reportedDn, oldPersistedOpState, newPersistedOpState);
  }

  private void maybeNotifyReplicationManager(
      DatanodeDetails datanode,
      NodeOperationalState oldState,
      NodeOperationalState newState) {
    if (!scmContext.isLeader()) {
      return;
    }

    if (oldState != newState) {
      // Notify when a node is entering maintenance, decommissioning or back to service
      if (newState == NodeOperationalState.ENTERING_MAINTENANCE
          || newState == NodeOperationalState.DECOMMISSIONING
          || newState == NodeOperationalState.IN_SERVICE) {
        LOG.info("Notifying ReplicationManager of node state change for {}: {} -> {}",
            datanode, oldState, newState);
        scmNodeEventPublisher.fireEvent(SCMEvents.REPLICATION_MANAGER_NOTIFY, datanode);
      }
    }
  }

  @Override
  public Boolean isNodeRegistered(DatanodeDetails datanodeDetails) {
    try {
      nodeStateManager.getNode(datanodeDetails);
      return true;
    } catch (NodeNotFoundException e) {
      return false;
    }
  }

  /**
   * Process node report.
   *
   * @param datanodeDetails
   * @param nodeReport
   */
  @Override
  public void processNodeReport(DatanodeDetails datanodeDetails,
      NodeReportProto nodeReport) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing node report from [datanode={}]",
          datanodeDetails.getHostName());
    }
    if (LOG.isTraceEnabled() && nodeReport != null) {
      LOG.trace("HB is received from [datanode={}]: <json>{}</json>",
          datanodeDetails.getHostName(),
          nodeReport.toString().replaceAll("\n", "\\\\n"));
    }
    try {
      DatanodeInfo datanodeInfo = nodeStateManager.getNode(datanodeDetails);
      if (nodeReport != null) {
        datanodeInfo.updateStorageReports(nodeReport.getStorageReportList());
        datanodeInfo.updateMetaDataStorageReports(nodeReport.
            getMetadataStorageReportList());
        metrics.incNumNodeReportProcessed();
      }
    } catch (NodeNotFoundException e) {
      metrics.incNumNodeReportProcessingFailed();
      LOG.warn("Got node report from unregistered datanode {}",
          datanodeDetails);
    }
  }

  /**
   * Process Layout Version report.
   *
   * @param datanodeDetails
   * @param layoutVersionReport
   */
  @Override
  public void processLayoutVersionReport(DatanodeDetails datanodeDetails,
                                LayoutVersionProto layoutVersionReport) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing Layout Version report from [datanode={}]",
          datanodeDetails.getHostName());
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("HB is received from [datanode={}]: <json>{}</json>",
          datanodeDetails.getHostName(),
          layoutVersionReport.toString().replaceAll("\n", "\\\\n"));
    }

    try {
      nodeStateManager.updateLastKnownLayoutVersion(datanodeDetails,
          layoutVersionReport);
    } catch (NodeNotFoundException e) {
      LOG.error("SCM trying to process Layout Version from an " +
          "unregistered node {}.", datanodeDetails);
      return;
    }

    sendFinalizeToDatanodeIfNeeded(datanodeDetails, layoutVersionReport);
  }

  protected void sendFinalizeToDatanodeIfNeeded(DatanodeDetails datanodeDetails,
      LayoutVersionProto layoutVersionReport) {
    // Software layout version is hardcoded to the SCM.
    int scmSlv = scmLayoutVersionManager.getSoftwareLayoutVersion();
    int dnSlv = layoutVersionReport.getSoftwareLayoutVersion();
    int dnMlv = layoutVersionReport.getMetadataLayoutVersion();

    // A datanode with a larger software layout version is from a future
    // version of ozone. It should not have been added to the cluster.
    if (dnSlv > scmSlv) {
      LOG.error("Invalid data node in the cluster : {}. " +
              "DataNode SoftwareLayoutVersion = {}, SCM " +
              "SoftwareLayoutVersion = {}",
          datanodeDetails.getHostName(), dnSlv, scmSlv);
    }

    if (FinalizationManager.shouldTellDatanodesToFinalize(
        scmContext.getFinalizationCheckpoint())) {
      // Because we have crossed the MLV_EQUALS_SLV checkpoint, SCM metadata
      // layout version will not change. We can now compare it to the
      // datanodes' metadata layout versions to tell them to finalize.
      int scmMlv = scmLayoutVersionManager.getMetadataLayoutVersion();

      // If the datanode mlv < scm mlv, it can not be allowed to be part of
      // any pipeline. However it can be allowed to join the cluster
      if (dnMlv < scmMlv) {
        LOG.warn("Data node {} can not be used in any pipeline in the " +
                "cluster. " + "DataNode MetadataLayoutVersion = {}, SCM " +
                "MetadataLayoutVersion = {}",
            datanodeDetails.getHostName(), dnMlv, scmMlv);

        FinalizeNewLayoutVersionCommand finalizeCmd =
            new FinalizeNewLayoutVersionCommand(true,
                LayoutVersionProto.newBuilder()
                    .setSoftwareLayoutVersion(dnSlv)
                    .setMetadataLayoutVersion(dnSlv).build());
        if (scmContext.isLeader()) {
          try {
            finalizeCmd.setTerm(scmContext.getTermOfLeader());

            // Send Finalize command to the data node. Its OK to
            // send Finalize command multiple times.
            scmNodeEventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
                new CommandForDatanode<>(datanodeDetails,
                    finalizeCmd));
          } catch (NotLeaderException ex) {
            LOG.warn("Skip sending finalize upgrade command since current SCM" +
                " is not leader.", ex);
          }
        }
      }
    }
  }

  /**
   * Process Command Queue Reports from the Datanode Heartbeat.
   *
   * @param datanodeDetails
   * @param commandQueueReportProto
   * @param commandsToBeSent
   */
  private void processNodeCommandQueueReport(DatanodeDetails datanodeDetails,
      CommandQueueReportProto commandQueueReportProto,
      Map<SCMCommandProto.Type, Integer> commandsToBeSent) {
    LOG.debug("Processing Command Queue Report from [datanode={}]",
        datanodeDetails.getHostName());
    if (commandQueueReportProto == null) {
      LOG.debug("The Command Queue Report from [datanode={}] is null",
          datanodeDetails.getHostName());
      return;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Command Queue Report is received from [datanode={}]: " +
          "<json>{}</json>", datanodeDetails.getHostName(),
          commandQueueReportProto.toString().replaceAll("\n", "\\\\n"));
    }
    try {
      DatanodeInfo datanodeInfo = nodeStateManager.getNode(datanodeDetails);
      datanodeInfo.setCommandCounts(commandQueueReportProto,
          commandsToBeSent);
      metrics.incNumNodeCommandQueueReportProcessed();
      scmNodeEventPublisher.fireEvent(
          SCMEvents.DATANODE_COMMAND_COUNT_UPDATED, datanodeDetails);
    } catch (NodeNotFoundException e) {
      metrics.incNumNodeCommandQueueReportProcessingFailed();
      LOG.warn("Got Command Queue Report from unregistered datanode {}",
          datanodeDetails);
    }
  }

  /**
   * Get the number of commands of the given type queued on the datanode at the
   * last heartbeat. If the Datanode has not reported information for the given
   * command type, -1 will be returned.
   * @param cmdType
   * @return The queued count or -1 if no data has been received from the DN.
   */
  @Override
  public int getNodeQueuedCommandCount(DatanodeDetails datanodeDetails,
      SCMCommandProto.Type cmdType) throws NodeNotFoundException {
    readLock().lock();
    try {
      DatanodeInfo datanodeInfo = nodeStateManager.getNode(datanodeDetails);
      return datanodeInfo.getCommandCount(cmdType);
    } finally {
      readLock().unlock();
    }
  }

  /**
   * Get the number of commands of the given type queued in the SCM CommandQueue
   * for the given datanode.
   * @param dnID The UUID of the datanode.
   * @param cmdType The Type of command to query the current count for.
   * @return The count of commands queued, or zero if none.
   */
  @Override
  public int getCommandQueueCount(DatanodeID dnID, SCMCommandProto.Type cmdType) {
    readLock().lock();
    try {
      return commandQueue.getDatanodeCommandCount(dnID, cmdType);
    } finally {
      readLock().unlock();
    }
  }

  /**
   * Get the total number of pending commands of the given type on the given
   * datanode. This includes both the number of commands queued in SCM which
   * will be sent to the datanode on the next heartbeat, and the number of
   * commands reported by the datanode in the last heartbeat.
   * If the datanode has not reported any information for the given command,
   * zero is assumed.
   * @param datanodeDetails The datanode to query.
   * @param cmdType The command Type To query.
   * @return The number of commands of the given type pending on the datanode.
   * @throws NodeNotFoundException
   */
  @Override
  public int getTotalDatanodeCommandCount(DatanodeDetails datanodeDetails,
      SCMCommandProto.Type cmdType) throws NodeNotFoundException {
    readLock().lock();
    try {
      int dnCount = getNodeQueuedCommandCount(datanodeDetails, cmdType);
      if (dnCount == -1) {
        LOG.warn("No command count information for datanode {} and command {}" +
            ". Assuming zero", datanodeDetails, cmdType);
        dnCount = 0;
      }
      return getCommandQueueCount(datanodeDetails.getID(), cmdType) + dnCount;
    } finally {
      readLock().unlock();
    }
  }

  /**
   * Get the total number of pending commands of the given types on the given
   * datanode. For each command, this includes both the number of commands
   * queued in SCM which will be sent to the datanode on the next heartbeat,
   * and the number of commands reported by the datanode in the last heartbeat.
   * If the datanode has not reported any information for the given command,
   * zero is assumed.
   * All commands are retrieved under a single read lock, so the counts are
   * consistent.
   * @param datanodeDetails The datanode to query.
   * @param cmdType The list of command Types To query.
   * @return A Map of commandType to Integer with an entry for each command type
   *         passed.
   * @throws NodeNotFoundException
   */
  @Override
  public Map<SCMCommandProto.Type, Integer> getTotalDatanodeCommandCounts(
      DatanodeDetails datanodeDetails, SCMCommandProto.Type... cmdType)
      throws NodeNotFoundException {
    Map<SCMCommandProto.Type, Integer> counts = new HashMap<>();
    readLock().lock();
    try {
      for (SCMCommandProto.Type type : cmdType) {
        counts.put(type, getTotalDatanodeCommandCount(datanodeDetails, type));
      }
      return counts;
    } finally {
      readLock().unlock();
    }
  }

  /**
   * Returns the aggregated node stats.
   *
   * @return the aggregated node stats.
   */
  @Override
  public SCMNodeStat getStats() {
    long capacity = 0L;
    long used = 0L;
    long remaining = 0L;
    long committed = 0L;
    long freeSpaceToSpare = 0L;
    long reserved = 0L;

    for (SCMNodeStat stat : getNodeStats().values()) {
      capacity += stat.getCapacity().get();
      used += stat.getScmUsed().get();
      remaining += stat.getRemaining().get();
      committed += stat.getCommitted().get();
      freeSpaceToSpare += stat.getFreeSpaceToSpare().get();
      reserved += stat.getReserved().get();
    }
    return new SCMNodeStat(capacity, used, remaining, committed,
        freeSpaceToSpare, reserved);
  }

  /**
   * Return a map of node stats.
   *
   * @return a map of individual node stats (live/stale but not dead).
   */
  @Override
  public Map<DatanodeDetails, SCMNodeStat> getNodeStats() {

    final Map<DatanodeDetails, SCMNodeStat> nodeStats = new HashMap<>();

    final List<DatanodeInfo> healthyNodes = nodeStateManager
        .getNodes(null, HEALTHY);
    final List<DatanodeInfo> staleNodes = nodeStateManager
        .getStaleNodes();
    final List<DatanodeInfo> datanodes = new ArrayList<>(healthyNodes);
    datanodes.addAll(staleNodes);

    for (DatanodeInfo dnInfo : datanodes) {
      SCMNodeStat nodeStat = getNodeStatInternal(dnInfo);
      if (nodeStat != null) {
        nodeStats.put(dnInfo, nodeStat);
      }
    }
    return nodeStats;
  }

  /**
   * Gets a sorted list of most or least used DatanodeUsageInfo containing
   * healthy, in-service nodes. If the specified mostUsed is true, the returned
   * list is in descending order of usage. Otherwise, the returned list is in
   * ascending order of usage.
   *
   * @param mostUsed true if most used, false if least used
   * @return List of DatanodeUsageInfo
   */
  @Override
  public List<DatanodeUsageInfo> getMostOrLeastUsedDatanodes(
      boolean mostUsed) {
    List<DatanodeDetails> healthyNodes =
        getNodes(IN_SERVICE, NodeState.HEALTHY);

    List<DatanodeUsageInfo> datanodeUsageInfoList =
        new ArrayList<>(healthyNodes.size());

    // create a DatanodeUsageInfo from each DatanodeDetails and add it to the
    // list
    for (DatanodeDetails node : healthyNodes) {
      DatanodeUsageInfo datanodeUsageInfo = getUsageInfo(node);
      datanodeUsageInfoList.add(datanodeUsageInfo);
    }

    // sort the list according to appropriate comparator
    if (mostUsed) {
      datanodeUsageInfoList.sort(
          DatanodeUsageInfo.getMostUtilized().reversed());
    } else {
      datanodeUsageInfoList.sort(
          DatanodeUsageInfo.getMostUtilized());
    }
    return datanodeUsageInfoList;
  }

  /**
   * Get the usage info of a specified datanode.
   *
   * @param dn the usage of which we want to get
   * @return DatanodeUsageInfo of the specified datanode
   */
  @Override
  public DatanodeUsageInfo getUsageInfo(DatanodeDetails dn) {
    SCMNodeStat stat = getNodeStatInternal(dn);
    DatanodeUsageInfo usageInfo = new DatanodeUsageInfo(dn, stat);
    try {
      usageInfo.setContainerCount(getContainerCount(dn));
      usageInfo.setPipelineCount(getPipeLineCount(dn));
      usageInfo.setReserved(getTotalReserved(dn));
      SpaceUsageSource.Fixed fs = getTotalFilesystemUsage(dn);
      if (fs != null) {
        usageInfo.setFilesystemUsage(fs.getCapacity(), fs.getAvailable());
      }
    } catch (NodeNotFoundException ex) {
      LOG.error("Unknown datanode {}.", dn, ex);
    }
    return usageInfo;
  }

  /**
   * Get the usage info of a specified datanode.
   *
   * @param dn the usage of which we want to get
   * @return DatanodeUsageInfo of the specified datanode
   */
  @Override
  @Nullable
  public DatanodeInfo getDatanodeInfo(DatanodeDetails dn) {
    try {
      return nodeStateManager.getNode(dn);
    } catch (NodeNotFoundException e) {
      LOG.warn("Cannot retrieve DatanodeInfo, datanode {} not found.",
          dn.getUuid());
      return null;
    }
  }

  /**
   * Return the node stat of the specified datanode.
   *
   * @param datanodeDetails - datanode ID.
   * @return node stat if it is live/stale, null if it is decommissioned or
   * doesn't exist.
   */
  @Override
  public SCMNodeMetric getNodeStat(DatanodeDetails datanodeDetails) {
    final SCMNodeStat nodeStat = getNodeStatInternal(datanodeDetails);
    return nodeStat != null ? new SCMNodeMetric(nodeStat) : null;
  }

  private SCMNodeStat getNodeStatInternal(DatanodeDetails datanodeDetails) {
    try {
      long capacity = 0L;
      long used = 0L;
      long remaining = 0L;
      long committed = 0L;
      long freeSpaceToSpare = 0L;
      long reserved = 0L;

      final DatanodeInfo datanodeInfo = nodeStateManager
          .getNode(datanodeDetails);
      final List<StorageReportProto> storageReportProtos = datanodeInfo
          .getStorageReports();
      for (StorageReportProto reportProto : storageReportProtos) {
        capacity += reportProto.getCapacity();
        used += reportProto.getScmUsed();
        remaining += reportProto.getRemaining();
        committed += reportProto.getCommitted();
        freeSpaceToSpare += reportProto.getFreeSpaceToSpare();
        reserved += reportProto.getReserved();
      }
      return new SCMNodeStat(capacity, used, remaining, committed,
          freeSpaceToSpare, reserved);
    } catch (NodeNotFoundException e) {
      LOG.warn("Cannot generate NodeStat, datanode {} not found.", datanodeDetails);
      return null;
    }
  }

  @Override // NodeManagerMXBean
  public Map<String, Map<String, Integer>> getNodeCount() {
    Map<String, Map<String, Integer>> nodes = new HashMap<>();
    for (NodeOperationalState opState : NodeOperationalState.values()) {
      Map<String, Integer> states = new HashMap<>();
      for (NodeState health : NodeState.values()) {
        if (health == NodeState.HEALTHY_READONLY) {
          // HEALTHY_READONLY is deprecated and can no longer occur in SCM, but it cannot be removed
          // from the protobuf for compatibility reasons. Skip it here to avoid confusion.
          continue;
        }
        states.put(health.name(), 0);
      }
      nodes.put(opState.name(), states);
    }
    for (DatanodeInfo dni : nodeStateManager.getAllNodes()) {
      NodeStatus status = dni.getNodeStatus();
      nodes.get(status.getOperationalState().name())
          .compute(status.getHealth().name(), (k, v) -> v + 1);
    }
    return nodes;
  }

  // We should introduce DISK, SSD, etc., notion in
  // SCMNodeStat and try to use it.
  @Override // NodeManagerMXBean
  public Map<String, Long> getNodeInfo() {
    Map<String, Long> nodeInfo = new HashMap<>();
    // Compute all the possible stats from the enums, and default to zero:
    for (UsageStates s : UsageStates.values()) {
      for (UsageMetrics stat : UsageMetrics.values()) {
        nodeInfo.put(s.label + stat.name(), 0L);
      }
    }
    nodeInfo.put("TotalOzoneCapacity", 0L);
    nodeInfo.put("TotalOzoneUsed", 0L);
    // Raw filesystem totals across non-dead nodes. -1 means old (older version) DN did not send fs stats.
    nodeInfo.put("TotalFilesystemCapacity", -1L);
    nodeInfo.put("TotalFilesystemUsed", -1L);
    nodeInfo.put("TotalFilesystemAvailable", -1L);

    long totalFsCapacity = 0L;
    long totalFsAvailable = 0L;
    /*
    If any storage report is missing fs stats, this is a rolling upgrade scenario in which some older dn versions
    aren't reporting fs stats. Better to not report aggregated fs stats at all in this case?
     */
    boolean fsPresent = false;
    boolean fsMissing = false;

    for (DatanodeInfo node : nodeStateManager.getAllNodes()) {
      String keyPrefix = "";
      NodeStatus status = node.getNodeStatus();
      if (status.isMaintenance()) {
        keyPrefix = UsageStates.MAINT.getLabel();
      } else if (status.isDecommission()) {
        keyPrefix = UsageStates.DECOM.getLabel();
      } else if (status.isAlive()) {
        // Inservice but not dead
        keyPrefix = UsageStates.ONLINE.getLabel();
      } else {
        // dead inservice node, skip it
        continue;
      }
      List<StorageReportProto> storageReportProtos = node.getStorageReports();
      for (StorageReportProto reportProto : storageReportProtos) {
        if (reportProto.getStorageType() == StorageTypeProto.DISK) {
          nodeInfo.compute(keyPrefix + UsageMetrics.DiskCapacity.name(),
              (k, v) -> v + reportProto.getCapacity());
          nodeInfo.compute(keyPrefix + UsageMetrics.DiskRemaining.name(),
              (k, v) -> v + reportProto.getRemaining());
          nodeInfo.compute(keyPrefix + UsageMetrics.DiskUsed.name(),
              (k, v) -> v + reportProto.getScmUsed());
        } else if (reportProto.getStorageType() == StorageTypeProto.SSD) {
          nodeInfo.compute(keyPrefix + UsageMetrics.SSDCapacity.name(),
              (k, v) -> v + reportProto.getCapacity());
          nodeInfo.compute(keyPrefix + UsageMetrics.SSDRemaining.name(),
              (k, v) -> v + reportProto.getRemaining());
          nodeInfo.compute(keyPrefix + UsageMetrics.SSDUsed.name(),
              (k, v) -> v + reportProto.getScmUsed());
        }
        nodeInfo.compute("TotalOzoneCapacity", (k, v) -> v + reportProto.getCapacity());
        nodeInfo.compute("TotalOzoneUsed", (k, v) -> v + reportProto.getScmUsed());

        if (reportProto.hasFailed() && reportProto.getFailed()) {
          continue;
        }
        if (reportProto.hasFsCapacity() && reportProto.hasFsAvailable()) {
          fsPresent = true;
          totalFsCapacity += reportProto.getFsCapacity();
          totalFsAvailable += reportProto.getFsAvailable();
        } else {
          fsMissing = true;
        }
      }
    }
    if (fsPresent && !fsMissing) {
      // don't report aggregated fs stats if some storage reports did not have them
      nodeInfo.put("TotalFilesystemCapacity", totalFsCapacity);
      nodeInfo.put("TotalFilesystemUsed", totalFsCapacity - totalFsAvailable);
      nodeInfo.put("TotalFilesystemAvailable", totalFsAvailable);
    }
    return nodeInfo;
  }

  @Override
  public Map<String, Map<String, String>> getNodeStatusInfo() {
    Map<String, Map<String, String>> nodes = new HashMap<>();
    for (DatanodeInfo dni : nodeStateManager.getAllNodes()) {
      String hostName = dni.getHostName();
      DatanodeDetails.Port httpPort = dni.getPort(HTTP);
      DatanodeDetails.Port httpsPort = dni.getPort(HTTPS);
      String opstate = "";
      String healthState = "";
      String heartbeatTimeDiff = "";
      if (dni.getNodeStatus() != null) {
        opstate = dni.getNodeStatus().getOperationalState().toString();
        healthState = dni.getNodeStatus().getHealth().toString();
        heartbeatTimeDiff = getLastHeartbeatTimeDiff(dni.getLastHeartbeatTime());
      }
      Map<String, String> map = new HashMap<>();
      map.put(OPESTATE, opstate);
      map.put(COMSTATE, healthState);
      map.put(LASTHEARTBEAT, heartbeatTimeDiff);
      if (httpPort != null) {
        map.put(httpPort.getName().toString(), httpPort.getValue().toString());
      }
      if (httpsPort != null) {
        map.put(httpsPort.getName().toString(),
                  httpsPort.getValue().toString());
      }
      String capacity = calculateStorageCapacity(dni.getStorageReports());
      map.put(TOTALCAPACITY, capacity);
      String[] storagePercentage = calculateStoragePercentage(
          dni.getStorageReports());
      String scmUsedPerc = storagePercentage[0];
      String nonScmUsedPerc = storagePercentage[1];
      map.put(USEDSPACEPERCENT,
          "Ozone: " + scmUsedPerc + "%, other: " + nonScmUsedPerc + "%");
      map.put(DNUUID, dni.getUuidString());
      map.put(VERSION, dni.getVersion());
      nodes.put(hostName, map);
    }
    return nodes;
  }

  /**
   * Calculate the storage capacity of the DataNode node.
   * @param storageReports Calculate the storage capacity corresponding
   *                       to the storage collection.
   */
  public static String calculateStorageCapacity(
      List<StorageReportProto> storageReports) {
    long capacityByte = 0;
    if (storageReports != null && !storageReports.isEmpty()) {
      for (StorageReportProto storageReport : storageReports) {
        capacityByte += storageReport.getCapacity();
      }
    }

    return convertUnit(capacityByte);
  }

  /**
   * Convert byte value to other units, such as KB, MB, GB, TB.
   * @param value Original value, in byte.
   * @return
   */
  private static String convertUnit(double value) {
    StringBuilder unit = new StringBuilder("B");
    if (value > 1024) {
      value = value / 1024;
      unit.replace(0, 1, "KB");
    }
    if (value > 1024) {
      value = value / 1024;
      unit.replace(0, 2, "MB");
    }
    if (value > 1024) {
      value = value / 1024;
      unit.replace(0, 2, "GB");
    }
    if (value > 1024) {
      value = value / 1024;
      unit.replace(0, 2, "TB");
    }

    DecimalFormat decimalFormat = new DecimalFormat("#0.0");
    decimalFormat.setRoundingMode(RoundingMode.HALF_UP);
    String newValue = decimalFormat.format(value);
    return newValue + unit.toString();
  }

  /**
   * Calculate the storage usage percentage of a DataNode node.
   * @param storageReports Calculate the storage percentage corresponding
   *                       to the storage collection.
   */
  public static String[] calculateStoragePercentage(
      List<StorageReportProto> storageReports) {
    String[] storagePercentage = new String[2];
    String usedPercentage = "N/A";
    String nonUsedPercentage = "N/A";
    if (storageReports != null && !storageReports.isEmpty()) {
      long capacity = 0;
      long scmUsed = 0;
      long remaining = 0;
      for (StorageReportProto storageReport : storageReports) {
        capacity += storageReport.getCapacity();
        scmUsed += storageReport.getScmUsed();
        remaining += storageReport.getRemaining();
      }
      long scmNonUsed = capacity - scmUsed - remaining;

      DecimalFormat decimalFormat = new DecimalFormat("#0.00");
      decimalFormat.setRoundingMode(RoundingMode.HALF_UP);

      double usedPerc = ((double) scmUsed / capacity) * 100;
      usedPerc = usedPerc > 100.0 ? 100.0 : usedPerc;
      double nonUsedPerc = ((double) scmNonUsed / capacity) * 100;
      nonUsedPerc = nonUsedPerc > 100.0 ? 100.0 : nonUsedPerc;
      usedPercentage = decimalFormat.format(usedPerc);
      nonUsedPercentage = decimalFormat.format(nonUsedPerc);
    }

    storagePercentage[0] = usedPercentage;
    storagePercentage[1] = nonUsedPercentage;
    return storagePercentage;
  }

  @Override
  public Map<String, String> getNodeStatistics() {
    Map<String, String> nodeStatistics = new HashMap<>();
    // Statistics node usaged
    nodeUsageStatistics(nodeStatistics);
    // Statistics node states
    nodeStateStatistics(nodeStatistics);
    // Statistics node space
    nodeSpaceStatistics(nodeStatistics);
    // Statistics node non-writable
    nodeNonWritableStatistics(nodeStatistics);
    // todo: Statistics of other instances
    return nodeStatistics;
  }

  private void nodeUsageStatistics(Map<String, String> nodeStatics) {
    if (nodeStateManager.getAllNodes().isEmpty()) {
      return;
    }
    float[] usages = new float[nodeStateManager.getAllNodes().size()];
    float totalOzoneUsed = 0;
    int i = 0;
    for (DatanodeInfo dni : nodeStateManager.getAllNodes()) {
      String[] storagePercentage = calculateStoragePercentage(
              dni.getStorageReports());
      if (storagePercentage[0].equals("N/A")) {
        usages[i++] = 0;
      } else {
        float storagePerc = Float.parseFloat(storagePercentage[0]);
        usages[i++] = storagePerc;
        totalOzoneUsed = totalOzoneUsed + storagePerc;
      }
    }

    totalOzoneUsed /= nodeStateManager.getAllNodes().size();
    Arrays.sort(usages);
    float median = usages[usages.length / 2];
    nodeStatics.put(UsageStatics.MEDIAN.getLabel(), String.valueOf(median));
    float max = usages[usages.length - 1];
    nodeStatics.put(UsageStatics.MAX.getLabel(), String.valueOf(max));
    float min = usages[0];
    nodeStatics.put(UsageStatics.MIN.getLabel(), String.valueOf(min));

    float dev = 0;
    for (i = 0; i < usages.length; i++) {
      dev += (usages[i] - totalOzoneUsed) * (usages[i] - totalOzoneUsed);
    }
    dev = (float) Math.sqrt(dev / usages.length);
    DecimalFormat decimalFormat = new DecimalFormat("#0.00");
    decimalFormat.setRoundingMode(RoundingMode.HALF_UP);
    nodeStatics.put(UsageStatics.STDEV.getLabel(), decimalFormat.format(dev));
  }

  private void nodeStateStatistics(Map<String, String> nodeStatics) {
    int healthyNodeCount = nodeStateManager.getHealthyNodeCount();
    int deadNodeCount = nodeStateManager.getDeadNodeCount();
    int decommissioningNodeCount = nodeStateManager.getDecommissioningNodeCount();
    int enteringMaintenanceNodeCount = nodeStateManager.getEnteringMaintenanceNodeCount();
    int volumeFailuresNodeCount = nodeStateManager.getVolumeFailuresNodeCount();
    nodeStatics.put(StateStatistics.HEALTHY.getLabel(), String.valueOf(healthyNodeCount));
    nodeStatics.put(StateStatistics.DEAD.getLabel(), String.valueOf(deadNodeCount));
    nodeStatics.put(StateStatistics.DECOMMISSIONING.getLabel(), String.valueOf(decommissioningNodeCount));
    nodeStatics.put(StateStatistics.ENTERING_MAINTENANCE.getLabel(), String.valueOf(enteringMaintenanceNodeCount));
    nodeStatics.put(StateStatistics.VOLUME_FAILURES.getLabel(), String.valueOf(volumeFailuresNodeCount));
  }

  private void nodeSpaceStatistics(Map<String, String> nodeStatics) {
    if (nodeStateManager.getAllNodes().isEmpty()) {
      return;
    }
    long capacityByte = 0;
    long scmUsedByte = 0;
    long remainingByte = 0;
    for (DatanodeInfo dni : nodeStateManager.getAllNodes()) {
      List<StorageReportProto> storageReports = dni.getStorageReports();
      if (storageReports != null && !storageReports.isEmpty()) {
        for (StorageReportProto storageReport : storageReports) {
          capacityByte += storageReport.getCapacity();
          scmUsedByte += storageReport.getScmUsed();
          remainingByte += storageReport.getRemaining();
        }
      }
    }

    long nonScmUsedByte = capacityByte - scmUsedByte - remainingByte;
    if (nonScmUsedByte < 0) {
      nonScmUsedByte = 0;
    }
    String capacity = convertUnit(capacityByte);
    String scmUsed = convertUnit(scmUsedByte);
    String remaining = convertUnit(remainingByte);
    String nonScmUsed = convertUnit(nonScmUsedByte);
    nodeStatics.put(SpaceStatistics.CAPACITY.getLabel(), capacity);
    nodeStatics.put(SpaceStatistics.SCM_USED.getLabel(), scmUsed);
    nodeStatics.put(SpaceStatistics.REMAINING.getLabel(), remaining);
    nodeStatics.put(SpaceStatistics.NON_SCM_USED.getLabel(), nonScmUsed);
  }

  private void nodeNonWritableStatistics(Map<String, String> nodeStatics) {
    int nonWritableNodesCount = (int) getAllNodes().parallelStream()
        .filter(nonWritableNodeFilter)
        .count();

    nodeStatics.put("NonWritableNodes", String.valueOf(nonWritableNodesCount));
  }

  static class NonWritableNodeFilter implements Predicate<DatanodeInfo> {

    private final long blockSize;
    private final long minRatisVolumeSizeBytes;
    private final long containerSize;

    NonWritableNodeFilter(ConfigurationSource conf) {
      blockSize = (long) conf.getStorageSize(
          OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
          OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT,
          StorageUnit.BYTES);
      minRatisVolumeSizeBytes = (long) conf.getStorageSize(
          ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
          ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN_DEFAULT,
          StorageUnit.BYTES);
      containerSize = (long) conf.getStorageSize(
          ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
          ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
          StorageUnit.BYTES);
    }

    @Override
    public boolean test(DatanodeInfo dn) {
      return !dn.getNodeStatus().isNodeWritable()
          || (!hasEnoughSpace(dn, minRatisVolumeSizeBytes, containerSize)
          && !hasEnoughCommittedVolumeSpace(dn));
    }

    /**
     * Check if any volume in the datanode has committed space >= blockSize.
     *
     * @return true if any volume has committed space >= blockSize, false otherwise
     */
    private boolean hasEnoughCommittedVolumeSpace(DatanodeInfo dnInfo) {
      for (StorageReportProto reportProto : dnInfo.getStorageReports()) {
        if (reportProto.getCommitted() >= blockSize) {
          return true;
        }
      }
      LOG.debug("Datanode {} has no volumes with committed space >= {} bytes",
          dnInfo.getID(), blockSize);
      return false;
    }
  }

  /**
   * Based on the current time and the last heartbeat, calculate the time difference
   * and get a string of the relative value. E.g. "2s ago", "1m 2s ago", etc.
   *
   * @return string with the relative value of the time diff.
   */
  public String getLastHeartbeatTimeDiff(long lastHeartbeatTime) {
    long currentTime = Time.monotonicNow();
    long timeDiff = currentTime - lastHeartbeatTime;

    // Time is in ms. Calculate total time in seconds.
    long seconds = TimeUnit.MILLISECONDS.toSeconds(timeDiff);
    // Calculate days, convert the number back to seconds and subtract it from seconds.
    long days = TimeUnit.SECONDS.toDays(seconds);
    seconds -= TimeUnit.DAYS.toSeconds(days);
    // Calculate hours, convert the number back to seconds and subtract it from seconds.
    long hours = TimeUnit.SECONDS.toHours(seconds);
    seconds -= TimeUnit.HOURS.toSeconds(hours);
    // Calculate minutes, convert the number back to seconds and subtract it from seconds.
    long minutes = TimeUnit.SECONDS.toMinutes(seconds);
    seconds -= TimeUnit.MINUTES.toSeconds(minutes);

    StringBuilder stringBuilder = new StringBuilder();
    if (days > 0) {
      stringBuilder.append(days).append("d ");
    }
    if (hours > 0) {
      stringBuilder.append(hours).append("h ");
    }
    if (minutes > 0) {
      stringBuilder.append(minutes).append("m ");
    }
    if (seconds > 0) {
      stringBuilder.append(seconds).append("s ");
    }
    String str = stringBuilder.length() == 0 ? "Just now" : "ago";

    stringBuilder.append(str);

    return stringBuilder.toString();
  }

  private enum UsageMetrics {
    DiskCapacity,
    DiskUsed,
    DiskRemaining,
    SSDCapacity,
    SSDUsed,
    SSDRemaining
  }

  private enum UsageStates {
    ONLINE(""),
    MAINT("Maintenance"),
    DECOM("Decommissioned");

    private final String label;

    public String getLabel() {
      return label;
    }

    UsageStates(String label) {
      this.label = label;
    }
  }

  private enum UsageStatics {
    MIN("Min"),
    MAX("Max"),
    MEDIAN("Median"),
    STDEV("Stdev");
    private String label;
    public String getLabel() {
      return label;
    }

    UsageStatics(String label) {
      this.label = label;
    }
  }

  private enum StateStatistics {
    HEALTHY("Healthy"),
    DEAD("Dead"),
    DECOMMISSIONING("Decommissioning"),
    ENTERING_MAINTENANCE("EnteringMaintenance"),
    VOLUME_FAILURES("VolumeFailures");
    private String label;

    public String getLabel() {
      return label;
    }

    StateStatistics(String label) {
      this.label = label;
    }
  }

  private enum SpaceStatistics {
    CAPACITY("Capacity"),
    SCM_USED("Scmused"),
    NON_SCM_USED("NonScmused"),
    REMAINING("Remaining");

    private String label;

    public String getLabel() {
      return label;
    }

    SpaceStatistics(String label) {
      this.label = label;
    }
  }

  @Override
  public int openContainerLimit(List<DatanodeDetails> datanodes) {
    int min = Integer.MAX_VALUE;
    for (DatanodeDetails dn : datanodes) {
      final int pipelineLimit = pipelineLimit(dn);
      if (pipelineLimit <= 0) {
        return 0;
      }

      final int containerLimit = 1 + (numContainerPerVolume * getHealthyVolumeCount(dn) - 1) / pipelineLimit;
      if (containerLimit < min) {
        min = containerLimit;
      }
    }
    return min;
  }

  @Override
  public int totalHealthyVolumeCount() {
    int sum = 0;
    for (DatanodeInfo dn : nodeStateManager.getNodes(IN_SERVICE, HEALTHY)) {
      sum += dn.getHealthyVolumeCount();
    }
    return sum;
  }

  /**
   * Returns the pipeline limit for the datanode.
   * if the datanode pipeline limit is set, consider that as the max
   * pipeline limit.
   * In case, the pipeline limit is not set, the max pipeline limit
   * will be based on the no of raft log volume reported and provided
   * that it has atleast one healthy data volume.
   */
  @Override
  public int pipelineLimit(DatanodeDetails dn) {
    try {
      if (datanodePipelineLimit > 0) {
        return datanodePipelineLimit;
      } else if (nodeStateManager.getNode(dn).getHealthyVolumeCount() > 0) {
        return numPipelinesPerMetadataVolume *
            nodeStateManager.getNode(dn).getMetaDataVolumeCount();
      }
    } catch (NodeNotFoundException e) {
      LOG.warn("Cannot generate NodeStat, datanode {} not found.",
          dn.getID());
    }
    return 0;
  }

  private int getHealthyVolumeCount(DatanodeDetails dn) {
    try {
      return nodeStateManager.getNode(dn).getHealthyVolumeCount();
    } catch (NodeNotFoundException e) {
      LOG.warn("Failed to getHealthyVolumeCount, datanode {} not found.", dn.getID());
      return 0;
    }
  }

  @Override
  public Collection<DatanodeDetails> getPeerList(DatanodeDetails dn) {
    HashSet<DatanodeDetails> dns = new HashSet<>();
    Objects.requireNonNull(dn, "dn == null");
    Set<PipelineID> pipelines =
        nodeStateManager.getPipelineByDnID(dn.getID());
    PipelineManager pipelineManager = scmContext.getScm().getPipelineManager();
    if (!pipelines.isEmpty()) {
      pipelines.forEach(id -> {
        try {
          Pipeline pipeline = pipelineManager.getPipeline(id);
          List<DatanodeDetails> peers = pipeline.getNodes();
          dns.addAll(peers);
        } catch (PipelineNotFoundException pnfe) {
          //ignore the pipeline not found exception here
        }
      });
    }
    // renove self node from the set
    dns.remove(dn);
    return dns;
  }

  /**
   * Get set of pipelines a datanode is part of.
   *
   * @param datanodeDetails - datanodeID
   * @return Set of PipelineID
   */
  @Override
  public Set<PipelineID> getPipelines(DatanodeDetails datanodeDetails) {
    return nodeStateManager.getPipelineByDnID(datanodeDetails.getID());
  }

  /**
   * Get the count of pipelines a datanodes is associated with.
   * @param datanodeDetails DatanodeDetails
   * @return The number of pipelines
   */
  @Override
  public int getPipelinesCount(DatanodeDetails datanodeDetails) {
    return nodeStateManager.getPipelinesCount(datanodeDetails);
  }

  /**
   * Add pipeline information in the NodeManager.
   *
   * @param pipeline - Pipeline to be added
   */
  @Override
  public void addPipeline(Pipeline pipeline) {
    nodeStateManager.addPipeline(pipeline);
  }

  /**
   * Remove a pipeline information from the NodeManager.
   *
   * @param pipeline - Pipeline to be removed
   */
  @Override
  public void removePipeline(Pipeline pipeline) {
    nodeStateManager.removePipeline(pipeline);
  }

  @Override
  public void addContainer(final DatanodeDetails datanodeDetails,
      final ContainerID containerId)
      throws NodeNotFoundException {
    nodeStateManager.addContainer(datanodeDetails.getID(), containerId);
  }

  @Override
  public void removeContainer(final DatanodeDetails datanodeDetails,
                           final ContainerID containerId)
      throws NodeNotFoundException {
    nodeStateManager.removeContainer(datanodeDetails.getID(), containerId);
  }

  /**
   * Return set of containerIDs available on a datanode. This is a copy of the
   * set which resides inside NodeManager and hence can be modified without
   * synchronization or side effects.
   *
   * @param datanodeDetails - DatanodeID
   * @return - set of containerIDs
   */
  @Override
  public Set<ContainerID> getContainers(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException {
    return nodeStateManager.getContainers(datanodeDetails.getID());
  }

  public int getContainerCount(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException {
    return nodeStateManager.getContainerCount(datanodeDetails.getID());
  }

  public int getPipeLineCount(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException {
    return nodeStateManager.getPipelinesCount(datanodeDetails);
  }

  public long getTotalReserved(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException {
    long reserved = 0L;
    final DatanodeInfo di = nodeStateManager.getNode(datanodeDetails);
    for (StorageReportProto r : di.getStorageReports()) {
      if (r.hasReserved()) {
        reserved += r.getReserved();
      }
    }
    return reserved;
  }

  /**
   * Compute aggregated raw filesystem capacity/available/used for a datanode
   * from the storage reports.
   */
  public SpaceUsageSource.Fixed getTotalFilesystemUsage(DatanodeDetails datanodeDetails) {
    final DatanodeInfo datanodeInfo;
    try {
      datanodeInfo = nodeStateManager.getNode(datanodeDetails);
    } catch (NodeNotFoundException exception) {
      LOG.error("Node not found when calculating fs usage for {}.", datanodeDetails, exception);
      return null;
    }

    long capacity = 0L;
    long available = 0L;
    boolean hasFsReport = false;
    for (StorageReportProto r : datanodeInfo.getStorageReports()) {
      if (r.hasFailed() && r.getFailed()) {
        continue;
      }
      if (r.hasFsCapacity() && r.hasFsAvailable()) {
        hasFsReport = true;
        capacity += r.getFsCapacity();
        available += r.getFsAvailable();
      }
    }
    if (!hasFsReport) {
      LOG.debug("Datanode {} does not have filesystem storage stats in its storage reports.", datanodeDetails);
    }
    return hasFsReport ? new SpaceUsageSource.Fixed(capacity, available, capacity - available) : null;
  }

  @Override
  public void addDatanodeCommand(DatanodeID datanodeID, SCMCommand<?> command) {
    writeLock().lock();
    try {
      this.commandQueue.addCommand(datanodeID, command);
    } finally {
      writeLock().unlock();
    }
  }

  /**
   * send refresh command to all the healthy datanodes to refresh
   * volume usage info immediately.
   */
  @Override
  public void refreshAllHealthyDnUsageInfo() {
    RefreshVolumeUsageCommand refreshVolumeUsageCommand =
        new RefreshVolumeUsageCommand();
    try {
      refreshVolumeUsageCommand.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending refreshVolumeUsage command,"
          + " since current SCM is not leader.", nle);
      return;
    }
    getNodes(IN_SERVICE, HEALTHY).forEach(datanode ->
        addDatanodeCommand(datanode.getID(), refreshVolumeUsageCommand));
  }

  /**
   * This method is called by EventQueue whenever someone adds a new
   * DATANODE_COMMAND to the Queue.
   *
   * @param commandForDatanode DatanodeCommand
   * @param ignored            publisher
   */
  @Override
  public void onMessage(CommandForDatanode commandForDatanode,
                        EventPublisher ignored) {
    addDatanodeCommand(commandForDatanode.getDatanodeId(),
        commandForDatanode.getCommand());
  }

  @Override
  public List<SCMCommand<?>> getCommandQueue(DatanodeID dnID) {
    // Getting the queue actually clears it and returns the commands, so this
    // is a write operation and not a read as the method name suggests.
    writeLock().lock();
    try {
      return commandQueue.getCommand(dnID);
    } finally {
      writeLock().unlock();
    }
  }

  @Override
  public DatanodeInfo getNode(DatanodeID id) {
    if (id == null) {
      return null;
    }

    try {
      return nodeStateManager.getNode(id);
    } catch (NodeNotFoundException e) {
      LOG.warn("Cannot find node for uuid {}", id);
      return null;
    }
  }

  /**
   * Given datanode address(Ipaddress or hostname), return a list of
   * DatanodeDetails for the datanodes registered on that address.
   *
   * @param address datanode address
   * @return the given datanode, or empty list if none found
   */
  @Override
  public List<DatanodeDetails> getNodesByAddress(String address) {
    if (Strings.isNullOrEmpty(address)) {
      return Collections.emptyList();
    }
    Set<DatanodeID> datanodeIDS = dnsToDnIdMap.get(address);
    if (datanodeIDS == null) {
      return Collections.emptyList();
    }
    return datanodeIDS.stream()
        .map(this::getNode)
        .collect(Collectors.toList());
  }

  /**
   * Get cluster map as in network topology for this node manager.
   * @return cluster map
   */
  @Override
  public NetworkTopology getClusterNetworkTopologyMap() {
    return clusterMap;
  }

  /**
   * For the given node, retried the last heartbeat time.
   * @param datanodeDetails DatanodeDetails of the node.
   * @return The last heartbeat time in milliseconds or -1 if the node does not
   *         exist.
   */
  @Override
  public long getLastHeartbeat(DatanodeDetails datanodeDetails) {
    try {
      DatanodeInfo node = nodeStateManager.getNode(datanodeDetails);
      return node.getLastHeartbeatTime();
    } catch (NodeNotFoundException e) {
      return -1;
    }
  }

  /**
   * Test utility to stop heartbeat check process.
   *
   * @return ScheduledFuture of next scheduled check that got cancelled.
   */
  @VisibleForTesting
  ScheduledFuture pauseHealthCheck() {
    return nodeStateManager.pause();
  }

  /**
   * Test utility to resume the paused heartbeat check process.
   *
   * @return ScheduledFuture of the next scheduled check
   */
  @VisibleForTesting
  ScheduledFuture unpauseHealthCheck() {
    return nodeStateManager.unpause();
  }

  /**
   * Test utility to get the count of skipped heartbeat check iterations.
   *
   * @return count of skipped heartbeat check iterations
   */
  @VisibleForTesting
  long getSkippedHealthChecks() {
    return nodeStateManager.getSkippedHealthChecks();
  }

  /**
   * @return  HDDSLayoutVersionManager
   */
  @VisibleForTesting
  @Override
  public HDDSLayoutVersionManager getLayoutVersionManager() {
    return scmLayoutVersionManager;
  }

  private ReentrantReadWriteLock.WriteLock writeLock() {
    return lock.writeLock();
  }

  private ReentrantReadWriteLock.ReadLock readLock() {
    return lock.readLock();
  }

  /**
   * This API allows removal of only DECOMMISSIONED and DEAD nodes from NodeManager data structures and cleanup memory.
   * This API call is having a pre-condition before removal of node like following resources to be removed:
   *   --- all pipelines for datanode should be closed.
   *   --- all containers for datanode should be closed.
   *   --- remove all containers replicas maintained by datanode.
   *   --- clears all SCM DeletedBlockLog transaction records associated with datanode.
   *
   * @param datanodeDetails
   * @throws NodeNotFoundException
   */
  @Override
  public void removeNode(DatanodeDetails datanodeDetails) throws NodeNotFoundException, IOException {
    writeLock().lock();
    try {
      NodeStatus nodeStatus = this.getNodeStatus(datanodeDetails);
      if (datanodeDetails.isDecommissioned() || nodeStatus.isDead()) {
        if (clusterMap.contains(datanodeDetails)) {
          clusterMap.remove(datanodeDetails);
        }
        nodeStateManager.removeNode(datanodeDetails.getID());
        removeFromDnsToDnIdMap(datanodeDetails.getID(), datanodeDetails.getIpAddress());
        final List<SCMCommand<?>> cmdList = getCommandQueue(datanodeDetails.getID());
        LOG.info("Clearing command queue of size {} for DN {}", cmdList.size(), datanodeDetails);
      } else {
        LOG.warn("Node not decommissioned or dead, cannot remove: {}", datanodeDetails);
      }
    } finally {
      writeLock().unlock();
    }
  }
}
