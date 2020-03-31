/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.node;

import javax.management.ObjectName;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto.ErrorCode;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.VersionInfo;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.states.NodeAlreadyExistsException;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeOperationalStateCommand;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.util.Time;
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
  private final DNSToSwitchMapping dnsToSwitchMapping;
  private final boolean useHostname;
  private final ConcurrentHashMap<String, Set<String>> dnsToUuidMap =
      new ConcurrentHashMap<>();

  /**
   * Constructs SCM machine Manager.
   */
  public SCMNodeManager(OzoneConfiguration conf,
      SCMStorageConfig scmStorageConfig, EventPublisher eventPublisher,
      NetworkTopology networkTopology) {
    this.nodeStateManager = new NodeStateManager(conf, eventPublisher);
    this.version = VersionInfo.getLatestVersion();
    this.commandQueue = new CommandQueue();
    this.scmStorageConfig = scmStorageConfig;
    LOG.info("Entering startup safe mode.");
    registerMXBean();
    this.metrics = SCMNodeMetrics.create(this);
    this.clusterMap = networkTopology;
    Class<? extends DNSToSwitchMapping> dnsToSwitchMappingClass =
        conf.getClass(DFSConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            TableMapping.class, DNSToSwitchMapping.class);
    DNSToSwitchMapping newInstance = ReflectionUtils.newInstance(
        dnsToSwitchMappingClass, conf);
    this.dnsToSwitchMapping =
        ((newInstance instanceof CachedDNSToSwitchMapping) ? newInstance
            : new CachedDNSToSwitchMapping(newInstance));
    this.useHostname = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
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
    return nodeStateManager.getNodes(nodeStatus)
        .stream()
        .map(node -> (DatanodeDetails)node).collect(Collectors.toList());
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

  /**
   * Returns all datanodes that are known to SCM.
   *
   * @return List of DatanodeDetails
   */
  @Override
  public List<DatanodeDetails> getAllNodes() {
    return nodeStateManager.getAllNodes().stream()
        .map(node -> (DatanodeDetails) node).collect(Collectors.toList());
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
   * @parem nodeOpState - The Operational State of the node
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
      NodeOperationalState newState) throws NodeNotFoundException{
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
      throws NodeNotFoundException{
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
   * @return SCMHeartbeatResponseProto
   */
  @Override
  public RegisteredCommand register(
      DatanodeDetails datanodeDetails, NodeReportProto nodeReport,
      PipelineReportsProto pipelineReportsProto) {

    InetAddress dnAddress = Server.getRemoteIp();
    if (dnAddress != null) {
      // Mostly called inside an RPC, update ip and peer hostname
      datanodeDetails.setHostName(dnAddress.getHostName());
      datanodeDetails.setIpAddress(dnAddress.getHostAddress());
    }
    try {
      String dnsName;
      String networkLocation;
      datanodeDetails.setNetworkName(datanodeDetails.getUuidString());
      if (useHostname) {
        dnsName = datanodeDetails.getHostName();
      } else {
        dnsName = datanodeDetails.getIpAddress();
      }
      networkLocation = nodeResolve(dnsName);
      if (networkLocation != null) {
        datanodeDetails.setNetworkLocation(networkLocation);
      }
      nodeStateManager.addNode(datanodeDetails);
      clusterMap.add(datanodeDetails);
      addEntryTodnsToUuidMap(dnsName, datanodeDetails.getUuidString());
      // Updating Node Report, as registration is successful
      processNodeReport(datanodeDetails, nodeReport);
      LOG.info("Registered Data node : {}", datanodeDetails);
    } catch (NodeAlreadyExistsException e) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Datanode is already registered. Datanode: {}",
            datanodeDetails.toString());
      }
    }
    registerInitialDatanodeOpState(datanodeDetails);

    return RegisteredCommand.newBuilder().setErrorCode(ErrorCode.success)
        .setDatanode(datanodeDetails)
        .setClusterID(this.scmStorageConfig.getClusterID())
        .build();
  }

  /**
   * When a node registers with SCM, the operational state stored on the
   * datanode is the source of truth. Therefore, if the datanode reports
   * anything other than IN_SERVICE on registration, the state in SCM should be
   * updated to reflect the datanode state.
   * @param dn
   */
  private void registerInitialDatanodeOpState(DatanodeDetails dn) {
    try {
      HddsProtos.NodeOperationalState dnOpState = dn.getPersistedOpState();
      if (dnOpState != NodeOperationalState.IN_SERVICE) {
        LOG.info("Updating nodeOperationalState on registration as the " +
            "datanode has a persisted state of {} and expiry of {}",
            dnOpState, dn.getPersistedOpStateExpiryEpochSec());
        setNodeOperationalState(dn, dnOpState,
            dn.getPersistedOpStateExpiryEpochSec());
      }
    } catch (NodeNotFoundException e) {
      LOG.error("Unable to find the node when setting the operational state",
          e);
    }
  }

  /**
   * Add an entry to the dnsToUuidMap, which maps hostname / IP to the DNs
   * running on that host. As each address can have many DNs running on it,
   * this is a one to many mapping.
   * @param dnsName String representing the hostname or IP of the node
   * @param uuid String representing the UUID of the registered node.
   */
  @SuppressFBWarnings(value="AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION",
      justification="The method is synchronized and this is the only place "+
          "dnsToUuidMap is modified")
  private synchronized void addEntryTodnsToUuidMap(
      String dnsName, String uuid) {
    Set<String> dnList = dnsToUuidMap.get(dnsName);
    if (dnList == null) {
      dnList = ConcurrentHashMap.newKeySet();
      dnsToUuidMap.put(dnsName, dnList);
    }
    dnList.add(uuid);
  }

  /**
   * Send heartbeat to indicate the datanode is alive and doing well.
   *
   * @param datanodeDetails - DatanodeDetailsProto.
   * @return SCMheartbeat response.
   */
  @Override
  public List<SCMCommand> processHeartbeat(DatanodeDetails datanodeDetails) {
    Preconditions.checkNotNull(datanodeDetails, "Heartbeat is missing " +
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
    return commandQueue.getCommand(datanodeDetails.getUuid());
  }

  /**
   * If the operational state or expiry reported in the datanode heartbeat do
   * not match those store in SCM, queue a command to update the state persisted
   * on the datanode. Additionally, ensure the datanodeDetails stored in SCM
   * match those reported in the heartbeat.
   * This method should only be called when processing the
   * heartbeat, and for a registered node, the information stored in SCM is the
   * source of truth.
   * @param reportedDn The DatanodeDetails taken from the node heartbeat.
   * @throws NodeNotFoundException
   */
  private void updateDatanodeOpState(DatanodeDetails reportedDn)
      throws NodeNotFoundException {
    NodeStatus scmStatus = getNodeStatus(reportedDn);
    if (scmStatus.getOperationalState() != reportedDn.getPersistedOpState()
        || scmStatus.getOpStateExpiryEpochSeconds()
        != reportedDn.getPersistedOpStateExpiryEpochSec()) {
      LOG.info("Scheduling a command to update the operationalState " +
          "persisted on the datanode as the reported value ({}, {}) does not " +
          "match the value stored in SCM ({}, {})",
          reportedDn.getPersistedOpState(),
          reportedDn.getPersistedOpStateExpiryEpochSec(),
          scmStatus.getOperationalState(),
          scmStatus.getOpStateExpiryEpochSeconds());
      commandQueue.addCommand(reportedDn.getUuid(),
          new SetNodeOperationalStateCommand(
              Time.monotonicNow(), scmStatus.getOperationalState(),
              scmStatus.getOpStateExpiryEpochSeconds()));
    }
    DatanodeDetails scmDnd = nodeStateManager.getNode(reportedDn);
    scmDnd.setPersistedOpStateExpiryEpochSec(
        reportedDn.getPersistedOpStateExpiryEpochSec());
    scmDnd.setPersistedOpState(reportedDn.getPersistedOpState());
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
    if (LOG.isTraceEnabled()) {
      LOG.trace("HB is received from [datanode={}]: <json>{}</json>",
          datanodeDetails.getHostName(),
          nodeReport.toString().replaceAll("\n", "\\\\n"));
    }
    try {
      DatanodeInfo datanodeInfo = nodeStateManager.getNode(datanodeDetails);
      if (nodeReport != null) {
        datanodeInfo.updateStorageReports(nodeReport.getStorageReportList());
        metrics.incNumNodeReportProcessed();
      }
    } catch (NodeNotFoundException e) {
      metrics.incNumNodeReportProcessingFailed();
      LOG.warn("Got node report from unregistered datanode {}",
          datanodeDetails);
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

    for (SCMNodeStat stat : getNodeStats().values()) {
      capacity += stat.getCapacity().get();
      used += stat.getScmUsed().get();
      remaining += stat.getRemaining().get();
    }
    return new SCMNodeStat(capacity, used, remaining);
  }

  /**
   * Return a map of node stats.
   *
   * @return a map of individual node stats (live/stale but not dead).
   */
  @Override
  public Map<DatanodeDetails, SCMNodeStat> getNodeStats() {

    final Map<DatanodeDetails, SCMNodeStat> nodeStats = new HashMap<>();

    final List<DatanodeInfo> healthyNodes =  nodeStateManager
        .getHealthyNodes();
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

      final DatanodeInfo datanodeInfo = nodeStateManager
          .getNode(datanodeDetails);
      final List<StorageReportProto> storageReportProtos = datanodeInfo
          .getStorageReports();
      for (StorageReportProto reportProto : storageReportProtos) {
        capacity += reportProto.getCapacity();
        used += reportProto.getScmUsed();
        remaining += reportProto.getRemaining();
      }
      return new SCMNodeStat(capacity, used, remaining);
    } catch (NodeNotFoundException e) {
      LOG.warn("Cannot generate NodeStat, datanode {} not found.",
          datanodeDetails.getUuid());
      return null;
    }
  }

  @Override // NodeManagerMXBean
  public Map<String, Map<String, Integer>> getNodeCount() {
    Map<String, Map<String, Integer>> nodes = new HashMap<>();
    for (NodeOperationalState opState : NodeOperationalState.values()) {
      Map<String, Integer> states = new HashMap<>();
      for (NodeState health : NodeState.values()) {
        states.put(health.name(), 0);
      }
      nodes.put(opState.name(), states);
    }
    for (DatanodeInfo dni : nodeStateManager.getAllNodes()) {
      NodeStatus status = dni.getNodeStatus();
      nodes.get(status.getOperationalState().name())
          .compute(status.getHealth().name(), (k, v) -> v+1);
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
        if (reportProto.getStorageType() ==
            StorageContainerDatanodeProtocolProtos.StorageTypeProto.DISK) {
          nodeInfo.compute(keyPrefix + UsageMetrics.DiskCapacity.name(),
              (k, v) -> v + reportProto.getCapacity());
          nodeInfo.compute(keyPrefix + UsageMetrics.DiskRemaining.name(),
              (k, v) -> v + reportProto.getRemaining());
          nodeInfo.compute(keyPrefix + UsageMetrics.DiskUsed.name(),
              (k, v) -> v + reportProto.getScmUsed());
        } else if (reportProto.getStorageType() ==
            StorageContainerDatanodeProtocolProtos.StorageTypeProto.SSD) {
          nodeInfo.compute(keyPrefix + UsageMetrics.SSDCapacity.name(),
              (k, v) -> v + reportProto.getCapacity());
          nodeInfo.compute(keyPrefix + UsageMetrics.SSDRemaining.name(),
              (k, v) -> v + reportProto.getRemaining());
          nodeInfo.compute(keyPrefix + UsageMetrics.SSDUsed.name(),
              (k, v) -> v + reportProto.getScmUsed());
        }
      }
    }
    return nodeInfo;
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

  /**
   * Get set of pipelines a datanode is part of.
   *
   * @param datanodeDetails - datanodeID
   * @return Set of PipelineID
   */
  @Override
  public Set<PipelineID> getPipelines(DatanodeDetails datanodeDetails) {
    return nodeStateManager.getPipelineByDnID(datanodeDetails.getUuid());
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
    nodeStateManager.addContainer(datanodeDetails.getUuid(), containerId);
  }

  /**
   * Update set of containers available on a datanode.
   *
   * @param datanodeDetails - DatanodeID
   * @param containerIds    - Set of containerIDs
   * @throws NodeNotFoundException - if datanode is not known. For new datanode
   *                               use addDatanodeInContainerMap call.
   */
  @Override
  public void setContainers(DatanodeDetails datanodeDetails,
      Set<ContainerID> containerIds) throws NodeNotFoundException {
    nodeStateManager.setContainers(datanodeDetails.getUuid(),
        containerIds);
  }

  /**
   * Return set of containerIDs available on a datanode.
   *
   * @param datanodeDetails - DatanodeID
   * @return - set of containerIDs
   */
  @Override
  public Set<ContainerID> getContainers(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException {
    return nodeStateManager.getContainers(datanodeDetails.getUuid());
  }

  // TODO:
  // Since datanode commands are added through event queue, onMessage method
  // should take care of adding commands to command queue.
  // Refactor and remove all the usage of this method and delete this method.
  @Override
  public void addDatanodeCommand(UUID dnId, SCMCommand command) {
    this.commandQueue.addCommand(dnId, command);
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
  public List<SCMCommand> getCommandQueue(UUID dnID) {
    return commandQueue.getCommand(dnID);
  }

  /**
   * Given datanode uuid, returns the DatanodeDetails for the node.
   *
   * @param uuid node host address
   * @return the given datanode, or null if not found
   */
  @Override
  public DatanodeDetails getNodeByUuid(String uuid) {
    if (Strings.isNullOrEmpty(uuid)) {
      LOG.warn("uuid is null");
      return null;
    }
    DatanodeDetails temp = DatanodeDetails.newBuilder().setUuid(uuid).build();
    try {
      return nodeStateManager.getNode(temp);
    } catch (NodeNotFoundException e) {
      LOG.warn("Cannot find node for uuid {}", uuid);
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
    List<DatanodeDetails> results = new LinkedList<>();
    if (Strings.isNullOrEmpty(address)) {
      LOG.warn("address is null");
      return results;
    }
    Set<String> uuids = dnsToUuidMap.get(address);
    if (uuids == null) {
      LOG.warn("Cannot find node for address {}", address);
      return results;
    }

    for (String uuid : uuids) {
      DatanodeDetails temp = DatanodeDetails.newBuilder().setUuid(uuid).build();
      try {
        results.add(nodeStateManager.getNode(temp));
      } catch (NodeNotFoundException e) {
        LOG.warn("Cannot find node for uuid {}", uuid);
      }
    }
    return results;
  }

  private String nodeResolve(String hostname) {
    List<String> hosts = new ArrayList<>(1);
    hosts.add(hostname);
    List<String> resolvedHosts = dnsToSwitchMapping.resolve(hosts);
    if (resolvedHosts != null && !resolvedHosts.isEmpty()) {
      String location = resolvedHosts.get(0);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Resolve datanode {} return location {}", hostname, location);
      }
      return location;
    } else {
      LOG.error("Node {} Resolution failed. Please make sure that DNS table " +
          "mapping or configured mapping is functional.", hostname);
      return null;
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
}
