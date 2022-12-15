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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
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
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.FinalizeNewLayoutVersionCommand;
import org.apache.hadoop.ozone.protocol.commands.RefreshVolumeUsageCommand;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeOperationalStateCommand;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY_READONLY;

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

  public static final Logger LOG =
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
  private final int numPipelinesPerMetadataVolume;
  private final int heavyNodeCriteria;
  private final HDDSLayoutVersionManager scmLayoutVersionManager;
  private final EventPublisher scmNodeEventPublisher;
  private final SCMContext scmContext;

  /**
   * Constructs SCM machine Manager.
   */
  public SCMNodeManager(OzoneConfiguration conf,
                        SCMStorageConfig scmStorageConfig,
                        EventPublisher eventPublisher,
                        NetworkTopology networkTopology,
                        SCMContext scmContext,
                        HDDSLayoutVersionManager layoutVersionManager) {
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
    Class<? extends DNSToSwitchMapping> dnsToSwitchMappingClass =
        conf.getClass(
            DFSConfigKeysLegacy.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            TableMapping.class, DNSToSwitchMapping.class);
    DNSToSwitchMapping newInstance = ReflectionUtils.newInstance(
        dnsToSwitchMappingClass, conf);
    this.dnsToSwitchMapping =
        ((newInstance instanceof CachedDNSToSwitchMapping) ? newInstance
            : new CachedDNSToSwitchMapping(newInstance));
    this.useHostname = conf.getBoolean(
        DFSConfigKeysLegacy.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeysLegacy.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    this.numPipelinesPerMetadataVolume =
        conf.getInt(ScmConfigKeys.OZONE_SCM_PIPELINE_PER_METADATA_VOLUME,
            ScmConfigKeys.OZONE_SCM_PIPELINE_PER_METADATA_VOLUME_DEFAULT);
    String dnLimit = conf.get(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT);
    this.heavyNodeCriteria = dnLimit == null ? 0 : Integer.parseInt(dnLimit);
    this.scmContext = scmContext;
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

    if (!isNodeRegistered(datanodeDetails)) {
      try {
        clusterMap.add(datanodeDetails);
        nodeStateManager.addNode(datanodeDetails, layoutInfo);
        // Check that datanode in nodeStateManager has topology parent set
        DatanodeDetails dn = nodeStateManager.getNode(datanodeDetails);
        Preconditions.checkState(dn.getParent() != null);
        addEntryToDnsToUuidMap(dnsName, datanodeDetails.getUuidString());
        // Updating Node Report, as registration is successful
        processNodeReport(datanodeDetails, nodeReport);
        LOG.info("Registered Data node : {}", datanodeDetails);
        scmNodeEventPublisher.fireEvent(SCMEvents.NEW_NODE, datanodeDetails);
      } catch (NodeAlreadyExistsException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Datanode is already registered. Datanode: {}",
              datanodeDetails.toString());
        }
      } catch (NodeNotFoundException e) {
        LOG.error("Cannot find datanode {} from nodeStateManager",
            datanodeDetails.toString());
      }
    } else {
      // Update datanode if it is registered but the ip or hostname changes
      try {
        final DatanodeInfo datanodeInfo =
                nodeStateManager.getNode(datanodeDetails);
        if (!datanodeInfo.getIpAddress().equals(datanodeDetails.getIpAddress())
                || !datanodeInfo.getHostName()
                .equals(datanodeDetails.getHostName())) {
          LOG.info("Updating data node {} from {} to {}",
                  datanodeDetails.getUuidString(),
                  datanodeInfo,
                  datanodeDetails);
          clusterMap.update(datanodeInfo, datanodeDetails);

          String oldDnsName;
          if (useHostname) {
            oldDnsName = datanodeInfo.getHostName();
          } else {
            oldDnsName = datanodeInfo.getIpAddress();
          }
          updateEntryFromDnsToUuidMap(oldDnsName,
                  dnsName,
                  datanodeDetails.getUuidString());

          nodeStateManager.updateNode(datanodeDetails, layoutInfo);
          DatanodeDetails dn = nodeStateManager.getNode(datanodeDetails);
          Preconditions.checkState(dn.getParent() != null);
          processNodeReport(datanodeDetails, nodeReport);
          LOG.info("Updated Datanode to: {}", dn);
          scmNodeEventPublisher
                  .fireEvent(SCMEvents.NODE_ADDRESS_UPDATE, dn);
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
   * this is a one to many mapping.
   *
   * @param dnsName String representing the hostname or IP of the node
   * @param uuid    String representing the UUID of the registered node.
   */
  @SuppressFBWarnings(value = "AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION")
  private synchronized void addEntryToDnsToUuidMap(
          String dnsName, String uuid) {
    Set<String> dnList = dnsToUuidMap.get(dnsName);
    if (dnList == null) {
      dnList = ConcurrentHashMap.newKeySet();
      dnsToUuidMap.put(dnsName, dnList);
    }
    dnList.add(uuid);
  }

  private synchronized void removeEntryFromDnsToUuidMap(String dnsName) {
    if (!dnsToUuidMap.containsKey(dnsName)) {
      return;
    }
    Set<String> dnSet = dnsToUuidMap.get(dnsName);
    if (dnSet.contains(dnsName)) {
      dnSet.remove(dnsName);
    }
    if (dnSet.isEmpty()) {
      dnsToUuidMap.remove(dnsName);
    }
  }

  private synchronized void updateEntryFromDnsToUuidMap(String oldDnsName,
                                                        String newDnsName,
                                                        String uuid) {
    removeEntryFromDnsToUuidMap(oldDnsName);
    addEntryToDnsToUuidMap(newDnsName, uuid);
  }

  /**
   * Send heartbeat to indicate the datanode is alive and doing well.
   *
   * @param datanodeDetails - DatanodeDetailsProto.
   * @param layoutInfo - Layout Version Proto.
   * @return SCMheartbeat response.
   */
  @Override
  public List<SCMCommand> processHeartbeat(DatanodeDetails datanodeDetails,
                                           LayoutVersionProto layoutInfo) {
    Preconditions.checkNotNull(datanodeDetails, "Heartbeat is missing " +
        "DatanodeDetails.");
    try {
      nodeStateManager.updateLastHeartbeatTime(datanodeDetails);
      nodeStateManager.updateLastKnownLayoutVersion(datanodeDetails,
          layoutInfo);
      metrics.incNumHBProcessed();
      updateDatanodeOpState(datanodeDetails);
    } catch (NodeNotFoundException e) {
      metrics.incNumHBProcessingFailed();
      LOG.error("SCM trying to process heartbeat from an " +
          "unregistered node {}. Ignoring the heartbeat.", datanodeDetails);
    }
    return commandQueue.getCommand(datanodeDetails.getUuid());
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
                "persisted on {} as the reported value does not " +
                "match the value stored in SCM ({}, {})",
            reportedDn,
            scmStatus.getOperationalState(),
            scmStatus.getOpStateExpiryEpochSeconds());

        try {
          SCMCommand<?> command = new SetNodeOperationalStateCommand(
              Time.monotonicNow(),
              scmStatus.getOperationalState(),
              scmStatus.getOpStateExpiryEpochSeconds());
          command.setTerm(scmContext.getTermOfLeader());
          addDatanodeCommand(reportedDn.getUuid(), command);
        } catch (NotLeaderException nle) {
          LOG.warn("Skip sending SetNodeOperationalStateCommand,"
              + " since current SCM is not leader.", nle);
          return;
        }
      } else {
        LOG.info("Update the operationalState saved in follower SCM " +
                "for {} as the reported value does not " +
                "match the value stored in SCM ({}, {})",
            reportedDn,
            scmStatus.getOperationalState(),
            scmStatus.getOpStateExpiryEpochSeconds());

        setNodeOperationalState(reportedDn, reportedDn.getPersistedOpState(),
            reportedDn.getPersistedOpStateExpiryEpochSec());
      }
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
                new CommandForDatanode<>(datanodeDetails.getUuid(),
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
  @Override
  public void processNodeCommandQueueReport(DatanodeDetails datanodeDetails,
      CommandQueueReportProto commandQueueReportProto,
      Map<SCMCommandProto.Type, Integer> commandsToBeSent) {
    LOG.debug("Processing Command Queue Report from [datanode={}]",
        datanodeDetails.getHostName());
    if (LOG.isTraceEnabled()) {
      LOG.trace("Command Queue Report is received from [datanode={}]: " +
          "<json>{}</json>", datanodeDetails.getHostName(),
          commandQueueReportProto.toString().replaceAll("\n", "\\\\n"));
    }
    try {
      DatanodeInfo datanodeInfo = nodeStateManager.getNode(datanodeDetails);
      if (commandQueueReportProto != null) {
        datanodeInfo.setCommandCounts(commandQueueReportProto,
            commandsToBeSent);
        metrics.incNumNodeCommandQueueReportProcessed();
        scmNodeEventPublisher.fireEvent(
            SCMEvents.DATANODE_COMMAND_COUNT_UPDATED, datanodeDetails);
      }
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
    DatanodeInfo datanodeInfo = nodeStateManager.getNode(datanodeDetails);
    return datanodeInfo.getCommandCount(cmdType);
  }

  /**
   * Get the number of commands of the given type queued in the SCM CommandQueue
   * for the given datanode.
   * @param dnID The UUID of the datanode.
   * @param cmdType The Type of command to query the current count for.
   * @return The count of commands queued, or zero if none.
   */
  @Override
  public int getCommandQueueCount(UUID dnID, SCMCommandProto.Type cmdType) {
    return commandQueue.getDatanodeCommandCount(dnID, cmdType);
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

    final List<DatanodeInfo> healthyNodes = nodeStateManager
        .getNodes(null, HEALTHY);
    final List<DatanodeInfo> healthyReadOnlyNodes = nodeStateManager
        .getNodes(null, HEALTHY_READONLY);
    final List<DatanodeInfo> staleNodes = nodeStateManager
        .getStaleNodes();
    final List<DatanodeInfo> datanodes = new ArrayList<>(healthyNodes);
    datanodes.addAll(healthyReadOnlyNodes);
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
      SCMNodeStat stat = getNodeStatInternal(node);
      datanodeUsageInfoList.add(new DatanodeUsageInfo(node, stat));
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
    return new DatanodeUsageInfo(dn, stat);
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
   * Returns the min of no healthy volumes reported out of the set
   * of datanodes constituting the pipeline.
   */
  @Override
  public int minHealthyVolumeNum(List<DatanodeDetails> dnList) {
    List<Integer> volumeCountList = new ArrayList<>(dnList.size());
    for (DatanodeDetails dn : dnList) {
      try {
        volumeCountList.add(nodeStateManager.getNode(dn).
                getHealthyVolumeCount());
      } catch (NodeNotFoundException e) {
        LOG.warn("Cannot generate NodeStat, datanode {} not found.",
                dn.getUuid());
      }
    }
    Preconditions.checkArgument(!volumeCountList.isEmpty());
    return Collections.min(volumeCountList);
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
      if (heavyNodeCriteria > 0) {
        return heavyNodeCriteria;
      } else if (nodeStateManager.getNode(dn).getHealthyVolumeCount() > 0) {
        return numPipelinesPerMetadataVolume *
            nodeStateManager.getNode(dn).getMetaDataVolumeCount();
      }
    } catch (NodeNotFoundException e) {
      LOG.warn("Cannot generate NodeStat, datanode {} not found.",
          dn.getUuid());
    }
    return 0;
  }

  /**
   * Returns the pipeline limit for set of datanodes.
   */
  @Override
  public int minPipelineLimit(List<DatanodeDetails> dnList) {
    List<Integer> pipelineCountList = new ArrayList<>(dnList.size());
    for (DatanodeDetails dn : dnList) {
      pipelineCountList.add(pipelineLimit(dn));
    }
    Preconditions.checkArgument(!pipelineCountList.isEmpty());
    return Collections.min(pipelineCountList);
  }

  @Override
  public Collection<DatanodeDetails> getPeerList(DatanodeDetails dn) {
    HashSet<DatanodeDetails> dns = new HashSet<>();
    Preconditions.checkNotNull(dn);
    Set<PipelineID> pipelines =
        nodeStateManager.getPipelineByDnID(dn.getUuid());
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
    return nodeStateManager.getPipelineByDnID(datanodeDetails.getUuid());
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
    nodeStateManager.addContainer(datanodeDetails.getUuid(), containerId);
  }

  @Override
  public void removeContainer(final DatanodeDetails datanodeDetails,
                           final ContainerID containerId)
      throws NodeNotFoundException {
    nodeStateManager.removeContainer(datanodeDetails.getUuid(), containerId);
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
        addDatanodeCommand(datanode.getUuid(), refreshVolumeUsageCommand));
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
    DatanodeDetails temp = DatanodeDetails.newBuilder()
        .setUuid(UUID.fromString(uuid)).build();
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
      DatanodeDetails temp = DatanodeDetails.newBuilder()
          .setUuid(UUID.fromString(uuid)).build();
      try {
        results.add(nodeStateManager.getNode(temp));
      } catch (NodeNotFoundException e) {
        LOG.warn("Cannot find node for uuid {}", uuid);
      }
    }
    return results;
  }

  /**
   * Get cluster map as in network topology for this node manager.
   * @return cluster map
   */
  @Override
  public NetworkTopology getClusterNetworkTopologyMap() {
    return clusterMap;
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

  /**
   * @return  HDDSLayoutVersionManager
   */
  @VisibleForTesting
  @Override
  public HDDSLayoutVersionManager getLayoutVersionManager() {
    return scmLayoutVersionManager;
  }

  @VisibleForTesting
  @Override
  public void forceNodesToHealthyReadOnly() {
    nodeStateManager.forceNodesToHealthyReadOnly();
  }
}