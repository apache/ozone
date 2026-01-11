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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.Node2PipelineMap;
import org.apache.hadoop.hdds.scm.node.states.NodeAlreadyExistsException;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.node.states.NodeStateMap;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Helper for testing container Mapping.
 */
public class MockNodeManager implements NodeManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(MockNodeManager.class);

  private static final int NUM_RAFT_LOG_DISKS_PER_DATANODE = 1;

  public static final int NUM_PIPELINE_PER_METADATA_DISK = 2;
  private static final NodeData[] NODES = {
      new NodeData(10L * OzoneConsts.TB, OzoneConsts.GB),
      new NodeData(64L * OzoneConsts.TB, 100 * OzoneConsts.GB),
      new NodeData(128L * OzoneConsts.TB, 256 * OzoneConsts.GB),
      new NodeData(40L * OzoneConsts.TB, OzoneConsts.TB),
      new NodeData(256L * OzoneConsts.TB, 200 * OzoneConsts.TB),
      new NodeData(20L * OzoneConsts.TB, 10 * OzoneConsts.GB),
      new NodeData(32L * OzoneConsts.TB, 16 * OzoneConsts.TB),
      new NodeData(OzoneConsts.TB, 900 * OzoneConsts.GB),
      new NodeData(OzoneConsts.TB, 900 * OzoneConsts.GB, NodeData.STALE),
      new NodeData(OzoneConsts.TB, 200L * OzoneConsts.GB, NodeData.STALE),
      new NodeData(OzoneConsts.TB, 200L * OzoneConsts.GB, NodeData.DEAD)
  };
  private final List<DatanodeDetails> healthyNodes;
  private final List<DatanodeDetails> staleNodes;
  private final List<DatanodeDetails> deadNodes;
  private final Map<DatanodeDetails, SCMNodeStat> nodeMetricMap;
  private final SCMNodeStat aggregateStat;
  private final Map<DatanodeID, List<SCMCommand<?>>> commandMap;
  private Node2PipelineMap node2PipelineMap;
  private final NodeStateMap node2ContainerMap;
  private NetworkTopology clusterMap;
  private ConcurrentMap<String, Set<String>> dnsToUuidMap;
  private int numHealthyDisksPerDatanode;
  private int numPipelinePerDatanode;

  {
    this.healthyNodes = new LinkedList<>();
    this.staleNodes = new LinkedList<>();
    this.deadNodes = new LinkedList<>();
    this.nodeMetricMap = new HashMap<>();
    this.node2PipelineMap = new Node2PipelineMap();
    this.node2ContainerMap = new NodeStateMap();
    this.dnsToUuidMap = new ConcurrentHashMap<>();
    this.aggregateStat = new SCMNodeStat();
    this.clusterMap = new NetworkTopologyImpl(new OzoneConfiguration());
  }

  public MockNodeManager(NetworkTopologyImpl clusterMap,
                         List<DatanodeDetails> nodes,
                         boolean initializeFakeNodes, int nodeCount) {
    this.clusterMap = clusterMap;
    if (!nodes.isEmpty()) {
      for (int x = 0; x < nodes.size(); x++) {
        DatanodeDetails node = nodes.get(x);
        register(node, null, null);
        populateNodeMetric(node, x);
      }
    }
    if (initializeFakeNodes) {
      for (int x = 0; x < nodeCount; x++) {
        DatanodeDetails dd = MockDatanodeDetails.randomDatanodeDetails();
        register(dd, null, null);
        populateNodeMetric(dd, x);
      }
    }
    this.commandMap = new HashMap<>();
    numHealthyDisksPerDatanode = 1;
    numPipelinePerDatanode = NUM_RAFT_LOG_DISKS_PER_DATANODE *
        NUM_PIPELINE_PER_METADATA_DISK;
  }

  public MockNodeManager(boolean initializeFakeNodes, int nodeCount) {
    this(new NetworkTopologyImpl(new OzoneConfiguration()), new ArrayList<>(),
        initializeFakeNodes, nodeCount);
  }

  public MockNodeManager(List<DatanodeUsageInfo> nodes)
      throws IllegalArgumentException {
    if (!nodes.isEmpty()) {
      for (DatanodeUsageInfo node : nodes) {
        register(node.getDatanodeDetails(), null, null);
        nodeMetricMap.put(node.getDatanodeDetails(), node.getScmNodeStat());
        aggregateStat.add(node.getScmNodeStat());
        healthyNodes.add(node.getDatanodeDetails());
      }
    } else {
      throw new IllegalArgumentException("The argument nodes list must not " +
          "be empty");
    }

    this.commandMap = new HashMap<>();
    numHealthyDisksPerDatanode = 1;
    numPipelinePerDatanode = NUM_RAFT_LOG_DISKS_PER_DATANODE *
        NUM_PIPELINE_PER_METADATA_DISK;
  }

  public MockNodeManager(
      Map<DatanodeUsageInfo, Set<ContainerID>> usageInfoToCidsMap)
      throws IllegalArgumentException {
    if (!usageInfoToCidsMap.isEmpty()) {
      // for each usageInfo, register it, add containers, and update metrics
      for (Map.Entry<DatanodeUsageInfo, Set<ContainerID>> entry:
          usageInfoToCidsMap.entrySet()) {
        DatanodeUsageInfo usageInfo = entry.getKey();
        register(usageInfo.getDatanodeDetails(), null, null);
        try {
          setContainers(usageInfo.getDatanodeDetails(), entry.getValue());
        } catch (NodeNotFoundException e) {
          LOG.warn("Could not find Datanode {} for adding containers to it. " +
                  "Skipping this node.", usageInfo.getDatanodeDetails());
          continue;
        }

        nodeMetricMap
            .put(usageInfo.getDatanodeDetails(), usageInfo.getScmNodeStat());
        aggregateStat.add(usageInfo.getScmNodeStat());
        healthyNodes.add(usageInfo.getDatanodeDetails());
      }
    } else {
      throw new IllegalArgumentException("The provided argument should not be" +
          " empty");
    }

    this.commandMap = new HashMap<>();
    numHealthyDisksPerDatanode = 1;
    numPipelinePerDatanode = NUM_RAFT_LOG_DISKS_PER_DATANODE *
        NUM_PIPELINE_PER_METADATA_DISK;
  }

  /**
   * Invoked from ctor to create some node Metrics.
   *
   * @param datanodeDetails - Datanode details
   */
  private void populateNodeMetric(DatanodeDetails datanodeDetails, int x) {
    SCMNodeStat newStat = new SCMNodeStat();
    long remaining =
        NODES[x % NODES.length].capacity - NODES[x % NODES.length].used;
    newStat.set(
        (NODES[x % NODES.length].capacity),
        (NODES[x % NODES.length].used), remaining, 0, 100000, 0);
    this.nodeMetricMap.put(datanodeDetails, newStat);
    aggregateStat.add(newStat);

    if (NODES[x % NODES.length].getCurrentState() == NodeData.HEALTHY) {
      healthyNodes.add(datanodeDetails);
    }

    if (NODES[x % NODES.length].getCurrentState() == NodeData.STALE) {
      staleNodes.add(datanodeDetails);
    }

    if (NODES[x % NODES.length].getCurrentState() == NodeData.DEAD) {
      deadNodes.add(datanodeDetails);
    }

  }

  /**
   * Gets all Live Datanodes that is currently communicating with SCM.
   *
   * @param status The status of the node
   * @return List of Datanodes that are Heartbeating SCM.
   */
  @Override
  public List<DatanodeDetails> getNodes(NodeStatus status) {
    return getNodes(status.getOperationalState(), status.getHealth());
  }

  /**
   * Gets all Live Datanodes that is currently communicating with SCM.
   *
   * @param opState - The operational State of the node
   * @param nodestate - The health of the node
   * @return List of Datanodes that are Heartbeating SCM.
   */
  @Override
  public List<DatanodeDetails> getNodes(
      HddsProtos.NodeOperationalState opState, HddsProtos.NodeState nodestate) {
    if (nodestate == HEALTHY) {
      // mock storage reports for SCMCommonPlacementPolicy.hasEnoughSpace()
      List<DatanodeDetails> healthyNodesWithInfo = new ArrayList<>();
      for (DatanodeDetails dd : healthyNodes) {
        DatanodeInfo di = new DatanodeInfo(dd, NodeStatus.inServiceHealthy(),
            UpgradeUtils.defaultLayoutVersionProto());

        long capacity = nodeMetricMap.get(dd).getCapacity().get();
        long used = nodeMetricMap.get(dd).getScmUsed().get();
        long remaining = nodeMetricMap.get(dd).getRemaining().get();
        StorageReportProto storage1 = HddsTestUtils.createStorageReport(
            di.getID(), "/data1-" + di.getID(),
            capacity, used, remaining, null);
        MetadataStorageReportProto metaStorage1 =
            HddsTestUtils.createMetadataStorageReport(
                "/metadata1-" + di.getID(), capacity, used, remaining, null);
        di.updateStorageReports(Collections.singletonList(storage1));
        di.updateMetaDataStorageReports(Collections.singletonList(metaStorage1));

        healthyNodesWithInfo.add(di);
      }
      return healthyNodesWithInfo;
    }

    if (nodestate == STALE) {
      return staleNodes;
    }

    if (nodestate == DEAD) {
      return deadNodes;
    }

    if (nodestate == null) {
      return new ArrayList<>(nodeMetricMap.keySet());
    }

    return null;
  }

  /**
   * Returns the Number of Datanodes that are communicating with SCM.
   *
   * @param status - Status of the node
   * @return int -- count
   */
  @Override
  public int getNodeCount(NodeStatus status) {
    return getNodeCount(status.getOperationalState(), status.getHealth());
  }

  /**
   * Returns the Number of Datanodes that are communicating with SCM.
   *
   * @param nodestate - State of the node
   * @return int -- count
   */
  @Override
  public int getNodeCount(
      HddsProtos.NodeOperationalState opState, HddsProtos.NodeState nodestate) {
    List<DatanodeDetails> nodes = getNodes(opState, nodestate);
    if (nodes != null) {
      return nodes.size();
    }
    return 0;
  }

  /**
   * Get all datanodes known to SCM.
   *
   * @return List of DatanodeDetails known to SCM.
   */
  @Override
  public List<DatanodeDetails> getAllNodes() {
    // mock storage reports for TestDiskBalancer
    List<DatanodeDetails> healthyNodesWithInfo = new ArrayList<>();
    for (Map.Entry<DatanodeDetails, SCMNodeStat> entry:
        nodeMetricMap.entrySet()) {
      NodeStatus nodeStatus = NodeStatus.inServiceHealthy();
      if (staleNodes.contains(entry.getKey())) {
        nodeStatus = NodeStatus.inServiceStale();
      } else if (deadNodes.contains(entry.getKey())) {
        nodeStatus = NodeStatus.inServiceDead();
      }
      DatanodeInfo di = new DatanodeInfo(entry.getKey(), nodeStatus,
          UpgradeUtils.defaultLayoutVersionProto());

      long capacity = entry.getValue().getCapacity().get();
      long used = entry.getValue().getScmUsed().get();
      long remaining = entry.getValue().getRemaining().get();
      StorageReportProto storage1 = HddsTestUtils.createStorageReport(
          di.getID(), "/data1-" + di.getUuidString(),
          capacity, used, remaining, null);
      MetadataStorageReportProto metaStorage1 =
          HddsTestUtils.createMetadataStorageReport(
              "/metadata1-" + di.getUuidString(), capacity, used,
              remaining, null);
      di.updateStorageReports(new ArrayList<>(Arrays.asList(storage1)));
      di.updateMetaDataStorageReports(
          new ArrayList<>(Arrays.asList(metaStorage1)));

      healthyNodesWithInfo.add(di);
    }
    return healthyNodesWithInfo;
  }

  /**
   * Returns the aggregated node stats.
   * @return the aggregated node stats.
   */
  @Override
  public SCMNodeStat getStats() {
    return aggregateStat;
  }

  /**
   * Return a map of nodes to their stats.
   * @return a list of individual node stats (live/stale but not dead).
   */
  @Override
  public Map<DatanodeDetails, SCMNodeStat> getNodeStats() {
    return nodeMetricMap;
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
    List<DatanodeDetails> datanodeDetailsList =
        getNodes(NodeOperationalState.IN_SERVICE, HEALTHY);
    if (datanodeDetailsList == null) {
      return new ArrayList<>();
    }
    Comparator<DatanodeUsageInfo> comparator;
    if (mostUsed) {
      comparator = DatanodeUsageInfo.getMostUtilized().reversed();
    } else {
      comparator = DatanodeUsageInfo.getMostUtilized();
    }

    return datanodeDetailsList.stream()
        .map(node -> new DatanodeUsageInfo(node, nodeMetricMap.get(node)))
        .sorted(comparator)
        .collect(Collectors.toList());
  }

  /**
   * Get the usage info of a specified datanode.
   *
   * @param datanodeDetails the usage of which we want to get
   * @return DatanodeUsageInfo of the specified datanode
   */
  @Override
  public DatanodeUsageInfo getUsageInfo(DatanodeDetails datanodeDetails) {
    SCMNodeStat stat = nodeMetricMap.get(datanodeDetails);
    return new DatanodeUsageInfo(datanodeDetails, stat);
  }

  @Override
  @Nullable
  public DatanodeInfo getDatanodeInfo(DatanodeDetails dd) {
    if (nodeMetricMap.get(dd) == null) {
      return null;
    }

    DatanodeInfo di = new DatanodeInfo(dd, NodeStatus.inServiceHealthy(),
        UpgradeUtils.defaultLayoutVersionProto());
    long capacity = nodeMetricMap.get(dd).getCapacity().get();
    long used = nodeMetricMap.get(dd).getScmUsed().get();
    long remaining = nodeMetricMap.get(dd).getRemaining().get();
    StorageReportProto storage1 = HddsTestUtils.createStorageReport(
        di.getID(), "/data1-" + di.getUuidString(),
        capacity, used, remaining, null);
    MetadataStorageReportProto metaStorage1 =
        HddsTestUtils.createMetadataStorageReport(
            "/metadata1-" + di.getUuidString(), capacity, used,
            remaining, null);
    di.updateStorageReports(new ArrayList<>(Arrays.asList(storage1)));
    di.updateMetaDataStorageReports(
        new ArrayList<>(Arrays.asList(metaStorage1)));
    return di;
  }

  /**
   * Return the node stat of the specified datanode.
   * @param datanodeDetails - datanode details.
   * @return node stat if it is live/stale, null if it is decommissioned or
   * doesn't exist.
   */
  @Override
  public SCMNodeMetric getNodeStat(DatanodeDetails datanodeDetails) {
    SCMNodeStat stat = nodeMetricMap.get(datanodeDetails);
    if (stat == null) {
      return null;
    }
    return new SCMNodeMetric(stat);
  }

  /**
   * Returns the node state of a specific node.
   *
   * @param dd - DatanodeDetails
   * @return Healthy/Stale/Dead.
   */
  @Override
  public NodeStatus getNodeStatus(DatanodeDetails dd)
      throws NodeNotFoundException {
    if (healthyNodes.contains(dd)) {
      return NodeStatus.inServiceHealthy();
    } else if (staleNodes.contains(dd)) {
      return NodeStatus.inServiceStale();
    } else if (deadNodes.contains(dd)) {
      return NodeStatus.inServiceDead();
    } else {
      throw new NodeNotFoundException(dd.getID());
    }
  }

  /**
   * Set the operation state of a node.
   * @param datanodeDetails The datanode to set the new state for
   * @param newState The new operational state for the node
   */
  @Override
  public void setNodeOperationalState(DatanodeDetails datanodeDetails,
      HddsProtos.NodeOperationalState newState) throws NodeNotFoundException {
  }

  /**
   * Set the operation state of a node.
   * @param datanodeDetails The datanode to set the new state for
   * @param newState The new operational state for the node
   */
  @Override
  public void setNodeOperationalState(DatanodeDetails datanodeDetails,
      HddsProtos.NodeOperationalState newState, long opStateExpiryEpocSec)
      throws NodeNotFoundException {
  }

  /**
   * Get set of pipelines a datanode is part of.
   * @param dnId - datanodeID
   * @return Set of PipelineID
   */
  @Override
  public Set<PipelineID> getPipelines(DatanodeDetails dnId) {
    return node2PipelineMap.getPipelines(dnId.getID());
  }

  /**
   * Get the count of pipelines a datanodes is associated with.
   * @param datanodeDetails DatanodeDetails
   * @return The number of pipelines
   */
  @Override
  public int getPipelinesCount(DatanodeDetails datanodeDetails) {
    return node2PipelineMap.getPipelinesCount(datanodeDetails.getID());
  }

  /**
   * Add pipeline information in the NodeManager.
   * @param pipeline - Pipeline to be added
   */
  @Override
  public void addPipeline(Pipeline pipeline) {
    node2PipelineMap.addPipeline(pipeline);
  }

  /**
   * Get the entire Node2PipelineMap.
   * @return Node2PipelineMap
   */
  public Node2PipelineMap getNode2PipelineMap() {
    return node2PipelineMap;
  }

  /**
   * Set the Node2PipelineMap.
   * @param node2PipelineMap Node2PipelineMap
   */
  public void setNode2PipelineMap(Node2PipelineMap node2PipelineMap) {
    this.node2PipelineMap = node2PipelineMap;
  }

  /**
   * Remove a pipeline information from the NodeManager.
   * @param pipeline - Pipeline to be removed
   */
  @Override
  public void removePipeline(Pipeline pipeline) {
    node2PipelineMap.removePipeline(pipeline);
  }

  @Override
  public void addContainer(DatanodeDetails dd,
                           ContainerID containerId)
      throws NodeNotFoundException {
    node2ContainerMap.getContainers(dd.getID()).add(containerId);
  }

  @Override
  public void removeContainer(DatanodeDetails dd,
      ContainerID containerId) throws NodeNotFoundException {
    node2ContainerMap.getContainers(dd.getID()).remove(containerId);
  }

  @Override
  public void addDatanodeCommand(DatanodeID datanodeID, SCMCommand<?> command) {
    if (commandMap.containsKey(datanodeID)) {
      List<SCMCommand<?>> commandList = commandMap.get(datanodeID);
      Objects.requireNonNull(commandList, "commandList == null");
      commandList.add(command);
    } else {
      List<SCMCommand<?>> commandList = new LinkedList<>();
      commandList.add(command);
      commandMap.put(datanodeID, commandList);
    }
  }

  /**
   * send refresh command to all the healthy datanodes to refresh
   * volume usage info immediately.
   */
  @Override
  public void refreshAllHealthyDnUsageInfo() {
    //no op
  }

  /**
   * Empty implementation for processNodeReport.
   *
   * @param dnUuid
   * @param nodeReport
   */
  @Override
  public void processNodeReport(DatanodeDetails dnUuid,
      NodeReportProto nodeReport) {
    // do nothing
  }

  /**
   * Empty implementation for processLayoutVersionReport.
   *
   * @param dnUuid
   * @param layoutReport
   */
  @Override
  public void processLayoutVersionReport(DatanodeDetails dnUuid,
                                         LayoutVersionProto layoutReport) {
    // do nothing
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
      SCMCommandProto.Type cmdType) {
    return -1;
  }

  /**
   * Get the number of commands of the given type queued in the SCM CommandQueue
   * for the given datanode.
   * @param dnID The ID of the datanode.
   * @param cmdType The Type of command to query the current count for.
   * @return The count of commands queued, or zero if none.
   */
  @Override
  public int getCommandQueueCount(DatanodeID dnID, SCMCommandProto.Type cmdType) {
    return 0;
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
    return 0;
  }

  @Override
  public Map<SCMCommandProto.Type, Integer> getTotalDatanodeCommandCounts(
      DatanodeDetails datanodeDetails, SCMCommandProto.Type... cmdType) {
    return Collections.emptyMap();
  }

  /**
   * Update set of containers available on a datanode.
   */
  public void setContainers(DatanodeDetails uuid, Set<ContainerID> containerIds)
      throws NodeNotFoundException {
    node2ContainerMap.setContainersForTesting(uuid.getID(), containerIds);
  }

  /**
   * Return set of containerIDs available on a datanode.
   * @param uuid - DatanodeID
   * @return - set of containerIDs
   */
  @Override
  public Set<ContainerID> getContainers(DatanodeDetails uuid) throws NodeNotFoundException {
    return node2ContainerMap.getContainers(uuid.getID());
  }

  // Returns the number of commands that is queued to this node manager.
  public int getCommandCount(DatanodeDetails dd) {
    List<SCMCommand<?>> list = commandMap.get(dd.getID());
    return (list == null) ? 0 : list.size();
  }

  public void clearCommandQueue(DatanodeID dnId) {
    if (commandMap.containsKey(dnId)) {
      commandMap.put(dnId, new LinkedList<>());
    }
  }

  public void setNodeState(DatanodeDetails dn, HddsProtos.NodeState state) {
    healthyNodes.remove(dn);
    staleNodes.remove(dn);
    deadNodes.remove(dn);
    if (state == HEALTHY) {
      healthyNodes.add(dn);
    } else if (state == STALE) {
      staleNodes.add(dn);
    } else {
      deadNodes.add(dn);
    }
  }

  /**
   * Closes this stream and releases any system resources associated with it. If
   * the stream is already closed then invoking this method has no effect.
   * <p>
   * <p> As noted in {@link AutoCloseable#close()}, cases where the close may
   * fail require careful attention. It is strongly advised to relinquish the
   * underlying resources and to internally <em>mark</em> the {@code Closeable}
   * as closed, prior to throwing the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {

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
    return null;
  }

  /**
   * Register the node if the node finds that it is not registered with any
   * SCM.
   *
   * @param datanodeDetails DatanodeDetails
   * @param nodeReport NodeReportProto
   * @return SCMRegisteredResponseProto
   */
  @Override
  public RegisteredCommand register(DatanodeDetails datanodeDetails,
                                    NodeReportProto nodeReport,
                                    PipelineReportsProto pipelineReportsProto,
                                    LayoutVersionProto layoutInfo) {
    final DatanodeInfo info = new DatanodeInfo(datanodeDetails, NodeStatus.inServiceHealthy(), layoutInfo);
    try {
      node2ContainerMap.addNode(info);
      addEntryTodnsToUuidMap(datanodeDetails.getIpAddress(),
          datanodeDetails.getUuidString());
      if (clusterMap != null) {
        datanodeDetails.setNetworkName(datanodeDetails.getUuidString());
        clusterMap.add(datanodeDetails);
      }
    } catch (NodeAlreadyExistsException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Add an entry to the dnsToUuidMap, which maps hostname / IP to the DNs
   * running on that host. As each address can have many DNs running on it,
   * this is a one to many mapping.
   * @param dnsName String representing the hostname or IP of the node
   * @param uuid String representing the UUID of the registered node.
   */
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
   * @param datanodeDetails - Datanode ID.
   * @param commandQueueReportProto - Command Queue Report Proto
   * @return SCMheartbeat response list
   */
  @Override
  public List<SCMCommand<?>> processHeartbeat(DatanodeDetails datanodeDetails,
      CommandQueueReportProto commandQueueReportProto) {
    return null;
  }

  @Override
  public Boolean isNodeRegistered(
      DatanodeDetails datanodeDetails) {
    return healthyNodes.contains(datanodeDetails);
  }

  @Override
  public Map<String, Map<String, Integer>> getNodeCount() {
    Map<String, Map<String, Integer>> nodes = new HashMap<>();
    for (NodeOperationalState opState : NodeOperationalState.values()) {
      Map<String, Integer> states = new HashMap<>();
      for (HddsProtos.NodeState health : HddsProtos.NodeState.values()) {
        states.put(health.name(), 0);
      }
      nodes.put(opState.name(), states);
    }
    // At the moment MockNodeManager is not aware of decommission and
    // maintenance states, therefore loop over all nodes and assume all nodes
    // are IN_SERVICE. This will be fixed as part of HDDS-2673
    for (HddsProtos.NodeState state : HddsProtos.NodeState.values()) {
      nodes.get(NodeOperationalState.IN_SERVICE.name())
          .compute(state.name(), (k, v) -> v + 1);
    }
    return nodes;
  }

  @Override
  public Map<String, Long> getNodeInfo() {
    Map<String, Long> nodeInfo = new HashMap<>();
    nodeInfo.put("Capacity", aggregateStat.getCapacity().get());
    nodeInfo.put("Used", aggregateStat.getScmUsed().get());
    nodeInfo.put("Remaining", aggregateStat.getRemaining().get());
    return nodeInfo;
  }

  @Override
  public Map<String, Map<String, String>> getNodeStatusInfo() {
    return null;
  }

  /**
   * Makes it easy to add a container.
   *
   * @param datanodeDetails datanode details
   * @param size number of bytes.
   */
  public void addContainer(DatanodeDetails datanodeDetails, long size) {
    SCMNodeStat stat = this.nodeMetricMap.get(datanodeDetails);
    if (stat != null) {
      aggregateStat.subtract(stat);
      stat.getScmUsed().add(size);
      stat.getRemaining().subtract(size);
      aggregateStat.add(stat);
      nodeMetricMap.put(datanodeDetails, stat);
    }
  }

  /**
   * Makes it easy to simulate a delete of a container.
   *
   * @param datanodeDetails datanode Details
   * @param size number of bytes.
   */
  public void delContainer(DatanodeDetails datanodeDetails, long size) {
    SCMNodeStat stat = this.nodeMetricMap.get(datanodeDetails);
    if (stat != null) {
      aggregateStat.subtract(stat);
      stat.getScmUsed().subtract(size);
      stat.getRemaining().add(size);
      aggregateStat.add(stat);
      nodeMetricMap.put(datanodeDetails, stat);
    }
  }

  @Override
  public void onMessage(CommandForDatanode commandForDatanode,
                        EventPublisher publisher) {
    this.addDatanodeCommand(commandForDatanode.getDatanodeId(),
        commandForDatanode.getCommand());
  }

  @Override
  public List<SCMCommand<?>> getCommandQueue(DatanodeID dnID) {
    return null;
  }

  @Override
  public DatanodeDetails getNode(DatanodeID id) {
    Node node = clusterMap.getNode(NetConstants.DEFAULT_RACK + "/" + id);
    return node == null ? null : (DatanodeDetails)node;
  }

  @Override
  public List<DatanodeDetails> getNodesByAddress(String address) {
    List<DatanodeDetails> results = new LinkedList<>();
    Set<String> uuids = dnsToUuidMap.get(address);
    if (uuids == null) {
      return results;
    }
    for (String uuid : uuids) {
      DatanodeDetails dn = getNode(DatanodeID.fromUuidString(uuid));
      if (dn != null) {
        results.add(dn);
      }
    }
    return results;
  }

  @Override
  public NetworkTopology getClusterNetworkTopologyMap() {
    return clusterMap;
  }

  public void setNetworkTopology(NetworkTopology topology) {
    this.clusterMap = topology;
  }

  @Override
  public int totalHealthyVolumeCount() {
    return healthyNodes.size() * numHealthyDisksPerDatanode;
  }

  @Override
  public int pipelineLimit(DatanodeDetails dn) {
    // by default 1 single node pipeline and 1 three node pipeline
    return numPipelinePerDatanode;
  }

  @Override
  public long getLastHeartbeat(DatanodeDetails datanodeDetails) {
    return -1;
  }

  public void setNumPipelinePerDatanode(int value) {
    numPipelinePerDatanode = value;
  }

  public void setNumHealthyVolumes(int value) {
    numHealthyDisksPerDatanode = value;
  }

  /**
   * A class to declare some values for the nodes so that our tests
   * won't fail.
   */
  private static class NodeData {
    public static final long HEALTHY = 1;
    public static final long STALE = 2;
    public static final long DEAD = 3;

    private long capacity;
    private long used;

    private long currentState;

    /**
     * By default nodes are healthy.
     * @param capacity
     * @param used
     */
    NodeData(long capacity, long used) {
      this(capacity, used, HEALTHY);
    }

    /**
     * Constructs a nodeDefinition.
     *
     * @param capacity capacity.
     * @param used used.
     * @param currentState - Healthy, Stale and DEAD nodes.
     */
    NodeData(long capacity, long used, long currentState) {
      this.capacity = capacity;
      this.used = used;
      this.currentState = currentState;
    }

    public long getCapacity() {
      return capacity;
    }

    public void setCapacity(long capacity) {
      this.capacity = capacity;
    }

    public long getUsed() {
      return used;
    }

    public void setUsed(long used) {
      this.used = used;
    }

    public long getCurrentState() {
      return currentState;
    }

    public void setCurrentState(long currentState) {
      this.currentState = currentState;
    }

  }
}
