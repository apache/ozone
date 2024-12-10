/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.testutils;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.CommandQueue;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.LinkedList;

/**
 * A Node Manager to test replication.
 */
public class ReplicationNodeManagerMock implements NodeManager {
  private final Map<DatanodeDetails, NodeStatus> nodeStateMap;
  private final CommandQueue commandQueue;

  /**
   * A list of Datanodes and current states.
   * @param nodeStatus A node state map.
   */
  public ReplicationNodeManagerMock(Map<DatanodeDetails, NodeStatus> nodeStatus,
                                    CommandQueue commandQueue) {
    Preconditions.checkNotNull(nodeStatus);
    this.nodeStateMap = nodeStatus;
    this.commandQueue = commandQueue;
  }

  /**
   * Get the number of data nodes that in all states.
   *
   * @return A state to number of nodes that in this state mapping
   */
  @Override
  public Map<String, Map<String, Integer>> getNodeCount() {
    return null;
  }

  @Override
  public Map<String, Long> getNodeInfo() {
    return null;
  }

  @Override
  public Map<String, Map<String, String>> getNodeStatusInfo() {
    return null;
  }

  /**
   * Gets all Live Datanodes that is currently communicating with SCM.
   *
   * @param nodestatus - State of the node
   * @return List of Datanodes that are Heartbeating SCM.
   */
  @Override
  public List<DatanodeDetails> getNodes(NodeStatus nodestatus) {
    return null;
  }

  /**
   * Gets all Live Datanodes that is currently communicating with SCM.
   *
   * @param opState - Operational state of the node
   * @param health - Health of the node
   * @return List of Datanodes that are Heartbeating SCM.
   */
  @Override
  public List<DatanodeDetails> getNodes(
      HddsProtos.NodeOperationalState opState, NodeState health) {
    return null;
  }

  /**
   * Returns the Number of Datanodes that are communicating with SCM.
   *
   * @param nodestatus - State of the node
   * @return int -- count
   */
  @Override
  public int getNodeCount(NodeStatus nodestatus) {
    return 0;
  }

  /**
   * Returns the Number of Datanodes that are communicating with SCM.
   *
   * @param opState - Operational state of the node
   * @param health - Health of the node
   * @return int -- count
   */
  @Override
  public int getNodeCount(
      HddsProtos.NodeOperationalState opState, NodeState health) {
    return 0;
  }

  /**
   * Get all datanodes known to SCM.
   *
   * @return List of DatanodeDetails known to SCM.
   */
  @Override
  public List<DatanodeDetails> getAllNodes() {
    return null;
  }

  /**
   * Returns the aggregated node stats.
   *
   * @return the aggregated node stats.
   */
  @Override
  public SCMNodeStat getStats() {
    return null;
  }

  /**
   * Return a map of node stats.
   *
   * @return a map of individual node stats (live/stale but not dead).
   */
  @Override
  public Map<DatanodeDetails, SCMNodeStat> getNodeStats() {
    return null;
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
  public List<DatanodeUsageInfo> getMostOrLeastUsedDatanodes(boolean mostUsed) {
    return null;
  }

  /**
   * Get the usage info of a specified datanode.
   *
   * @param dn the usage of which we want to get
   * @return DatanodeUsageInfo of the specified datanode
   */
  @Override
  public DatanodeUsageInfo getUsageInfo(DatanodeDetails dn) {
    return null;
  }

  /**
   * Return the node stat of the specified datanode.
   *
   * @param dd - datanode details.
   * @return node stat if it is live/stale, null if it is decommissioned or
   * doesn't exist.
   */
  @Override
  public SCMNodeMetric getNodeStat(DatanodeDetails dd) {
    return null;
  }


  /**
   * Returns the node state of a specific node.
   *
   * @param dd - DatanodeDetails
   * @return Healthy/Stale/Dead.
   */
  @Override
  public NodeStatus getNodeStatus(DatanodeDetails dd) {
    return nodeStateMap.get(dd);
  }

  /**
   * Set the operation state of a node.
   * @param dd The datanode to set the new state for
   * @param newState The new operational state for the node
   */
  @Override
  public void setNodeOperationalState(DatanodeDetails dd,
      HddsProtos.NodeOperationalState newState) throws NodeNotFoundException {
    setNodeOperationalState(dd, newState, 0);
  }

  /**
   * Set the operation state of a node.
   * @param dd The datanode to set the new state for
   * @param newState The new operational state for the node
   */
  @Override
  public void setNodeOperationalState(DatanodeDetails dd,
      HddsProtos.NodeOperationalState newState, long opStateExpiryEpocSec)
      throws NodeNotFoundException {
    NodeStatus currentStatus = nodeStateMap.get(dd);
    if (currentStatus != null) {
      nodeStateMap.put(dd, new NodeStatus(newState, currentStatus.getHealth(),
          opStateExpiryEpocSec));
    } else {
      throw new NodeNotFoundException();
    }
  }

  /**
   * Get set of pipelines a datanode is part of.
   * @param dnId - datanodeID
   * @return Set of PipelineID
   */
  @Override
  public Set<PipelineID> getPipelines(DatanodeDetails dnId) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Get the count of pipelines a datanodes is associated with.
   * @param dn DatanodeDetails
   * @return The number of pipelines
   */
  @Override
  public int getPipelinesCount(DatanodeDetails dn) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Add pipeline information in the NodeManager.
   * @param pipeline - Pipeline to be added
   */
  @Override
  public void addPipeline(Pipeline pipeline) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Remove a pipeline information from the NodeManager.
   * @param pipeline - Pipeline to be removed
   */
  @Override
  public void removePipeline(Pipeline pipeline) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void addContainer(DatanodeDetails datanodeDetails,
                           ContainerID containerId)
      throws NodeNotFoundException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void removeContainer(DatanodeDetails datanodeDetails,
                           ContainerID containerId) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Update set of containers available on a datanode.
   * @param uuid - DatanodeID
   * @param containerIds - Set of containerIDs
   * @throws NodeNotFoundException - if datanode is not known. For new datanode
   *                                 use addDatanodeInContainerMap call.
   */
  @Override
  public void setContainers(DatanodeDetails uuid, Set<ContainerID> containerIds)
      throws NodeNotFoundException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Return set of containerIDs available on a datanode.
   * @param uuid - DatanodeID
   * @return - set of containerIDs
   */
  @Override
  public Set<ContainerID> getContainers(DatanodeDetails uuid) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   * <p>
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised
   * to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
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
   * Register the node if the node finds that it is not registered with any SCM.
   *
   * @param dd DatanodeDetailsProto
   * @param nodeReport NodeReportProto
   * @return SCMRegisteredResponseProto
   */
  @Override
  public RegisteredCommand register(DatanodeDetails dd,
                                    NodeReportProto nodeReport,
                                    PipelineReportsProto pipelineReportsProto,
                                    LayoutVersionProto layoutInfo) {
    return null;
  }

  /**
   * Send heartbeat to indicate the datanode is alive and doing well.
   *
   * @param dd - Datanode Details.
   * @param commandQueueReportProto - Command Queue Report Proto
   * @return SCMheartbeat response list
   */
  @Override
  public List<SCMCommand> processHeartbeat(DatanodeDetails dd,
      CommandQueueReportProto commandQueueReportProto) {
    return null;
  }

  @Override
  public Boolean isNodeRegistered(
      DatanodeDetails datanodeDetails) {
    return false;
  }

  /**
   * Clears all nodes from the node Manager.
   */
  public void clearMap() {
    this.nodeStateMap.clear();
  }

  /**
   * Adds a node to the existing Node manager. This is used only for test
   * purposes.
   * @param id DatanodeDetails
   * @param status State you want to put that node to.
   */
  public void addNode(DatanodeDetails id, NodeStatus status) {
    nodeStateMap.put(id, status);
  }

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
    //no op
  }
  /**
   * Empty implementation for processNodeReport.
   * @param dnUuid
   * @param nodeReport
   */
  @Override
  public void processNodeReport(DatanodeDetails dnUuid,
                                NodeReportProto nodeReport) {
    // do nothing.
  }

  /**
   * Empty implementation for processLayoutVersionReport.
   * @param dnUuid
   * @param layoutVersionReport
   */
  @Override
  public void processLayoutVersionReport(DatanodeDetails dnUuid,
                                LayoutVersionProto layoutVersionReport) {
    // do nothing.
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
   * @param dnID The UUID of the datanode.
   * @param cmdType The Type of command to query the current count for.
   * @return The count of commands queued, or zero if none.
   */
  @Override
  public int getCommandQueueCount(UUID dnID, SCMCommandProto.Type cmdType) {
    return commandQueue.getDatanodeCommandCount(dnID, cmdType);
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

  @Override
  public void onMessage(CommandForDatanode commandForDatanode,
                        EventPublisher publisher) {
    // do nothing.
  }

  @Override
  public List<SCMCommand> getCommandQueue(UUID dnID) {
    return null;
  }

  @Override
  public DatanodeDetails getNodeByUuid(String address) {
    return null;
  }

  @Override
  public Map<String, String> getNodeContainersReplicationMetrics(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException {
    return null;
  }

  @Override
  public List<DatanodeDetails> getNodesByAddress(String address) {
    return new LinkedList<>();
  }

  @Override
  public NetworkTopology getClusterNetworkTopologyMap() {
    return null;
  }

  @Override
  public int minHealthyVolumeNum(List<DatanodeDetails> dnList) {
    return 0;
  }

  @Override
  public int totalHealthyVolumeCount() {
    return 0;
  }

  @Override
  public int pipelineLimit(DatanodeDetails dn) {
    return 0;
  }

  @Override
  public int minPipelineLimit(List<DatanodeDetails> dn) {
    return 0;
  }

  @Override
  public long getLastHeartbeat(DatanodeDetails datanodeDetails) {
    return -1;
  }
}
