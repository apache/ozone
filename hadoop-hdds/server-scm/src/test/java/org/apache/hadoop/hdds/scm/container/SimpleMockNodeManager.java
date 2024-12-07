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
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Basic implementation of the NodeManager interface which can be used in tests.
 *
 * TODO - Merge the functionality with MockNodeManager, as it needs refactored
 *        after the introduction of decommission and maintenance states.
 */
public class SimpleMockNodeManager implements NodeManager {

  private Map<UUID, DatanodeInfo> nodeMap = new ConcurrentHashMap<>();
  private Map<UUID, Set<PipelineID>> pipelineMap = new ConcurrentHashMap<>();
  private Map<UUID, Set<ContainerID>> containerMap = new ConcurrentHashMap<>();

  public void register(DatanodeDetails dd, NodeStatus status) {
    dd.setPersistedOpState(status.getOperationalState());
    dd.setPersistedOpStateExpiryEpochSec(status.getOpStateExpiryEpochSeconds());
    nodeMap.put(dd.getUuid(), new DatanodeInfo(dd, status, null));
  }

  public void setNodeStatus(DatanodeDetails dd, NodeStatus status) {
    dd.setPersistedOpState(status.getOperationalState());
    dd.setPersistedOpStateExpiryEpochSec(status.getOpStateExpiryEpochSeconds());
    DatanodeInfo dni = nodeMap.get(dd.getUuid());
    dni.setNodeStatus(status);
  }

  /**
   * Set the number of pipelines for the given node. This simply generates
   * new PipelineID objects and places them in a set. No actual pipelines are
   * created.
   *
   * Setting the count to zero effectively deletes the pipelines for the node
   *
   * @param dd The DatanodeDetails for which to create the pipelines
   * @param count The number of pipelines to create or zero to delete all
   *              pipelines
   */
  public void setPipelines(DatanodeDetails dd, int count) {
    Set<PipelineID> pipelines = new HashSet<>();
    for (int i = 0; i < count; i++) {
      pipelines.add(PipelineID.randomId());
    }
    pipelineMap.put(dd.getUuid(), pipelines);
  }

  /**
   * If the given node was registered with the nodeManager, return the
   * NodeStatus for the node. Otherwise return a NodeStatus of "In Service
   * and Healthy".
   * @param datanodeDetails DatanodeDetails
   * @return The NodeStatus of the node if it is registered, otherwise an
   *         Inservice and Healthy NodeStatus.
   */
  @Override
  public NodeStatus getNodeStatus(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException {
    DatanodeInfo dni = nodeMap.get(datanodeDetails.getUuid());
    if (dni != null) {
      return dni.getNodeStatus();
    } else {
      return NodeStatus.inServiceHealthy();
    }
  }

  @Override
  public void setNodeOperationalState(DatanodeDetails dn,
                                      NodeOperationalState newState)
      throws NodeNotFoundException {
    setNodeOperationalState(dn, newState, 0);
  }

  @Override
  public void setNodeOperationalState(DatanodeDetails dn,
                                      NodeOperationalState newState,
                                      long opStateExpiryEpocSec)
      throws NodeNotFoundException {
    DatanodeInfo dni = nodeMap.get(dn.getUuid());
    if (dni == null) {
      throw new NodeNotFoundException();
    }
    dni.setNodeStatus(
        new NodeStatus(
            newState, dni.getNodeStatus().getHealth(), opStateExpiryEpocSec));
  }

  /**
   * Return the set of PipelineID associated with the given DatanodeDetails.
   *
   * If there are no pipelines, null is return, to mirror the behaviour of
   * SCMNodeManager.
   *
   * @param datanodeDetails The datanode for which to return the pipelines
   * @return A set of PipelineID or null if there are none
   */
  @Override
  public Set<PipelineID> getPipelines(DatanodeDetails datanodeDetails) {
    Set<PipelineID> p = pipelineMap.get(datanodeDetails.getUuid());
    if (p == null || p.size() == 0) {
      return null;
    } else {
      return p;
    }
  }

  @Override
  public int getPipelinesCount(DatanodeDetails datanodeDetails) {
    return 0;
  }

  @Override
  public void setContainers(DatanodeDetails dn,
                            Set<ContainerID> containerIds)
      throws NodeNotFoundException {
    containerMap.put(dn.getUuid(), containerIds);
  }

  /**
   * Return the set of ContainerID associated with the datanode. If there are no
   * container present, an empty set is return to mirror the behaviour of
   * SCMNodeManaer
   *
   * @param dn The datanodeDetails for which to return the containers
   * @return A Set of ContainerID or an empty Set if none are present
   * @throws NodeNotFoundException
   */
  @Override
  public Set<ContainerID> getContainers(DatanodeDetails dn)
      throws NodeNotFoundException {
    // The concrete implementation of this method in SCMNodeManager will return
    // an empty set if there are no containers, and will never return null.
    return containerMap
        .computeIfAbsent(dn.getUuid(), key -> new HashSet<>());
  }

  /**
   * Below here, are all auto-generate placeholder methods to implement the
   * interface.
   */

  @Override
  public List<DatanodeDetails> getNodes(NodeStatus nodeStatus) {
    return null;
  }

  @Override
  public List<DatanodeDetails> getNodes(
      NodeOperationalState opState, HddsProtos.NodeState health) {
    return null;
  }

  @Override
  public int getNodeCount(NodeStatus nodeStatus) {
    return 0;
  }

  @Override
  public int getNodeCount(NodeOperationalState opState,
                          HddsProtos.NodeState health) {
    return 0;
  }

  @Override
  public List<DatanodeDetails> getAllNodes() {
    return null;
  }

  @Override
  public SCMNodeStat getStats() {
    return null;
  }

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

  @Override
  public DatanodeUsageInfo getUsageInfo(DatanodeDetails datanodeDetails) {
    return null;
  }

  @Override
  public SCMNodeMetric getNodeStat(DatanodeDetails datanodeDetails) {
    return null;
  }

  @Override
  public void addPipeline(Pipeline pipeline) {
  }

  @Override
  public void removePipeline(Pipeline pipeline) {
  }

  @Override
  public void addContainer(DatanodeDetails datanodeDetails,
                           ContainerID containerId)
      throws NodeNotFoundException {
  }

  @Override
  public void removeContainer(DatanodeDetails datanodeDetails,
                           ContainerID containerId) {
  }

  @Override
  public void addDatanodeCommand(UUID dnId, SCMCommand command) {
  }

  /**
   * send refresh command to all the healthy datanodes to refresh
   * volume usage info immediately.
   */
  @Override
  public void refreshAllHealthyDnUsageInfo() {
    //no op
  }

  @Override
  public void processNodeReport(DatanodeDetails datanodeDetails,
                                NodeReportProto nodeReport) {
  }

  @Override
  public void processLayoutVersionReport(DatanodeDetails datanodeDetails,
                                         LayoutVersionProto layoutReport) {
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

  @Override
  public List<SCMCommand> getCommandQueue(UUID dnID) {
    return null;
  }

  @Override
  public DatanodeDetails getNodeByUuid(String uuid) {
    return null;
  }

  @Override
  public Map<String, String> getNodeContainersReplicationMetrics(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException {
    return null;
  }

  @Override
  public List<DatanodeDetails> getNodesByAddress(String address) {
    return null;
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
    return 1;
  }

  @Override
  public int minPipelineLimit(List<DatanodeDetails> dn) {
    return 0;
  }

  @Override
  public long getLastHeartbeat(DatanodeDetails datanodeDetails) {
    return -1;
  }

  @Override
  public void close() throws IOException {

  }

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

  @Override
  public void onMessage(CommandForDatanode commandForDatanode,
                        EventPublisher publisher) {
  }

  @Override
  public VersionResponse getVersion(
      StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto
          versionRequest) {
    return null;
  }

  @Override
  public RegisteredCommand register(DatanodeDetails datanodeDetails,
                                    NodeReportProto nodeReport,
                                    PipelineReportsProto pipelineReport,
                                    LayoutVersionProto layoutreport) {
    return null;
  }

  @Override
  public List<SCMCommand> processHeartbeat(DatanodeDetails datanodeDetails,
      CommandQueueReportProto commandQueueReportProto) {
    return null;
  }

  @Override
  public Boolean isNodeRegistered(DatanodeDetails datanodeDetails) {
    return false;
  }

}
