/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.node.states.ReportResult;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.ozone.protocol.StorageContainerNodeProtocol;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A node manager supports a simple interface for managing a datanode.
 * <p/>
 * 1. A datanode registers with the NodeManager.
 * <p/>
 * 2. If the node is allowed to register, we add that to the nodes that we need
 * to keep track of.
 * <p/>
 * 3. A heartbeat is made by the node at a fixed frequency.
 * <p/>
 * 4. A node can be in any of these 4 states: {HEALTHY, STALE, DEAD,
 * DECOMMISSIONED}
 * <p/>
 * HEALTHY - It is a datanode that is regularly heartbeating us.
 *
 * STALE - A datanode for which we have missed few heart beats.
 *
 * DEAD - A datanode that we have not heard from for a while.
 *
 * DECOMMISSIONED - Someone told us to remove this node from the tracking
 * list, by calling removeNode. We will throw away this nodes info soon.
 */
public interface NodeManager extends StorageContainerNodeProtocol,
    EventHandler<CommandForDatanode>, NodeManagerMXBean, Closeable {
  /**
   * Removes a data node from the management of this Node Manager.
   *
   * @param node - DataNode.
   * @throws NodeNotFoundException
   */
  void removeNode(DatanodeDetails node) throws NodeNotFoundException;

  /**
   * Gets all Live Datanodes that is currently communicating with SCM.
   * @param nodeState - State of the node
   * @return List of Datanodes that are Heartbeating SCM.
   */
  List<DatanodeDetails> getNodes(NodeState nodeState);

  /**
   * Returns the Number of Datanodes that are communicating with SCM.
   * @param nodeState - State of the node
   * @return int -- count
   */
  int getNodeCount(NodeState nodeState);

  /**
   * Get all datanodes known to SCM.
   *
   * @return List of DatanodeDetails known to SCM.
   */
  List<DatanodeDetails> getAllNodes();

  /**
   * Chill mode is the period when node manager waits for a minimum
   * configured number of datanodes to report in. This is called chill mode
   * to indicate the period before node manager gets into action.
   *
   * Forcefully exits the chill mode, even if we have not met the minimum
   * criteria of the nodes reporting in.
   */
  void forceExitChillMode();

  /**
   * Puts the node manager into manual chill mode.
   */
  void enterChillMode();

  /**
   * Brings node manager out of manual chill mode.
   */
  void exitChillMode();

  /**
   * Returns the aggregated node stats.
   * @return the aggregated node stats.
   */
  SCMNodeStat getStats();

  /**
   * Return a map of node stats.
   * @return a map of individual node stats (live/stale but not dead).
   */
  Map<UUID, SCMNodeStat> getNodeStats();

  /**
   * Return the node stat of the specified datanode.
   * @param datanodeDetails DatanodeDetails.
   * @return node stat if it is live/stale, null if it is decommissioned or
   * doesn't exist.
   */
  SCMNodeMetric getNodeStat(DatanodeDetails datanodeDetails);

  /**
   * Returns the node state of a specific node.
   * @param datanodeDetails DatanodeDetails
   * @return Healthy/Stale/Dead.
   */
  NodeState getNodeState(DatanodeDetails datanodeDetails);

  /**
   * Get set of pipelines a datanode is part of.
   * @param dnId - datanodeID
   * @return Set of PipelineID
   */
  Set<PipelineID> getPipelineByDnID(UUID dnId);

  /**
   * Add pipeline information in the NodeManager.
   * @param pipeline - Pipeline to be added
   */
  void addPipeline(Pipeline pipeline);

  /**
   * Remove a pipeline information from the NodeManager.
   * @param pipeline - Pipeline to be removed
   */
  void removePipeline(Pipeline pipeline);

  /**
   * Update set of containers available on a datanode.
   * @param uuid - DatanodeID
   * @param containerIds - Set of containerIDs
   * @throws SCMException - if datanode is not known. For new datanode use
   *                        addDatanodeInContainerMap call.
   */
  void setContainersForDatanode(UUID uuid, Set<ContainerID> containerIds)
      throws SCMException;

  /**
   * Process containerReport received from datanode.
   * @param uuid - DataonodeID
   * @param containerIds - Set of containerIDs
   * @return The result after processing containerReport
   */
  ReportResult<ContainerID> processContainerReport(UUID uuid,
      Set<ContainerID> containerIds);

  /**
   * Return set of containerIDs available on a datanode.
   * @param uuid - DatanodeID
   * @return - set of containerIDs
   */
  Set<ContainerID> getContainers(UUID uuid);

  /**
   * Insert a new datanode with set of containerIDs for containers available
   * on it.
   * @param uuid - DatanodeID
   * @param containerIDs - Set of ContainerIDs
   * @throws SCMException - if datanode already exists
   */
  void addDatanodeInContainerMap(UUID uuid, Set<ContainerID> containerIDs)
      throws SCMException;

  /**
   * Add a {@link SCMCommand} to the command queue, which are
   * handled by HB thread asynchronously.
   * @param dnId datanode uuid
   * @param command
   */
  void addDatanodeCommand(UUID dnId, SCMCommand command);

  /**
   * Process node report.
   *
   * @param dnUuid
   * @param nodeReport
   */
  void processNodeReport(UUID dnUuid, NodeReportProto nodeReport);

  /**
   * Process a dead node event in this Node Manager.
   *
   * @param dnUuid datanode uuid.
   */
  void processDeadNode(UUID dnUuid);
}
