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

import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.defaultLayoutVersionProto;

import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.StorageContainerNodeProtocol;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

/**
 * A node manager supports a simple interface for managing a datanode.
 * <p>
 * 1. A datanode registers with the NodeManager.
 * <p>
 * 2. If the node is allowed to register, we add that to the nodes that we need
 * to keep track of.
 * <p>
 * 3. A heartbeat is made by the node at a fixed frequency.
 * <p>
 * 4. A node can be in any of these 4 states: {HEALTHY, STALE, DEAD,
 * DECOMMISSIONED}
 * <p>
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
   * Register API without a layout version info object passed in. Useful for
   * tests.
   * @param datanodeDetails DN details
   * @param nodeReport Node report
   * @param pipelineReportsProto Pipeline reports
   * @return whatever the regular register command returns with default
   * layout version passed in.
   */
  default RegisteredCommand register(
      DatanodeDetails datanodeDetails, NodeReportProto nodeReport,
      PipelineReportsProto pipelineReportsProto) {
    return register(datanodeDetails, nodeReport, pipelineReportsProto,
        defaultLayoutVersionProto());
  }

  /**
   * Register a SendCommandNotify handler for a specific type of SCMCommand.
   * @param type The type of the SCMCommand.
   * @param scmCommand A BiConsumer that takes a DatanodeDetails and a
   *                   SCMCommand object and performs the necessary actions.
   */
  default void registerSendCommandNotify(SCMCommandProto.Type type,
      BiConsumer<DatanodeDetails, SCMCommand<?>> scmCommand) {
  }

  /**
   * Gets all Live Datanodes that are currently communicating with SCM.
   * @param nodeStatus - Status of the node to return
   * @return List of Datanodes that are Heartbeating SCM.
   */
  List<DatanodeDetails> getNodes(NodeStatus nodeStatus);

  /**
   * Gets all Live Datanodes that is currently communicating with SCM.
   * @param opState - The operational state of the node
   * @param health - The health of the node
   * @return List of Datanodes that are Heartbeating SCM.
   */
  List<DatanodeDetails> getNodes(
      NodeOperationalState opState, NodeState health);

  /**
   * Returns the Number of Datanodes that are communicating with SCM with the
   * given status.
   * @param nodeStatus - State of the node
   * @return int -- count
   */
  int getNodeCount(NodeStatus nodeStatus);

  /**
   * Returns the Number of Datanodes that are communicating with SCM in the
   * given state.
   * @param opState - The operational state of the node
   * @param health - The health of the node
   * @return int -- count
   */
  int getNodeCount(
      NodeOperationalState opState, NodeState health);

  /**
   * @return all datanodes known to SCM.
   */
  List<? extends DatanodeDetails> getAllNodes();

  /** @return the number of datanodes. */
  default int getAllNodeCount() {
    return getAllNodes().size();
  }

  /**
   * Returns the aggregated node stats.
   * @return the aggregated node stats.
   */
  SCMNodeStat getStats();

  /**
   * Return a map of node stats.
   * @return a map of individual node stats (live/stale but not dead).
   */
  Map<DatanodeDetails, SCMNodeStat> getNodeStats();

  /**
   * Gets a sorted list of most or least used DatanodeUsageInfo containing
   * healthy, in-service nodes. If the specified mostUsed is true, the returned
   * list is in descending order of usage. Otherwise, the returned list is in
   * ascending order of usage.
   *
   * @param mostUsed true if most used, false if least used
   * @return List of DatanodeUsageInfo
   */
  List<DatanodeUsageInfo> getMostOrLeastUsedDatanodes(boolean mostUsed);

  /**
   * Get the usage info of a specified datanode.
   *
   * @param dn the usage of which we want to get
   * @return DatanodeUsageInfo of the specified datanode
   */
  DatanodeUsageInfo getUsageInfo(DatanodeDetails dn);

  /**
   * Get the datanode info of a specified datanode.
   *
   * @param dn the usage of which we want to get
   * @return DatanodeInfo of the specified datanode
   */
  @Nullable
  DatanodeInfo getDatanodeInfo(DatanodeDetails dn);

  /**
   * Return the node stat of the specified datanode.
   * @param datanodeDetails DatanodeDetails.
   * @return node stat if it is live/stale, null if it is decommissioned or
   * doesn't exist.
   */
  SCMNodeMetric getNodeStat(DatanodeDetails datanodeDetails);

  /**
   * Returns the node status of a specific node.
   * @param datanodeDetails DatanodeDetails
   * @return NodeStatus for the node
   * @throws NodeNotFoundException if the node does not exist
   */
  NodeStatus getNodeStatus(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException;

  /**
   * Set the operation state of a node.
   * @param datanodeDetails The datanode to set the new state for
   * @param newState The new operational state for the node
   */
  void setNodeOperationalState(DatanodeDetails datanodeDetails,
      NodeOperationalState newState) throws NodeNotFoundException;

  /**
   * Set the operation state of a node.
   * @param datanodeDetails The datanode to set the new state for
   * @param newState The new operational state for the node
   * @param opStateExpiryEpocSec Seconds from the epoch when the operational
   *                             state should end. Zero indicates the state
   *                             never end.
   */
  void setNodeOperationalState(DatanodeDetails datanodeDetails,
       NodeOperationalState newState,
       long opStateExpiryEpocSec) throws NodeNotFoundException;

  /**
   * Get set of pipelines a datanode is part of.
   * @param datanodeDetails DatanodeDetails
   * @return Set of PipelineID
   */
  Set<PipelineID> getPipelines(DatanodeDetails datanodeDetails);

  /**
   * Get the count of pipelines a datanodes is associated with.
   * @param datanodeDetails DatanodeDetails
   * @return The number of pipelines
   */
  int getPipelinesCount(DatanodeDetails datanodeDetails);

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
   * Adds the given container to the specified datanode.
   *
   * @param datanodeDetails - DatanodeDetails
   * @param containerId - containerID
   * @throws NodeNotFoundException - if datanode is not known. For new datanode
   *                        use addDatanodeInContainerMap call.
   */
  void addContainer(DatanodeDetails datanodeDetails,
                    ContainerID containerId) throws NodeNotFoundException;

  /**
   * Removes the given container from the specified datanode.
   *
   * @param datanodeDetails - DatanodeDetails
   * @param containerId - containerID
   * @throws NodeNotFoundException - if datanode is not known. For new datanode
   *                        use addDatanodeInContainerMap call.
   */
  void removeContainer(DatanodeDetails datanodeDetails,
      ContainerID containerId) throws NodeNotFoundException;

  /**
   * Return set of containerIDs available on a datanode.
   * @param datanodeDetails DatanodeDetails
   * @return set of containerIDs
   */
  Set<ContainerID> getContainers(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException;

  /**
   * Add a {@link SCMCommand} to the command queue of the given datanode.
   * The command will be handled by the HB thread asynchronously.
   */
  void addDatanodeCommand(DatanodeID datanodeID, SCMCommand<?> command);

  /**
   * send refresh command to all the healthy datanodes to refresh
   * volume usage info immediately.
   */
  void refreshAllHealthyDnUsageInfo();

  /**
   * Process node report.
   *
   * @param datanodeDetails
   * @param nodeReport
   */
  void processNodeReport(DatanodeDetails datanodeDetails,
                         NodeReportProto nodeReport);

  /**
   * Process Node LayoutVersion report.
   *
   * @param datanodeDetails
   * @param layoutReport
   */
  void processLayoutVersionReport(DatanodeDetails datanodeDetails,
                         LayoutVersionProto layoutReport);

  /**
   * Get the number of commands of the given type queued on the datanode at the
   * last heartbeat. If the Datanode has not reported information for the given
   * command type, -1 will be returned.
   * @param cmdType
   * @return The queued count or -1 if no data has been received from the DN.
   */
  int getNodeQueuedCommandCount(DatanodeDetails datanodeDetails,
      SCMCommandProto.Type cmdType) throws NodeNotFoundException;

  /**
   * Get the number of commands of the given type queued in the SCM CommandQueue
   * for the given datanode.
   * @param dnID The ID of the datanode.
   * @param cmdType The Type of command to query the current count for.
   * @return The count of commands queued, or zero if none.
   */
  int getCommandQueueCount(DatanodeID dnID, SCMCommandProto.Type cmdType);

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
  int getTotalDatanodeCommandCount(DatanodeDetails datanodeDetails,
      SCMCommandProto.Type cmdType) throws NodeNotFoundException;

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
  Map<SCMCommandProto.Type, Integer> getTotalDatanodeCommandCounts(
      DatanodeDetails datanodeDetails, SCMCommandProto.Type... cmdType)
      throws NodeNotFoundException;

  /**
   * Get list of SCMCommands in the Command Queue for a particular Datanode.
   * @return list of commands
   */
  // TODO: We can give better name to this method!
  List<SCMCommand<?>> getCommandQueue(DatanodeID dnID);

  /** @return the datanode of the given id if it exists; otherwise, return null. */
  @Nullable DatanodeDetails getNode(@Nullable DatanodeID id);

  /**
   * Given datanode address(Ipaddress or hostname), returns a list of
   * DatanodeDetails for the datanodes running at that address.
   *
   * @param address datanode address
   * @return the given datanode, or empty list if none found
   */
  List<DatanodeDetails> getNodesByAddress(String address);

  /**
   * For the given node, retried the last heartbeat time.
   * @param datanodeDetails DatanodeDetails of the node.
   * @return The last heartbeat time in milliseconds or -1 if the node does not
   *         existing in the nodeManager.
   */
  long getLastHeartbeat(DatanodeDetails datanodeDetails);

  /**
   * Get cluster map as in network topology for this node manager.
   * @return cluster map
   */
  NetworkTopology getClusterNetworkTopologyMap();

  int totalHealthyVolumeCount();

  int pipelineLimit(DatanodeDetails dn);

  /**
   * Gets the peers in all the pipelines for the particular datnode.
   * @param dn datanode
   */
  default Collection<DatanodeDetails> getPeerList(DatanodeDetails dn) {
    return null;
  }

  default HDDSLayoutVersionManager getLayoutVersionManager() {
    return null;
  }

  default void forceNodesToHealthyReadOnly() { }

  /**
   * This API allows removal of only DECOMMISSIONED, IN_MAINTENANCE and DEAD nodes
   * from NodeManager data structures and cleanup memory.
   * @param datanodeDetails
   * @throws NodeNotFoundException
   */
  default void removeNode(DatanodeDetails datanodeDetails) throws NodeNotFoundException, IOException {

  }

  default int openContainerLimit(List<DatanodeDetails> dnList,
      int numContainerPerVolume) {
    throw new UnsupportedOperationException(
        "openContainerLimit is not supported by this NodeManager implementation");
  }
}
