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

package org.apache.hadoop.hdds.scm.node.states;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;

/**
 * Maintains the state of datanodes in SCM. This class should only be used by
 * NodeStateManager to maintain the state. If anyone wants to change the
 * state of a node they should call NodeStateManager, do not directly use
 * this class.
 * <p>
 * Concurrency consideration:
 *   - thread-safe
 */
public class NodeStateMap {
  /**
   * Node id to node info map.
   */
  private final Map<DatanodeID, DatanodeInfo> nodeMap = new HashMap<>();
  /**
   * Node to set of containers on the node.
   */
  private final Map<DatanodeID, Set<ContainerID>> nodeToContainer = new HashMap<>();

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Creates a new instance of NodeStateMap with no nodes.
   */
  public NodeStateMap() { }

  /**
   * Adds a node to NodeStateMap.
   *
   * @param datanodeDetails DatanodeDetails
   * @param nodeStatus initial NodeStatus
   * @param layoutInfo initial LayoutVersionProto
   *
   * @throws NodeAlreadyExistsException if the node already exist
   */
  public void addNode(DatanodeDetails datanodeDetails, NodeStatus nodeStatus,
                      LayoutVersionProto layoutInfo)

      throws NodeAlreadyExistsException {
    lock.writeLock().lock();
    try {
      final DatanodeID id = datanodeDetails.getID();
      if (nodeMap.containsKey(id)) {
        throw new NodeAlreadyExistsException(id);
      }
      nodeMap.put(id, new DatanodeInfo(datanodeDetails, nodeStatus,
          layoutInfo));
      nodeToContainer.put(id, new HashSet<>());
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Removes a node from NodeStateMap.
   */
  public void removeNode(DatanodeID datanodeID) {
    lock.writeLock().lock();
    try {
      nodeMap.remove(datanodeID);
      nodeToContainer.remove(datanodeID);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Update a node in NodeStateMap.
   *
   * @param datanodeDetails DatanodeDetails
   * @param nodeStatus initial NodeStatus
   * @param layoutInfo initial LayoutVersionProto
   *
   */
  public void updateNode(DatanodeDetails datanodeDetails, NodeStatus nodeStatus,
                         LayoutVersionProto layoutInfo)

          throws NodeNotFoundException {
    lock.writeLock().lock();
    try {
      final DatanodeID id = datanodeDetails.getID();
      if (!nodeMap.containsKey(id)) {
        throw new NodeNotFoundException(id);
      }
      nodeMap.put(id, new DatanodeInfo(datanodeDetails, nodeStatus,
              layoutInfo));
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Updates the node health state.
   *
   * @param nodeId Node Id
   * @param newHealth new health state
   *
   * @throws NodeNotFoundException if the node is not present
   */
  public NodeStatus updateNodeHealthState(DatanodeID nodeId, NodeState newHealth)
      throws NodeNotFoundException {
    lock.writeLock().lock();
    try {
      DatanodeInfo dn = getNodeInfoUnsafe(nodeId);
      final NodeStatus newStatus = dn.getNodeStatus().newNodeState(newHealth);
      dn.setNodeStatus(newStatus);
      return newStatus;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Updates the node operational state.
   *
   * @param nodeId Node Id
   * @param newOpState new operational state
   *
   * @throws NodeNotFoundException if the node is not present
   */
  public NodeStatus updateNodeOperationalState(DatanodeID nodeId,
      NodeOperationalState newOpState, long opStateExpiryEpochSeconds)
      throws NodeNotFoundException {
    lock.writeLock().lock();
    try {
      DatanodeInfo dn = getNodeInfoUnsafe(nodeId);
      final NodeStatus newStatus = dn.getNodeStatus().newOperationalState(newOpState, opStateExpiryEpochSeconds);
      dn.setNodeStatus(newStatus);
      return newStatus;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * @return the info for the given node id.
   * @throws NodeNotFoundException if the node is not present
   */
  public DatanodeInfo getNodeInfo(DatanodeID datanodeID) throws NodeNotFoundException {
    lock.readLock().lock();
    try {
      return getNodeInfoUnsafe(datanodeID);
    } finally {
      lock.readLock().unlock();
    }
  }

  public int getNodeCount() {
    lock.readLock().lock();
    try {
      return nodeMap.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the list of all the nodes as DatanodeInfo objects.
   *
   * @return list of all the node ids
   */
  public List<DatanodeInfo> getAllDatanodeInfos() {
    try {
      lock.readLock().lock();
      return new ArrayList<>(nodeMap.values());
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns a list of the nodes as DatanodeInfo objects matching the passed
   * status.
   *
   * @param status - The status of the nodes to return
   * @return List of DatanodeInfo for the matching nodes
   */
  public List<DatanodeInfo> getDatanodeInfos(NodeStatus status) {
    return filterNodes(matching(status));
  }

  /**
   * Returns a list of the nodes as DatanodeInfo objects matching the passed
   * states. Passing null for either of the state values acts as a wildcard
   * for that state.
   *
   * @param opState - The node operational state
   * @param health - The node health
   * @return List of DatanodeInfo for the matching nodes
   */
  public List<DatanodeInfo> getDatanodeInfos(
      NodeOperationalState opState, NodeState health) {
    return filterNodes(opState, health);
  }

  /**
   * Returns the count of nodes in the specified state.
   *
   * @param state NodeStatus
   *
   * @return Number of nodes in the specified state
   */
  public int getNodeCount(NodeStatus state) {
    return getDatanodeInfos(state).size();
  }

  /**
   * Returns the count of node ids which match the desired operational state
   * and health. Passing a null for either value is equivalent to a wild card.
   *
   * Therefore, passing opState=null, health=stale will count all stale nodes
   * regardless of their operational state.
   *
   * @return Number of nodes in the specified state
   */
  public int getNodeCount(NodeOperationalState opState, NodeState health) {
    return filterNodes(opState, health).size();
  }

  /**
   * Returns the total node count.
   *
   * @return node count
   */
  public int getTotalNodeCount() {
    lock.readLock().lock();
    try {
      return nodeMap.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the current state of the node.
   *
   * @param datanodeID node id
   *
   * @return NodeState
   *
   * @throws NodeNotFoundException if the node is not found
   */
  public NodeStatus getNodeStatus(DatanodeID datanodeID) throws NodeNotFoundException {
    lock.readLock().lock();
    try {
      return getNodeInfoUnsafe(datanodeID).getNodeStatus();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Adds the given container to the specified datanode.
   * @throws NodeNotFoundException - if datanode is not known. For new datanode
   *                        use addDatanodeInContainerMap call.
   */
  public void addContainer(final DatanodeID datanodeID,
                           final ContainerID containerId)
      throws NodeNotFoundException {
    lock.writeLock().lock();
    try {
      getExisting(datanodeID).add(containerId);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void setContainers(DatanodeID id, Set<ContainerID> containers)
      throws NodeNotFoundException {
    lock.writeLock().lock();
    try {
      getExisting(id);
      nodeToContainer.put(id, containers);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Set<ContainerID> getContainers(DatanodeID id)
      throws NodeNotFoundException {
    lock.readLock().lock();
    try {
      return new HashSet<>(getExisting(id));
    } finally {
      lock.readLock().unlock();
    }
  }

  public int getContainerCount(DatanodeID datanodeID) throws NodeNotFoundException {
    lock.readLock().lock();
    try {
      return getExisting(datanodeID).size();
    } finally {
      lock.readLock().unlock();
    }
  }

  public void removeContainer(DatanodeID datanodeID, ContainerID containerID) throws NodeNotFoundException {
    lock.writeLock().lock();
    try {
      getExisting(datanodeID).remove(containerID);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Since we don't hold a global lock while constructing this string,
   * the result might be inconsistent. If someone has changed the state of node
   * while we are constructing the string, the result will be inconsistent.
   * This should only be used for logging. We should not parse this string and
   * use it for any critical calculations.
   *
   * @return current state of NodeStateMap
   */
  @Override
  public String toString() {
    // TODO - fix this method to include the commented out values
    StringBuilder builder = new StringBuilder();
    builder.append("Total number of nodes: ").append(getTotalNodeCount());
   // for (NodeState state : NodeState.values()) {
   //   builder.append("Number of nodes in ").append(state).append(" state: ")
   //       .append(getNodeCount(state));
   // }
    return builder.toString();
  }

  /**
   * @return the container set mapping to the given id.
   * @throws NodeNotFoundException If the node is missing.
   */
  private Set<ContainerID> getExisting(DatanodeID id) throws NodeNotFoundException {
    final Set<ContainerID> containers = nodeToContainer.get(id);
    if (containers == null) {
      throw new NodeNotFoundException(id);
    }
    return containers;
  }

  /**
   * Create a list of datanodeInfo for all nodes matching the passed states.
   * Passing null for one of the states acts like a wildcard for that state.
   *
   * @param opState
   * @param health
   * @return List of DatanodeInfo objects matching the passed state
   */
  private List<DatanodeInfo> filterNodes(
      NodeOperationalState opState, NodeState health) {
    if (opState != null && health != null) {
      return filterNodes(matching(opState, health));
    }
    if (opState != null) {
      return filterNodes(matching(opState));
    }
    if (health != null) {
      return filterNodes(matching(health));
    }
    return getAllDatanodeInfos();
  }

  /**
   * @return a list of all nodes matching the {@code filter}
   */
  private List<DatanodeInfo> filterNodes(Predicate<DatanodeInfo> filter) {
    List<DatanodeInfo> result = new LinkedList<>();
    lock.readLock().lock();
    try {
      for (DatanodeInfo dn : nodeMap.values()) {
        if (filter.test(dn)) {
          result.add(dn);
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  private @Nonnull DatanodeInfo getNodeInfoUnsafe(@Nonnull DatanodeID id) throws NodeNotFoundException {
    final DatanodeInfo info = nodeMap.get(id);
    if (info == null) {
      throw new NodeNotFoundException(id);
    }
    return info;
  }

  private static Predicate<DatanodeInfo> matching(NodeStatus status) {
    return dn -> status.equals(dn.getNodeStatus());
  }

  private static Predicate<DatanodeInfo> matching(NodeOperationalState op, NodeState health) {
    return dn -> matching(op).test(dn) && matching(health).test(dn);
  }

  private static Predicate<DatanodeInfo> matching(NodeOperationalState state) {
    return dn -> state.equals(dn.getNodeStatus().getOperationalState());
  }

  private static Predicate<DatanodeInfo> matching(NodeState health) {
    return dn -> health.equals(dn.getNodeStatus().getHealth());
  }
}
