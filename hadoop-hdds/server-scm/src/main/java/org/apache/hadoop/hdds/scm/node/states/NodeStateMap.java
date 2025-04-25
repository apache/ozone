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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;

/**
 * Map: {@link DatanodeID} to {@link DatanodeEntry}.
 * <p>
 * This class is thread-safe.
 */
public class NodeStateMap {
  /** Map: {@link DatanodeID} -> ({@link DatanodeInfo}, {@link ContainerID}s). */
  private final Map<DatanodeID, DatanodeEntry> nodeMap = new HashMap<>();

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Creates a new instance of NodeStateMap with no nodes.
   */
  public NodeStateMap() { }

  /**
   * Adds the given datanode.
   *
   * @throws NodeAlreadyExistsException if the node already exist
   */
  public void addNode(DatanodeInfo datanode) throws NodeAlreadyExistsException {
    final DatanodeID id = datanode.getID();
    lock.writeLock().lock();
    try {
      if (nodeMap.containsKey(id)) {
        throw new NodeAlreadyExistsException(id);
      }
      nodeMap.put(id, new DatanodeEntry(datanode));
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
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Update the given datanode.
   *
   * @return the existing {@link DatanodeInfo}.
   */
  public DatanodeInfo updateNode(DatanodeInfo datanode) throws NodeNotFoundException {
    final DatanodeID id = datanode.getID();
    final DatanodeInfo oldInfo;
    lock.writeLock().lock();
    try {
      oldInfo = getNodeInfo(id);
      if (oldInfo == null) {
        throw new NodeNotFoundException(id);
      }
      nodeMap.put(id, new DatanodeEntry(datanode));
    } finally {
      lock.writeLock().unlock();
    }
    return oldInfo;
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
      final DatanodeInfo dn = getExisting(nodeId).getInfo();
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
      final DatanodeInfo dn = getExisting(nodeId).getInfo();
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
      return getExisting(datanodeID).getInfo();
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
    lock.readLock().lock();
    try {
      return nodeMap.values().stream()
          .map(DatanodeEntry::getInfo)
          .collect(Collectors.toList());
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
    return opState != null && health != null ? filterNodes(matching(opState, health))
        : opState != null ? filterNodes(matching(opState))
        : health != null ? filterNodes(matching(health))
        : getAllDatanodeInfos();
  }

  /** @return Number of nodes in the given status */
  public int getNodeCount(NodeStatus status) {
    return countNodes(matching(status));
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
    return opState != null && health != null ? countNodes(matching(opState, health))
        : opState != null ? countNodes(matching(opState))
        : health != null ? countNodes(matching(health))
        : getTotalNodeCount();
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
      return getExisting(datanodeID).getInfo().getNodeStatus();
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

  /**
   * Set the containers for the given datanode.
   * This method is only used for testing.
   */
  public void setContainersForTesting(DatanodeID id, Set<ContainerID> containers)
      throws NodeNotFoundException {
    lock.writeLock().lock();
    try {
      getExisting(id).setContainersForTesting(containers);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Set<ContainerID> getContainers(DatanodeID id)
      throws NodeNotFoundException {
    lock.readLock().lock();
    try {
      return getExisting(id).copyContainers();
    } finally {
      lock.readLock().unlock();
    }
  }

  public int getContainerCount(DatanodeID datanodeID) throws NodeNotFoundException {
    lock.readLock().lock();
    try {
      return getExisting(datanodeID).getContainerCount();
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
   * @return the entry mapping to the given id.
   * @throws NodeNotFoundException If the node is missing.
   */
  private DatanodeEntry getExisting(DatanodeID id) throws NodeNotFoundException {
    final DatanodeEntry entry = nodeMap.get(id);
    if (entry == null) {
      throw new NodeNotFoundException(id);
    }
    return entry;
  }

  private int countNodes(Predicate<DatanodeInfo> filter) {
    final long count;
    lock.readLock().lock();
    try {
      count = nodeMap.values().stream()
          .map(DatanodeEntry::getInfo)
          .filter(filter)
          .count();
    } finally {
      lock.readLock().unlock();
    }
    return Math.toIntExact(count);
  }

  /**
   * @return a list of all nodes matching the {@code filter}
   */
  private List<DatanodeInfo> filterNodes(Predicate<DatanodeInfo> filter) {
    lock.readLock().lock();
    try {
      return nodeMap.values().stream()
          .map(DatanodeEntry::getInfo)
          .filter(filter)
          .collect(Collectors.toList());
    } finally {
      lock.readLock().unlock();
    }
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
