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

package org.apache.hadoop.ozone.om.jobworker.node;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.ozone.om.jobworker.states.JobworkerNodeAlreadyExistsException;
import org.apache.hadoop.ozone.om.jobworker.states.JobworkerNodeNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains the state of all the Jobworker in the cluster.
 * All the Jobworker state change should happen only via OM side.
 */
public class JobworkerNodeStateManager implements Closeable {

  public static final Logger LOG = LoggerFactory.getLogger(JobworkerNodeStateManager.class);

  private final ReadWriteLock lock;
  /**
   * Node id to node info map.
   */
  private final ConcurrentHashMap<UUID, JobworkerInfo> nodeMap;

  public JobworkerNodeStateManager() {
    this.lock = new ReentrantReadWriteLock();
    nodeMap = new ConcurrentHashMap<>();
  }

  /**
   * Returns JobworkerInfo for the given node id.
   * @param uuid Node Id
   * @return JobworkerInfo of the node
   * @throws JobworkerNodeNotFoundException - if datanode is not known. For new datanode
   *                               use addNode call.
   */
  public JobworkerInfo getNodeInfo(UUID uuid) throws JobworkerNodeNotFoundException {
    lock.readLock().lock();
    try {
      if (!nodeMap.containsKey(uuid)) {
        throw new JobworkerNodeNotFoundException("Jobworker UUID: " + uuid);
      }
      return nodeMap.get(uuid);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Adds a new jobworker node to the state manager.
   * @param jobworkerDetails jobworkerDetails
   * @throws JobworkerNodeAlreadyExistsException if the jobworker node is already present
   */
  public void addNode(JobworkerDetails jobworkerDetails) throws JobworkerNodeAlreadyExistsException {
    JobworkerNodeStatus newNodeStatus = newNodeStatus(jobworkerDetails);
    addNodeInternal(jobworkerDetails, newNodeStatus);
  }

  /**
   * Removes a JobWorker node from the state manager.
   * @param uuid The UUID of the node to remove
   * @throws JobworkerNodeNotFoundException if the node doesn't exist
   */
  public void removeNode(UUID uuid) throws JobworkerNodeNotFoundException {
    lock.writeLock().lock();
    try {
      if (!nodeMap.containsKey(uuid)) {
        throw new JobworkerNodeNotFoundException("Jobworker UUID: " + uuid);
      }
      nodeMap.remove(uuid);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * @param jobworker JobworkerDetails reported by the jobworker
   */
  private JobworkerNodeStatus newNodeStatus(JobworkerDetails jobworker) {
    HddsProtos.NodeOperationalState opState =
        jobworker.getOperationalState() != null ?
            jobworker.getOperationalState() : NodeOperationalState.IN_SERVICE;

    if (opState != NodeOperationalState.IN_SERVICE) {
      LOG.info("Updating nodeOperationalState on registration as the " +
          "jobworker has a persisted state of {}", opState);
      return new JobworkerNodeStatus(opState, HEALTHY);
    } else {
      return new JobworkerNodeStatus(NodeOperationalState.IN_SERVICE, HEALTHY);
    }
  }

  /**
   * Adds a node to NodeStateMap.
   * @param jobworkerDetails jobworkerDetails
   * @param nodeStatus initial JobworkerNodeStatus
   * @throws JobworkerNodeAlreadyExistsException if the node already exists
   */
  private void addNodeInternal(JobworkerDetails jobworkerDetails, JobworkerNodeStatus nodeStatus)
      throws JobworkerNodeAlreadyExistsException {
    lock.writeLock().lock();
    try {
      UUID id = jobworkerDetails.getUuid();
      if (nodeMap.containsKey(id)) {
        throw new JobworkerNodeAlreadyExistsException("Node UUID: " + id);
      }
      nodeMap.put(id, new JobworkerInfo(jobworkerDetails, nodeStatus));
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Updates the last heartbeat time of the node.
   *
   * @throws JobworkerNodeNotFoundException if the node is not present
   */
  public void updateLastHeartbeatTime(JobworkerDetails jobworkerDetails)
      throws JobworkerNodeNotFoundException {
    lock.readLock().lock();
    try {
      checkIfNodeExist(jobworkerDetails.getUuid());
      nodeMap.get(jobworkerDetails.getUuid()).updateLastHeartbeatTime();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns all JobWorkerInfos in the cluster.
   * @return List of all JobWorkerInfo objects
   */
  public List<JobworkerInfo> getAllJobworkerInfos() {
    lock.readLock().lock();
    try {
      return new ArrayList<>(nodeMap.values());
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns all JobWorkerInfos which nodeStatus is HEALTH in the cluster.
   * @return List of all JobWorkerInfo objects
   */
  public List<JobworkerInfo> getHealthyJobworkerInfos() {
    lock.readLock().lock();
    try {
      return nodeMap.values().stream()
          .filter(info -> info.getNodeStatus().isHealthy())
          .collect(toList());
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the count of nodes with the specified health state.
   * @param state NodeState (HEALTHY, STALE)
   * @return number of nodes in that state
   */
  public int getNodeCount(NodeState state) {
    lock.readLock().lock();
    try {
      return (int) nodeMap.values().stream()
          .filter(info -> info.getNodeStatus().getHealthState() == state)
          .count();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the total node count.
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
   * Throws NodeNotFoundException if the Node for given id doesn't exist.
   *
   * @param uuid Node UUID
   * @throws JobworkerNodeNotFoundException If the node is missing.
   */
  private void checkIfNodeExist(UUID uuid) throws JobworkerNodeNotFoundException {
    if (!nodeMap.containsKey(uuid)) {
      throw new JobworkerNodeNotFoundException("Node UUID: " + uuid);
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
    StringBuilder builder = new StringBuilder();
    builder.append("Total number of JobWorker nodes: ").append(getTotalNodeCount());
    builder.append(", Healthy nodes: ").append(getNodeCount(HEALTHY));
    builder.append(", Stale nodes: ").append(getNodeCount(NodeState.STALE));
    return builder.toString();
  }

  @Override
  public void close() throws IOException {
    // No resources to close
  }
}
