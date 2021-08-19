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
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaCount;
import org.apache.ratis.util.Preconditions;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Queues for replication commands to be sent to datanodes
 * with several priorities defined.
 */
public class ReplicationPriorityQueues {
  /**
   * Replication Priorities in regards of the current redundancy of containers.
   */
  public enum ReplicationPriority {
    /**
     * Containers with only one replica or
     * only decommissioned/maintenance replicas.
     */
    HIGHEST_PRIORITY,
    /**
     * Containers with very few replicas, healthy replica ratio under 1/3.
     */
    VERY_LOW_REDUNDANCY,
    /**
     * Containers with missing replicas.
     */
    LOW_REDUNDANCY,
    /**
     * Containers with full replicas, but some are badly distributed
     * according to placement policies.
     */
    BADLY_DISTRIBUTED,
    /**
     * Containers with no replicas at all.
     */
    CORRUPTED
  }

  /**
   * Wrap the info for a replication command.
   */
  public static class ReplicationWork {
    private ContainerInfo container;
    private DatanodeDetails dest;
    private List<DatanodeDetails> sources;

    public ReplicationWork(ContainerInfo container, DatanodeDetails dest,
        List<DatanodeDetails> sources) {
      this.container = container;
      this.dest = dest;
      this.sources = sources;
    }

    public ContainerInfo getContainer() {
      return this.container;
    }

    public DatanodeDetails getDest() {
      return this.dest;
    }

    public List<DatanodeDetails> getSources() {
      return this.sources;
    }
  }

  /**
   * Plain queues for different priorities.
   */
  private Map<ReplicationPriority,
      LinkedList<ReplicationWork>> replicationQueues;

  /**
   * Tracks the priority for containers so we could do potential updates
   * to its priority.
   */
  private Map<ContainerInfo, ReplicationPriority> containerPriorityMap;

  /**
   * Tracks number of replication works of each container then we could
   * remove a container from the priority map once all works are done.
   */
  private Map<ContainerInfo, Integer> containerReplicationWorkCounter;

  public ReplicationPriorityQueues() {
    this.replicationQueues = new HashMap<>();
    for (ReplicationPriority priority : ReplicationPriority.values()) {
      this.replicationQueues.put(priority, new LinkedList<>());
    }
    this.containerPriorityMap = new HashMap<>();
    this.containerReplicationWorkCounter = new HashMap<>();
  }

  /**
   * Get the priority of a container according to the replica counting info.
   * @param replicaCount replica counting info.
   * @param placementStatus placement status.
   * @return priority for sending out a replication command
   */
  public ReplicationPriority getPriority(ContainerReplicaCount replicaCount,
      ContainerPlacementStatus placementStatus) {
    Preconditions.assertTrue(!replicaCount.isSufficientlyReplicated()
        || !placementStatus.isPolicySatisfied());

    if (replicaCount.getHealthyCount() == 0) {
      if (replicaCount.getDecommissionCount()
          + replicaCount.getMaintenanceCount() > 0) {
        return ReplicationPriority.HIGHEST_PRIORITY;
      }
      return ReplicationPriority.CORRUPTED;
    } else if (replicaCount.getHealthyCount() == 1) {
      return ReplicationPriority.HIGHEST_PRIORITY;
    } else if (replicaCount.getHealthyCount() * 3 < replicaCount
        .getReplicationFactor()) {
      return ReplicationPriority.VERY_LOW_REDUNDANCY;
    } else if (!placementStatus.isPolicySatisfied()) {
      return ReplicationPriority.BADLY_DISTRIBUTED;
    }
    return ReplicationPriority.LOW_REDUNDANCY;
  }

  public synchronized boolean offer(ContainerInfo container,
      List<DatanodeDetails> dests, List<DatanodeDetails> sources,
      ContainerReplicaCount replicaCount,
      ContainerPlacementStatus placementStatus) {
    // prevent duplicate commands
    if (containerPriorityMap.containsKey(container)) {
      return false;
    }

    ReplicationPriority priority = getPriority(replicaCount, placementStatus);
    for (DatanodeDetails destDn : dests) {
      replicationQueues.get(priority).offer(new ReplicationWork(
          container, destDn, sources));
    }
    containerPriorityMap.put(container, priority);
    containerReplicationWorkCounter.put(container, dests.size());
    return true;
  }

  public synchronized List<ReplicationWork> pollN(
      final ReplicationDatanodeThrottling throttling) {
    List<ReplicationWork> deliverWorks = new LinkedList<>();
    List<ReplicationWork> throttledWorks = new LinkedList<>();
    Map<DatanodeDetails, Integer> datanodeWorkCounter = new HashMap<>();

    for (ReplicationPriority priority : ReplicationPriority.values())  {
      List<ReplicationWork> queue = replicationQueues.get(priority);
      while (!queue.isEmpty()) {
        ReplicationWork work = replicationQueues.get(priority).poll();
        DatanodeDetails destDn = work.getDest();
        int destDnWorkCount = datanodeWorkCounter.getOrDefault(destDn, 0);
        if (throttling.shouldThrottle(destDn, destDnWorkCount + 1)) {
          throttledWorks.add(work);
          continue;
        }
        deliverWorks.add(work);
        datanodeWorkCounter.putIfAbsent(destDn, destDnWorkCount + 1);

        // Once a replication work is polled, we could remove
        // it from the container work counter.
        ContainerInfo container = work.getContainer();
        int containerWorkCount = containerReplicationWorkCounter
            .getOrDefault(container, 0);
        if (containerWorkCount - 1 == 0) {
          // Once all replication works for a container are polled
          // remove the container from the priority map.
          containerReplicationWorkCounter.remove(container);
          containerPriorityMap.remove(container);
        } else {
          containerReplicationWorkCounter.put(container,
              containerWorkCount - 1);
        }
      }

      // requeue those works that is throttled on a per datanode base
      queue.addAll(throttledWorks);
      throttledWorks.clear();
    }

    return deliverWorks;
  }

  public void clear() {
    this.containerPriorityMap.clear();
    this.replicationQueues.clear();
  }
}
