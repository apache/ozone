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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.container.common.volume.VolumeUsage;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks per-datanode pending container allocations at SCM using a Two Window Tumbling Bucket
 * pattern (similar to HDFS HADOOP-3707).
 *
 * Two Window Tumbling Bucket for automatic aging and cleanup.
 *
 * How It Works:
 *   <li>Each DataNode has two sets: <b>currentWindow</b> and <b>previousWindow</b></li>
 *   <li>New allocations go into <b>currentWindow</b></li>
 *   <li>Every <b>ROLL_INTERVAL</b> (default 5 minutes):
 *     <ul>
 *       <li>previousWindow = currentWindow (shift)</li>
 *       <li>currentWindow = new empty set (reset)</li>
 *       <li>Old previousWindow is discarded (automatic aging)</li>
 *     </ul>
 *   </li>
 *   <li>When checking pending: return <b>union</b> of currentWindow + previousWindow</li>
 *
 *
 * Example Timeline:
 * <pre>
 * Time  | Action                    | CurrentWindow | PreviousWindow | Total Pending
 * ------+---------------------------+---------------+----------------+--------------
 * 00:00 | Allocate Container-1      | {C1}          | {}             | {C1}
 * 00:02 | Allocate Container-2      | {C1, C2}      | {}             | {C1, C2}
 * 00:05 | [ROLL] Window tumbles     | {}            | {C1, C2}       | {C1, C2}
 * 00:07 | Allocate Container-3      | {C3}          | {C1, C2}       | {C1, C2, C3}
 * 00:08 | Report confirms C1        | {C3}          | {C2}           | {C2, C3}
 * 00:10 | [ROLL] Window tumbles     | {}            | {C3}           | {C3}
 *       | (C2 aged out if not reported)
 * </pre>
 *
 */
public class PendingContainerTracker {

  private static final Logger LOG = LoggerFactory.getLogger(PendingContainerTracker.class);

  /**
   * Maximum container size in bytes.
   */
  private final long maxContainerSize;

  /**
   * Metrics for tracking pending containers (same instance as {@link SCMNodeManager}'s node metrics).
   */
  private final SCMNodeMetrics metrics;

  /**
   * Two-window bucket for a single DataNode.
   * Contains current and previous window maps, plus last roll timestamp.
   */
  public static class TwoWindowBucket {
    private Map<ContainerID, StorageType> currentWindow = new HashMap<>();
    private Map<ContainerID, StorageType> previousWindow = new HashMap<>();
    private long lastRollTime = Time.monotonicNow();
    private final long rollIntervalMs;
    private final DatanodeID datanodeID;

    TwoWindowBucket(DatanodeID id, long rollIntervalMs) {
      this.datanodeID = id;
      this.rollIntervalMs = rollIntervalMs;
    }

    /**
     * Roll one or both windows based on elapsed time.
     */
    synchronized void rollIfNeeded() {
      long now = Time.monotonicNow();
      long elapsed = now - lastRollTime;

      if (elapsed >= 2 * rollIntervalMs) {
        int dropped = getCount();
        previousWindow.clear();
        currentWindow.clear();
        lastRollTime = now;
        LOG.debug("Double roll interval elapsed ({}ms): dropped {} pending containers", elapsed, dropped);
      } else if (elapsed >= rollIntervalMs) {
        previousWindow.clear();
        final Map<ContainerID, StorageType> tmp = previousWindow;
        previousWindow = currentWindow;
        currentWindow = tmp;
        lastRollTime = now;
        LOG.debug("Rolled window. Previous window size: {} elapsed: ({}ms), Current window reset to empty",
            previousWindow.size(), elapsed);
      }
    }

    synchronized boolean contains(ContainerID containerID) {
      return currentWindow.containsKey(containerID) || previousWindow.containsKey(containerID);
    }

    /**
     * Add container to current window.
     */
    synchronized boolean add(ContainerID containerID) {
      return add(containerID, null);
    }

    /**
     * Add container with its storage type to current window.
     */
    synchronized boolean add(ContainerID containerID, StorageType storageType) {
      boolean added = !currentWindow.containsKey(containerID);
      if (added) {
        currentWindow.put(containerID, storageType);
      }
      LOG.debug("Recorded pending container {} on DataNode {} with storageType {}. Added={}, Total pending={}",
          containerID, datanodeID, storageType, added, getCount());
      return added;
    }

    /**
     * Remove container from both windows.
     */
    synchronized boolean remove(ContainerID containerID) {
      boolean removedFromCurrent = currentWindow.containsKey(containerID);
      currentWindow.remove(containerID);
      boolean removedFromPrevious = previousWindow.containsKey(containerID);
      previousWindow.remove(containerID);
      boolean removed = removedFromCurrent || removedFromPrevious;
      LOG.debug("Removed pending container {} from DataNode {}. Removed={}, Remaining={}",
          containerID, datanodeID, removed, getCount());
      return removed;
    }

    /**
     * Count of pending containers in both windows.
     */
    synchronized int getCount() {
      return currentWindow.size() + previousWindow.size();
    }

    /**
     * Count pending containers of the given storage type.
     */
    synchronized int getCount(StorageType storageType) {
      if (checksAllStorageTypes(storageType)) {
        return getCount();
      }
      return countByStorageType(currentWindow, storageType)
          + countByStorageType(previousWindow, storageType);
    }

    private static int countByStorageType(
        Map<ContainerID, StorageType> window, StorageType storageType) {
      return (int) window.values().stream()
          .filter(storageType::equals)
          .count();
    }
  }

  public PendingContainerTracker(long maxContainerSize, long rollIntervalMs, SCMNodeMetrics metrics) {
    this.maxContainerSize = maxContainerSize;
    this.metrics = metrics;
    LOG.info("PendingContainerTracker initialized with maxContainerSize={}B, rollInterval={}ms",
        maxContainerSize, rollIntervalMs);
  }

  /**
   * Whether the datanode can fit another container of {@link #maxContainerSize} after accounting for
   * SCM pending allocations for {@code node} (this tracker) and usable space across volumes on
   * {@code datanodeInfo}. Pending bytes are count × {@code maxContainerSize};
   * effective allocatable space sums full-container slots per storage report.
   *
   * @param datanodeInfo storage reports for the datanode
   */
  public boolean hasEffectiveAllocatableSpaceForNewContainer(DatanodeInfo datanodeInfo) {
    return hasEffectiveAllocatableSpaceForNewContainer(datanodeInfo, null);
  }

  /**
   * Whether the datanode can fit another container of {@link #maxContainerSize}
   * in the requested storage type after accounting for SCM pending allocations.
   * A null storage type preserves the legacy behavior of checking all reports.
   *
   * @param datanodeInfo storage reports for the datanode
   * @param storageType storage type for this allocation, or null for all reports
   */
  public boolean hasEffectiveAllocatableSpaceForNewContainer(
      DatanodeInfo datanodeInfo, StorageType storageType) {
    Objects.requireNonNull(datanodeInfo, "datanodeInfo == null");

    long pendingAllocationSize =
        datanodeInfo.getPendingContainerAllocations().getCount(storageType) * maxContainerSize;
    List<StorageReportProto> storageReports = datanodeInfo.getStorageReports();
    Objects.requireNonNull(storageReports, "storageReports == null");
    if (storageReports.isEmpty()) {
      return false;
    }
    long effectiveAllocatableSpace = 0L;
    for (StorageReportProto report : storageReports) {
      if (!matchesStorageType(report, storageType)) {
        continue;
      }
      long usableSpace = VolumeUsage.getUsableSpace(report);
      long containersOnThisDisk = usableSpace / maxContainerSize;
      effectiveAllocatableSpace += containersOnThisDisk * maxContainerSize;
      if (effectiveAllocatableSpace - pendingAllocationSize >= maxContainerSize) {
        return true;
      }
    }
    if (metrics != null) {
      metrics.incNumSkippedFullNodeContainerAllocation();
    }
    return false;
  }

  /**
   * Record a pending container allocation for a single DataNode.
   * Container is added to the current window.
   *
   * @param datanodeInfo The DataNode receiving the allocation
   * @param containerID The container being allocated/replicated
   */
  public void recordPendingAllocationForDatanode(DatanodeInfo datanodeInfo, ContainerID containerID) {
    recordPendingAllocationForDatanode(datanodeInfo, containerID, null);
  }

  /**
   * Record a pending container allocation for a single DataNode.
   * Container is added to the current window.
   *
   * @param datanodeInfo The DataNode receiving the allocation
   * @param containerID The container being allocated/replicated
   * @param storageType The storage type selected for the allocation
   */
  public void recordPendingAllocationForDatanode(
      DatanodeInfo datanodeInfo, ContainerID containerID, StorageType storageType) {
    Objects.requireNonNull(containerID, "containerID == null");
    if (datanodeInfo == null) {
      return;
    }
    final boolean added =
        datanodeInfo.getPendingContainerAllocations().add(containerID, storageType);
    if (added && metrics != null) {
      metrics.incNumPendingContainersAdded();
    }
  }

  /**
   * Remove a pending container allocation from a specific DataNode.
   * Removes from both current and previous windows.
   * Called when container is confirmed.
   *
   * @param containerID The container to remove from pending
   */
  public void removePendingAllocation(TwoWindowBucket bucket, ContainerID containerID) {
    Objects.requireNonNull(containerID, "containerID == null");

    boolean removed = bucket.remove(containerID);

    if (removed && metrics != null) {
      metrics.incNumPendingContainersRemoved();
    }
  }

  private static boolean matchesStorageType(
      StorageReportProto report, StorageType storageType) {
    return checksAllStorageTypes(storageType)
        || StorageTypeUtils.getFromProtobuf(report.getStorageType()).equals(storageType);
  }

  private static boolean checksAllStorageTypes(StorageType storageType) {
    return storageType == null;
  }
}
