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

import com.google.common.annotations.VisibleForTesting;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
 * Pending allocations are grouped by storage type. Each group uses a two-window
 * tumbling bucket for automatic aging and cleanup.
 *
 * How It Works:
 *   <li>Each bucket has two sets: <b>currentWindow</b> and <b>previousWindow</b></li>
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
   * Contains current and previous window sets, plus last roll timestamp.
   */
  public static class TwoWindowBucket {
    private Set<ContainerID> currentWindow = new HashSet<>();
    private Set<ContainerID> previousWindow = new HashSet<>();
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
        if (dropped > 0) {
          LOG.warn("PendingContainerTracker: force-dropped {} unconfirmed pending containers "
              + "on DN {} after {}ms (2x rollInterval). "
              + "Container reports may have been lost.", dropped, datanodeID, elapsed);
        }
      } else if (elapsed >= rollIntervalMs) {
        previousWindow.clear();
        final Set<ContainerID> tmp = previousWindow;
        previousWindow = currentWindow;
        currentWindow = tmp;
        lastRollTime = now;
        LOG.debug("Rolled window. Previous window size: {} elapsed: ({}ms), Current window reset to empty",
            previousWindow.size(), elapsed);
      }
    }

    synchronized boolean contains(ContainerID containerID) {
      return currentWindow.contains(containerID) || previousWindow.contains(containerID);
    }

    synchronized boolean containsInCurrentWindow(ContainerID containerID) {
      return currentWindow.contains(containerID);
    }

    /**
     * Add container to current window.
     */
    synchronized boolean add(ContainerID containerID) {
      return currentWindow.add(containerID);
    }

    /**
     * Remove container from both windows.
     */
    synchronized boolean remove(ContainerID containerID) {
      boolean removedFromCurrent = currentWindow.remove(containerID);
      boolean removedFromPrevious = previousWindow.remove(containerID);
      boolean removed = removedFromCurrent || removedFromPrevious;
      return removed;
    }

    /**
     * Count of pending containers in both windows.
     */
    synchronized int getCount() {
      return currentWindow.size() + previousWindow.size();
    }
  }

  /**
   * Pending container allocations for one datanode, grouped by storage type.
   */
  public static class PendingContainerAllocations {
    private final Map<StorageType, TwoWindowBucket> typedBuckets =
        new EnumMap<>(StorageType.class);
    private final TwoWindowBucket unknownBucket;
    private final long rollIntervalMs;
    private final DatanodeID datanodeID;

    PendingContainerAllocations(DatanodeID id, long rollIntervalMs) {
      this.datanodeID = id;
      this.rollIntervalMs = rollIntervalMs;
      this.unknownBucket = new TwoWindowBucket(id, rollIntervalMs);
    }

    synchronized void rollIfNeeded() {
      unknownBucket.rollIfNeeded();
      typedBuckets.values().forEach(TwoWindowBucket::rollIfNeeded);
    }

    synchronized boolean contains(ContainerID containerID) {
      return unknownBucket.contains(containerID)
          || typedBuckets.values().stream()
          .anyMatch(bucket -> bucket.contains(containerID));
    }

    /**
     * Count pending containers of the given storage type.
     * Unknown storage type entries are counted for typed checks because they
     * may occupy the requested storage type.
     */
    synchronized int getCount(StorageType storageType) {
      if (checksAllStorageTypes(storageType)) {
        return getCount();
      }
      TwoWindowBucket bucket = typedBuckets.get(storageType);
      return unknownBucket.getCount() + (bucket != null ? bucket.getCount() : 0);
    }

    /**
     * Count of pending containers in all buckets.
     */
    synchronized int getCount() {
      return unknownBucket.getCount()
          + typedBuckets.values().stream()
          .mapToInt(TwoWindowBucket::getCount)
          .sum();
    }

    /**
     * Records a container allocation in the current window,
     * without checking available space. Use this when the space check has
     * already been performed by the placement policy.
     */
    synchronized boolean record(ContainerID containerID, StorageType storageType) {
      return add(containerID, storageType);
    }

    synchronized boolean add(ContainerID containerID, StorageType storageType) {
      if (containsInCurrentWindow(containerID)) {
        LOG.debug("Recorded pending container {} on DataNode {} with storageType {}. "
            + "Added=false, Total pending={}",
            containerID, datanodeID, storageType, getCount());
        return false;
      }
      boolean added = bucketFor(storageType).add(containerID);
      LOG.debug("Recorded pending container {} on DataNode {} with storageType {}. "
          + "Added={}, Total pending={}",
          containerID, datanodeID, storageType, added, getCount());
      return added;
    }

    private boolean containsInCurrentWindow(ContainerID containerID) {
      return unknownBucket.containsInCurrentWindow(containerID)
          || typedBuckets.values().stream()
          .anyMatch(bucket -> bucket.containsInCurrentWindow(containerID));
    }

    /**
     * Remove container from all buckets.
     */
    synchronized boolean remove(ContainerID containerID) {
      boolean removed = unknownBucket.remove(containerID);
      for (TwoWindowBucket bucket : typedBuckets.values()) {
        removed |= bucket.remove(containerID);
      }
      LOG.debug("Removed pending container {} from DataNode {}. Removed={}, Remaining={}",
          containerID, datanodeID, removed, getCount());
      return removed;
    }

    /**
     * Atomically checks whether there is allocatable space for one more container of
     * {@code maxContainerSize} given the current pending count, and adds {@code containerID}
     * to the current window if so.
     *
     * @param storageReports storage reports for the datanode
     * @param maxContainerSize maximum size of a single container in bytes
     * @param containerID the container being allocated
     * @param storageType the storage type selected for the allocation
     * @return true if space was available and the container was recorded, false otherwise
     */
    synchronized boolean checkSpaceAndAdd(
        List<StorageReportProto> storageReports, long maxContainerSize,
        ContainerID containerID, StorageType storageType) {
      rollIfNeeded();
      final int pendingAllocationCount = getCount(storageType);
      long allocatableCount = 0;
      for (StorageReportProto report : storageReports) {
        if (report.hasFailed() && report.getFailed()) {
          continue;
        }
        if (!matchesStorageType(report, storageType)) {
          continue;
        }
        final long allocatableCountOnThisDisk =
            Math.max(0L, VolumeUsage.getUsableSpace(report)) / maxContainerSize;
        allocatableCount += allocatableCountOnThisDisk;
        if (allocatableCount > pendingAllocationCount) {
          final boolean added = add(containerID, storageType);
          return added;
        }
      }
      return false;
    }

    private TwoWindowBucket bucketFor(StorageType storageType) {
      if (checksAllStorageTypes(storageType)) {
        return unknownBucket;
      }
      return typedBuckets.computeIfAbsent(storageType,
          ignored -> new TwoWindowBucket(datanodeID, rollIntervalMs));
    }
  }

  public PendingContainerTracker(long maxContainerSize, long rollIntervalMs, SCMNodeMetrics metrics) {
    this.maxContainerSize = maxContainerSize;
    this.metrics = metrics;
    LOG.info("PendingContainerTracker initialized with maxContainerSize={}B, rollInterval={}ms",
        maxContainerSize, rollIntervalMs);
  }

  /**
   * Atomically checks if the datanode has space for a new container and records the allocation
   * if space is available. The check-and-add atomicity is enforced inside
   * {@link PendingContainerAllocations#checkSpaceAndAdd}.
   *
   * @param datanodeInfo datanode whose storage reports and pending allocations are checked
   * @param containerID the container being allocated
   * @return true if space was available and the allocation was recorded, false otherwise
   */
  public boolean checkSpaceAndRecordAllocation(
      DatanodeInfo datanodeInfo, ContainerID containerID, StorageType storageType) {
    Objects.requireNonNull(datanodeInfo, "datanodeInfo == null");
    Objects.requireNonNull(containerID, "containerID == null");

    List<StorageReportProto> storageReports = datanodeInfo.getStorageReports();
    Objects.requireNonNull(storageReports, "storageReports == null");
    if (storageReports.isEmpty()) {
      return false;
    }

    boolean added = datanodeInfo.getPendingContainerAllocations()
        .checkSpaceAndAdd(storageReports, maxContainerSize, containerID, storageType);
    if (metrics != null) {
      if (added) {
        metrics.incNumPendingContainersAdded();
      } else {
        metrics.incNumSkippedFullNodeContainerAllocation();
      }
    }
    return added;
  }

  /**
   * Records a container allocation on the given datanode in the
   * current window, without performing a space check. This is used when the
   * space check was already done by the placement policy (e.g. from
   * {@link org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps}).
   *
   * @param datanodeInfo the datanode receiving the container
   * @param containerID  the container being allocated
   * @param storageType the storage type selected for the allocation
   */
  public void recordAllocation(
      DatanodeInfo datanodeInfo, ContainerID containerID, StorageType storageType) {
    Objects.requireNonNull(datanodeInfo, "datanodeInfo == null");
    Objects.requireNonNull(containerID, "containerID == null");
    final boolean added =
        datanodeInfo.getPendingContainerAllocations().record(containerID, storageType);
    if (added && metrics != null) {
      metrics.incNumPendingContainersAdded();
    }
  }

  /**
   * Returns true if the given datanode has at least one allocatable container
   * slot available, accounting for pending in-flight allocations.
   *
   * <p>Slot availability is based on {@code maxContainerSize}: a slot exists for each
   * {@code maxContainerSize}-worth of usable space on any volume. This check is intended for the placement policy.
   * This rolls expired-window entries but does not consume a slot.
   *
   * @param datanodeInfo the datanode to check
   * @param storageType storage type to check
   * @return true if at least one container slot is available
   */
  public boolean hasAvailableSpace(
      DatanodeInfo datanodeInfo, StorageType storageType) {
    Objects.requireNonNull(datanodeInfo, "datanodeInfo == null");
    List<StorageReportProto> storageReports = datanodeInfo.getStorageReports();
    if (storageReports.isEmpty()) {
      return false;
    }
    PendingContainerAllocations pendingAllocations =
        datanodeInfo.getPendingContainerAllocations();
    pendingAllocations.rollIfNeeded();
    final int pendingCount = pendingAllocations.getCount(storageType);
    long allocatableCount = 0;
    for (StorageReportProto report : storageReports) {
      if (report.hasFailed() && report.getFailed()) {
        continue;
      }
      if (!matchesStorageType(report, storageType)) {
        continue;
      }
      allocatableCount += Math.max(0L, VolumeUsage.getUsableSpace(report)) / maxContainerSize;
      if (allocatableCount > pendingCount) {
        return true;
      }
    }
    LOG.debug("Datanode {} has no available container slots. Pending: {}, Allocatable: {}",
        datanodeInfo.getID(), pendingCount, allocatableCount);
    return false;
  }

  /**
   * Remove pending allocation for the given container.
   *
   * @param pendingAllocations pending allocations of the datanode
   * @param containerID containerID
   */
  public void removePendingAllocation(
      PendingContainerAllocations pendingAllocations, ContainerID containerID) {
    Objects.requireNonNull(containerID, "containerID == null");

    boolean removed = pendingAllocations.remove(containerID);

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

  @VisibleForTesting
  public SCMNodeMetrics getMetrics() {
    return metrics;
  }
}
