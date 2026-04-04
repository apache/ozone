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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
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
 * <p>Thread-safety: all mutations and reads for a given datanode run under
 * {@code synchronized (bucket)} on that datanode's {@link TwoWindowBucket}, including
 * map insert/remove, so concurrent record/remove/report paths cannot interleave or orphan
 * a bucket after it was dropped from the map.</p>
 */
public class PendingContainerTracker {

  private static final Logger LOG = LoggerFactory.getLogger(PendingContainerTracker.class);

  /**
   * Roll interval in milliseconds.
   * Configurable via hdds.scm.container.pending.allocation.roll.interval.
   * Default: 5 minutes.
   * Containers automatically age out after 2 × rollIntervalMs.
   */
  private final long rollIntervalMs;

  /**
   * Map of DataNode UUID to TwoWindowBucket.
   */
  private final ConcurrentHashMap<UUID, TwoWindowBucket> datanodeBuckets;

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
  private static class TwoWindowBucket {
    private Set<ContainerID> currentWindow = new HashSet<>();
    private Set<ContainerID> previousWindow = new HashSet<>();
    private long lastRollTime = Time.monotonicNow();
    private final long rollIntervalMs;

    TwoWindowBucket(long rollIntervalMs) {
      this.rollIntervalMs = rollIntervalMs;
    }

    /**
     * Roll the windows: previous = current, current = empty.
     * Called when current time exceeds lastRollTime + rollIntervalMs.
     */
    synchronized void rollIfNeeded() {
      long now = Time.monotonicNow();
      if (now - lastRollTime >= rollIntervalMs) {
        // Shift: current becomes previous
        previousWindow = currentWindow;
        // Reset: new empty current window
        currentWindow = new HashSet<>();
        lastRollTime = now;
        LOG.debug("Rolled window. Previous window size: {}, Current window reset to empty", previousWindow.size());
      }
    }

    /**
     * Get union of both windows (all pending containers).
     */
    synchronized Set<ContainerID> getAllPending() {
      Set<ContainerID> all = new HashSet<>();
      all.addAll(currentWindow);
      all.addAll(previousWindow);
      return all;
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
      return removedFromCurrent || removedFromPrevious;
    }

    /**
     * Check if either window is non-empty.
     */
    synchronized boolean isEmpty() {
      return currentWindow.isEmpty() && previousWindow.isEmpty();
    }

    /**
     * Count of pending containers in both windows.
     */
    synchronized int getCount() {
      return currentWindow.size() + previousWindow.size();
    }
  }

  public PendingContainerTracker(long maxContainerSize) {
    this(maxContainerSize, 10 * 60 * 1000, null); // Default 10 minutes
  }

  public PendingContainerTracker(long maxContainerSize, long rollIntervalMs,
      SCMNodeMetrics metrics) {
    this.datanodeBuckets = new ConcurrentHashMap<>();
    this.maxContainerSize = maxContainerSize;
    this.rollIntervalMs = rollIntervalMs;
    this.metrics = metrics;
    LOG.info("PendingContainerTracker initialized with maxContainerSize={}B, rollInterval={}ms",
        maxContainerSize, rollIntervalMs);
  }

  /**
   * Whether the datanode can fit another container of {@code containerSize} after accounting for
   * SCM pending allocations for {@code node} (this tracker) and usable space on {@code datanodeInfo}.
   * Combines {@link #getPendingAllocationSize} with the per-disk slot check in one call.
   *
   * @param node identity used to look up pending allocations (same DN as {@code datanodeInfo})
   * @param datanodeInfo storage reports for the datanode
   * @param containerSize required container size in bytes (typically SCM max container size)
   */
  /**
   * Advances the two-window tumbling bucket for this datanode when the roll interval has elapsed.
   * Call on periodic paths (e.g. node report) so windows age even when there are no new
   * allocations or container reports touching this tracker.
   */
  public void rollWindowsIfNeeded(DatanodeDetails node) {
    if (node == null) {
      return;
    }
    UUID uuid = node.getUuid();
    TwoWindowBucket bucket = datanodeBuckets.get(uuid);
    if (bucket == null) {
      return;
    }
    synchronized (bucket) {
      bucket.rollIfNeeded();
      if (bucket.isEmpty()) {
        datanodeBuckets.remove(uuid, bucket);
      }
    }
  }

  public boolean hasEffectiveAllocatableSpaceForNewContainer(
      DatanodeDetails node, DatanodeInfo datanodeInfo, long containerSize) {
    if (node == null || datanodeInfo == null || containerSize <= 0) {
      return false;
    }
    long pendingBytes = getPendingAllocationSize(node);
    return hasAllocatableSpaceAfterPending(datanodeInfo, containerSize, pendingBytes);
  }

  private static boolean hasAllocatableSpaceAfterPending(
      DatanodeInfo datanodeInfo, long containerSize, long pendingAllocationBytes) {
    List<StorageReportProto> storageReports = datanodeInfo.getStorageReports();
    if (storageReports == null || storageReports.isEmpty()) {
      return false;
    }
    long effectiveAllocatableSpace = 0L;
    for (StorageReportProto report : storageReports) {
      long usableSpace = VolumeUsage.getUsableSpace(report);
      long containersOnThisDisk = usableSpace / containerSize;
      effectiveAllocatableSpace += containersOnThisDisk * containerSize;
      if (effectiveAllocatableSpace - pendingAllocationBytes >= containerSize) {
        return true;
      }
    }
    return false;
  }

  /**
   * Drops all pending allocation state for a datanode (e.g. stale/dead cleanup).
   */
  public void clearPendingForDatanode(DatanodeDetails node) {
    if (node == null) {
      return;
    }
    UUID uuid = node.getUuid();
    TwoWindowBucket bucket = datanodeBuckets.get(uuid);
    if (bucket == null) {
      return;
    }
    synchronized (bucket) {
      datanodeBuckets.remove(uuid, bucket);
    }
    LOG.debug("Cleared pending container allocations for datanode {}",
        node.getUuidString());
  }

  /**
   * Record a pending container allocation for all DataNodes in the pipeline.
   * Container is added to the current window.
   *
   * @param pipeline The pipeline where container is allocated
   * @param containerID The container being allocated
   */
  public void recordPendingAllocation(Pipeline pipeline, ContainerID containerID) {
    if (pipeline == null || containerID == null) {
      LOG.warn("Ignoring null pipeline or containerID");
      return;
    }

    for (DatanodeDetails node : pipeline.getNodes()) {
      recordPendingAllocationForDatanode(node, containerID);
    }
  }

  /**
   * Record a pending container allocation for a single DataNode.
   * Container is added to the current window.
   *
   * @param node The DataNode where container is being allocated/replicated
   * @param containerID The container being allocated/replicated
   */
  public void recordPendingAllocationForDatanode(DatanodeDetails node, ContainerID containerID) {
    if (node == null || containerID == null) {
      LOG.warn("Ignoring null node or containerID");
      return;
    }

    UUID uuid = node.getUuid();
    TwoWindowBucket bucket = datanodeBuckets.computeIfAbsent(
        uuid,
        k -> new TwoWindowBucket(rollIntervalMs)
    );

    synchronized (bucket) {
      bucket.rollIfNeeded();
      boolean added = bucket.add(containerID);
      if (!bucket.isEmpty()) {
        datanodeBuckets.put(uuid, bucket);
      }
      LOG.debug("Recorded pending container {} on DataNode {}. Added={}, Total pending={}",
          containerID, node.getUuidString(), added, bucket.getCount());

      if (added && metrics != null) {
        metrics.incNumPendingContainersAdded();
      }
    }
  }

  /**
   * Remove a pending container allocation from a specific DataNode.
   * Removes from both current and previous windows.
   * Called when container is confirmed.
   *
   * @param node The DataNode
   * @param containerID The container to remove from pending
   */
  public void removePendingAllocation(DatanodeDetails node, ContainerID containerID) {
    if (node == null || containerID == null) {
      return;
    }

    UUID uuid = node.getUuid();
    TwoWindowBucket bucket = datanodeBuckets.get(uuid);
    if (bucket == null) {
      return;
    }

    synchronized (bucket) {
      bucket.rollIfNeeded();

      boolean removed = bucket.remove(containerID);
      LOG.debug("Removed pending container {} from DataNode {}. Removed={}, Remaining={}",
          containerID, node.getUuidString(), removed, bucket.getCount());

      if (removed && metrics != null) {
        metrics.incNumPendingContainersRemoved();
      }

      if (bucket.isEmpty()) {
        LOG.debug("Cleanup pending bucket containerID {}", containerID);
        datanodeBuckets.remove(uuid, bucket);
      }
    }
  }

  /**
   * Bytes of SCM-side pending container allocations for this datanode (count × configured max
   * container size). For whether a new container can be placed, prefer
   * {@link #hasEffectiveAllocatableSpaceForNewContainer}.
   *
   * @param node The DataNode
   * @return Total bytes of pending container allocations
   */
  public long getPendingAllocationSize(DatanodeDetails node) {
    if (node == null) {
      return 0;
    }

    TwoWindowBucket bucket = datanodeBuckets.get(node.getUuid());
    if (bucket == null) {
      return 0;
    }

    synchronized (bucket) {
      bucket.rollIfNeeded();
      return (long) bucket.getCount() * maxContainerSize;
    }
  }

  /**
   * Get the set of pending container IDs for a DataNode.
   * Returns union of current and previous windows.
   * Useful for debugging and monitoring.
   *
   * @param node The DataNode
   * @return Set of pending container IDs
   */
  public Set<ContainerID> getPendingContainers(DatanodeDetails node) {
    if (node == null) {
      return Collections.emptySet();
    }

    TwoWindowBucket bucket = datanodeBuckets.get(node.getUuid());
    if (bucket == null) {
      return Collections.emptySet();
    }

    synchronized (bucket) {
      bucket.rollIfNeeded();
      return bucket.getAllPending();
    }
  }

  /**
   * Get total number of DataNodes with pending allocations.
   *
   * @return Count of DataNodes
   */
  public int getDataNodeCount() {
    return datanodeBuckets.size();
  }

  /**
   * Get total number of pending containers across all DataNodes.
   * Note: Same container on multiple DataNodes is counted once per DataNode.
   * The count may include containers from the previous window (up to 10 minutes old).
   *
   * @return Total pending container count
   */
  public long getTotalPendingCount() {
    return datanodeBuckets.values().stream()
        .mapToLong(TwoWindowBucket::getCount)
        .sum();
  }

  @VisibleForTesting
  public SCMNodeMetrics getMetrics() {
    return metrics;
  }
}
