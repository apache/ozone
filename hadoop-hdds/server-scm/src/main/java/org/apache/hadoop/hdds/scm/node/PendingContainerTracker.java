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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
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
 */
public class PendingContainerTracker {

  private static final Logger LOG = LoggerFactory.getLogger(PendingContainerTracker.class);

  private final DatanodeBuckets datanodeBuckets;

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
     * Roll one or both windows based on elapsed time.
     */
    synchronized void rollIfNeeded() {
      long now = Time.monotonicNow();
      long elapsed = now - lastRollTime;

      if (elapsed >= 2 * rollIntervalMs) {
        previousWindow.clear();
        currentWindow.clear();
        lastRollTime = now;
      } else if (elapsed >= rollIntervalMs) {
        previousWindow.clear();
        final Set<ContainerID> tmp = previousWindow;
        previousWindow = currentWindow;
        currentWindow = tmp;
        lastRollTime = now;
        LOG.debug("Rolled window. Previous window size: {}, Current window reset to empty", previousWindow.size());
      }
    }

    synchronized boolean contains(ContainerID containerID) {
      return currentWindow.contains(containerID) || previousWindow.contains(containerID);
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
     * Count of pending containers in both windows.
     */
    synchronized int getCount() {
      return currentWindow.size() + previousWindow.size();
    }
  }

  /**
   * Per-datanode two-window buckets.
   */
  private static class DatanodeBuckets {
    private final ConcurrentHashMap<DatanodeID, TwoWindowBucket> map = new ConcurrentHashMap<>();
    private final long rollIntervalMs;

    DatanodeBuckets(long rollIntervalMs) {
      this.rollIntervalMs = rollIntervalMs;
    }

    TwoWindowBucket get(DatanodeID id) {
      final TwoWindowBucket bucket = map.compute(id, (k, b) -> b != null ? b : new TwoWindowBucket(rollIntervalMs));
      bucket.rollIfNeeded();
      return bucket;
    }

    TwoWindowBucket get(DatanodeDetails dn) {
      if (dn == null) {
        return null;
      }
      return get(dn.getID());
    }
  }

  public PendingContainerTracker(long maxContainerSize) {
    this(maxContainerSize, 5 * 60 * 1000, null); // Default 5 minutes
  }

  public PendingContainerTracker(long maxContainerSize, long rollIntervalMs,
      SCMNodeMetrics metrics) {
    this.datanodeBuckets = new DatanodeBuckets(rollIntervalMs);
    this.maxContainerSize = maxContainerSize;
    this.metrics = metrics;
    LOG.info("PendingContainerTracker initialized with maxContainerSize={}B, rollInterval={}ms",
        maxContainerSize, rollIntervalMs);
  }

  /**
   * Advances the two-window tumbling bucket for this datanode when the roll interval has elapsed.
   * Call on periodic paths (node report) so windows age even when there are no new
   * allocations or container reports touching this tracker.
   */
  public void rollWindowsIfNeeded(DatanodeDetails node) {
    if (node == null) {
      return;
    }
    datanodeBuckets.get(node.getID());
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
  public boolean hasEffectiveAllocatableSpaceForNewContainer(
      DatanodeDetails node, DatanodeInfo datanodeInfo, long containerSize) {
    if (node == null || datanodeInfo == null || containerSize <= 0) {
      return false;
    }
    long pendingBytes = getPendingAllocationSize(node);
    return hasAllocatableSpaceAfterPending(datanodeInfo, containerSize, pendingBytes);
  }

  private boolean hasAllocatableSpaceAfterPending(
      DatanodeInfo datanodeInfo, long containerSize, long pendingAllocationBytes) {
    List<StorageReportProto> storageReports = datanodeInfo.getStorageReports();
    if (storageReports == null || storageReports.isEmpty()) {
      return false;
    }
    for (StorageReportProto report : storageReports) {
      long usableSpace = VolumeUsage.getUsableSpace(report);
      if (usableSpace - pendingAllocationBytes >= containerSize) {
        return true;
      }
    }
    if (metrics != null) {
      metrics.incNumSkippedFullNodeContainerAllocation();
    }
    return false;
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

    DatanodeID dnID = node.getID();
    boolean added = addContainerToBucket(containerID, dnID);

    if (added && metrics != null) {
      metrics.incNumPendingContainersAdded();
    }
  }

  private boolean addContainerToBucket(ContainerID containerID, DatanodeID dnID) {
    TwoWindowBucket bucket = datanodeBuckets.get(dnID);
    synchronized (bucket) {
      boolean added = bucket.add(containerID);
      LOG.debug("Recorded pending container {} on DataNode {}. Added={}, Total pending={}",
          containerID, dnID, added, bucket.getCount());
      return added;
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

    DatanodeID dnID = node.getID();
    boolean removed = removeContainerFromBucket(containerID, dnID);

    if (removed && metrics != null) {
      metrics.incNumPendingContainersRemoved();
    }
  }

  private boolean removeContainerFromBucket(ContainerID containerID, DatanodeID dnID) {
    TwoWindowBucket bucket = datanodeBuckets.get(dnID);
    synchronized (bucket) {
      boolean removed = bucket.remove(containerID);
      LOG.debug("Removed pending container {} from DataNode {}. Removed={}, Remaining={}",
          containerID, dnID, removed, bucket.getCount());
      return removed;
    }
  }

  /**
   * Bytes of SCM-side pending container allocations for this datanode (count × configured max
   * container size).
   * <p>Note: this call may advance the internal tumbling window if the roll interval has elapsed,
   * ensuring the returned value reflects the most up-to-date pending state.</p>
   *
   * @param node The DataNode
   * @return Total bytes of pending container allocations
   */
  public long getPendingAllocationSize(DatanodeDetails node) {
    if (node == null) {
      return 0;
    }
    return getPendingContainerCount(node) * maxContainerSize;
  }

  /**
   * Number of pending container allocations for this datanode (union of current and previous
   * windows). This call may advance the internal tumbling window if the roll interval has elapsed.
   *
   * @param node The DataNode
   * @return Pending container count
   */
  public long getPendingContainerCount(DatanodeDetails node) {
    if (node == null) {
      return 0;
    }
    TwoWindowBucket bucket = datanodeBuckets.get(node);
    synchronized (bucket) {
      return bucket.getCount();
    }
  }

  /**
   * Whether container is in the current or previous window for this datanode.
   */
  @VisibleForTesting
  public boolean containsPendingContainer(DatanodeDetails node, ContainerID containerID) {
    if (node == null || containerID == null) {
      return false;
    }
    TwoWindowBucket bucket = datanodeBuckets.get(node);
    synchronized (bucket) {
      return bucket.contains(containerID);
    }
  }

  @VisibleForTesting
  public SCMNodeMetrics getMetrics() {
    return metrics;
  }
}
