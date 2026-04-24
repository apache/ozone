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
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo.TwoWindowBucket;
import org.apache.hadoop.ozone.container.common.volume.VolumeUsage;
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

  public PendingContainerTracker(long maxContainerSize, SCMNodeMetrics metrics) {
    this.maxContainerSize = maxContainerSize;
    this.metrics = metrics;
    LOG.info("PendingContainerTracker initialized with maxContainerSize={}B", maxContainerSize);
  }

  private TwoWindowBucket getBucket(DatanodeInfo datanodeInfo) {
    return datanodeInfo.getTwoWindowBucket();
  }

  /**
   * Advances the two-window tumbling bucket for this datanode when the roll interval has elapsed.
   * Call on periodic paths (node report) so windows age even when there are no new
   * allocations or container reports touching this tracker.
   */
  public void rollWindowsIfNeeded(DatanodeInfo datanodeInfo) {
    Objects.requireNonNull(datanodeInfo, "datanodeInfo == null");
    getBucket(datanodeInfo).rollIfNeeded();
  }

  /**
   * Whether the datanode can fit another container of {@link #maxContainerSize} after accounting for
   * SCM pending allocations for {@code datanodeInfo} (this tracker) and usable space across volumes.
   * Pending bytes are {@link #getPendingContainerCount} × {@code maxContainerSize};
   * effective allocatable space sums full-container slots per storage report.
   *
   * @param datanodeInfo storage reports and pending-allocation bucket for the datanode
   */
  public boolean hasEffectiveAllocatableSpaceForNewContainer(DatanodeInfo datanodeInfo) {
    Objects.requireNonNull(datanodeInfo, "datanodeInfo == null");

    long pendingAllocationSize = getPendingContainerCount(datanodeInfo) * maxContainerSize;
    List<StorageReportProto> storageReports = datanodeInfo.getStorageReports();
    Objects.requireNonNull(storageReports, "storageReports == null");
    if (storageReports.isEmpty()) {
      return false;
    }
    long effectiveAllocatableSpace = 0L;
    for (StorageReportProto report : storageReports) {
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
   * @param datanodeInfo The DataNode where container is being allocated/replicated
   * @param containerID The container being allocated/replicated
   */
  public void recordPendingAllocationForDatanode(DatanodeInfo datanodeInfo, ContainerID containerID) {
    Objects.requireNonNull(datanodeInfo, "datanodeInfo == null");
    Objects.requireNonNull(containerID, "containerID == null");

    boolean added = getBucket(datanodeInfo).add(containerID);

    if (added && metrics != null) {
      metrics.incNumPendingContainersAdded();
    }
  }

  /**
   * Remove a pending container allocation from a specific DataNode.
   * Removes from both current and previous windows.
   * Called when container is confirmed.
   *
   * @param datanodeInfo The DataNode
   * @param containerID The container to remove from pending
   */
  public void removePendingAllocation(DatanodeInfo datanodeInfo, ContainerID containerID) {
    Objects.requireNonNull(datanodeInfo, "datanodeInfo == null");
    Objects.requireNonNull(containerID, "containerID == null");

    boolean removed = getBucket(datanodeInfo).remove(containerID);

    if (removed && metrics != null) {
      metrics.incNumPendingContainersRemoved();
    }
  }

  /**
   * Number of pending container allocations for this datanode (union of current and previous
   * windows). This call may advance the internal tumbling window if the roll interval has elapsed.
   *
   * @param datanodeInfo The DataNode
   * @return Pending container count
   */
  public long getPendingContainerCount(DatanodeInfo datanodeInfo) {
    Objects.requireNonNull(datanodeInfo, "datanodeInfo == null");
    return getBucket(datanodeInfo).getCount();
  }

  /**
   * Whether container is in the current or previous window for this datanode.
   */
  @VisibleForTesting
  public boolean containsPendingContainer(DatanodeInfo datanodeInfo, ContainerID containerID) {
    Objects.requireNonNull(datanodeInfo, "datanodeInfo == null");
    Objects.requireNonNull(containerID, "containerID == null");
    return getBucket(datanodeInfo).contains(containerID);
  }

  @VisibleForTesting
  public SCMNodeMetrics getMetrics() {
    return metrics;
  }
}
