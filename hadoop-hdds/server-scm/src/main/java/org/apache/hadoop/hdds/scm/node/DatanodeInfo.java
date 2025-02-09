/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.toLayoutVersionProto;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class extends the primary identifier of a Datanode with ephemeral
 * state, eg last reported time, usage information etc.
 */
public class DatanodeInfo extends DatanodeDetails {

  private static final Logger LOG = LoggerFactory.getLogger(DatanodeInfo.class);

  private final ReadWriteLock lock;

  private volatile long lastHeartbeatTime;
  private long lastStatsUpdatedTime;
  private int failedVolumeCount;

  private List<StorageReportProto> storageReports;
  private List<MetadataStorageReportProto> metadataStorageReports;
  private LayoutVersionProto lastKnownLayoutVersion;
  private final Map<SCMCommandProto.Type, Integer> commandCounts;

  private NodeStatus nodeStatus;
  private Map<String, String> containersReplicationMetrics;

  /**
   * Constructs DatanodeInfo from DatanodeDetails.
   *
   * @param datanodeDetails Details about the datanode
   * @param layoutInfo Details about the LayoutVersionProto
   */
  public DatanodeInfo(DatanodeDetails datanodeDetails, NodeStatus nodeStatus,
        LayoutVersionProto layoutInfo) {
    super(datanodeDetails);
    this.lock = new ReentrantReadWriteLock();
    this.lastHeartbeatTime = Time.monotonicNow();
    lastKnownLayoutVersion = toLayoutVersionProto(
        layoutInfo != null ? layoutInfo.getMetadataLayoutVersion() : 0,
        layoutInfo != null ? layoutInfo.getSoftwareLayoutVersion() : 0);
    this.storageReports = Collections.emptyList();
    this.nodeStatus = nodeStatus;
    this.metadataStorageReports = Collections.emptyList();
    this.commandCounts = new HashMap<>();
  }

  /**
   * Updates the last heartbeat time with current time.
   */
  public void updateLastHeartbeatTime() {
    updateLastHeartbeatTime(Time.monotonicNow());
  }

  /**
   * Sets the last heartbeat time to a given value. Intended to be used
   * only for tests.
   *
   * @param milliSecondsSinceEpoch - ms since Epoch to set as the heartbeat time
   */
  @VisibleForTesting
  public void updateLastHeartbeatTime(long milliSecondsSinceEpoch) {
    try {
      lock.writeLock().lock();
      lastHeartbeatTime = milliSecondsSinceEpoch;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Updates the last LayoutVersion.
   */
  public void updateLastKnownLayoutVersion(LayoutVersionProto version) {
    if (version == null) {
      return;
    }
    try {
      lock.writeLock().lock();
      lastKnownLayoutVersion = toLayoutVersionProto(
          version.getMetadataLayoutVersion(),
          version.getSoftwareLayoutVersion());
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns the last heartbeat time.
   *
   * @return last heartbeat time.
   */
  public long getLastHeartbeatTime() {
    try {
      lock.readLock().lock();
      return lastHeartbeatTime;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the last known Layout Version .
   *
   * @return last  Layout Version.
   */
  public LayoutVersionProto getLastKnownLayoutVersion() {
    try {
      lock.readLock().lock();
      return lastKnownLayoutVersion;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Updates the datanode storage reports.
   *
   * @param reports list of storage report
   */
  public void updateStorageReports(List<StorageReportProto> reports) {
    final int failedCount = (int) reports.stream()
        .filter(e -> e.hasFailed() && e.getFailed())
        .count();

    try {
      lock.writeLock().lock();
      lastStatsUpdatedTime = Time.monotonicNow();
      failedVolumeCount = failedCount;
      storageReports = reports;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Updates the datanode metadata storage reports.
   *
   * @param reports list of metadata storage report
   */
  public void updateMetaDataStorageReports(
      List<MetadataStorageReportProto> reports) {
    try {
      lock.writeLock().lock();
      lastStatsUpdatedTime = Time.monotonicNow();
      metadataStorageReports = reports;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns the storage reports associated with this datanode.
   *
   * @return list of storage report
   */
  public List<StorageReportProto> getStorageReports() {
    try {
      lock.readLock().lock();
      return storageReports;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the storage reports associated with this datanode.
   *
   * @return list of storage report
   */
  public List<MetadataStorageReportProto> getMetadataStorageReports() {
    try {
      lock.readLock().lock();
      return metadataStorageReports;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns count of healthy volumes reported from datanode.
   * @return count of healthy volumes
   */
  public int getHealthyVolumeCount() {
    try {
      lock.readLock().lock();
      return storageReports.size() - failedVolumeCount;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns count of failed volumes reported by the data node.
   * @return count of failed volumes
   */
  public int getFailedVolumeCount() {
    try {
      lock.readLock().lock();
      return failedVolumeCount;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns count of healthy metadata volumes reported from datanode.
   * @return count of healthy metdata log volumes
   */
  public int getMetaDataVolumeCount() {
    try {
      lock.readLock().lock();
      return metadataStorageReports.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the last updated time of datanode info.
   * @return the last updated time of datanode info.
   */
  public long getLastStatsUpdatedTime() {
    return lastStatsUpdatedTime;
  }

  /**
   * Return the current NodeStatus for the datanode.
   *
   * @return NodeStatus - the current nodeStatus
   */
  public NodeStatus getNodeStatus() {
    try {
      lock.readLock().lock();
      return nodeStatus;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Update the NodeStatus for this datanode. When using this method
   * be ware of the potential for lost updates if two threads read the
   * current status, update one field and then write it back without
   * locking enforced outside of this class.
   *
   * @param newNodeStatus - the new NodeStatus object
   */
  public void setNodeStatus(NodeStatus newNodeStatus) {
    try {
      lock.writeLock().lock();
      this.nodeStatus = newNodeStatus;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Set the current command counts for this datanode, as reported in the last
   * heartbeat.
   * @param cmds Proto message containing a list of command count pairs.
   * @param commandsToBeSent Summary of commands which will be sent to the DN
   *                         as the heartbeat is processed and should be added
   *                         to the command count.
   */
  public void setCommandCounts(CommandQueueReportProto cmds,
      Map<SCMCommandProto.Type, Integer> commandsToBeSent) {
    try {
      int count = cmds.getCommandCount();
      Map<SCMCommandProto.Type, Integer> mutableCmds
          = new HashMap<>(commandsToBeSent);
      lock.writeLock().lock();
      // Purge the existing counts, as each report should completely replace
      // the existing counts.
      commandCounts.clear();
      for (int i = 0; i < count; i++) {
        SCMCommandProto.Type command = cmds.getCommand(i);
        if (command == SCMCommandProto.Type.unknownScmCommand) {
          LOG.warn("Unknown SCM Command received from {} in the "
              + "heartbeat. SCM and the DN may not be at the same version.",
              this);
          continue;
        }
        int cmdCount = cmds.getCount(i);
        if (cmdCount < 0) {
          LOG.warn("Command count of {} from {} should be greater than zero. " +
              "Setting it to zero", cmdCount, this);
          cmdCount = 0;
        }
        cmdCount += mutableCmds.getOrDefault(command, 0);
        // Each CommandType will be in the report once only. So we remove any
        // we have seen, so we can add anything the DN has not reported but
        // there is a command queued for. The DNs should return a count for all
        // command types even if they have a zero count, so this is really to
        // handle something being wrong on the DN where it sends a spare report.
        // It really should never happen.
        mutableCmds.remove(command);
        commandCounts.put(command, cmdCount);
      }
      // Add any counts which the DN did not report. See comment above. This
      // should not happen.
      commandCounts.putAll(mutableCmds);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Retrieve the number of queued commands of the given type, as reported by
   * the datanode at the last heartbeat.
   * @param cmd The command for which to receive the queued command count
   * @return -1 if we have no information about the count, or an integer &gt;= 0
   *         indicating the command count at the last heartbeat.
   */
  public int getCommandCount(SCMCommandProto.Type cmd) {
    try {
      lock.readLock().lock();
      return commandCounts.getOrDefault(cmd, -1);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setContainersReplicationMetrics(
      Map<String, String> containersReplicationMetrics) {
    lock.writeLock().lock();
    try {
      this.containersReplicationMetrics =
          containersReplicationMetrics;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Map<String, String> getContainersReplicationMetrics() {
    lock.readLock().lock();
    try {
      return containersReplicationMetrics;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }
}
