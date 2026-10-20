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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerStorageReportProto;
import org.apache.hadoop.util.Time;

/**
 * This class extends the primary identifier of a Jobworker with ephemeral
 * state, e.g., last reported time, usage information, etc.
 */
public class JobworkerInfo extends JobworkerDetails {

  private final ReadWriteLock lock;
  private JobworkerNodeStatus nodeStatus;
  private volatile long lastHeartbeatTime;
  private int failedVolumeCount;
  private List<JobworkerStorageReportProto> storageReports;

  /**
   * Copy constructor for JobworkerDetails.
   *
   * @param jobworkerDetails JobworkerDetails to copy
   */
  public JobworkerInfo(JobworkerDetails jobworkerDetails, JobworkerNodeStatus nodeStatus) {
    super(jobworkerDetails);
    this.lock = new ReentrantReadWriteLock();
    this.nodeStatus = nodeStatus;
    this.storageReports = Collections.emptyList();
    this.lastHeartbeatTime = Time.monotonicNow();
  }

  /**
   * Return the current NodeStatus for the jobworker.
   *
   * @return NodeStatus - the current nodeStatus
   */
  public JobworkerNodeStatus getNodeStatus() {
    try {
      lock.readLock().lock();
      return nodeStatus;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Update the NodeStatus for this jobworker.
   *
   * @param newNodeStatus - the new NodeStatus object
   */
  public void setNodeStatus(JobworkerNodeStatus newNodeStatus) {
    try {
      lock.writeLock().lock();
      this.nodeStatus = newNodeStatus;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Updates the last heartbeat time with current time.
   */
  public void updateLastHeartbeatTime() {
    updateLastHeartbeatTime(Time.monotonicNow());
  }

  /**
   * Sets the last heartbeat time to a given value.
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

  @VisibleForTesting
  public long getLastHeartbeatTime() {
    return lastHeartbeatTime;
  }

  /**
   * Updates the datanode storage reports.
   *
   * @param reports list of storage report
   */
  public void updateStorageReports(List<JobworkerStorageReportProto> reports) {
    final int failedCount = (int) reports.stream()
        .filter(e -> e.hasFailed() && e.getFailed())
        .count();

    try {
      lock.writeLock().lock();
      updateLastHeartbeatTime();
      failedVolumeCount = failedCount;
      storageReports = reports;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns the storage reports associated with this jobworker.
   *
   * @return list of storage report
   */
  public List<JobworkerStorageReportProto> getStorageReports() {
    try {
      lock.readLock().lock();
      return storageReports;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns count of healthy volumes reported from jobworker.
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
}
