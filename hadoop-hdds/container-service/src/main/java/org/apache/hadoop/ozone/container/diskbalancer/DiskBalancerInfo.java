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

package org.apache.hadoop.ozone.container.diskbalancer;

import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;

/**
 * DiskBalancer's information to persist.
 */
public class DiskBalancerInfo {
  private DiskBalancerRunningStatus operationalState;
  private double threshold;
  private long bandwidthInMB;
  private int parallelThread;
  private boolean stopAfterDiskEven;
  private DiskBalancerVersion version;
  private long successCount;
  private long failureCount;
  private long bytesToMove;
  private long balancedBytes;
  private double volumeDataDensity;

  public DiskBalancerInfo(DiskBalancerRunningStatus operationalState, double threshold,
      long bandwidthInMB, int parallelThread, boolean stopAfterDiskEven) {
    this(operationalState, threshold, bandwidthInMB, parallelThread, stopAfterDiskEven,
        DiskBalancerVersion.DEFAULT_VERSION);
  }

  public DiskBalancerInfo(DiskBalancerRunningStatus operationalState, double threshold,
      long bandwidthInMB, int parallelThread, boolean stopAfterDiskEven, DiskBalancerVersion version) {
    this.operationalState = operationalState;
    this.threshold = threshold;
    this.bandwidthInMB = bandwidthInMB;
    this.parallelThread = parallelThread;
    this.stopAfterDiskEven = stopAfterDiskEven;
    this.version = version;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public DiskBalancerInfo(DiskBalancerRunningStatus operationalState, double threshold,
      long bandwidthInMB, int parallelThread, boolean stopAfterDiskEven, DiskBalancerVersion version,
      long successCount, long failureCount, long bytesToMove, long balancedBytes, double volumeDataDensity) {
    this.operationalState = operationalState;
    this.threshold = threshold;
    this.bandwidthInMB = bandwidthInMB;
    this.parallelThread = parallelThread;
    this.stopAfterDiskEven = stopAfterDiskEven;
    this.version = version;
    this.successCount = successCount;
    this.failureCount = failureCount;
    this.bytesToMove = bytesToMove;
    this.balancedBytes = balancedBytes;
    this.volumeDataDensity = volumeDataDensity;
  }

  public DiskBalancerInfo(boolean shouldRun,
      DiskBalancerConfiguration diskBalancerConf) {
    if (shouldRun) {
      this.operationalState = DiskBalancerRunningStatus.RUNNING;
    } else {
      this.operationalState = DiskBalancerRunningStatus.STOPPED;
    }
    this.threshold = diskBalancerConf.getThreshold();
    this.bandwidthInMB = diskBalancerConf.getDiskBandwidthInMB();
    this.parallelThread = diskBalancerConf.getParallelThread();
    this.stopAfterDiskEven = diskBalancerConf.isStopAfterDiskEven();
    this.version = DiskBalancerVersion.DEFAULT_VERSION;
  }

  public void updateFromConf(DiskBalancerConfiguration diskBalancerConf) {
    if (threshold != diskBalancerConf.getThreshold()) {
      setThreshold(diskBalancerConf.getThreshold());
    }
    if (bandwidthInMB != diskBalancerConf.getDiskBandwidthInMB()) {
      setBandwidthInMB(diskBalancerConf.getDiskBandwidthInMB());
    }
    if (parallelThread != diskBalancerConf.getParallelThread()) {
      setParallelThread(diskBalancerConf.getParallelThread());
    }
    if (stopAfterDiskEven != diskBalancerConf.isStopAfterDiskEven()) {
      setStopAfterDiskEven(diskBalancerConf.isStopAfterDiskEven());
    }
  }

  /**
   * Gives DiskBalancerConfiguration object of already persisting value.
   *
   * @return a DiskBalancerConfiguration with persisting values
   */
  public DiskBalancerConfiguration toConfiguration() {
    DiskBalancerConfiguration config = new DiskBalancerConfiguration();
    config.setThreshold(this.threshold);
    config.setDiskBandwidthInMB(this.bandwidthInMB);
    config.setParallelThread(this.parallelThread);
    config.setStopAfterDiskEven(this.stopAfterDiskEven);
    return config;
  }

  public DiskBalancerRunningStatus getOperationalState() {
    return operationalState;
  }

  public void setOperationalState(DiskBalancerRunningStatus operationalState) {
    this.operationalState = operationalState;
  }

  public boolean isShouldRun() {
    return this.operationalState == DiskBalancerRunningStatus.RUNNING;
  }

  public double getThreshold() {
    return threshold;
  }

  public void setThreshold(double threshold) {
    this.threshold = threshold;
  }

  public long getBandwidthInMB() {
    return bandwidthInMB;
  }

  public void setBandwidthInMB(long bandwidthInMB) {
    this.bandwidthInMB = bandwidthInMB;
  }

  public int getParallelThread() {
    return parallelThread;
  }

  public void setParallelThread(int parallelThread) {
    this.parallelThread = parallelThread;
  }

  public boolean isStopAfterDiskEven() {
    return stopAfterDiskEven;
  }

  public void setStopAfterDiskEven(boolean stopAfterDiskEven) {
    this.stopAfterDiskEven = stopAfterDiskEven;
  }

  public boolean isPaused() {
    return this.operationalState == DiskBalancerRunningStatus.PAUSED;
  }

  public DiskBalancerVersion getVersion() {
    return version;
  }

  public void setVersion(DiskBalancerVersion version) {
    this.version = version;
  }

  public long getSuccessCount() {
    return successCount;
  }

  public void setSuccessCount(long successCount) {
    this.successCount = successCount;
  }

  public long getFailureCount() {
    return failureCount;
  }

  public void setFailureCount(long failureCount) {
    this.failureCount = failureCount;
  }

  public long getBytesToMove() {
    return bytesToMove;
  }

  public void setBytesToMove(long bytesToMove) {
    this.bytesToMove = bytesToMove;
  }

  public long getBalancedBytes() {
    return balancedBytes;
  }

  public void setBalancedBytes(long balancedBytes) {
    this.balancedBytes = balancedBytes;
  }

  public double getVolumeDataDensity() {
    return volumeDataDensity;
  }

  public void setVolumeDataDensity(double volumeDataDensity) {
    this.volumeDataDensity = volumeDataDensity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DiskBalancerInfo that = (DiskBalancerInfo) o;
    return operationalState == that.operationalState &&
        Double.compare(that.threshold, threshold) == 0 &&
        bandwidthInMB == that.bandwidthInMB &&
        parallelThread == that.parallelThread &&
        stopAfterDiskEven == that.stopAfterDiskEven &&
        version == that.version;
  }

  @Override
  public int hashCode() {
    return Objects.hash(operationalState, threshold, bandwidthInMB, parallelThread, stopAfterDiskEven,
        version);
  }
}
