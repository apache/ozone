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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.VolumeReportProto;

/**
 * DiskBalancer's information to persist and for report.
 * Report-only fields (idealUsage, volumeInfo) are NOT persisted to YAML.
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
  private String containerStates;
  // Report-only: ideal usage from volume snapshot. NOT persisted.
  private double idealUsage;
  // Report-only: per-volume info. NOT persisted.
  private List<VolumeReportProto> volumeInfo;

  public DiskBalancerInfo(DiskBalancerRunningStatus operationalState, double threshold,
      long bandwidthInMB, int parallelThread, boolean stopAfterDiskEven) {
    this(operationalState, threshold, bandwidthInMB, parallelThread, stopAfterDiskEven,
        DiskBalancerConfiguration.DEFAULT_CONTAINER_STATES, DiskBalancerVersion.DEFAULT_VERSION);
  }

  public DiskBalancerInfo(DiskBalancerRunningStatus operationalState, double threshold,
      long bandwidthInMB, int parallelThread, boolean stopAfterDiskEven,
      String containerStates, DiskBalancerVersion version) {
    this.operationalState = operationalState;
    this.threshold = threshold;
    this.bandwidthInMB = bandwidthInMB;
    this.parallelThread = parallelThread;
    this.stopAfterDiskEven = stopAfterDiskEven;
    this.containerStates = containerStates;
    this.version = version;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public DiskBalancerInfo(DiskBalancerRunningStatus operationalState, double threshold,
      long bandwidthInMB, int parallelThread, boolean stopAfterDiskEven, DiskBalancerVersion version,
      String containerStates, long successCount, long failureCount, long bytesToMove,
      long balancedBytes, double volumeDataDensity) {
    this.operationalState = operationalState;
    this.threshold = threshold;
    this.bandwidthInMB = bandwidthInMB;
    this.parallelThread = parallelThread;
    this.stopAfterDiskEven = stopAfterDiskEven;
    this.version = version;
    this.containerStates = containerStates;
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
    this.containerStates = diskBalancerConf.getContainerStates();
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
    if (!Objects.equals(containerStates, diskBalancerConf.getContainerStates())) {
      setContainerStates(diskBalancerConf.getContainerStates());
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
    config.setContainerStates(this.containerStates);
    return config;
  }

  public String getContainerStates() {
    return containerStates;
  }

  public void setContainerStates(String containerStates) {
    this.containerStates = containerStates;
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

  public double getIdealUsage() {
    return idealUsage;
  }

  public void setIdealUsage(double idealUsage) {
    this.idealUsage = idealUsage;
  }

  public List<VolumeReportProto> getVolumeInfo() {
    return volumeInfo != null ? volumeInfo : Collections.emptyList();
  }

  public void setVolumeInfo(List<VolumeReportProto> volumeInfo) {
    this.volumeInfo = volumeInfo;
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
        version == that.version &&
        Objects.equals(containerStates, that.containerStates);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operationalState, threshold, bandwidthInMB, parallelThread, stopAfterDiskEven,
        version, containerStates);
  }
}
