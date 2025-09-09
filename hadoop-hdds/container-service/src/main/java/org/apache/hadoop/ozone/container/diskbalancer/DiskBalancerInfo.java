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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.storage.DiskBalancerConfiguration;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerService.DiskBalancerOperationalState;

/**
 * DiskBalancer's information to persist.
 */
public class DiskBalancerInfo {
  private DiskBalancerOperationalState operationalState;
  private double threshold;
  private long bandwidthInMB;
  private int parallelThread;
  private boolean stopAfterDiskEven;
  private DiskBalancerVersion version;
  private long successCount;
  private long failureCount;
  private long bytesToMove;
  private long balancedBytes;

  public DiskBalancerInfo(DiskBalancerOperationalState operationalState, double threshold,
      long bandwidthInMB, int parallelThread, boolean stopAfterDiskEven) {
    this(operationalState, threshold, bandwidthInMB, parallelThread, stopAfterDiskEven,
        DiskBalancerVersion.DEFAULT_VERSION);
  }

  public DiskBalancerInfo(DiskBalancerOperationalState operationalState, double threshold,
      long bandwidthInMB, int parallelThread, boolean stopAfterDiskEven, DiskBalancerVersion version) {
    this.operationalState = operationalState;
    this.threshold = threshold;
    this.bandwidthInMB = bandwidthInMB;
    this.parallelThread = parallelThread;
    this.stopAfterDiskEven = stopAfterDiskEven;
    this.version = version;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public DiskBalancerInfo(DiskBalancerOperationalState operationalState, double threshold,
      long bandwidthInMB, int parallelThread, boolean stopAfterDiskEven, DiskBalancerVersion version,
      long successCount, long failureCount, long bytesToMove, long balancedBytes) {
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
  }

  public DiskBalancerInfo(boolean shouldRun,
      DiskBalancerConfiguration diskBalancerConf) {
    if (shouldRun) {
      this.operationalState = DiskBalancerOperationalState.RUNNING;
    } else {
      this.operationalState = DiskBalancerOperationalState.STOPPED;
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

  public StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto toDiskBalancerReportProto() {
    DiskBalancerConfiguration conf = new DiskBalancerConfiguration(threshold,
        bandwidthInMB, parallelThread, stopAfterDiskEven);
    HddsProtos.DiskBalancerConfigurationProto confProto = conf.toProtobufBuilder().build();

    StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto.Builder builder =
        StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto.newBuilder();
    builder.setIsRunning(this.operationalState == DiskBalancerOperationalState.RUNNING);
    builder.setDiskBalancerConf(confProto);
    builder.setSuccessMoveCount(successCount);
    builder.setFailureMoveCount(failureCount);
    builder.setBytesToMove(bytesToMove);
    builder.setBalancedBytes(balancedBytes);
    return builder.build();
  }

  public DiskBalancerOperationalState getOperationalState() {
    return operationalState;
  }

  public void setOperationalState(DiskBalancerOperationalState operationalState) {
    this.operationalState = operationalState;
  }

  public boolean isShouldRun() {
    return this.operationalState == DiskBalancerOperationalState.RUNNING;
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
    return this.operationalState == DiskBalancerOperationalState.PAUSED_BY_NODE_STATE;
  }

  public DiskBalancerVersion getVersion() {
    return version;
  }

  public void setVersion(DiskBalancerVersion version) {
    this.version = version;
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
