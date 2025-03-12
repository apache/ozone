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
import java.util.Optional;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.storage.DiskBalancerConfiguration;

/**
 * DiskBalancer's information to persist.
 */
public class DiskBalancerInfo {
  private boolean shouldRun;
  private double threshold;
  private long bandwidthInMB;
  private int parallelThread;
  private DiskBalancerVersion version;
  private long successCount;
  private long failureCount;
  private long totalDataPendingToMove;

  public DiskBalancerInfo(boolean shouldRun, double threshold,
      long bandwidthInMB, int parallelThread) {
    this(shouldRun, threshold, bandwidthInMB, parallelThread,
        DiskBalancerVersion.DEFAULT_VERSION);
  }

  public DiskBalancerInfo(boolean shouldRun, double threshold,
      long bandwidthInMB, int parallelThread, DiskBalancerVersion version) {
    this.shouldRun = shouldRun;
    this.threshold = threshold;
    this.bandwidthInMB = bandwidthInMB;
    this.parallelThread = parallelThread;
    this.version = version;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public DiskBalancerInfo(boolean shouldRun, double threshold,
      long bandwidthInMB, int parallelThread, DiskBalancerVersion version,
      long successCount, long failureCount, long totalDataPendingToMove) {
    this.shouldRun = shouldRun;
    this.threshold = threshold;
    this.bandwidthInMB = bandwidthInMB;
    this.parallelThread = parallelThread;
    this.version = version;
    this.successCount = successCount;
    this.failureCount = failureCount;
    this.totalDataPendingToMove = totalDataPendingToMove;
  }

  public DiskBalancerInfo(boolean shouldRun,
      DiskBalancerConfiguration diskBalancerConf) {
    this.shouldRun = shouldRun;
    this.threshold = diskBalancerConf.getThreshold();
    this.bandwidthInMB = diskBalancerConf.getDiskBandwidthInMB();
    this.parallelThread = diskBalancerConf.getParallelThread();
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
  }

  public StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto toDiskBalancerReportProto() {
    DiskBalancerConfiguration conf = new DiskBalancerConfiguration(Optional.of(threshold),
        Optional.of(bandwidthInMB), Optional.of(parallelThread));
    HddsProtos.DiskBalancerConfigurationProto confProto = conf.toProtobufBuilder().build();

    StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto.Builder builder =
        StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto.newBuilder();
    builder.setIsRunning(shouldRun);
    builder.setDiskBalancerConf(confProto);
    builder.setSuccessMoveCount(successCount);
    builder.setFailureMoveCount(failureCount);
    builder.setEstimatedTotalSizePendingToMove(totalDataPendingToMove);
    return builder.build();
  }

  public boolean isShouldRun() {
    return shouldRun;
  }

  public void setShouldRun(boolean shouldRun) {
    this.shouldRun = shouldRun;
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

  public DiskBalancerVersion getVersion() {
    return version;
  };

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
    return shouldRun == that.shouldRun &&
        Double.compare(that.threshold, threshold) == 0 &&
        bandwidthInMB == that.bandwidthInMB &&
        parallelThread == that.parallelThread &&
        version == that.version;
  }

  @Override
  public int hashCode() {
    return Objects.hash(shouldRun, threshold, bandwidthInMB, parallelThread,
        version);
  }
}
