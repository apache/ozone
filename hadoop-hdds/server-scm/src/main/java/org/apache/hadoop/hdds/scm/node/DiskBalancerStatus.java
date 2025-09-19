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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;
import org.apache.hadoop.hdds.scm.storage.DiskBalancerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains DiskBalancerConfiguration and isRunning.
 */
public class DiskBalancerStatus {
  public static final Logger LOG =
      LoggerFactory.getLogger(DiskBalancerStatus.class);

  private DiskBalancerRunningStatus isRunning;
  private DiskBalancerConfiguration diskBalancerConfiguration;
  private long successMoveCount;
  private long failureMoveCount;
  private long bytesToMove;
  private long balancedBytes;
  private double volumeDataDensity;

  public DiskBalancerStatus(DiskBalancerRunningStatus isRunning, DiskBalancerConfiguration conf,
      long successMoveCount, long failureMoveCount, long bytesToMove, long balancedBytes, 
      double volumeDataDensity) {
    this.isRunning = isRunning;
    this.diskBalancerConfiguration = conf;
    this.successMoveCount = successMoveCount;
    this.failureMoveCount = failureMoveCount;
    this.bytesToMove = bytesToMove;
    this.balancedBytes = balancedBytes;
    this.volumeDataDensity = volumeDataDensity;
  }

  public DiskBalancerRunningStatus getRunningStatus() {
    return isRunning;
  }

  public DiskBalancerConfiguration getDiskBalancerConfiguration() {
    return diskBalancerConfiguration;
  }

  public long getSuccessMoveCount() {
    return successMoveCount;
  }

  public long getFailureMoveCount() {
    return failureMoveCount;
  }

  public long getBytesToMove() {
    return bytesToMove;
  }

  public long getBalancedBytes() {
    return balancedBytes;
  }

  public double getVolumeDataDensity() {
    return volumeDataDensity;
  }
}
