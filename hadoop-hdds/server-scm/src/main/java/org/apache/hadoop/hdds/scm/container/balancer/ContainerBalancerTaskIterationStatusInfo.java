/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.balancer;

import java.util.Map;
import java.util.UUID;

/**
 * Information about balancer task iteration.
 */
public class ContainerBalancerTaskIterationStatusInfo {
  private final Integer iterationNumber;
  private final String iterationResult;
  private final long sizeScheduledForMove;
  private final long dataSizeMovedGB;
  private final long containerMovesScheduled;
  private final long containerMovesCompleted;
  private final long containerMovesFailed;
  private final long containerMovesTimeout;
  private final Map<UUID, Long> sizeEnteringNodes;
  private final Map<UUID, Long> sizeLeavingNodes;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public ContainerBalancerTaskIterationStatusInfo(
      Integer iterationNumber,
      String iterationResult,
      long sizeScheduledForMove,
      long dataSizeMovedGB,
      long containerMovesScheduled,
      long containerMovesCompleted,
      long containerMovesFailed,
      long containerMovesTimeout,
      Map<UUID, Long> sizeEnteringNodes,
      Map<UUID, Long> sizeLeavingNodes) {
    this.iterationNumber = iterationNumber;
    this.iterationResult = iterationResult;
    this.sizeScheduledForMove = sizeScheduledForMove;
    this.dataSizeMovedGB = dataSizeMovedGB;
    this.containerMovesScheduled = containerMovesScheduled;
    this.containerMovesCompleted = containerMovesCompleted;
    this.containerMovesFailed = containerMovesFailed;
    this.containerMovesTimeout = containerMovesTimeout;
    this.sizeEnteringNodes = sizeEnteringNodes;
    this.sizeLeavingNodes = sizeLeavingNodes;
  }

  public Integer getIterationNumber() {
    return iterationNumber;
  }

  public String getIterationResult() {
    return iterationResult;
  }

  public long getSizeScheduledForMove() {
    return sizeScheduledForMove;
  }

  public long getDataSizeMovedGB() {
    return dataSizeMovedGB;
  }

  public long getContainerMovesScheduled() {
    return containerMovesScheduled;
  }

  public long getContainerMovesCompleted() {
    return containerMovesCompleted;
  }

  public long getContainerMovesFailed() {
    return containerMovesFailed;
  }

  public long getContainerMovesTimeout() {
    return containerMovesTimeout;
  }

  public Map<UUID, Long> getSizeEnteringNodes() {
    return sizeEnteringNodes;
  }

  public Map<UUID, Long> getSizeLeavingNodes() {
    return sizeLeavingNodes;
  }
}


