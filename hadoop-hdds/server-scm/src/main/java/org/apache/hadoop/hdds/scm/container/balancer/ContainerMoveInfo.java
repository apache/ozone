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

package org.apache.hadoop.hdds.scm.container.balancer;

import java.util.List;
import java.util.Map;

/**
 * Information about moving containers.
 */
public class ContainerMoveInfo {
  private final long containerMovesScheduled;
  private final long containerMovesCompleted;
  private final long containerMovesFailed;
  private final long containerMovesTimeout;
  private final Map<String, Long> failuresByReason;
  private final List<ContainerMoveFailureDetail> failureDetails;

  public ContainerMoveInfo(long containerMovesScheduled, long containerMovesCompleted, long containerMovesFailed,
                           long containerMovesTimeout, Map<String, Long> failuresByReason,
                           List<ContainerMoveFailureDetail> failureDetails) {
    this.containerMovesScheduled = containerMovesScheduled;
    this.containerMovesCompleted = containerMovesCompleted;
    this.containerMovesFailed = containerMovesFailed;
    this.containerMovesTimeout = containerMovesTimeout;
    this.failuresByReason = failuresByReason;
    this.failureDetails = failureDetails;
  }

  public ContainerMoveInfo(ContainerBalancerMetrics metrics, ContainerMoveFailureTracker failureTracker) {
    this.containerMovesScheduled = metrics.getNumContainerMovesScheduledInLatestIteration();
    this.containerMovesCompleted = metrics.getNumContainerMovesCompletedInLatestIteration();
    this.containerMovesFailed = metrics.getNumContainerMovesFailedInLatestIteration();
    this.containerMovesTimeout = metrics.getNumContainerMovesTimeoutInLatestIteration();
    this.failuresByReason = failureTracker.getFailuresByReason();
    this.failureDetails = failureTracker.getFailureDetails();
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

  public Map<String, Long> getFailuresByReason() {
    return failuresByReason;
  }

  public List<ContainerMoveFailureDetail> getFailureDetails() {
    return failureDetails;
  }
}
