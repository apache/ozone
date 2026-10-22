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

/**
 * Information about moving containers.
 */
public class ContainerMoveInfo {
  private final long containerMovesScheduled;
  private final long containerMovesCompleted;
  private final long containerMovesFailed;
  private final long containerMovesTimeout;
  private final List<ContainerMoveFailureDetail> failures;

  public ContainerMoveInfo(long containerMovesScheduled, long containerMovesCompleted, long containerMovesFailed,
                           long containerMovesTimeout, List<ContainerMoveFailureDetail> failures) {
    this.containerMovesScheduled = containerMovesScheduled;
    this.containerMovesCompleted = containerMovesCompleted;
    this.containerMovesFailed = containerMovesFailed;
    this.containerMovesTimeout = containerMovesTimeout;
    this.failures = failures;
  }

  public ContainerMoveInfo(ContainerBalancerMetrics metrics, ContainerMoveFailureTracker failureTracker) {
    this.containerMovesScheduled = metrics.getNumContainerMovesScheduledInLatestIteration();
    this.containerMovesCompleted = metrics.getNumContainerMovesCompletedInLatestIteration();
    this.containerMovesFailed = metrics.getNumContainerMovesFailedInLatestIteration();
    this.containerMovesTimeout = metrics.getNumContainerMovesTimeoutInLatestIteration();
    this.failures = failureTracker.getFailures();
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

  public List<ContainerMoveFailureDetail> getFailures() {
    return failures;
  }
}
