/**
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

package org.apache.hadoop.hdds.scm.container.balancer.iteration;

import org.apache.hadoop.hdds.scm.container.balancer.MoveManager;

/**
 * Metrics related to Container Balancer Iteration running in SCM.
 * Collected inside one thread, doesn't require atomic read|write
 */

public final class IterationMetrics {
  private long completedContainerMovesCount;
  private long timeoutContainerMovesCount;
  private long failedContainerMovesCount;
  private long scheduledContainerMovesCount;
  private long involvedDatanodeCount;
  private long movedBytesCount;
  private long unbalancedDataSizeInBytes;
  private int unbalancedDatanodeCount;

  public long getTimeoutContainerMovesCount() {
    return timeoutContainerMovesCount;
  }

  public long getInvolvedDatanodeCount() {
    return involvedDatanodeCount;
  }

  public long getMovedBytesCount() {
    return movedBytesCount;
  }

  public long getCompletedContainerMovesCount() {
    return completedContainerMovesCount;
  }

  public long getUnbalancedDataSizeInBytes() {
    return unbalancedDataSizeInBytes;
  }

  public long getUnbalancedDatanodeCount() {
    return unbalancedDatanodeCount;
  }

  public long getFailedContainerMovesCount() {
    return failedContainerMovesCount;
  }

  public long getScheduledContainerMovesCount() {
    return scheduledContainerMovesCount;
  }

  void addToTimeoutContainerMovesCount(long value) {
    timeoutContainerMovesCount += value;
  }

  void addToInvolvedDatanodeCount(long value) {
    involvedDatanodeCount += value;
  }

  void setInvolvedDatanodeCount(long value) {
    involvedDatanodeCount = value;
  }

  void addToMovedBytesCount(long value) {
    movedBytesCount += value;
  }

  void addToCompletedContainerMovesCount(long value) {
    completedContainerMovesCount += value;
  }

  void addToUnbalancedDataSizeInBytes(long value) {
    unbalancedDataSizeInBytes = value;
  }

  void addToUnbalancedDatanodeCount(int value) {
    unbalancedDatanodeCount += value;
  }

  void addToFailedContainerMovesCount(long value) {
    failedContainerMovesCount += value;
  }

  void addToScheduledContainerMovesCount(long value) {
    scheduledContainerMovesCount += value;
  }

  void addToContainerMoveMetrics(MoveManager.MoveResult result, long value) {
    switch (result) {
    case COMPLETED:
      addToCompletedContainerMovesCount(value);
      break;
    case REPLICATION_FAIL_TIME_OUT:
    case DELETION_FAIL_TIME_OUT:
      addToTimeoutContainerMovesCount(value);
      break;
    // TODO: Add metrics for other errors that need to be tracked.
    case FAIL_LEADER_NOT_READY:
    case REPLICATION_FAIL_INFLIGHT_REPLICATION:
    case REPLICATION_FAIL_NOT_EXIST_IN_SOURCE:
    case REPLICATION_FAIL_EXIST_IN_TARGET:
    case REPLICATION_FAIL_CONTAINER_NOT_CLOSED:
    case REPLICATION_FAIL_INFLIGHT_DELETION:
    case REPLICATION_FAIL_NODE_NOT_IN_SERVICE:
    case DELETION_FAIL_NODE_NOT_IN_SERVICE:
    case REPLICATION_FAIL_NODE_UNHEALTHY:
    case DELETION_FAIL_NODE_UNHEALTHY:
    case DELETE_FAIL_POLICY:
    case REPLICATION_NOT_HEALTHY_BEFORE_MOVE:
    case REPLICATION_NOT_HEALTHY_AFTER_MOVE:
    case FAIL_CONTAINER_ALREADY_BEING_MOVED:
    case FAIL_UNEXPECTED_ERROR:
      addToFailedContainerMovesCount(value);
      break;
    default:
      break;
    }
  }
}
