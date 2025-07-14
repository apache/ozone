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

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * Metrics related to Container Balancer running in SCM.
 */
@Metrics(name = "ContainerBalancer Metrics", about = "Metrics related to " +
    "Container Balancer running in SCM", context = "SCM")
public final class ContainerBalancerMetrics {
  public static final String NAME =
      ContainerBalancerMetrics.class.getSimpleName();

  private final MetricsSystem ms;

  @Metric(about = "Amount of Gigabytes that Container Balancer moved" +
      " in the latest iteration.")
  private MutableCounterLong dataSizeMovedGBInLatestIteration;

  @Metric(about = "Amount of bytes that the Container Balancer moved in the latest iteration.")
  private MutableCounterLong dataSizeMovedBytesInLatestIteration;

  @Metric(about = "Number of completed container moves performed by " +
      "Container Balancer in the latest iteration.")
  private MutableCounterLong numContainerMovesCompletedInLatestIteration;

  @Metric(about = "Number of timeout container moves performed by " +
      "Container Balancer in the latest iteration.")
  private MutableCounterLong numContainerMovesTimeoutInLatestIteration;

  @Metric(about = "Number of iterations that Container Balancer has run for.")
  private MutableCounterLong numIterations;

  @Metric(about = "Number of datanodes that were involved in balancing in the" +
      " latest iteration.")
  private MutableCounterLong numDatanodesInvolvedInLatestIteration;

  @Metric(about = "Amount of data in Gigabytes that is causing unbalance.")
  private MutableCounterLong dataSizeUnbalancedGB;

  @Metric(about = "Number of unbalanced datanodes.")
  private MutableCounterLong numDatanodesUnbalanced;

  @Metric(about = "Total number of completed container moves across all " +
      "iterations of Container Balancer.")
  private MutableCounterLong numContainerMovesCompleted;

  @Metric(about = "Total number of timeout container moves across " +
      "all iterations of Container Balancer.")
  private MutableCounterLong numContainerMovesTimeout;

  @Metric(about = "Total data size in GB moved across all iterations of " +
      "Container Balancer.")
  private MutableCounterLong dataSizeMovedGB;

  @Metric(about = "Total number container for which moves failed " +
      "exceptionally across all iterations of Container Balancer.")
  private MutableCounterLong numContainerMovesFailed;

  @Metric(about = "Total number container for which moves failed " +
      "exceptionally in latest iteration of Container Balancer.")
  private MutableCounterLong numContainerMovesFailedInLatestIteration;

  @Metric(about = "Number of container moves that were scheduled in the " +
      "latest iteration of Container Balancer.")
  private MutableCounterLong numContainerMovesScheduledInLatestIteration;

  @Metric(about = "Total number of container moves that were scheduled across" +
      " all iterations of Container Balancer.")
  private MutableCounterLong numContainerMovesScheduled;

  /**
   * Create and register metrics named {@link ContainerBalancerMetrics#NAME}
   * for {@link ContainerBalancer}.
   *
   * @return {@link ContainerBalancerMetrics}
   */
  public static ContainerBalancerMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(NAME, "Container Balancer Metrics",
        new ContainerBalancerMetrics(ms));
  }

  private ContainerBalancerMetrics(MetricsSystem ms) {
    this.ms = ms;
  }

  /**
   * Gets the number of container moves scheduled across all iterations of
   * Container Balancer.
   * @return number of moves
   */
  public long getNumContainerMovesScheduled() {
    return numContainerMovesScheduled.value();
  }

  void incrementNumContainerMovesScheduled(long valueToAdd) {
    this.numContainerMovesScheduled.incr(valueToAdd);
  }

  /**
   * Gets the number of container moves scheduled in the latest iteration of
   * Container Balancer.
   * @return number of moves
   */
  public long getNumContainerMovesScheduledInLatestIteration() {
    return numContainerMovesScheduledInLatestIteration.value();
  }

  void incrementNumContainerMovesScheduledInLatestIteration(long valueToAdd) {
    this.numContainerMovesScheduledInLatestIteration.incr(valueToAdd);
  }

  /**
   * Reset the number of containers scheduled to move in the last iteration.
   */
  public void resetNumContainerMovesScheduledInLatestIteration() {
    numContainerMovesScheduledInLatestIteration.incr(
            -getNumContainerMovesScheduledInLatestIteration());
  }

  /**
   * Retrieves the amount of data moved by the Container Balancer in the latest iteration.
   * @return size in GB
   */
  public long getDataSizeMovedGBInLatestIteration() {
    return dataSizeMovedGBInLatestIteration.value();
  }

  public void incrementDataSizeMovedGBInLatestIteration(long valueToAdd) {
    this.dataSizeMovedGBInLatestIteration.incr(valueToAdd);
  }

  public void resetDataSizeMovedGBInLatestIteration() {
    dataSizeMovedGBInLatestIteration.incr(
        -getDataSizeMovedGBInLatestIteration());
  }

  /**
   * Retrieves the amount of data moved by the Container Balancer in the latest iteration.
   * @return size in bytes
   */
  public long getDataSizeMovedInLatestIteration() {
    return dataSizeMovedBytesInLatestIteration.value();
  }

  /**
   * Increment the amount of data moved in the last iteration.
   * @param bytes bytes to add
   */
  public void incrementDataSizeMovedInLatestIteration(long bytes) {
    this.dataSizeMovedBytesInLatestIteration.incr(bytes);
  }

  /**
   * Reset the amount of data moved in the last iteration.
   */
  public void resetDataSizeMovedInLatestIteration() {
    dataSizeMovedBytesInLatestIteration.incr(-getDataSizeMovedInLatestIteration());
  }

  /**
   * Gets the number of container moves performed by Container Balancer in the
   * latest iteration.
   * @return number of container moves
   */
  public long getNumContainerMovesCompletedInLatestIteration() {
    return numContainerMovesCompletedInLatestIteration.value();
  }

  public void incrementCurrentIterationContainerMoveMetric(
      MoveManager.MoveResult result, long valueToAdd) {
    if (result == null) {
      return;
    }
    switch (result) {
    case COMPLETED:
      this.numContainerMovesCompletedInLatestIteration.incr(valueToAdd);
      break;
    case REPLICATION_FAIL_TIME_OUT:
    case DELETION_FAIL_TIME_OUT:
      this.numContainerMovesTimeoutInLatestIteration.incr(valueToAdd);
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
      incrementNumContainerMovesFailedInLatestIteration(valueToAdd);
      break;
    default:
      break;
    }
  }

  /**
   * Reset the number of containers moved in the last iteration.
   */
  public void resetNumContainerMovesCompletedInLatestIteration() {
    numContainerMovesCompletedInLatestIteration.incr(-getNumContainerMovesCompletedInLatestIteration());
  }

  /**
   * Gets the number of timeout container moves performed by
   * Container Balancer in the latest iteration.
   * @return number of timeout container moves
   */
  public long getNumContainerMovesTimeoutInLatestIteration() {
    return numContainerMovesTimeoutInLatestIteration.value();
  }

  /**
   * Increases the number of timeout container moves in the latest iteration.
   */
  public void incrementNumContainerMovesTimeoutInLatestIteration(
      long valueToAdd) {
    this.numContainerMovesTimeoutInLatestIteration.incr(valueToAdd);
  }

  /**
   * Reset the number of timeout container moves in the latest iteration.
   */
  public void resetNumContainerMovesTimeoutInLatestIteration() {
    numContainerMovesTimeoutInLatestIteration.incr(-getNumContainerMovesTimeoutInLatestIteration());
  }

  /**
   * Gets the number of iterations that Container Balancer has run for.
   * @return number of iterations
   */
  public long getNumIterations() {
    return numIterations.value();
  }

  public void incrementNumIterations(long valueToAdd) {
    numIterations.incr(valueToAdd);
  }

  /**
   * Gets number of datanodes that were involved in balancing in the latest
   * iteration.
   * @return number of datanodes
   */
  public long getNumDatanodesInvolvedInLatestIteration() {
    return numDatanodesInvolvedInLatestIteration.value();
  }

  public void incrementNumDatanodesInvolvedInLatestIteration(long valueToAdd) {
    numDatanodesInvolvedInLatestIteration.incr(valueToAdd);
  }

  public void resetNumDatanodesInvolvedInLatestIteration() {
    numDatanodesInvolvedInLatestIteration.incr(
        -getNumDatanodesInvolvedInLatestIteration());
  }

  /**
   * Gets the amount of data in Gigabytes that is causing unbalance.
   * @return size of data as a long value
   */
  public long getDataSizeUnbalancedGB() {
    return dataSizeUnbalancedGB.value();
  }

  public void incrementDataSizeUnbalancedGB(long valueToAdd) {
    dataSizeUnbalancedGB.incr(valueToAdd);
  }

  public void resetDataSizeUnbalancedGB() {
    dataSizeUnbalancedGB.incr(-getDataSizeUnbalancedGB());
  }

  /**
   * Gets the number of datanodes that are unbalanced.
   * @return long value
   */
  public long getNumDatanodesUnbalanced() {
    return numDatanodesUnbalanced.value();
  }

  public void incrementNumDatanodesUnbalanced(long valueToAdd) {
    numDatanodesUnbalanced.incr(valueToAdd);
  }

  public void resetNumDatanodesUnbalanced() {
    numDatanodesUnbalanced.incr(-getNumDatanodesUnbalanced());
  }

  public long getNumContainerMovesCompleted() {
    return numContainerMovesCompleted.value();
  }

  public void incrementNumContainerMovesCompleted(long valueToAdd) {
    numContainerMovesCompleted.incr(valueToAdd);
  }

  public long getNumContainerMovesTimeout() {
    return numContainerMovesTimeout.value();
  }

  public void incrementNumContainerMovesTimeout(long valueToAdd) {
    numContainerMovesTimeout.incr(valueToAdd);
  }

  public long getDataSizeMovedGB() {
    return dataSizeMovedGB.value();
  }

  public void incrementDataSizeMovedGB(long valueToAdd) {
    dataSizeMovedGB.incr(valueToAdd);
  }

  public long getNumContainerMovesFailed() {
    return numContainerMovesFailed.value();
  }

  public void incrementNumContainerMovesFailed(long valueToAdd) {
    numContainerMovesFailed.incr(valueToAdd);
  }

  public long getNumContainerMovesFailedInLatestIteration() {
    return numContainerMovesFailedInLatestIteration.value();
  }

  public void incrementNumContainerMovesFailedInLatestIteration(
      long valueToAdd) {
    numContainerMovesFailedInLatestIteration.incr(valueToAdd);
  }

  public void resetNumContainerMovesFailedInLatestIteration() {
    numContainerMovesFailedInLatestIteration.incr(
        -getNumContainerMovesFailedInLatestIteration());
  }
}
