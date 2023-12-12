/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
@Metrics(name = "ContainerBalancer Metrics",
    about = "Metrics related toContainer Balancer running in SCM", context = "SCM")
public final class ContainerBalancerMetrics {
  public static final String NAME = ContainerBalancerMetrics.class.getSimpleName();

  private final MetricsSystem ms;

  @Metric(about = "Number of iterations that Container Balancer has run for.")
  private MutableCounterLong numIterations;

  @Metric(about = "Total number of completed container moves across all iterations of Container Balancer.")
  private MutableCounterLong numContainerMovesCompleted;

  @Metric(about = "Total number of timeout container moves across all iterations of Container Balancer.")
  private MutableCounterLong numContainerMovesTimeout;

  @Metric(about = "Total data size in Bytes moved across all iterations of Container Balancer.")
  private MutableCounterLong dataSizeMovedBytes;

  @Metric(about = "Total number container for which moves failed exceptionally across" +
      " all iterations of Container Balancer.")
  private MutableCounterLong numContainerMovesFailed;

  @Metric(about = "Total number of container moves that were scheduled across" +
      " all iterations of Container Balancer.")
  private MutableCounterLong numContainerMovesScheduled;

  /**
   * Create and register metrics named {@link ContainerBalancerMetrics#NAME} for {@link ContainerBalancer}.
   *
   * @return {@link ContainerBalancerMetrics}
   */
  public static ContainerBalancerMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(NAME, "Container Balancer Metrics", new ContainerBalancerMetrics(ms));
  }

  private ContainerBalancerMetrics(MetricsSystem ms) {
    this.ms = ms;
  }

  /**
   * Gets the number of container moves scheduled across all iterations of Container Balancer.
   * @return number of moves
   */
  public long getNumContainerMovesScheduled() {
    return numContainerMovesScheduled.value();
  }

  public void incrementNumContainerMovesScheduled(long valueToAdd) {
    this.numContainerMovesScheduled.incr(valueToAdd);
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

  public long getDataSizeMovedBytes() {
    return dataSizeMovedBytes.value();
  }

  public void incrementDataSizeMovedBytes(long valueToAdd) {
    dataSizeMovedBytes.incr(valueToAdd);
  }

  public long getNumContainerMovesFailed() {
    return numContainerMovesFailed.value();
  }

  public void incrementNumContainerMovesFailed(long valueToAdd) {
    numContainerMovesFailed.incr(valueToAdd);
  }
}
