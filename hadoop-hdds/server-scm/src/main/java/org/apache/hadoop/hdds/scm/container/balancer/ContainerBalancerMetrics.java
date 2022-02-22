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
@Metrics(name = "ContainerBalancer Metrics", about = "Metrics related to " +
    "Container Balancer running in SCM", context = "SCM")
public final class ContainerBalancerMetrics {
  public static final String NAME =
      ContainerBalancerMetrics.class.getSimpleName();

  private final MetricsSystem ms;

  @Metric(about = "Amount of Gigabytes that Container Balancer moved" +
      " in the latest iteration.")
  private MutableCounterLong dataSizeMovedGBInLatestIteration;

  @Metric(about = "Number of containers that Container Balancer moved" +
      " in the latest iteration.")
  private MutableCounterLong movedContainersNumInLatestIteration;

  @Metric(about = "Number of iterations that Container Balancer has run for.")
  private MutableCounterLong numIterations;

  @Metric(about = "Number of datanodes that were involved in balancing in the" +
      " latest iteration.")
  private MutableCounterLong datanodesNumInvolvedInLatestIteration;

  @Metric(about = "Amount of data in Gigabytes that is causing unbalance.")
  private MutableCounterLong dataSizeUnbalancedGB;

  @Metric(about = "Number of unbalanced datanodes.")
  private MutableCounterLong datanodesNumUnbalanced;

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
   * Gets the amount of data moved by Container Balancer in the latest
   * iteration.
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
   * Gets the number of containers moved by Container Balancer in the latest
   * iteration.
   * @return number of containers
   */
  public long getMovedContainersNumInLatestIteration() {
    return movedContainersNumInLatestIteration.value();
  }

  public void incrementMovedContainersNumInLatestIteration(long valueToAdd) {
    this.movedContainersNumInLatestIteration.incr(valueToAdd);
  }

  public void resetMovedContainersNumInLatestIteration() {
    movedContainersNumInLatestIteration.incr(
        -getMovedContainersNumInLatestIteration());
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
  public long getDatanodesNumInvolvedInLatestIteration() {
    return datanodesNumInvolvedInLatestIteration.value();
  }

  public void incrementDatanodesNumInvolvedInLatestIteration(long valueToAdd) {
    datanodesNumInvolvedInLatestIteration.incr(valueToAdd);
  }

  public void resetDatanodesNumInvolvedInLatestIteration() {
    datanodesNumInvolvedInLatestIteration.incr(
        -getDatanodesNumInvolvedInLatestIteration());
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
  public long getDatanodesNumUnbalanced() {
    return datanodesNumUnbalanced.value();
  }

  public void incrementDatanodesNumUnbalanced(long valueToAdd) {
    datanodesNumUnbalanced.incr(valueToAdd);
  }

  public void resetDatanodesNumUnbalanced() {
    datanodesNumUnbalanced.incr(-getDatanodesNumUnbalanced());
  }
}
