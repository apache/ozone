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

  @Metric(about = "The amount of Gigabytes that Container Balancer moved" +
      " in the last iteration.")
  private MutableCounterLong dataSizeMovedGB;

  @Metric(about = "Number of containers that Container Balancer moved" +
      " in the last iteration.")
  private MutableCounterLong movedContainersNum;

  @Metric(about = "Number of iterations that Container Balancer has run for.")
  private MutableCounterLong countIterations;

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
   * Gets the amount of data moved by Container Balancer in the last iteration.
   * @return size in GB
   */
  public long getDataSizeMovedGB() {
    return dataSizeMovedGB.value();
  }

  public void incrementDataSizeMovedGB(long valueToAdd) {
    this.dataSizeMovedGB.incr(valueToAdd);
  }

  public void resetDataSizeMovedGB() {
    dataSizeMovedGB.incr(-getDataSizeMovedGB());
  }

  /**
   * Gets the number of containers moved by Container Balancer in the last
   * iteration.
   * @return number of containers
   */
  public long getMovedContainersNum() {
    return movedContainersNum.value();
  }

  public void incrementMovedContainersNum(long valueToAdd) {
    this.movedContainersNum.incr(valueToAdd);
  }

  public void resetMovedContainersNum() {
    movedContainersNum.incr(-getMovedContainersNum());
  }

  /**
   * Gets the number of iterations that Container Balancer has run for.
   * @return number of iterations
   */
  public long getCountIterations() {
    return countIterations.value();
  }

  public void incrementCountIterations(long valueToAdd) {
    countIterations.incr(valueToAdd);
  }
}
