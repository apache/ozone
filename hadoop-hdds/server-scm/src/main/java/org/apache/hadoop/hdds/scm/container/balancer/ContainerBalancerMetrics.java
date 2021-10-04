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
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * Metrics related to Container Balancer running in SCM.
 */
@Metrics(name = "ContainerBalancer Metrics", about = "Metrics related to " +
    "Container Balancer running in SCM", context = "SCM")
public final class ContainerBalancerMetrics {
  public static final String NAME =
      ContainerBalancerMetrics.class.getSimpleName();

  private final MetricsSystem ms;

  @Metric(about = "The total amount of used space in GigaBytes that needs to " +
      "be balanced.")
  private MutableGaugeLong dataSizeToBalanceGB;

  @Metric(about = "The amount of Giga Bytes that have been moved to achieve " +
      "balance.")
  private MutableGaugeLong dataSizeMovedGB;

  @Metric(about = "Number of containers that Container Balancer has moved" +
      " until now.")
  private MutableGaugeLong movedContainersNum;

  @Metric(about = "The total number of datanodes that need to be balanced.")
  private MutableGaugeLong datanodesNumToBalance;

  @Metric(about = "Number of datanodes that Container Balancer has balanced " +
      "until now.")
  private MutableGaugeLong datanodesNumBalanced;

  @Metric(about = "Utilisation value of the current maximum utilised datanode.")
  private MutableGaugeInt maxDatanodeUtilizedPercentage;

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

  public long getDataSizeToBalanceGB() {
    return dataSizeToBalanceGB.value();
  }

  public void setDataSizeToBalanceGB(long size) {
    this.dataSizeToBalanceGB.set(size);
  }

  public long getDataSizeMovedGB() {
    return dataSizeMovedGB.value();
  }

  public void setDataSizeMovedGB(long dataSizeMovedGB) {
    this.dataSizeMovedGB.set(dataSizeMovedGB);
  }

  public long incrementDataSizeMovedGB(long valueToAdd) {
    this.dataSizeMovedGB.incr(valueToAdd);
    return this.dataSizeMovedGB.value();
  }

  public long getMovedContainersNum() {
    return movedContainersNum.value();
  }

  public void setMovedContainersNum(long movedContainersNum) {
    this.movedContainersNum.set(movedContainersNum);
  }

  public long incrementMovedContainersNum(long valueToAdd) {
    this.movedContainersNum.incr(valueToAdd);
    return this.movedContainersNum.value();
  }

  public long getDatanodesNumToBalance() {
    return datanodesNumToBalance.value();
  }

  public void setDatanodesNumToBalance(long datanodesNumToBalance) {
    this.datanodesNumToBalance.set(datanodesNumToBalance);
  }

  /**
   * Add specified valueToAdd to the number of datanodes that need to be
   * balanced.
   *
   * @param valueToAdd number of datanodes to add
   */
  public void incrementDatanodesNumToBalance(long valueToAdd) {
    this.datanodesNumToBalance.incr(valueToAdd);
  }

  public long getDatanodesNumBalanced() {
    return datanodesNumBalanced.value();
  }

  public void setDatanodesNumBalanced(long datanodesNumBalanced) {
    this.datanodesNumBalanced.set(datanodesNumBalanced);
  }

  /**
   * Add specified valueToAdd to datanodesNumBalanced.
   *
   * @param valueToAdd The value to add.
   * @return The result after addition.
   */
  public long incrementDatanodesNumBalanced(long valueToAdd) {
    datanodesNumBalanced.incr(valueToAdd);
    return datanodesNumBalanced.value();
  }

  public int getMaxDatanodeUtilizedPercentage() {
    return maxDatanodeUtilizedPercentage.value();
  }

  public void setMaxDatanodeUtilizedPercentage(int percentage) {
    this.maxDatanodeUtilizedPercentage.set(percentage);
  }
}
