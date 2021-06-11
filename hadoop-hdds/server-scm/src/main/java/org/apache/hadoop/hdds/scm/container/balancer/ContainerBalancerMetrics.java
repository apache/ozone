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

import org.apache.hadoop.hdds.scm.container.placement.metrics.LongMetric;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;

/**
 * Metrics related to Container Balancer running in SCM.
 */
@Metrics(name = "ContainerBalancer Metrics", about = "Metrics related to " +
    "Container Balancer running in SCM", context = "SCM")
public final class ContainerBalancerMetrics {

  @Metric(about = "The total amount of used space in GigaBytes that needs to " +
      "be balanced.")
  private LongMetric dataSizeToBalanceGB;

  @Metric(about = "The amount of Giga Bytes that have been moved to achieve " +
      "balance.")
  private LongMetric dataSizeBalancedGB;

  @Metric(about = "Number of containers that Container Balancer has moved" +
      " until now.")
  private LongMetric movedContainersNum;

  @Metric(about = "The total number of datanodes that need to be balanced.")
  private LongMetric datanodesNumToBalance;

  @Metric(about = "Number of datanodes that Container Balancer has balanced " +
      "until now.")
  private LongMetric datanodesNumBalanced;

  @Metric(about = "Utilisation value of the current maximum utilised datanode.")
  private double maxDatanodeUtilizedRatio;

  /**
   * Initialise metrics for ContainerBalancer.
   */
  public ContainerBalancerMetrics() {
    dataSizeToBalanceGB = new LongMetric(0L);
    dataSizeBalancedGB = new LongMetric(0L);
    movedContainersNum = new LongMetric(0L);
    datanodesNumToBalance = new LongMetric(0L);
    datanodesNumBalanced = new LongMetric(0L);
    maxDatanodeUtilizedRatio = 0D;
  }

  public LongMetric getDataSizeToBalanceGB() {
    return dataSizeToBalanceGB;
  }

  public void setDataSizeToBalanceGB(long size) {
    this.dataSizeToBalanceGB = new LongMetric(size);
  }

  public LongMetric getDataSizeBalancedGB() {
    return dataSizeBalancedGB;
  }

  public void setDataSizeBalancedGB(
      LongMetric dataSizeBalancedGB) {
    this.dataSizeBalancedGB = dataSizeBalancedGB;
  }

  public LongMetric getMovedContainersNum() {
    return movedContainersNum;
  }

  public void setMovedContainersNum(
      LongMetric movedContainersNum) {
    this.movedContainersNum = movedContainersNum;
  }

  public LongMetric getDatanodesNumToBalance() {
    return datanodesNumToBalance;
  }

  public void setDatanodesNumToBalance(
      LongMetric datanodesNumToBalance) {
    this.datanodesNumToBalance = datanodesNumToBalance;
  }

  public LongMetric getDatanodesNumBalanced() {
    return datanodesNumBalanced;
  }

  public void setDatanodesNumBalanced(
      LongMetric datanodesNumBalanced) {
    this.datanodesNumBalanced = datanodesNumBalanced;
  }

  /**
   * Add specified valueToAdd to datanodesNumBalanced.
   *
   * @param valueToAdd The value to add.
   * @return The result after addition.
   */
  public long incrementDatanodesNumBalanced(long valueToAdd) {
    datanodesNumBalanced.add(valueToAdd);
    return datanodesNumBalanced.get();
  }

  public double getMaxDatanodeUtilizedRatio() {
    return maxDatanodeUtilizedRatio;
  }

  public void setMaxDatanodeUtilizedRatio(
      double maxDatanodeUtilizedRatio) {
    this.maxDatanodeUtilizedRatio = maxDatanodeUtilizedRatio;
  }
}
