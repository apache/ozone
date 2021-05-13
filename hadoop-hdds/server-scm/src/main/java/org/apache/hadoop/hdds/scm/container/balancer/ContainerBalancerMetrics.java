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

@Metrics(name = "ContainerBalancer Metrics", about = "Metrics related to " +
    "Container Balancer running in SCM", context = "SCM")
public final class ContainerBalancerMetrics {

  @Metric(about = "The total amount of used space in GigaBytes that needs to " +
      "be balanced.")
  private LongMetric totalSizeToBalanceGB;

  @Metric(about = "The amount of Giga Bytes that have been moved to achieve " +
      "balance.")
  private LongMetric gigaBytesMoved;

  @Metric(about = "Number of containers that Container Balancer has moved" +
      " until now.")
  private LongMetric numContainersMoved;

  @Metric(about = "The total number of datanodes that need to be balanced.")
  private LongMetric totalNumDatanodesToBalance;

  @Metric(about = "Number of datanodes that Container Balancer has balanced " +
      "until now.")
  private LongMetric numDatanodesBalanced;

  @Metric(about = "Utilisation value of the current maximum utilised datanode.")
  private double maxUtilizedDatanodeRatio;

  /**
   * Initialise metrics for ContainerBalancer.
   */
  public ContainerBalancerMetrics() {
    totalSizeToBalanceGB = new LongMetric(0L);
    gigaBytesMoved = new LongMetric(0L);
    numContainersMoved = new LongMetric(0L);
    totalNumDatanodesToBalance = new LongMetric(0L);
    numDatanodesBalanced = new LongMetric(0L);
    maxUtilizedDatanodeRatio = 0D;
  }

  public LongMetric getTotalSizeToBalanceGB() {
    return totalSizeToBalanceGB;
  }

  public void setTotalSizeToBalanceGB(long size) {
    this.totalSizeToBalanceGB = new LongMetric(size);
  }

  public LongMetric getGigaBytesMoved() {
    return gigaBytesMoved;
  }

  public void setGigaBytesMoved(
      LongMetric gigaBytesMoved) {
    this.gigaBytesMoved = gigaBytesMoved;
  }

  public LongMetric getNumContainersMoved() {
    return numContainersMoved;
  }

  public void setNumContainersMoved(
      LongMetric numContainersMoved) {
    this.numContainersMoved = numContainersMoved;
  }

  public LongMetric getTotalNumDatanodesToBalance() {
    return totalNumDatanodesToBalance;
  }

  public void setTotalNumDatanodesToBalance(
      LongMetric totalNumDatanodesToBalance) {
    this.totalNumDatanodesToBalance = totalNumDatanodesToBalance;
  }

  public LongMetric getNumDatanodesBalanced() {
    return numDatanodesBalanced;
  }

  public void setNumDatanodesBalanced(
      LongMetric numDatanodesBalanced) {
    this.numDatanodesBalanced = numDatanodesBalanced;
  }

  /**
   * Add specified valueToAdd to NumDatanodesBalanced.
   *
   * @param valueToAdd The value to add.
   * @return The result after addition.
   */
  public long addToNumDatanodesBalanced(long valueToAdd) {
    numDatanodesBalanced.add(valueToAdd);
    return numDatanodesBalanced.get();
  }

  public double getMaxUtilizedDatanodeRatio() {
    return maxUtilizedDatanodeRatio;
  }

  public void setMaxUtilizedDatanodeRatio(
      double maxUtilizedDatanodeRatio) {
    this.maxUtilizedDatanodeRatio = maxUtilizedDatanodeRatio;
  }
}
