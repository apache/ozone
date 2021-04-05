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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ContainerBalancer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerBalancer.class);

  private NodeManager nodeManager;
  private ContainerManagerV2 containerManager;
  private ReplicationManager replicationManager;
  private OzoneConfiguration ozoneConfiguration;
  private double threshold;
  private int maxDatanodesToBalance;
  private long maxSizeToMove;
  private boolean balancerRunning;
  private List<DatanodeUsageInfo> sourceNodes;
  private List<DatanodeUsageInfo> targetNodes;
  private ContainerBalancerConfiguration config;

  public ContainerBalancer(
      NodeManager nodeManager,
      ContainerManagerV2 containerManager,
      ReplicationManager replicationManager,
      OzoneConfiguration ozoneConfiguration) {
    this.nodeManager = nodeManager;
    this.containerManager = containerManager;
    this.replicationManager = replicationManager;
    this.ozoneConfiguration = ozoneConfiguration;
    this.balancerRunning = false;
    this.config = new ContainerBalancerConfiguration();
  }

  /**
   * Start ContainerBalancer. Current implementation is incomplete.
   *
   * @param balancerConfiguration Configuration values.
   */
  public void start(ContainerBalancerConfiguration balancerConfiguration) {
    this.balancerRunning = true;

    ozoneConfiguration = new OzoneConfiguration();

    // initialise configs
    this.config = balancerConfiguration;
    this.threshold = config.getThreshold();
    this.maxDatanodesToBalance =
        config.getMaxDatanodesToBalance();
    this.maxSizeToMove = config.getMaxSizeToMove();

    LOG.info("Starting Container Balancer...");

    // sorted list in order from most to least used
    List<DatanodeUsageInfo> nodes = nodeManager.
        getMostOrLeastUsedDatanodes(true);
    double avgUtilisation = calculateAvgUtilisation(nodes);

    // under utilized nodes have utilization(that is, used / capacity) less
    // than lower limit
    double lowerLimit = avgUtilisation - threshold;

    // over utilized nodes have utilization(that is, used / capacity) greater
    // than upper limit
    double upperLimit = avgUtilisation + threshold;
    LOG.info("Lower limit for utilization is {}", lowerLimit);
    LOG.info("Upper limit for utilization is {}", upperLimit);

    // find over utilised(source) and under utilised(target) nodes
    sourceNodes = new ArrayList<>();
    targetNodes = new ArrayList<>();
//    for (DatanodeUsageInfo node : nodes) {
//      SCMNodeStat stat = node.getScmNodeStat();
//      double utilization = stat.getScmUsed().get().doubleValue() /
//          stat.getCapacity().get().doubleValue();
//      if (utilization > upperLimit) {
//        sourceNodes.add(node);
//      } else if (utilization < lowerLimit || utilization < avgUtilisation) {
//        targetNodes.add(node);
//      }
//    }
  }

  // calculate the average datanode utilisation across the cluster
  private double calculateAvgUtilisation(List<DatanodeUsageInfo> nodes) {
    SCMNodeStat aggregatedStats = new SCMNodeStat(
        0, 0, 0);
    for (DatanodeUsageInfo node : nodes) {
      aggregatedStats.add(node.getScmNodeStat());
    }
    return aggregatedStats.getScmUsed().get().doubleValue() /
        aggregatedStats.getCapacity().get().doubleValue();
  }

  public void stop() {
    LOG.info("Stopping Container Balancer...");
    balancerRunning = false;
    LOG.info("Container Balancer stopped.");
  }

  @Override
  public String toString() {
    String status = String.format("Container Balancer status:%n" +
        "%-30s %s%n" +
        "%-30s %b%n", "Key", "Value", "Running", balancerRunning);
    return status + config.toString();
  }
}
