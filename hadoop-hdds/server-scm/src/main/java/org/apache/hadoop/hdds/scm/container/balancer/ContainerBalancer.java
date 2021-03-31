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

import java.util.List;

public class ContainerBalancer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerBalancer.class);

  private NodeManager nodeManager;
  private ContainerManagerV2 containerManager;
  private ReplicationManager replicationManager;
  private OzoneConfiguration conf;
  private double threshold;
  private int maxDatanodesToBalance;
  private long maxSizeToMove;
  private boolean balancerRunning;

  public ContainerBalancer(
      NodeManager nodeManager,
      ContainerManagerV2 containerManager,
      ReplicationManager replicationManager,
      OzoneConfiguration conf) {
    this.nodeManager = nodeManager;
    this.containerManager = containerManager;
    this.replicationManager = replicationManager;
    this.conf = conf;
    this.balancerRunning = false;
  }

  /**
   * Start ContainerBalancer. Current implementation is incomplete.
   * @param config Configuration values.
   */
  public void start(ContainerBalancerConfiguration config) {
    conf = new OzoneConfiguration();
    this.balancerRunning = true;
    this.threshold = config.getThreshold();
    this.maxDatanodesToBalance =
        config.getMaxDatanodesToBalance();
    this.maxSizeToMove = config.getMaxSizeToMove();

    LOG.info("Starting Container Balancer...");

    List<DatanodeUsageInfo> nodes = nodeManager.
        getMostOrLeastUsedDatanodes(true);
    double avgUtilisation = calculateAvgUtilisation(nodes);

    double lowerLimit = avgUtilisation - threshold;
    double upperLimit = avgUtilisation + threshold;

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
    balancerRunning = false;
  }

}
