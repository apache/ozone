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
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests for {@link ContainerBalancer}.
 */
public class TestContainerBalancer {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerBalancer.class);

  private ReplicationManager replicationManager;
  private ContainerManagerV2 containerManager;
  private ContainerBalancer containerBalancer;
  private MockNodeManager mockNodeManager;
  private OzoneConfiguration conf;
  private ContainerBalancerConfiguration balancerConfiguration;
  private List<DatanodeUsageInfo> nodesInCluster;
  private List<Double> nodeUtilizations;
  private double averageUtilization;
  private int numberOfNodes;

  /**
   * Sets up configuration values and creates a mock cluster.
   */
  @Before
  public void setup() {
    conf = new OzoneConfiguration();
    containerManager = Mockito.mock(ContainerManagerV2.class);
    replicationManager = Mockito.mock(ReplicationManager.class);

    balancerConfiguration = new ContainerBalancerConfiguration();
    balancerConfiguration.setThreshold(0.1);
    balancerConfiguration.setMaxDatanodesToBalance(10);
    balancerConfiguration.setMaxSizeToMove(500 * OzoneConsts.GB);
    conf.setFromObject(balancerConfiguration);

    // create datanodes with the generated nodeUtilization values
    this.averageUtilization = createNodesInCluster();
    mockNodeManager = new MockNodeManager(nodesInCluster);
    containerBalancer = new ContainerBalancer(mockNodeManager, containerManager,
        replicationManager, conf, SCMContext.emptyContext());
  }

  /**
   * Checks whether ContainerBalancer is correctly updating the list of
   * unBalanced nodes with varying values of Threshold.
   */
  @Test
  public void
      initializeIterationShouldUpdateUnBalancedNodesWhenThresholdChanges() {
    List<DatanodeUsageInfo> expectedUnBalancedNodes;
    List<DatanodeUsageInfo> unBalancedNodesAccordingToBalancer;

    // check for random threshold values
    for (int i = 0; i < 50; i++) {
      double randomThreshold = Math.random();

      balancerConfiguration.setThreshold(randomThreshold);
      containerBalancer.start(balancerConfiguration);
      expectedUnBalancedNodes =
          determineExpectedUnBalancedNodes(randomThreshold);
      unBalancedNodesAccordingToBalancer =
          containerBalancer.getUnBalancedNodes();

      Assert.assertEquals(
          expectedUnBalancedNodes.size(),
          unBalancedNodesAccordingToBalancer.size());

      for (int j = 0; j < expectedUnBalancedNodes.size(); j++) {
        Assert.assertEquals(expectedUnBalancedNodes.get(j).getDatanodeDetails(),
            unBalancedNodesAccordingToBalancer.get(j).getDatanodeDetails());
      }
      containerBalancer.stop();
    }

  }

  /**
   * Checks whether the list of unBalanced nodes is empty when the cluster is
   * balanced.
   */
  @Test
  public void unBalancedNodesListShouldBeEmptyWhenClusterIsBalanced() {
    balancerConfiguration.setThreshold(0.99);
    containerBalancer.start(balancerConfiguration);

    Assert.assertEquals(0, containerBalancer.getUnBalancedNodes().size());
    containerBalancer.stop();
  }

  /**
   * Checks whether ContainerBalancer stops when the limit of
   * MaxDatanodesToBalance is reached.
   */
  @Test
  public void containerBalancerShouldStopWhenMaxDatanodesToBalanceIsReached() {
    balancerConfiguration.setMaxDatanodesToBalance(2);
    balancerConfiguration.setThreshold(0);
    containerBalancer.start(balancerConfiguration);

    Assert.assertFalse(containerBalancer.isBalancerRunning());
    containerBalancer.stop();
  }

  /**
   * Determines unBalanced nodes, that is, over and under utilized nodes,
   * according to the generated utilization values for nodes and the threshold.
   *
   * @param threshold A fraction from range 0 to 1.
   * @return List of DatanodeUsageInfo containing the expected(correct)
   * unBalanced nodes.
   */
  private List<DatanodeUsageInfo> determineExpectedUnBalancedNodes(
      double threshold) {
    double lowerLimit = averageUtilization - threshold;
    double upperLimit = averageUtilization + threshold;

    // use node utilizations to determine over and under utilized nodes
    List<DatanodeUsageInfo> expectedUnBalancedNodes = new ArrayList<>();
    for (int i = 0; i < numberOfNodes; i++) {
      if (nodeUtilizations.get(numberOfNodes - i - 1) > upperLimit) {
        expectedUnBalancedNodes.add(nodesInCluster.get(numberOfNodes - i - 1));
      }
    }
    for (int i = 0; i < numberOfNodes; i++) {
      if (nodeUtilizations.get(i) < lowerLimit) {
        expectedUnBalancedNodes.add(nodesInCluster.get(i));
      }
    }
    return expectedUnBalancedNodes;
  }

  /**
   * Generates a range of equally spaced utilization(that is, used / capacity)
   * values from 0 to 1.
   *
   * @param count Number of values to generate. Count must be greater than or
   *             equal to 1.
   * @throws IllegalArgumentException If the value of the parameter count is
   * less than 1.
   */
  private void generateUtilizations(int count) throws IllegalArgumentException {
    if (count < 1) {
      LOG.warn("The value of argument count is {}. However, count must be " +
          "greater than 0.", count);
      throw new IllegalArgumentException();
    }
    nodeUtilizations = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      nodeUtilizations.add(i / (double) count);
    }
  }

  /**
   * Creates DatanodeUsageInfo nodes using the generated utilization values.
   * Capacities are chosen randomly from a list.
   *
   * @return Average utilization of the created cluster.
   */
  private double createNodesInCluster() {
    this.numberOfNodes = 10;
    generateUtilizations(numberOfNodes);
    nodesInCluster = new ArrayList<>(nodeUtilizations.size());
    long[] capacities = {1000000, 2000000, 3000000, 4000000, 5000000};
    double totalUsed = 0, totalCapacity = 0;

    for (double utilization : nodeUtilizations) {
      // select a random index from 0 to capacities.length
      int index = ThreadLocalRandom.current().nextInt(0, capacities.length);
      long capacity = capacities[index];
      long used = (long) (capacity * utilization);
      totalCapacity += capacity;
      totalUsed += used;
      SCMNodeStat stat = new SCMNodeStat(capacity, used, capacity - used);

      nodesInCluster.add(
          new DatanodeUsageInfo(MockDatanodeDetails.randomDatanodeDetails(),
              stat));
    }
    return totalUsed / totalCapacity;
  }

}
