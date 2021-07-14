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

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementStatusDefault;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.mockito.Mockito.when;

/**
 * Tests for {@link ContainerBalancer}.
 */
public class TestContainerBalancer {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerBalancer.class);

  private ReplicationManager replicationManager;
  private ContainerManagerV2 containerManager;
  private ContainerStateManager containerStateManager;
  private ContainerBalancer containerBalancer;
  private MockNodeManager mockNodeManager;
  private OzoneConfiguration conf;
  private PlacementPolicy placementPolicy;
  private ContainerPlacementStatus placementStatus;
  private ContainerBalancerConfiguration balancerConfiguration;
  private List<DatanodeUsageInfo> nodesInCluster;
  private List<Double> nodeUtilizations;
  private double averageUtilization;
  private int numberOfNodes;
  private Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap =
      new HashMap<>();
  private Map<ContainerID, ContainerInfo> cidToInfoMap = new HashMap<>();
  private Map<DatanodeUsageInfo, Set<ContainerID>> datanodeToCidMap =
      new HashMap<>();
  private static final ThreadLocalRandom random = ThreadLocalRandom.current();

  /**
   * Sets up configuration values and creates a mock cluster.
   */
  @Before
  public void setup() throws ContainerNotFoundException {
    conf = new OzoneConfiguration();
    containerManager = Mockito.mock(ContainerManagerV2.class);
    replicationManager = Mockito.mock(ReplicationManager.class);
    placementPolicy = Mockito.mock(PlacementPolicy.class);
    placementStatus = Mockito.mock(ContainerPlacementStatus.class);

    balancerConfiguration = new ContainerBalancerConfiguration();
    balancerConfiguration.setThreshold(0.1);
    balancerConfiguration.setIdleIteration(1);
    balancerConfiguration.setMaxDatanodesToBalance(10);
    balancerConfiguration.setMaxSizeToMove(500 * OzoneConsts.GB);
    balancerConfiguration.setMaxSizeEnteringTarget(8 * OzoneConsts.GB);
    conf.setFromObject(balancerConfiguration);

    // create datanodes with the generated nodeUtilization values
    this.averageUtilization = generateData();
    
    mockNodeManager = new MockNodeManager(datanodeToCidMap);

    Mockito.when(placementPolicy.validateContainerPlacement(
        Mockito.anyListOf(DatanodeDetails.class),
        Mockito.anyInt()))
        .thenAnswer(invocation -> new ContainerPlacementStatusDefault(2, 2, 3));

    Mockito.when(placementStatus.isPolicySatisfied()).thenReturn(true);

    Mockito.when(replicationManager
        .isContainerReplicatingOrDeleting(Mockito.any(ContainerID.class)))
        .thenReturn(false);

    when(containerManager.getContainerReplicas(Mockito.any(ContainerID.class)))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          return cidToReplicasMap.get(cid);
        });

    when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          return cidToInfoMap.get(cid);
        });

    when(containerManager.getContainers())
        .thenReturn(new ArrayList<>(cidToInfoMap.values()));

    containerBalancer = new ContainerBalancer(mockNodeManager, containerManager,
        replicationManager, conf, SCMContext.emptyContext(), placementPolicy);
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

//      double randomThreshold = 0.34347427272493825;
      balancerConfiguration.setThreshold(randomThreshold);
      containerBalancer.start(balancerConfiguration);

      // waiting for balance completed.
      // TODO: this is a temporary implementation for now
      // modify this after balancer is fully completed
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {}

      expectedUnBalancedNodes =
          determineExpectedUnBalancedNodes(randomThreshold);
      unBalancedNodesAccordingToBalancer =
          containerBalancer.getUnBalancedNodes();

      containerBalancer.stop();

      Assert.assertEquals(
          expectedUnBalancedNodes.size(),
          unBalancedNodesAccordingToBalancer.size());

      for (int j = 0; j < expectedUnBalancedNodes.size(); j++) {
        Assert.assertEquals(expectedUnBalancedNodes.get(j).getDatanodeDetails(),
            unBalancedNodesAccordingToBalancer.get(j).getDatanodeDetails());
      }
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

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {}

    containerBalancer.stop();
    Assert.assertEquals(0, containerBalancer.getUnBalancedNodes().size());
  }

  /**
   * Checks whether ContainerBalancer stops when the limit of
   * MaxDatanodesToBalance is reached.
   */
  @Test
  public void containerBalancerShouldStopWhenMaxDatanodesToBalanceIsReached() {
    balancerConfiguration.setMaxDatanodesToBalance(4);
    balancerConfiguration.setThreshold(0.01);
    containerBalancer.start(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {}

    Assert.assertFalse(containerBalancer.isBalancerRunning());
    containerBalancer.stop();
  }

  @Test
  public void containerBalancerShouldSelectOnlyClosedContainers() {
    // make all containers open, balancer should not select any of them
    for (ContainerInfo containerInfo : cidToInfoMap.values()) {
      containerInfo.setState(HddsProtos.LifeCycleState.OPEN);
    }
    balancerConfiguration.setThreshold(0.1);
    containerBalancer.start(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {}

    containerBalancer.stop();

    // balancer should have identified unbalanced nodes
    Assert.assertFalse(containerBalancer.getUnBalancedNodes().isEmpty());
    // no container should have been selected
    Assert.assertTrue(containerBalancer.getSourceToTargetMap().isEmpty());

    // now, close all containers
    for (ContainerInfo containerInfo : cidToInfoMap.values()) {
      containerInfo.setState(HddsProtos.LifeCycleState.CLOSED);
    }
    containerBalancer.start(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {}

    containerBalancer.stop();
    for (ContainerMoveSelection moveSelection:
         containerBalancer.getSourceToTargetMap().values()) {
      Assert.assertSame(
          cidToInfoMap.get(moveSelection.getContainerID()).getState(),
          HddsProtos.LifeCycleState.CLOSED);
    }
  }

  @Test
  public void containerBalancerShouldStopWhenMaxSizeToMoveIsReached() {
    balancerConfiguration.setThreshold(0.01);
    balancerConfiguration.setMaxSizeToMove(10 * OzoneConsts.GB);
    containerBalancer.start(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {}

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
      int index = random.nextInt(0, capacities.length);
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

//  private void createContainersForNodes() {
//    for (int i = 0; i < nodeUtilizations.size(); i++) {
//      Set<ContainerID> containerIDSet = new HashSet<>();
//
//      // each datanode contains container proportional to its utilization value
//      // here, more container
//      int numContainersPerNode = (int) (nodeUtilizations.get(i) * 10d);
//      for (int j = 0; j < numContainersPerNode; j++) {
//        ContainerInfo container = createContainer(numContainersPerNode * i + j);
//        cidToInfoMap.put(container.containerID(), container);
//        containerIDSet.add(container.containerID());
//      }
//
//      try {
//        mockNodeManager
//            .setContainers(nodesInCluster.get(i).getDatanodeDetails(),
//                containerIDSet);
//      } catch (NodeNotFoundException e) {
//        LOG.warn("Could not find Datanode {} while creating containers for " +
//                "it. Removing it from the list of nodes in cluster.",
//            nodesInCluster.get(i).getDatanodeDetails().getUuidString(), e);
//        nodesInCluster.remove(i);
//        nodeUtilizations.remove(i);
//        containerIDSet
//            .forEach(containerID -> cidToInfoMap.remove(containerID));
//      }
//    }
//  }

  private ContainerInfo createContainer(long id, int multiple) {
    return new ContainerInfo.Builder()
        .setContainerID(id)
        .setReplicationConfig(
            new RatisReplicationConfig(HddsProtos.ReplicationFactor.THREE))
        .setState(HddsProtos.LifeCycleState.CLOSED)
        .setOwner("TestContainerBalancer")
        .setUsedBytes(OzoneConsts.GB * multiple)
        .build();
  }

  private double generateData() {
    this.numberOfNodes = 10;
    generateUtilizations(numberOfNodes);
    nodesInCluster = new ArrayList<>(nodeUtilizations.size());
    long clusterCapacity = 0, clusterUsedSpace = 0;

    // for each utilization value, create a datanode and add containers to it
    // such that the datanode has that utilization value
    for (int i = 0; i < nodeUtilizations.size(); i++) {
      Set<ContainerID> containerIDSet = new HashSet<>();

      // each datanode contains number of containers proportional to its
      // utilization value
      int numContainersPerNode = (int) (nodeUtilizations.get(i) * 10d);
      int sizeMultiple = 0;
      long datanodeUsedSpace = 0, datanodeCapacity = 0;

      // create containers with varying used space size
      for (int j = 0; j < numContainersPerNode; j++) {
        sizeMultiple %= 5;
        sizeMultiple++;
        ContainerInfo container =
            createContainer((long) numContainersPerNode * i + j, sizeMultiple);
        datanodeUsedSpace += container.getUsedBytes();
        cidToInfoMap.put(container.containerID(), container);
        containerIDSet.add(container.containerID());
        cidToReplicasMap.put(container.containerID(), new HashSet<>());
      }

      // create a datanode using node utilization to calculate capacity
      if (nodeUtilizations.get(i) == 0) {
        datanodeCapacity = OzoneConsts.GB * random.nextInt(10,60);
      } else {
        datanodeCapacity = (long) (datanodeUsedSpace / nodeUtilizations.get(i));
      }
      SCMNodeStat stat = new SCMNodeStat(datanodeCapacity, datanodeUsedSpace,
          datanodeCapacity - datanodeUsedSpace);
      nodesInCluster.add(
          new DatanodeUsageInfo(MockDatanodeDetails.randomDatanodeDetails(),
              stat));
      datanodeToCidMap
          .put(nodesInCluster.get(nodesInCluster.size() - 1), containerIDSet);

      clusterCapacity += datanodeCapacity;
      clusterUsedSpace += datanodeUsedSpace;
    }
    return (double) clusterUsedSpace / clusterCapacity;
  }

}
