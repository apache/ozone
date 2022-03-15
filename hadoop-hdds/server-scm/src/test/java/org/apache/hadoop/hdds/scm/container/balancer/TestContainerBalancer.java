/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicyFactory;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;

/**
 * Tests for {@link ContainerBalancer}.
 */
public class TestContainerBalancer {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerBalancer.class);

  private ReplicationManager replicationManager;
  private ContainerManager containerManager;
  private ContainerBalancer containerBalancer;
  private MockNodeManager mockNodeManager;
  private OzoneConfiguration conf;
  private PlacementPolicy placementPolicy;
  private ContainerBalancerConfiguration balancerConfiguration;
  private List<DatanodeUsageInfo> nodesInCluster;
  private List<Double> nodeUtilizations;
  private double averageUtilization;
  private int numberOfNodes;
  private Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap =
      new HashMap<>();
  private Map<ContainerID, ContainerInfo> cidToInfoMap = new HashMap<>();
  private Map<DatanodeUsageInfo, Set<ContainerID>> datanodeToContainersMap =
      new HashMap<>();
  private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  /**
   * Sets up configuration values and creates a mock cluster.
   */
  @Before
  public void setup() throws SCMException, NodeNotFoundException {
    conf = new OzoneConfiguration();
    containerManager = Mockito.mock(ContainerManager.class);
    replicationManager = Mockito.mock(ReplicationManager.class);

    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);
    balancerConfiguration.setMaxSizeEnteringTarget(50 * OzoneConsts.GB);
    conf.setFromObject(balancerConfiguration);
    GenericTestUtils.setLogLevel(ContainerBalancer.LOG, Level.DEBUG);

    averageUtilization = createCluster();
    mockNodeManager = new MockNodeManager(datanodeToContainersMap);

    placementPolicy = ContainerPlacementPolicyFactory
        .getPolicy(conf, mockNodeManager,
            mockNodeManager.getClusterNetworkTopologyMap(), true,
            SCMContainerPlacementMetrics.create());

    Mockito.when(replicationManager
        .isContainerReplicatingOrDeleting(Mockito.any(ContainerID.class)))
        .thenReturn(false);

    Mockito.when(replicationManager.move(Mockito.any(ContainerID.class),
        Mockito.any(DatanodeDetails.class),
        Mockito.any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.completedFuture(
            ReplicationManager.MoveResult.COMPLETED));

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
        replicationManager, conf, SCMContext.emptyContext(),
        new NetworkTopologyImpl(conf), placementPolicy);
  }

  @Test
  public void testCalculationOfUtilization() {
    Assert.assertEquals(nodesInCluster.size(), nodeUtilizations.size());
    for (int i = 0; i < nodesInCluster.size(); i++) {
      Assert.assertEquals(nodeUtilizations.get(i),
          nodesInCluster.get(i).calculateUtilization(), 0.0001);
    }

    // should be equal to average utilization of the cluster
    Assert.assertEquals(averageUtilization,
        containerBalancer.calculateAvgUtilization(nodesInCluster), 0.0001);
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
      double randomThreshold = RANDOM.nextDouble() * 100;

      balancerConfiguration.setThreshold(randomThreshold);
      startBalancer(balancerConfiguration);

      // waiting for balance completed.
      // TODO: this is a temporary implementation for now
      // modify this after balancer is fully completed
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) { }

      expectedUnBalancedNodes =
          determineExpectedUnBalancedNodes(randomThreshold);
      unBalancedNodesAccordingToBalancer =
          containerBalancer.getUnBalancedNodes();

      containerBalancer.stopBalancer();
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
    balancerConfiguration.setThreshold(99.99);
    startBalancer(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) { }

    containerBalancer.stopBalancer();
    Assert.assertEquals(0, containerBalancer.getUnBalancedNodes().size());
  }

  /**
   * ContainerBalancer should not involve more datanodes than the
   * maxDatanodesRatioToInvolvePerIteration limit.
   */
  @Test
  public void containerBalancerShouldObeyMaxDatanodesToInvolveLimit() {
    int percent = 20;
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(
        percent);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * OzoneConsts.GB);
    balancerConfiguration.setThreshold(1);
    balancerConfiguration.setIterations(1);
    startBalancer(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) { }

    Assert.assertFalse(
        containerBalancer.getCountDatanodesInvolvedPerIteration() >
            (percent * numberOfNodes / 100));
    containerBalancer.stopBalancer();
  }

  @Test
  public void containerBalancerShouldSelectOnlyClosedContainers() {
    // make all containers open, balancer should not select any of them
    for (ContainerInfo containerInfo : cidToInfoMap.values()) {
      containerInfo.setState(HddsProtos.LifeCycleState.OPEN);
    }
    balancerConfiguration.setThreshold(10);
    startBalancer(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) { }

    containerBalancer.stopBalancer();

    // balancer should have identified unbalanced nodes
    Assert.assertFalse(containerBalancer.getUnBalancedNodes().isEmpty());
    // no container should have been selected
    Assert.assertTrue(containerBalancer.getSourceToTargetMap().isEmpty());

    // now, close all containers
    for (ContainerInfo containerInfo : cidToInfoMap.values()) {
      containerInfo.setState(HddsProtos.LifeCycleState.CLOSED);
    }
    startBalancer(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) { }

    containerBalancer.stopBalancer();
    // check whether all selected containers are closed
    for (ContainerMoveSelection moveSelection:
         containerBalancer.getSourceToTargetMap().values()) {
      Assert.assertSame(
          cidToInfoMap.get(moveSelection.getContainerID()).getState(),
          HddsProtos.LifeCycleState.CLOSED);
    }
  }

  @Test
  public void containerBalancerShouldObeyMaxSizeToMoveLimit() {
    balancerConfiguration.setThreshold(1);
    balancerConfiguration.setMaxSizeToMovePerIteration(10 * OzoneConsts.GB);
    balancerConfiguration.setIterations(1);
    startBalancer(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) { }

    // balancer should not have moved more size than the limit
    Assert.assertFalse(containerBalancer.getSizeMovedPerIteration() >
        10 * OzoneConsts.GB);
    containerBalancer.stopBalancer();
  }

  @Test
  public void targetDatanodeShouldNotAlreadyContainSelectedContainer() {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * OzoneConsts.GB);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    startBalancer(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) { }

    containerBalancer.stopBalancer();
    Map<DatanodeDetails, ContainerMoveSelection> sourceToTargetMap =
        containerBalancer.getSourceToTargetMap();
    for (ContainerMoveSelection moveSelection : sourceToTargetMap.values()) {
      ContainerID container = moveSelection.getContainerID();
      DatanodeDetails target = moveSelection.getTargetNode();
      Assert.assertTrue(cidToReplicasMap.get(container)
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .noneMatch(target::equals));
    }
  }

  @Test
  public void containerMoveSelectionShouldFollowPlacementPolicy() {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    startBalancer(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) { }

    containerBalancer.stopBalancer();
    Map<DatanodeDetails, ContainerMoveSelection> sourceToTargetMap =
        containerBalancer.getSourceToTargetMap();

    // for each move selection, check if {replicas - source + target}
    // satisfies placement policy
    for (Map.Entry<DatanodeDetails, ContainerMoveSelection> entry :
        sourceToTargetMap.entrySet()) {
      ContainerMoveSelection moveSelection = entry.getValue();
      ContainerID container = moveSelection.getContainerID();
      DatanodeDetails target = moveSelection.getTargetNode();

      List<DatanodeDetails> replicas = cidToReplicasMap.get(container)
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());
      replicas.remove(entry.getKey());
      replicas.add(target);

      ContainerInfo containerInfo = cidToInfoMap.get(container);
      ContainerPlacementStatus placementStatus =
          placementPolicy.validateContainerPlacement(replicas,
              containerInfo.getReplicationConfig().getRequiredNodes());
      Assert.assertTrue(placementStatus.isPolicySatisfied());
    }
  }

  @Test
  public void targetDatanodeShouldBeInServiceHealthy()
      throws NodeNotFoundException {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);
    balancerConfiguration.setMaxSizeEnteringTarget(50 * OzoneConsts.GB);
    startBalancer(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }

    containerBalancer.stopBalancer();
    for (ContainerMoveSelection moveSelection :
        containerBalancer.getSourceToTargetMap().values()) {
      DatanodeDetails target = moveSelection.getTargetNode();
      NodeStatus status = mockNodeManager.getNodeStatus(target);
      Assert.assertSame(HddsProtos.NodeOperationalState.IN_SERVICE,
          status.getOperationalState());
      Assert.assertTrue(status.isHealthy());
    }
  }

  @Test
  public void selectedContainerShouldNotAlreadyHaveBeenSelected() {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);
    balancerConfiguration.setMaxSizeEnteringTarget(50 * OzoneConsts.GB);

    startBalancer(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) { }

    containerBalancer.stopBalancer();
    Set<ContainerID> containers = new HashSet<>();
    for (ContainerMoveSelection moveSelection :
        containerBalancer.getSourceToTargetMap().values()) {
      ContainerID container = moveSelection.getContainerID();
      Assert.assertFalse(containers.contains(container));
      containers.add(container);
    }
  }

  @Test
  public void balancerShouldNotSelectConfiguredExcludeContainers() {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);
    balancerConfiguration.setMaxSizeEnteringTarget(50 * OzoneConsts.GB);
    balancerConfiguration.setExcludeContainers("1, 4, 5");

    startBalancer(balancerConfiguration);

    // waiting for balance completed.
    // TODO: this is a temporary implementation for now
    // modify this after balancer is fully completed
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) { }

    containerBalancer.stopBalancer();
    Set<ContainerID> excludeContainers =
        balancerConfiguration.getExcludeContainers();
    for (ContainerMoveSelection moveSelection :
        containerBalancer.getSourceToTargetMap().values()) {
      ContainerID container = moveSelection.getContainerID();
      Assert.assertFalse(excludeContainers.contains(container));
    }
  }

  @Test
  public void balancerShouldObeyMaxSizeEnteringTargetLimit() {
    conf.set("ozone.scm.container.size", "1MB");
    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);

    // no containers should be selected when the limit is just 2 MB
    balancerConfiguration.setMaxSizeEnteringTarget(2 * OzoneConsts.MB);
    startBalancer(balancerConfiguration);
    sleepWhileBalancing(500);

    Assert.assertFalse(containerBalancer.getUnBalancedNodes().isEmpty());
    Assert.assertTrue(containerBalancer.getSourceToTargetMap().isEmpty());
    containerBalancer.stopBalancer();

    // some containers should be selected when using default values
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ContainerBalancerConfiguration cbc = ozoneConfiguration.
        getObject(ContainerBalancerConfiguration.class);
    startBalancer(cbc);

    sleepWhileBalancing(500);

    containerBalancer.stopBalancer();
    // balancer should have identified unbalanced nodes
    Assert.assertFalse(containerBalancer.getUnBalancedNodes().isEmpty());
    Assert.assertFalse(containerBalancer.getSourceToTargetMap().isEmpty());
  }

  @Test
  public void testMetrics() {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(10 * OzoneConsts.GB);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * OzoneConsts.GB);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);

    startBalancer(balancerConfiguration);

    sleepWhileBalancing(500);

    containerBalancer.stopBalancer();
    ContainerBalancerMetrics metrics = containerBalancer.getMetrics();
    Assert.assertEquals(determineExpectedUnBalancedNodes(
            balancerConfiguration.getThreshold()).size(),
        metrics.getDatanodesNumToBalance());
    Assert.assertEquals(ContainerBalancer.ratioToPercent(
            nodeUtilizations.get(nodeUtilizations.size() - 1)),
        metrics.getMaxDatanodeUtilizedPercentage());
  }

  /**
   * Tests if {@link ContainerBalancer} follows the includeNodes and
   * excludeNodes configurations in {@link ContainerBalancerConfiguration}.
   * If the includeNodes configuration is not empty, only the specified
   * includeNodes should be included in balancing. excludeNodes should be
   * excluded from balancing. If a datanode is specified in both include and
   * exclude configurations, then it should be excluded.
   */
  @Test
  public void balancerShouldFollowExcludeAndIncludeDatanodesConfigurations() {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(10 * OzoneConsts.GB);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * OzoneConsts.GB);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);

    // only these nodes should be included
    // the ones also specified in excludeNodes should be excluded
    int firstIncludeIndex = 0, secondIncludeIndex = 1;
    int thirdIncludeIndex = nodesInCluster.size() - 2;
    int fourthIncludeIndex = nodesInCluster.size() - 1;
    String includeNodes =
        nodesInCluster.get(firstIncludeIndex).getDatanodeDetails()
            .getIpAddress() + ", " +
            nodesInCluster.get(secondIncludeIndex).getDatanodeDetails()
                .getIpAddress() + ", " +
            nodesInCluster.get(thirdIncludeIndex).getDatanodeDetails()
                .getHostName() + ", " +
            nodesInCluster.get(fourthIncludeIndex).getDatanodeDetails()
                .getHostName();

    // these nodes should be excluded
    int firstExcludeIndex = 0, secondExcludeIndex = nodesInCluster.size() - 1;
    String excludeNodes =
        nodesInCluster.get(firstExcludeIndex).getDatanodeDetails()
            .getIpAddress() + ", " +
            nodesInCluster.get(secondExcludeIndex).getDatanodeDetails()
                .getHostName();

    balancerConfiguration.setExcludeNodes(excludeNodes);
    balancerConfiguration.setIncludeNodes(includeNodes);
    startBalancer(balancerConfiguration);
    sleepWhileBalancing(500);
    containerBalancer.stopBalancer();

    // finally, these should be the only nodes included in balancing
    // (included - excluded)
    DatanodeDetails dn1 =
        nodesInCluster.get(secondIncludeIndex).getDatanodeDetails();
    DatanodeDetails dn2 =
        nodesInCluster.get(thirdIncludeIndex).getDatanodeDetails();
    for (Map.Entry<DatanodeDetails, ContainerMoveSelection> entry :
        containerBalancer.getSourceToTargetMap().entrySet()) {
      DatanodeDetails source = entry.getKey();
      DatanodeDetails target = entry.getValue().getTargetNode();
      Assert.assertTrue(source.equals(dn1) || source.equals(dn2));
      Assert.assertTrue(target.equals(dn1) || target.equals(dn2));
    }
  }

  @Test
  public void testContainerBalancerConfiguration() {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set("ozone.scm.container.size", "5GB");
    ozoneConfiguration.setDouble(
        "hdds.container.balancer.utilization.threshold", 1);

    ContainerBalancerConfiguration cbConf =
        ozoneConfiguration.getObject(ContainerBalancerConfiguration.class);
    Assert.assertEquals(1, cbConf.getThreshold(), 0.001);

    Assert.assertEquals(26 * 1024 * 1024 * 1024L,
        cbConf.getMaxSizeLeavingSource());

    Assert.assertEquals(30 * 60 * 1000,
        cbConf.getMoveTimeout().toMillis());
  }

  /**
   * Determines unBalanced nodes, that is, over and under utilized nodes,
   * according to the generated utilization values for nodes and the threshold.
   *
   * @param threshold A percentage in the range 0 to 100
   * @return List of DatanodeUsageInfo containing the expected(correct)
   * unBalanced nodes.
   */
  private List<DatanodeUsageInfo> determineExpectedUnBalancedNodes(
      double threshold) {
    threshold /= 100;
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
   * Create an unbalanced cluster by generating some data. Nodes in the
   * cluster have utilization values determined by generateUtilizations method.
   * @return average utilization (used space / capacity) of the cluster
   */
  private double createCluster() {
    generateData();
    createReplicasForContainers();
    long clusterCapacity = 0, clusterUsedSpace = 0;

    // for each node utilization, calculate that datanode's used space and
    // capacity
    for (int i = 0; i < nodeUtilizations.size(); i++) {
      long datanodeUsedSpace = 0, datanodeCapacity = 0;
      Set<ContainerID> containerIDSet =
          datanodeToContainersMap.get(nodesInCluster.get(i));

      for (ContainerID containerID : containerIDSet) {
        datanodeUsedSpace += cidToInfoMap.get(containerID).getUsedBytes();
      }

      // use node utilization and used space to determine node capacity
      if (nodeUtilizations.get(i) == 0) {
        datanodeCapacity = OzoneConsts.GB * RANDOM.nextInt(10, 60);
      } else {
        datanodeCapacity = (long) (datanodeUsedSpace / nodeUtilizations.get(i));
      }
      SCMNodeStat stat = new SCMNodeStat(datanodeCapacity, datanodeUsedSpace,
          datanodeCapacity - datanodeUsedSpace);
      nodesInCluster.get(i).setScmNodeStat(stat);
      clusterUsedSpace += datanodeUsedSpace;
      clusterCapacity += datanodeCapacity;
    }
    return (double) clusterUsedSpace / clusterCapacity;
  }

  /**
   * Create some datanodes and containers for each node.
   */
  private void generateData() {
    this.numberOfNodes = 10;
    generateUtilizations(numberOfNodes);
    nodesInCluster = new ArrayList<>(nodeUtilizations.size());

    // create datanodes and add containers to them
    for (int i = 0; i < numberOfNodes; i++) {
      Set<ContainerID> containerIDSet = new HashSet<>();
      DatanodeUsageInfo usageInfo =
          new DatanodeUsageInfo(MockDatanodeDetails.randomDatanodeDetails(),
              new SCMNodeStat());

      // create containers with varying used space
      int sizeMultiple = 0;
      for (int j = 0; j < i; j++) {
        sizeMultiple %= 5;
        sizeMultiple++;
        ContainerInfo container =
            createContainer((long) i * i + j, sizeMultiple);

        cidToInfoMap.put(container.containerID(), container);
        containerIDSet.add(container.containerID());

        // create initial replica for this container and add it
        Set<ContainerReplica> containerReplicaSet = new HashSet<>();
        containerReplicaSet.add(createReplica(container.containerID(),
            usageInfo.getDatanodeDetails(), container.getUsedBytes()));
        cidToReplicasMap.put(container.containerID(), containerReplicaSet);
      }
      nodesInCluster.add(usageInfo);
      datanodeToContainersMap.put(usageInfo, containerIDSet);
    }
  }

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

  /**
   * Create the required number of replicas for each container. Note that one
   * replica already exists and nodes with utilization value 0 should not
   * have any replicas.
   */
  private void createReplicasForContainers() {
    for (ContainerInfo container : cidToInfoMap.values()) {

      // one replica already exists; create the remaining ones
      for (int i = 0;
           i < container.getReplicationConfig().getRequiredNodes() - 1; i++) {

        // randomly pick a datanode for this replica
        int datanodeIndex = RANDOM.nextInt(0, numberOfNodes);
        if (nodeUtilizations.get(i) != 0.0d) {
          DatanodeDetails node =
              nodesInCluster.get(datanodeIndex).getDatanodeDetails();
          Set<ContainerReplica> replicas =
              cidToReplicasMap.get(container.containerID());
          replicas.add(createReplica(container.containerID(), node,
              container.getUsedBytes()));
          cidToReplicasMap.put(container.containerID(), replicas);
        }
      }
    }
  }

  private ContainerReplica createReplica(ContainerID containerID,
                                         DatanodeDetails datanodeDetails,
                                         long usedBytes) {
    return ContainerReplica.newBuilder()
        .setContainerID(containerID)
        .setContainerState(ContainerReplicaProto.State.CLOSED)
        .setDatanodeDetails(datanodeDetails)
        .setOriginNodeId(datanodeDetails.getUuid())
        .setSequenceId(1000L)
        .setBytesUsed(usedBytes)
        .build();
  }

  private void sleepWhileBalancing(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
  }

  private void startBalancer(ContainerBalancerConfiguration config) {
    containerBalancer.setConfig(config);
    try {
      containerBalancer.startBalancer();
    } catch (IllegalContainerBalancerStateException |
        InvalidContainerBalancerConfigurationException e) {
      LOG.info("Could not start ContainerBalancer while testing", e);
    }
  }

}
