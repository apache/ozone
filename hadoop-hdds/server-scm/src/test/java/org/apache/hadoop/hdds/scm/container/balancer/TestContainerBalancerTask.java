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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.balancer.iteration.ContainerBalanceIteration;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.slf4j.event.Level;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;

/**
 * Tests for {@link ContainerBalancer}.
 */
public class TestContainerBalancerTask {
  @BeforeAll
  public static void setup() {
    GenericTestUtils.setLogLevel(ContainerBalancerTask.LOG, Level.DEBUG);
  }

  private static Stream<Arguments> createMockedSCMs() {
    return Stream.of(
        Arguments.of(MockedSCM.getMockedSCM(4)),
        Arguments.of(MockedSCM.getMockedSCM(5)),
        Arguments.of(MockedSCM.getMockedSCM(6)),
        Arguments.of(MockedSCM.getMockedSCM(7)),
        Arguments.of(MockedSCM.getMockedSCM(8)),
        Arguments.of(MockedSCM.getMockedSCM(9)),
        Arguments.of(MockedSCM.getMockedSCM(10)),
        Arguments.of(MockedSCM.getMockedSCM(17)),
        Arguments.of(MockedSCM.getMockedSCM(30)));
  }

  private static Stream<Arguments> createMockedSCMWithDatanodeLimits() {
    return Stream.of(
        // Doesn't make sense to set limits formit for maximum datanode count
        // per iteration for cluster of 4 nodes
        Arguments.of(MockedSCM.getMockedSCM(4), false),
        Arguments.of(MockedSCM.getMockedSCM(5), true),
        Arguments.of(MockedSCM.getMockedSCM(5), false),
        Arguments.of(MockedSCM.getMockedSCM(6), true),
        Arguments.of(MockedSCM.getMockedSCM(6), false),
        Arguments.of(MockedSCM.getMockedSCM(7), true),
        Arguments.of(MockedSCM.getMockedSCM(7), false),
        Arguments.of(MockedSCM.getMockedSCM(8), true),
        Arguments.of(MockedSCM.getMockedSCM(8), false),
        Arguments.of(MockedSCM.getMockedSCM(9), true),
        Arguments.of(MockedSCM.getMockedSCM(9), false),
        Arguments.of(MockedSCM.getMockedSCM(10), true),
        Arguments.of(MockedSCM.getMockedSCM(10), false),
        Arguments.of(MockedSCM.getMockedSCM(11), true),
        Arguments.of(MockedSCM.getMockedSCM(11), false),
        Arguments.of(MockedSCM.getMockedSCM(12), true),
        Arguments.of(MockedSCM.getMockedSCM(12), false),
        Arguments.of(MockedSCM.getMockedSCM(15), true),
        Arguments.of(MockedSCM.getMockedSCM(15), false),
        Arguments.of(MockedSCM.getMockedSCM(19), true),
        Arguments.of(MockedSCM.getMockedSCM(19), false),
        Arguments.of(MockedSCM.getMockedSCM(20), true),
        Arguments.of(MockedSCM.getMockedSCM(20), false));
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void testCalculationOfUtilization(@Nonnull MockedSCM mockedSCM) {
    TestableCluster cluster = mockedSCM.getCluster();
    DatanodeUsageInfo[] nodesInCluster = cluster.getNodesInCluster();
    double[] nodeUtilizationList = cluster.getNodeUtilizationList();
    assertEquals(nodesInCluster.length, nodeUtilizationList.length);
    for (int i = 0; i < nodesInCluster.length; i++) {
      assertEquals(nodeUtilizationList[i],
          nodesInCluster[i].calculateUtilization(), 0.0001);
    }
    // should be equal to average utilization of the cluster
    assertEquals(cluster.getAverageUtilization(),
        ContainerBalanceIteration.calculateAvgUtilization(
            Arrays.asList(nodesInCluster)), 0.0001);
  }

  /**
   * Checks whether ContainerBalancer is correctly updating the list of
   * unBalanced nodes with varying values of Threshold.
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void testBalancerTaskAfterChangingThresholdValue(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    // check for random threshold values
    ContainerBalancer balancer = mockedSCM.createContainerBalancer();
    TestableCluster cluster = mockedSCM.getCluster();
    for (int i = 0; i < 50; i++) {
      double randomThreshold = TestableCluster.RANDOM.nextDouble() * 100;

      List<DatanodeUsageInfo> expectedUnBalancedNodes =
          cluster.getUnBalancedNodes(randomThreshold);
      // sort unbalanced nodes as it is done inside ContainerBalancerTask:337
      //  in descending order by node utilization (most used node)
      expectedUnBalancedNodes.sort(
          (d1, d2) ->
              Double.compare(
                  d2.calculateUtilization(),
                  d1.calculateUtilization()
              )
      );

      ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
      config.setThreshold(randomThreshold);
      ContainerBalancerTask task =
          mockedSCM.startBalancerTask(balancer, config);

      List<DatanodeUsageInfo> actualUnBalancedNodes = new ArrayList<>();
      actualUnBalancedNodes.addAll(task.getOverUtilizedNodes());
      actualUnBalancedNodes.addAll(task.getUnderUtilizedNodes());

      assertEquals(
          expectedUnBalancedNodes.size(),
          actualUnBalancedNodes.size());

      for (int j = 0; j < expectedUnBalancedNodes.size(); j++) {
        assertEquals(
            expectedUnBalancedNodes.get(j).getDatanodeDetails(),
            actualUnBalancedNodes.get(j).getDatanodeDetails());
      }
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void testBalancerWithMoveManager(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws IOException, TimeoutException, NodeNotFoundException {
    mockedSCM.disableLegacyReplicationManager();
    mockedSCM.startBalancerTask(mockedSCM.getBalancerConfig());

    Mockito.verify(mockedSCM.getMoveManager(), atLeastOnce())
        .move(
            any(ContainerID.class),
            any(DatanodeDetails.class),
            any(DatanodeDetails.class));
    Mockito.verify(mockedSCM.getReplicationManager(), times(0))
        .move(
            any(ContainerID.class),
            any(DatanodeDetails.class),
            any(DatanodeDetails.class));
  }

  /**
   * Checks whether the list of unBalanced nodes is empty when the cluster is
   * balanced.
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void unBalancedNodesListShouldBeEmptyWhenClusterIsBalanced(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(99.99);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    ContainerBalancerMetrics metrics = task.getMetrics();
    assertFalse(stillHaveUnbalancedNodes(task));
    assertEquals(0, metrics.getNumDatanodesUnbalanced());
  }

  /**
   * ContainerBalancer should not involve more datanodes than the
   * maxDatanodesRatioToInvolvePerIteration limit.
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void containerBalancerShouldObeyMaxDatanodesToInvolveLimit(
      @Nonnull MockedSCM mockedSCM,
      boolean useDatanodeLimits
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    int percent = 40;
    config.setMaxDatanodesPercentageToInvolvePerIteration(percent);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setThreshold(1);
    config.setIterations(1);
    config.setAdaptBalanceWhenCloseToLimit(useDatanodeLimits);
    config.setAdaptBalanceWhenReachTheLimit(useDatanodeLimits);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);
    ContainerBalancerMetrics metrics = task.getMetrics();
    assertTrue(metrics.getNumDatanodesInvolvedInLatestIteration() > 0);
    if (useDatanodeLimits) {
      int number = percent * mockedSCM.getCluster().getNodeCount() / 100;
      assertFalse(task.getCountDatanodesInvolvedPerIteration() > number);
      assertFalse(metrics.getNumDatanodesInvolvedInLatestIteration() > number);
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void containerBalancerShouldSelectOnlyClosedContainers(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    // make all containers open, balancer should not select any of them
    Collection<ContainerInfo> containers = mockedSCM.getCluster().
        getCidToInfoMap().values();
    for (ContainerInfo containerInfo : containers) {
      containerInfo.setState(HddsProtos.LifeCycleState.OPEN);
    }

    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    // balancer should have identified unbalanced nodes
    assertTrue(stillHaveUnbalancedNodes(task));
    // no container should have been selected
    assertTrue(task.getContainerToSourceMap().isEmpty());
    /*
    Iteration result should be CAN_NOT_BALANCE_ANY_MORE because no container
    move is generated
     */
    assertTrue(canNotBalanceAnyMore(task));

    // now, close all containers
    for (ContainerInfo containerInfo : containers) {
      containerInfo.setState(HddsProtos.LifeCycleState.CLOSED);
    }
    mockedSCM.startBalancerTask(config);

    // check whether all selected containers are closed
    for (ContainerInfo cid : containers) {
      Assertions.assertSame(cid.getState(), HddsProtos.LifeCycleState.CLOSED);
    }
  }

  /**
   * Container Balancer should not select a non-CLOSED replica for moving.
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void balancerShouldNotSelectNonClosedContainerReplicas(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws IOException {
    // let's mock such that all replicas have CLOSING state
    ContainerManager containerManager = mockedSCM.getContainerManager();
    Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap =
        mockedSCM.getCluster().getCidToReplicasMap();
    Mockito
        .when(containerManager.getContainerReplicas(any(ContainerID.class)))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          Set<ContainerReplica> replicas = cidToReplicasMap.get(cid);
          Set<ContainerReplica> replicasToReturn =
              new HashSet<>(replicas.size());
          for (ContainerReplica replica : replicas) {
            ContainerReplica newReplica = replica.toBuilder()
                .setContainerState(ContainerReplicaProto.State.CLOSING)
                .build();
            replicasToReturn.add(newReplica);
          }
          return replicasToReturn;
        });

    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * MockedSCM.STORAGE_UNIT);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    // balancer should have identified unbalanced nodes
    assertTrue(stillHaveUnbalancedNodes(task));
    // no container should have moved because all replicas are CLOSING
    assertTrue(task.getContainerToSourceMap().isEmpty());
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void containerBalancerShouldObeyMaxSizeToMoveLimit(
      @Nonnull MockedSCM mockedSCM, boolean useDatanodeLimits
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(1);
    config.setMaxSizeToMovePerIteration(10 * MockedSCM.STORAGE_UNIT);
    config.setIterations(1);
    config.setMaxDatanodesPercentageToInvolvePerIteration(20);
    config.setAdaptBalanceWhenCloseToLimit(useDatanodeLimits);
    config.setAdaptBalanceWhenReachTheLimit(useDatanodeLimits);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    // balancer should not have moved more size than the limit
    long sizeForMove = task.getSizeScheduledForMoveInLatestIteration();
    assertFalse(sizeForMove > 10 * MockedSCM.STORAGE_UNIT);

    long size = task.getMetrics().getDataSizeMovedGBInLatestIteration();
    if (sizeForMove == 0) {
      assertEquals(0, size);
    } else {
      assertTrue(size > 0);
      assertFalse(size > 10);
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void targetDatanodeShouldNotAlreadyContainSelectedContainer(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap =
        mockedSCM.getCluster().getCidToReplicasMap();
    Map<ContainerID, DatanodeDetails> map = task.getContainerToTargetMap();
    for (Map.Entry<ContainerID, DatanodeDetails> entry : map.entrySet()) {
      ContainerID container = entry.getKey();
      DatanodeDetails target = entry.getValue();
      assertTrue(cidToReplicasMap.get(container)
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .noneMatch(target::equals));
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void containerMoveSelectionShouldFollowPlacementPolicy(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setIterations(1);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    TestableCluster cluster = mockedSCM.getCluster();
    Map<ContainerID, ContainerInfo> cidToInfoMap = cluster.getCidToInfoMap();

    Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap = cluster
        .getCidToReplicasMap();
    Map<ContainerID, DatanodeDetails> containerToTargetMap =
        task.getContainerToTargetMap();

    PlacementPolicy policy = mockedSCM.getPlacementPolicy();
    PlacementPolicy ecPolicy = mockedSCM.getEcPlacementPolicy();

    // for each move selection, check if {replicas - source + target}
    // satisfies placement policy
    Set<Map.Entry<ContainerID, DatanodeDetails>> cnToDnDetailsMap =
        task.getContainerToSourceMap().entrySet();
    for (Map.Entry<ContainerID, DatanodeDetails> entry : cnToDnDetailsMap) {
      ContainerID container = entry.getKey();
      DatanodeDetails source = entry.getValue();

      List<DatanodeDetails> replicas = cidToReplicasMap
          .get(container)
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());
      // remove source and add target
      replicas.remove(source);
      replicas.add(containerToTargetMap.get(container));

      ContainerInfo info = cidToInfoMap.get(container);
      int requiredNodes = info.getReplicationConfig().getRequiredNodes();
      ContainerPlacementStatus status =
          (info.getReplicationType() == HddsProtos.ReplicationType.RATIS)
              ? policy.validateContainerPlacement(replicas, requiredNodes)
              : ecPolicy.validateContainerPlacement(replicas, requiredNodes);

      assertTrue(status.isPolicySatisfied());
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void targetDatanodeShouldBeInServiceHealthy(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws NodeNotFoundException {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * MockedSCM.STORAGE_UNIT);
    config.setIterations(1);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    MockNodeManager nodeManager = mockedSCM.getNodeManager();
    for (DatanodeDetails target : task.getSelectedTargets()) {
      NodeStatus status = nodeManager.getNodeStatus(target);
      Assertions.assertSame(HddsProtos.NodeOperationalState.IN_SERVICE,
          status.getOperationalState());
      assertTrue(status.isHealthy());
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void selectedContainerShouldNotAlreadyHaveBeenSelected(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws IOException, NodeNotFoundException, TimeoutException {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * MockedSCM.STORAGE_UNIT);
    config.setIterations(1);

    mockedSCM.enableLegacyReplicationManager();
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    int numContainers = task.getContainerToTargetMap().size();

  /*
  Assuming move is called exactly once for each unique container, number of
   calls to move should equal number of unique containers. If number of
   calls to move is more than number of unique containers, at least one
   container has been re-selected. It's expected that number of calls to
   move should equal number of unique, selected containers (from
   containerToTargetMap).
   */
    Mockito.verify(mockedSCM.getReplicationManager(), times(numContainers))
        .move(
            any(ContainerID.class),
            any(DatanodeDetails.class),
            any(DatanodeDetails.class));
    /*
     Try the same test by disabling LegacyReplicationManager so that
     MoveManager is used.
     */
    mockedSCM.disableLegacyReplicationManager();
    task = mockedSCM.startBalancerTask(config);

    numContainers = task.getContainerToTargetMap().size();
    Mockito.verify(mockedSCM.getMoveManager(), times(numContainers))
        .move(
            any(ContainerID.class),
            any(DatanodeDetails.class),
            any(DatanodeDetails.class));
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void balancerShouldNotSelectConfiguredExcludeContainers(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * MockedSCM.STORAGE_UNIT);
    config.setExcludeContainers("1, 4, 5");

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    Set<ContainerID> excludeContainers = config.getExcludeContainers();
    for (ContainerID container : task.getContainerToSourceMap().keySet()) {
      assertFalse(excludeContainers.contains(container));
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void balancerShouldObeyMaxSizeEnteringTargetLimit(
      @Nonnull MockedSCM mockedSCM,
      boolean useDatanodeLimits
  ) {
    OzoneConfiguration ozoneConfig = mockedSCM.getOzoneConfig();
    ozoneConfig.set("ozone.scm.container.size", "1MB");
    ContainerBalancerConfiguration config =
        mockedSCM.getBalancerConfigByOzoneConfig(ozoneConfig);
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    config.setAdaptBalanceWhenCloseToLimit(useDatanodeLimits);
    config.setAdaptBalanceWhenReachTheLimit(useDatanodeLimits);

    // no containers should be selected when the limit is just 2 MB
    config.setMaxSizeEnteringTarget(2 * OzoneConsts.MB);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    assertTrue(stillHaveUnbalancedNodes(task));
    assertTrue(task.getContainerToSourceMap().isEmpty());

    // some containers should be selected when using default values
    ContainerBalancerConfiguration balancerConfig =
        mockedSCM.getBalancerConfigByOzoneConfig(new OzoneConfiguration());
    balancerConfig.setBalancingInterval(1);
    balancerConfig.setAdaptBalanceWhenCloseToLimit(useDatanodeLimits);
    balancerConfig.setAdaptBalanceWhenReachTheLimit(useDatanodeLimits);

    task = mockedSCM.startBalancerTask(balancerConfig);

    // balancer should have identified unbalanced nodes
    assertTrue(stillHaveUnbalancedNodes(task));
    if (canNotBalanceAnyMore(task)) {
      assertTrue(task.getContainerToSourceMap().isEmpty());
    } else {
      assertFalse(task.getContainerToSourceMap().isEmpty());
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void balancerShouldObeyMaxSizeLeavingSourceLimit(
      @Nonnull MockedSCM mockedSCM,
      boolean useDatanodeLimits
  ) {
    OzoneConfiguration ozoneConfig = mockedSCM.getOzoneConfig();
    ozoneConfig.set("ozone.scm.container.size", "1MB");
    ContainerBalancerConfiguration config =
        mockedSCM.getBalancerConfigByOzoneConfig(ozoneConfig);
    config.setThreshold(10);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMaxSizeToMovePerIteration(50 * MockedSCM.STORAGE_UNIT);
    config.setAdaptBalanceWhenCloseToLimit(useDatanodeLimits);
    config.setAdaptBalanceWhenReachTheLimit(useDatanodeLimits);

    // no source containers should be selected when the limit is just 2 MB
    config.setMaxSizeLeavingSource(2 * OzoneConsts.MB);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    assertTrue(stillHaveUnbalancedNodes(task));
    assertTrue(task.getContainerToSourceMap().isEmpty());

    // some containers should be selected when using default values
    ContainerBalancerConfiguration newBalancerConfig =
        mockedSCM.getBalancerConfigByOzoneConfig(new OzoneConfiguration());
    newBalancerConfig.setBalancingInterval(1);
    newBalancerConfig.setAdaptBalanceWhenCloseToLimit(useDatanodeLimits);
    newBalancerConfig.setAdaptBalanceWhenReachTheLimit(useDatanodeLimits);

    task = mockedSCM.startBalancerTask(newBalancerConfig);

    // balancer should have identified unbalanced nodes
    assertTrue(stillHaveUnbalancedNodes(task));

    if (canNotBalanceAnyMore(task)) {
      assertTrue(task.getContainerToSourceMap().isEmpty());
      assertEquals(0, task.getSizeScheduledForMoveInLatestIteration());
    } else {
      assertFalse(task.getContainerToSourceMap().isEmpty());
      assertTrue(0 != task.getSizeScheduledForMoveInLatestIteration());
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void testMetrics(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws IOException, NodeNotFoundException {
    OzoneConfiguration ozoneConfig = mockedSCM.getOzoneConfig();
    ozoneConfig.set("ozone.scm.container.size", "1MB");
    ContainerBalancerConfiguration config =
        mockedSCM.getBalancerConfigByOzoneConfig(ozoneConfig);
    config.setBalancingInterval(Duration.ofMillis(2));
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(6 * MockedSCM.STORAGE_UNIT);
    // deliberately set max size per iteration to a low value, 6 GB
    config.setMaxSizeToMovePerIteration(6 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);

    Mockito
        .when(mockedSCM.getMoveManager().move(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.COMPLETED));

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    ContainerBalancerMetrics metrics = task.getMetrics();
    assertEquals(
        mockedSCM.getCluster().getUnBalancedNodes(config.getThreshold()).size(),
        metrics.getNumDatanodesUnbalanced());
    assertTrue(metrics.getDataSizeMovedGBInLatestIteration() <= 6);
    assertTrue(metrics.getDataSizeMovedGB() > 0);
    assertEquals(1, metrics.getNumIterations());
    assertTrue(metrics.getNumContainerMovesScheduledInLatestIteration() > 0);
    assertEquals(metrics.getNumContainerMovesScheduled(),
        metrics.getNumContainerMovesScheduledInLatestIteration());
    assertEquals(metrics.getNumContainerMovesScheduled(),
        metrics.getNumContainerMovesCompleted() +
            metrics.getNumContainerMovesFailed() +
            metrics.getNumContainerMovesTimeout());
    assertEquals(0, metrics.getNumContainerMovesTimeout());
    assertEquals(1, metrics.getNumContainerMovesFailed());
  }

  /**
   * Tests if {@link ContainerBalancer} follows the includeNodes and
   * excludeNodes configurations in {@link ContainerBalancerConfiguration}.
   * If the includeNodes configuration is not empty, only the specified
   * includeNodes should be included in balancing. excludeNodes should be
   * excluded from balancing. If a datanode is specified in both include and
   * exclude configurations, then it should be excluded.
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void balancerShouldFollowExcludeAndIncludeDatanodesConfigurations(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(10 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);

    DatanodeUsageInfo[] nodesInCluster =
        mockedSCM.getCluster().getNodesInCluster();

    // only these nodes should be included
    // the ones also specified in excludeNodes should be excluded
    int firstIncludeIndex = 0, secondIncludeIndex = 1;
    int thirdIncludeIndex = nodesInCluster.length - 2;
    int fourthIncludeIndex = nodesInCluster.length - 1;
    String includeNodes = String.join(", ",
        nodesInCluster[firstIncludeIndex].getDatanodeDetails().getIpAddress(),
        nodesInCluster[secondIncludeIndex].getDatanodeDetails().getIpAddress(),
        nodesInCluster[thirdIncludeIndex].getDatanodeDetails().getHostName(),
        nodesInCluster[fourthIncludeIndex].getDatanodeDetails().getHostName());

    // these nodes should be excluded
    int firstExcludeIndex = 0, secondExcludeIndex = nodesInCluster.length - 1;
    String excludeNodes = String.join(", ",
        nodesInCluster[firstExcludeIndex].getDatanodeDetails().getIpAddress(),
        nodesInCluster[secondExcludeIndex].getDatanodeDetails().getHostName());

    config.setExcludeNodes(excludeNodes);
    config.setIncludeNodes(includeNodes);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    // finally, these should be the only nodes included in balancing
    // (included - excluded)
    DatanodeDetails dn1 =
        nodesInCluster[secondIncludeIndex].getDatanodeDetails();
    DatanodeDetails dn2 =
        nodesInCluster[thirdIncludeIndex].getDatanodeDetails();
    Map<ContainerID, DatanodeDetails> containerFromSourceMap =
        task.getContainerToSourceMap();
    Map<ContainerID, DatanodeDetails> containerToTargetMap =
        task.getContainerToTargetMap();
    for (Map.Entry<ContainerID, DatanodeDetails> entry :
        containerFromSourceMap.entrySet()) {
      DatanodeDetails source = entry.getValue();
      DatanodeDetails target = containerToTargetMap.get(entry.getKey());
      assertTrue(source.equals(dn1) || source.equals(dn2));
      assertTrue(target.equals(dn1) || target.equals(dn2));
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void testContainerBalancerConfiguration(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set("ozone.scm.container.size", "5GB");
    ozoneConfiguration.setDouble(
        "hdds.container.balancer.utilization.threshold", 1);
    long maxSizeLeavingSource = 26;
    ozoneConfiguration.setStorageSize(
        "hdds.container.balancer.size.leaving.source.max", maxSizeLeavingSource,
        StorageUnit.GB);
    long moveTimeout = 90;
    ozoneConfiguration.setTimeDuration("hdds.container.balancer.move.timeout",
        moveTimeout, TimeUnit.MINUTES);
    long replicationTimeout = 60;
    ozoneConfiguration.setTimeDuration(
        "hdds.container.balancer.move.replication.timeout",
        replicationTimeout, TimeUnit.MINUTES);

    ContainerBalancerConfiguration cbConf =
        mockedSCM.getBalancerConfigByOzoneConfig(ozoneConfiguration);
    assertEquals(1, cbConf.getThreshold(), 0.001);

    // Expected is 26 GB
    assertEquals(maxSizeLeavingSource * 1024 * 1024 * 1024,
        cbConf.getMaxSizeLeavingSource());
    assertEquals(moveTimeout, cbConf.getMoveTimeout().toMinutes());
    assertEquals(replicationTimeout,
        cbConf.getMoveReplicationTimeout().toMinutes());
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void checkIterationResult(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws NodeNotFoundException, IOException, TimeoutException {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(10 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);

    mockedSCM.enableLegacyReplicationManager();
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);
    /*
    According to the setup and configurations, this iteration's result should
    be ITERATION_COMPLETED.
     */
    assertEquals(ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
    /*
    Now, limit maxSizeToMovePerIteration but fail all container moves. The
    result should still be ITERATION_COMPLETED.
     */
    Mockito
        .when(mockedSCM.getReplicationManager()
            .move(
                any(ContainerID.class),
                any(DatanodeDetails.class),
                any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY));
    config.setMaxSizeToMovePerIteration(10 * MockedSCM.STORAGE_UNIT);

    mockedSCM.startBalancerTask(config);

    assertEquals(ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());

    /*
    Try the same but use MoveManager for container move instead of legacy RM.
     */
    mockedSCM.disableLegacyReplicationManager();
    mockedSCM.startBalancerTask(config);
    assertEquals(ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
  }

  /**
   * Tests the situation where some container moves time out because they
   * take longer than "move.timeout".
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void checkIterationResultTimeout(
      @Nonnull MockedSCM mockedSCM,
      boolean useDatanodeLimits
  ) throws NodeNotFoundException, IOException, TimeoutException {
    CompletableFuture<MoveManager.MoveResult> completedFuture =
        CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED);
    Mockito
        .when(mockedSCM.getReplicationManager()
            .move(
                any(ContainerID.class),
                any(DatanodeDetails.class),
                any(DatanodeDetails.class)))
        .thenReturn(completedFuture)
        .thenAnswer(invocation -> genCompletableFuture(2000, false));

    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(10 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMoveTimeout(Duration.ofMillis(500));
    config.setAdaptBalanceWhenCloseToLimit(useDatanodeLimits);
    config.setAdaptBalanceWhenReachTheLimit(useDatanodeLimits);

    mockedSCM.enableLegacyReplicationManager();
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    /*
    According to the setup and configurations, this iteration's result should
    be ITERATION_COMPLETED.
     */
    assertEquals(ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());

    ContainerBalancerMetrics metrics = task.getMetrics();
    assertEquals(1, metrics.getNumContainerMovesCompletedInLatestIteration());
    assertTrue(metrics.getNumContainerMovesTimeoutInLatestIteration() >= 1);

    /*
    Test the same but use MoveManager instead of LegacyReplicationManager.
    The first move being 10ms falls within the timeout duration of 500ms. It
    should be successful. The rest should fail.
     */
    mockedSCM.disableLegacyReplicationManager();
    Mockito
        .when(mockedSCM.getMoveManager()
            .move(
                any(ContainerID.class),
                any(DatanodeDetails.class),
                any(DatanodeDetails.class)))
        .thenReturn(completedFuture)
        .thenAnswer(invocation -> genCompletableFuture(2000, false));

    assertEquals(ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
    assertEquals(1, metrics.getNumContainerMovesCompletedInLatestIteration());
    assertTrue(metrics.getNumContainerMovesTimeoutInLatestIteration() >= 1);
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void checkIterationResultTimeoutFromReplicationManager(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws NodeNotFoundException, IOException, TimeoutException {
    CompletableFuture<MoveManager.MoveResult> future = CompletableFuture
        .supplyAsync(() -> MoveManager.MoveResult.REPLICATION_FAIL_TIME_OUT);
    CompletableFuture<MoveManager.MoveResult> future2 = CompletableFuture
        .supplyAsync(() -> MoveManager.MoveResult.DELETION_FAIL_TIME_OUT);

    Mockito
        .when(mockedSCM.getReplicationManager()
            .move(
                any(ContainerID.class),
                any(DatanodeDetails.class),
                any(DatanodeDetails.class)))
        .thenReturn(future, future2);

    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(10 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMoveTimeout(Duration.ofMillis(500));
    mockedSCM.enableLegacyReplicationManager();
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    ContainerBalancerMetrics metrics = task.getMetrics();
    assertTrue(metrics.getNumContainerMovesTimeoutInLatestIteration() > 0);
    assertEquals(0, metrics.getNumContainerMovesCompletedInLatestIteration());

    /*
    Try the same test with MoveManager instead of LegacyReplicationManager.
     */
    Mockito
        .when(mockedSCM.getMoveManager()
            .move(
                any(ContainerID.class),
                any(DatanodeDetails.class),
                any(DatanodeDetails.class)))
        .thenReturn(future)
        .thenAnswer(invocation -> future2);

    mockedSCM.disableLegacyReplicationManager();

    assertTrue(metrics.getNumContainerMovesTimeoutInLatestIteration() > 0);
    assertEquals(0, metrics.getNumContainerMovesCompletedInLatestIteration());
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void checkIterationResultException(
      @Nonnull MockedSCM mockedSCM,
      boolean useDatanodeLimits
  ) throws NodeNotFoundException, IOException, TimeoutException {
    CompletableFuture<MoveManager.MoveResult> future =
        new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException("Runtime Exception"));
    Mockito
        .when(mockedSCM.getReplicationManager()
            .move(
                any(ContainerID.class),
                any(DatanodeDetails.class),
                any(DatanodeDetails.class)))
        .thenReturn(genCompletableFuture(1, true))
        .thenThrow(new ContainerNotFoundException("Test Container not found"))
        .thenReturn(future);

    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(10 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    config.setMoveTimeout(Duration.ofMillis(500));
    config.setAdaptBalanceWhenCloseToLimit(useDatanodeLimits);
    config.setAdaptBalanceWhenReachTheLimit(useDatanodeLimits);

    mockedSCM.enableLegacyReplicationManager();
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    assertEquals(ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());

    int nodeCount = mockedSCM.getCluster().getNodeCount();
    int expectedMovesFailed = (nodeCount > 6) ? 3 : 1;
    assertTrue(task.getMetrics().getNumContainerMovesFailed()
        >= expectedMovesFailed);
    /*
    Try the same test but with MoveManager instead of ReplicationManager.
     */
    Mockito
        .when(mockedSCM.getMoveManager()
            .move(
                any(ContainerID.class),
                any(DatanodeDetails.class),
                any(DatanodeDetails.class)))
        .thenReturn(genCompletableFuture(1, true))
        .thenThrow(new ContainerNotFoundException("Test Container not found"))
        .thenReturn(future);

    mockedSCM.disableLegacyReplicationManager();
    task = mockedSCM.startBalancerTask(config);

    assertEquals(ContainerBalancerTask.IterationResult.ITERATION_COMPLETED,
        task.getIterationResult());
    assertTrue(task.getMetrics().getNumContainerMovesFailed()
            >= expectedMovesFailed);
  }

  @Unhealthy("HDDS-8941")
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void testDelayedStart(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) throws InterruptedException, TimeoutException {
    OzoneConfiguration ozoneConfig = mockedSCM.getOzoneConfig();
    ozoneConfig.setTimeDuration(
        "hdds.scm.wait.time.after.safemode.exit", 10, TimeUnit.SECONDS);
    ContainerBalancerConfiguration config =
        mockedSCM.getBalancerConfigByOzoneConfig(ozoneConfig);

    StorageContainerManager scm = mockedSCM.getStorageContainerManager();
    ContainerBalancer balancer = new ContainerBalancer(scm);
    ContainerBalancerTask task =
        new ContainerBalancerTask(scm, balancer, config);
    Thread balancingThread = new Thread(() -> task.run(2, true));
    // start the thread and assert that balancer is RUNNING
    balancingThread.start();
    assertEquals(ContainerBalancerTask.Status.RUNNING,
        task.getBalancerStatus());

    /*
     Wait for the thread to start sleeping and assert that it's sleeping.
     This is the delay before it starts balancing.
     */
    GenericTestUtils.waitFor(
        () -> balancingThread.getState() == Thread.State.TIMED_WAITING, 1, 20);
    assertEquals(
        Thread.State.TIMED_WAITING, balancingThread.getState());

    // interrupt the thread from its sleep, wait and assert that balancer has
    // STOPPED
    balancingThread.interrupt();
    GenericTestUtils.waitFor(() -> task.getBalancerStatus() ==
        ContainerBalancerTask.Status.STOPPED, 1, 20);
    assertEquals(ContainerBalancerTask.Status.STOPPED,
        task.getBalancerStatus());

    // ensure the thread dies
    GenericTestUtils.waitFor(() -> !balancingThread.isAlive(), 1, 20);
    assertFalse(balancingThread.isAlive());
  }

  /**
   * The expectation is that only RATIS containers should be selected for
   * balancing when LegacyReplicationManager is enabled. This is because
   * LegacyReplicationManager does not support moving EC containers.
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource("createMockedSCMWithDatanodeLimits")
  public void balancerShouldExcludeECContainersWhenLegacyRmIsEnabled(
      @Nonnull MockedSCM mockedSCM,
      boolean ignored
  ) {
    // Enable LegacyReplicationManager
    mockedSCM.enableLegacyReplicationManager();
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(10 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    /*
     Get all containers that were selected by balancer and assert none of
     them is an EC container.
     */
    Map<ContainerID, DatanodeDetails> containerToSource =
        task.getContainerToSourceMap();
    assertFalse(containerToSource.isEmpty());
    Map<ContainerID, ContainerInfo> cidToInfoMap =
        mockedSCM.getCluster().getCidToInfoMap();
    for (ContainerID containerID : containerToSource.keySet()) {
      ContainerInfo containerInfo = cidToInfoMap.get(containerID);
      Assertions.assertNotSame(HddsProtos.ReplicationType.EC,
          containerInfo.getReplicationType());
    }
  }

  private static
      @Nonnull CompletableFuture<MoveManager.MoveResult> genCompletableFuture(
          int sleepMilSec, boolean doThrowException
  ) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        Thread.sleep(sleepMilSec);
      } catch (InterruptedException e) {
        if (doThrowException) {
          throw new RuntimeException("Runtime Exception after doing work");
        } else {
          e.printStackTrace();
        }
      }
      return MoveManager.MoveResult.COMPLETED;
    });
  }

  private static boolean canNotBalanceAnyMore(
      @Nonnull ContainerBalancerTask task
  ) {
    return task.getIterationResult() ==
        ContainerBalancerTask.IterationResult.CAN_NOT_BALANCE_ANY_MORE;
  }
  private static boolean stillHaveUnbalancedNodes(
      @Nonnull ContainerBalancerTask task
  ) {
    return !task.getOverUtilizedNodes().isEmpty() ||
        !task.getUnderUtilizedNodes().isEmpty();
  }
}
