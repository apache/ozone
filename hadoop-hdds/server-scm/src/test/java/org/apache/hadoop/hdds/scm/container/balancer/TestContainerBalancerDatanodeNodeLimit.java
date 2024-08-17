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

import jakarta.annotation.Nonnull;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.event.Level;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.scm.container.balancer.TestableCluster.RANDOM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ContainerBalancerTask} moved from {@link TestContainerBalancerTask} to run them on clusters
 * with different datanode count.
 */
public class TestContainerBalancerDatanodeNodeLimit {
  private static final long STORAGE_UNIT = OzoneConsts.GB;
  private static final int DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER = 15;

  @BeforeAll
  public static void setup() {
    GenericTestUtils.setLogLevel(ContainerBalancerTask.LOG, Level.DEBUG);
  }

  private static Stream<Arguments> createMockedSCMs() {
    return Stream.of(
        Arguments.of(getMockedSCM(4)),
        Arguments.of(getMockedSCM(5)),
        Arguments.of(getMockedSCM(6)),
        Arguments.of(getMockedSCM(7)),
        Arguments.of(getMockedSCM(8)),
        Arguments.of(getMockedSCM(9)),
        Arguments.of(getMockedSCM(10)),
        Arguments.of(getMockedSCM(11)),
        Arguments.of(getMockedSCM(12)),
        Arguments.of(getMockedSCM(13)),
        Arguments.of(getMockedSCM(14)),
        Arguments.of(getMockedSCM(15)),
        Arguments.of(getMockedSCM(17)),
        Arguments.of(getMockedSCM(19)),
        Arguments.of(getMockedSCM(20)),
        Arguments.of(getMockedSCM(30)));
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void containerBalancerShouldObeyMaxDatanodesToInvolveLimit(@Nonnull MockedSCM mockedSCM) {
    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setIterations(1);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);
    ContainerBalancerMetrics metrics = task.getMetrics();

    int maxDatanodePercentage = config.getMaxDatanodesPercentageToInvolvePerIteration();
    int number = maxDatanodePercentage * nodeCount / 100;
    int datanodesInvolvedPerIteration = task.getCountDatanodesInvolvedPerIteration();
    assertThat(datanodesInvolvedPerIteration).isGreaterThan(0);
    assertThat(datanodesInvolvedPerIteration).isLessThanOrEqualTo(number);
    long numDatanodesInvolvedInLatestIteration = metrics.getNumDatanodesInvolvedInLatestIteration();
    assertThat(numDatanodesInvolvedInLatestIteration).isGreaterThan(0);
    assertThat(numDatanodesInvolvedInLatestIteration).isLessThanOrEqualTo(number);
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void balancerShouldObeyMaxSizeEnteringTargetLimit(@Nonnull MockedSCM mockedSCM) {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set("ozone.scm.container.size", "1MB");
    ContainerBalancerConfiguration config = balancerConfigByOzoneConfig(ozoneConfig);
    if (mockedSCM.getCluster().getNodeCount() < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setIterations(1);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    // No containers should be selected when the limit is just 2 MB.
    config.setMaxSizeEnteringTarget(2 * OzoneConsts.MB);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);
    // Container balancer still has unbalanced nodes due to MaxSizeEnteringTarget limit
    assertTrue(stillHaveUnbalancedNodes(task));
    // ContainerToSourceMap is empty due to MaxSizeEnteringTarget limit
    assertTrue(task.getContainerToSourceMap().isEmpty());
    // SizeScheduledForMoveInLatestIteration equals to 0 because there are no containers was selected
    assertEquals(0, task.getSizeScheduledForMoveInLatestIteration());

    // Some containers should be selected when using default values.
    ContainerBalancerConfiguration balancerConfig = balancerConfigByOzoneConfig(new OzoneConfiguration());
    if (mockedSCM.getCluster().getNodeCount() < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      balancerConfig.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    balancerConfig.setIterations(1);

    task = mockedSCM.startBalancerTask(balancerConfig);
    // Balancer should have identified unbalanced nodes.
    assertTrue(stillHaveUnbalancedNodes(task));
    // ContainerToSourceMap is not empty due to some containers should be selected
    assertFalse(task.getContainerToSourceMap().isEmpty());
    // SizeScheduledForMoveInLatestIteration doesn't equal to 0 because some containers should be selected
    assertNotEquals(0, task.getSizeScheduledForMoveInLatestIteration());
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void balancerShouldObeyMaxSizeLeavingSourceLimit(@Nonnull MockedSCM mockedSCM) {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set("ozone.scm.container.size", "1MB");
    ContainerBalancerConfiguration config = balancerConfigByOzoneConfig(ozoneConfig);
    if (mockedSCM.getCluster().getNodeCount() < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    // No source containers should be selected when the limit is just 2 MB.
    config.setMaxSizeLeavingSource(2 * OzoneConsts.MB);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);
    // Container balancer still has unbalanced nodes due to MaxSizeLeavingSource limit
    assertTrue(stillHaveUnbalancedNodes(task));
    // ContainerToSourceMap is empty due to MaxSizeLeavingSource limit
    assertTrue(task.getContainerToSourceMap().isEmpty());
    // SizeScheduledForMoveInLatestIteration equals to 0 because there are no containers was selected
    assertEquals(0, task.getSizeScheduledForMoveInLatestIteration());

    // Some containers should be selected when using default values.
    ContainerBalancerConfiguration newBalancerConfig = balancerConfigByOzoneConfig(new OzoneConfiguration());
    if (mockedSCM.getCluster().getNodeCount() < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      newBalancerConfig.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    newBalancerConfig.setIterations(1);

    task = mockedSCM.startBalancerTask(newBalancerConfig);
    // Balancer should have identified unbalanced nodes.
    assertTrue(stillHaveUnbalancedNodes(task));
    // ContainerToSourceMap is not empty due to some containers should be selected
    assertFalse(task.getContainerToSourceMap().isEmpty());
    // SizeScheduledForMoveInLatestIteration doesn't equal to 0 because some containers should be selected
    assertNotEquals(0, task.getSizeScheduledForMoveInLatestIteration());
  }

  /**
   * Checks whether ContainerBalancer is correctly updating the list of
   * unBalanced nodes with varying values of Threshold.
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void initializeIterationShouldUpdateUnBalancedNodesWhenThresholdChanges(@Nonnull MockedSCM mockedSCM) {
    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);

    // check for random threshold values
    for (int i = 0; i < 50; i++) {
      double randomThreshold = RANDOM.nextDouble() * 100;
      List<DatanodeUsageInfo> expectedUnBalancedNodes = mockedSCM.getCluster().getUnBalancedNodes(randomThreshold);
      config.setThreshold(randomThreshold);

      ContainerBalancerTask task = mockedSCM.startBalancerTask(config);
      List<DatanodeUsageInfo> unBalancedNodesAccordingToBalancer = getUnBalancedNodes(task);

      assertEquals(expectedUnBalancedNodes.size(), unBalancedNodesAccordingToBalancer.size());

      for (int j = 0; j < expectedUnBalancedNodes.size(); j++) {
        assertEquals(expectedUnBalancedNodes.get(j).getDatanodeDetails(),
            unBalancedNodesAccordingToBalancer.get(j).getDatanodeDetails());
      }
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void testCalculationOfUtilization(@Nonnull MockedSCM mockedSCM) {
    TestableCluster cluster = mockedSCM.getCluster();
    DatanodeUsageInfo[] nodesInCluster = cluster.getNodesInCluster();
    double[] nodeUtilizations = cluster.getNodeUtilizationList();
    assertEquals(nodesInCluster.length, nodeUtilizations.length);
    for (int i = 0; i < nodesInCluster.length; i++) {
      assertEquals(nodeUtilizations[i], nodesInCluster[i].calculateUtilization(), 0.0001);
    }

    // should be equal to average utilization of the cluster
    assertEquals(cluster.getAverageUtilization(),
        ContainerBalancerTask.calculateAvgUtilization(Arrays.asList(nodesInCluster)), 0.0001);
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void testBalancerWithMoveManager(@Nonnull MockedSCM mockedSCM)
      throws IOException, NodeNotFoundException, TimeoutException {
    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);

    mockedSCM.disableLegacyReplicationManager();
    mockedSCM.startBalancerTask(config);

    verify(mockedSCM.getMoveManager(), atLeastOnce()).
        move(any(ContainerID.class), any(DatanodeDetails.class), any(DatanodeDetails.class));

    verify(mockedSCM.getReplicationManager(), times(0))
        .move(any(ContainerID.class), any(DatanodeDetails.class), any(DatanodeDetails.class));
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void unBalancedNodesListShouldBeEmptyWhenClusterIsBalanced(@Nonnull MockedSCM mockedSCM) {
    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);
    config.setThreshold(99.99);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);
    ContainerBalancerMetrics metrics = task.getMetrics();
    assertEquals(0, getUnBalancedNodes(task).size());
    assertEquals(0, metrics.getNumDatanodesUnbalanced());
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  @Flaky("HDDS-11093")
  public void testMetrics(@Nonnull MockedSCM mockedSCM) throws IOException, NodeNotFoundException {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set("hdds.datanode.du.refresh.period", "1ms");
    ContainerBalancerConfiguration config = balancerConfigByOzoneConfig(ozoneConfig);
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setBalancingInterval(Duration.ofMillis(2));
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(6 * STORAGE_UNIT);
    // deliberately set max size per iteration to a low value, 6 GB
    config.setMaxSizeToMovePerIteration(6 * STORAGE_UNIT);

    when(mockedSCM.getMoveManager().move(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY))
        .thenReturn(CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED));
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    ContainerBalancerMetrics metrics = task.getMetrics();
    assertEquals(mockedSCM.getCluster().getUnBalancedNodes(config.getThreshold()).size(),
        metrics.getNumDatanodesUnbalanced());
    assertThat(metrics.getDataSizeMovedGBInLatestIteration()).isLessThanOrEqualTo(6);
    assertThat(metrics.getDataSizeMovedGB()).isGreaterThan(0);
    assertEquals(1, metrics.getNumIterations());
    assertThat(metrics.getNumContainerMovesScheduledInLatestIteration()).isGreaterThan(0);
    assertEquals(metrics.getNumContainerMovesScheduled(), metrics.getNumContainerMovesScheduledInLatestIteration());
    assertEquals(metrics.getNumContainerMovesScheduled(),
        metrics.getNumContainerMovesCompleted() +
            metrics.getNumContainerMovesFailed() +
            metrics.getNumContainerMovesTimeout());
    assertEquals(0, metrics.getNumContainerMovesTimeout());
    assertEquals(1, metrics.getNumContainerMovesFailed());
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void containerBalancerShouldSelectOnlyClosedContainers(@Nonnull MockedSCM mockedSCM) {
    ContainerBalancerConfiguration config = balancerConfigByOzoneConfig(new OzoneConfiguration());
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setIterations(1);
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);

    Map<ContainerID, ContainerInfo> cidToInfoMap = mockedSCM.getCluster().getCidToInfoMap();
    // Make all containers open, balancer should not select any of them
    for (ContainerInfo containerInfo : cidToInfoMap.values()) {
      containerInfo.setState(HddsProtos.LifeCycleState.OPEN);
    }

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    // Balancer should have identified unbalanced nodes
    assertFalse(TestContainerBalancerDatanodeNodeLimit.getUnBalancedNodes(task).isEmpty());
    // No container should have been selected
    assertTrue(task.getContainerToSourceMap().isEmpty());

    // Iteration result should be CAN_NOT_BALANCE_ANY_MORE because no container move is generated
    assertEquals(ContainerBalancerTask.IterationResult.CAN_NOT_BALANCE_ANY_MORE, task.getIterationResult());

    // Now, close all containers
    for (ContainerInfo containerInfo : cidToInfoMap.values()) {
      containerInfo.setState(HddsProtos.LifeCycleState.CLOSED);
    }
    ContainerBalancerTask nextTask = mockedSCM.startBalancerTask(config);

    // Check whether all selected containers are closed
    for (ContainerID cid: nextTask.getContainerToSourceMap().keySet()) {
      assertSame(cidToInfoMap.get(cid).getState(), HddsProtos.LifeCycleState.CLOSED);
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void balancerShouldNotSelectNonClosedContainerReplicas(@Nonnull MockedSCM mockedSCM)
      throws ContainerNotFoundException {
    ContainerBalancerConfiguration config = balancerConfigByOzoneConfig(new OzoneConfiguration());
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setIterations(1);
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);

    // Let's mock such that all replicas have CLOSING state
    Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap = mockedSCM.getCluster().getCidToReplicasMap();
    when(mockedSCM.getContainerManager().getContainerReplicas(any(ContainerID.class)))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          Set<ContainerReplica> replicas = cidToReplicasMap.get(cid);
          Set<ContainerReplica> replicasToReturn = new HashSet<>(replicas.size());
          for (ContainerReplica replica : replicas) {
            ContainerReplica newReplica = replica.toBuilder()
                .setContainerState(StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSING)
                .build();
            replicasToReturn.add(newReplica);
          }

          return replicasToReturn;
        });

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    // Balancer should have identified unbalanced nodes
    assertFalse(TestContainerBalancerDatanodeNodeLimit.getUnBalancedNodes(task).isEmpty());
    // No container should have moved because all replicas are CLOSING
    assertTrue(task.getContainerToSourceMap().isEmpty());
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void containerBalancerShouldObeyMaxSizeToMoveLimit(@Nonnull MockedSCM mockedSCM) {
    ContainerBalancerConfiguration config = balancerConfigByOzoneConfig(new OzoneConfiguration());
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setIterations(1);
    config.setThreshold(1);
    config.setMaxSizeToMovePerIteration(10 * STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(10 * STORAGE_UNIT);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    // Balancer should not have moved more size than the limit
    assertThat(task.getSizeScheduledForMoveInLatestIteration()).isLessThanOrEqualTo(10 * STORAGE_UNIT);

    long size = task.getMetrics().getDataSizeMovedGBInLatestIteration();
    assertThat(size).isGreaterThan(0);
    assertThat(size).isLessThanOrEqualTo(10);
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void targetDatanodeShouldNotAlreadyContainSelectedContainer(@Nonnull MockedSCM mockedSCM) {
    ContainerBalancerConfiguration config = balancerConfigByOzoneConfig(new OzoneConfiguration());
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setIterations(1);
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    Map<ContainerID, DatanodeDetails> map = task.getContainerToTargetMap();
    Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap = mockedSCM.getCluster().getCidToReplicasMap();
    for (Map.Entry<ContainerID, DatanodeDetails> entry : map.entrySet()) {
      ContainerID container = entry.getKey();
      DatanodeDetails target = entry.getValue();
      assertTrue(cidToReplicasMap.get(container)
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .noneMatch(target::equals));
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void containerMoveSelectionShouldFollowPlacementPolicy(@Nonnull MockedSCM mockedSCM) {
    ContainerBalancerConfiguration config = balancerConfigByOzoneConfig(new OzoneConfiguration());
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setIterations(1);
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    Map<ContainerID, DatanodeDetails> containerFromSourceMap = task.getContainerToSourceMap();
    Map<ContainerID, DatanodeDetails> containerToTargetMap = task.getContainerToTargetMap();

    // For each move selection, check if {replicas - source + target} satisfies placement policy
    for (Map.Entry<ContainerID, DatanodeDetails> entry : containerFromSourceMap.entrySet()) {
      ContainerID container = entry.getKey();
      DatanodeDetails source = entry.getValue();

      List<DatanodeDetails> replicas = mockedSCM.getCluster().getCidToReplicasMap().get(container)
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());
      // Remove source and add target
      replicas.remove(source);
      replicas.add(containerToTargetMap.get(container));

      ContainerInfo containerInfo = mockedSCM.getCluster().getCidToInfoMap().get(container);
      ContainerPlacementStatus placementStatus;
      int requiredNodes = containerInfo.getReplicationConfig().getRequiredNodes();
      if (containerInfo.getReplicationType() == HddsProtos.ReplicationType.RATIS) {
        placementStatus = mockedSCM.getPlacementPolicy().validateContainerPlacement(replicas, requiredNodes);
      } else {
        placementStatus = mockedSCM.getEcPlacementPolicy().validateContainerPlacement(replicas, requiredNodes);
      }
      assertTrue(placementStatus.isPolicySatisfied());
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void targetDatanodeShouldBeInServiceHealthy(@Nonnull MockedSCM mockedSCM) throws NodeNotFoundException {
    ContainerBalancerConfiguration config = balancerConfigByOzoneConfig(new OzoneConfiguration());
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setIterations(1);
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    for (DatanodeDetails target : task.getSelectedTargets()) {
      NodeStatus status = mockedSCM.getNodeManager().getNodeStatus(target);
      assertSame(HddsProtos.NodeOperationalState.IN_SERVICE, status.getOperationalState());
      assertTrue(status.isHealthy());
    }
  }


  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void selectedContainerShouldNotAlreadyHaveBeenSelected(@Nonnull MockedSCM mockedSCM)
      throws NodeNotFoundException, ContainerNotFoundException, TimeoutException, ContainerReplicaNotFoundException {
    ContainerBalancerConfiguration config = balancerConfigByOzoneConfig(new OzoneConfiguration());
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setIterations(1);
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);

    mockedSCM.enableLegacyReplicationManager();

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);
    int numContainers = task.getContainerToTargetMap().size();

    /* Assuming move is called exactly once for each unique container, number of  calls to move should equal number of
     unique containers. If number of  calls to move is more than number of unique containers, at least one container
     has been re-selected. It's expected that number of calls to move should equal number of unique, selected containers
      (from containerToTargetMap).
     */
    verify(mockedSCM.getReplicationManager(), times(numContainers))
        .move(any(ContainerID.class), any(DatanodeDetails.class), any(DatanodeDetails.class));

    // Try the same test by disabling LegacyReplicationManager so that MoveManager is used.
    mockedSCM.disableLegacyReplicationManager();
    ContainerBalancerTask nextTask = mockedSCM.startBalancerTask(config);

    numContainers = nextTask.getContainerToTargetMap().size();
    verify(mockedSCM.getMoveManager(), times(numContainers))
        .move(any(ContainerID.class), any(DatanodeDetails.class), any(DatanodeDetails.class));
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void balancerShouldNotSelectConfiguredExcludeContainers(@Nonnull MockedSCM mockedSCM) {
    ContainerBalancerConfiguration config = balancerConfigByOzoneConfig(new OzoneConfiguration());
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    config.setIterations(1);
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);
    config.setExcludeContainers("1, 4, 5");

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    Set<ContainerID> excludeContainers = config.getExcludeContainers();
    for (ContainerID container : task.getContainerToSourceMap().keySet()) {
      assertThat(excludeContainers).doesNotContain(container);
    }
  }

  public static List<DatanodeUsageInfo> getUnBalancedNodes(@Nonnull ContainerBalancerTask task) {
    ArrayList<DatanodeUsageInfo> result = new ArrayList<>();
    result.addAll(task.getOverUtilizedNodes());
    result.addAll(task.getUnderUtilizedNodes());
    return result;
  }

  private static boolean stillHaveUnbalancedNodes(@Nonnull ContainerBalancerTask task) {
    return !getUnBalancedNodes(task).isEmpty();
  }

  public static @Nonnull MockedSCM getMockedSCM(int datanodeCount) {
    return new MockedSCM(new TestableCluster(datanodeCount, STORAGE_UNIT));
  }

  private static @Nonnull ContainerBalancerConfiguration balancerConfigByOzoneConfig(
      @Nonnull OzoneConfiguration ozoneConfiguration
  ) {
    return ozoneConfiguration.getObject(ContainerBalancerConfiguration.class);
  }
}
