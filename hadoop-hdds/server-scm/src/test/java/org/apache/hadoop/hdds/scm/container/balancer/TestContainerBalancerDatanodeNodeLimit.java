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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.event.Level;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  private static boolean stillHaveUnbalancedNodes(@Nonnull ContainerBalancerTask task) {
    return !task.getUnBalancedNodes().isEmpty();
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
