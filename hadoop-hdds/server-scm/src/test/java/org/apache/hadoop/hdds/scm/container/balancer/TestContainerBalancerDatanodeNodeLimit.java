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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    config.setThreshold(1);
    config.setIterations(1);

    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);
    ContainerBalancerMetrics metrics = task.getMetrics();

    int maxDatanodePercentage = 40;
    int nodeCount = mockedSCM.getCluster().getNodeCount();
    int number =
        (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) ? nodeCount : maxDatanodePercentage * nodeCount / 100;
    int datanodesInvolvedPerIteration = task.getCountDatanodesInvolvedPerIteration();
    assertThat(datanodesInvolvedPerIteration).isGreaterThan(0);
    assertThat(datanodesInvolvedPerIteration).isLessThanOrEqualTo(number);
    assertThat(metrics.getNumDatanodesInvolvedInLatestIteration()).isLessThanOrEqualTo(number);
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void balancerShouldObeyMaxSizeEnteringTargetLimit(@Nonnull MockedSCM mockedSCM) {
    OzoneConfiguration ozoneConfig = mockedSCM.getOzoneConfig();
    ozoneConfig.set("ozone.scm.container.size", "1MB");
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfigByOzoneConfig(ozoneConfig);
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);

    // No containers should be selected when the limit is just 2 MB.
    config.setMaxSizeEnteringTarget(2 * OzoneConsts.MB);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    assertTrue(stillHaveUnbalancedNodes(task));
    assertTrue(task.getContainerToSourceMap().isEmpty());

    // Some containers should be selected when using default values.
    ContainerBalancerConfiguration balancerConfig = mockedSCM.getBalancerConfigByOzoneConfig(new OzoneConfiguration());
    balancerConfig.setBalancingInterval(1);

    task = mockedSCM.startBalancerTask(balancerConfig);

    // Balancer should have identified unbalanced nodes.
    assertTrue(stillHaveUnbalancedNodes(task));
    if (canNotBalanceAnyMore(task)) {
      assertTrue(task.getContainerToSourceMap().isEmpty());
    } else {
      assertFalse(task.getContainerToSourceMap().isEmpty());
    }
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}")
  @MethodSource("createMockedSCMs")
  public void balancerShouldObeyMaxSizeLeavingSourceLimit(@Nonnull MockedSCM mockedSCM) {
    OzoneConfiguration ozoneConfig = mockedSCM.getOzoneConfig();
    ozoneConfig.set("ozone.scm.container.size", "1MB");
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfigByOzoneConfig(ozoneConfig);
    config.setThreshold(10);
    config.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);

    // No source containers should be selected when the limit is just 2 MB.
    config.setMaxSizeLeavingSource(2 * OzoneConsts.MB);
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    assertTrue(stillHaveUnbalancedNodes(task));
    assertTrue(task.getContainerToSourceMap().isEmpty());

    // Some containers should be selected when using default values.
    ContainerBalancerConfiguration newBalancerConfig =
        mockedSCM.getBalancerConfigByOzoneConfig(new OzoneConfiguration());
    newBalancerConfig.setBalancingInterval(1);

    task = mockedSCM.startBalancerTask(newBalancerConfig);

    // Balancer should have identified unbalanced nodes.
    assertTrue(stillHaveUnbalancedNodes(task));

    if (canNotBalanceAnyMore(task)) {
      assertTrue(task.getContainerToSourceMap().isEmpty());
      assertEquals(0, task.getSizeScheduledForMoveInLatestIteration());
    } else {
      assertFalse(task.getContainerToSourceMap().isEmpty());
      assertNotEquals(0, task.getSizeScheduledForMoveInLatestIteration());
    }
  }

  private static boolean canNotBalanceAnyMore(@Nonnull ContainerBalancerTask task) {
    return task.getIterationResult() == ContainerBalancerTask.IterationResult.CAN_NOT_BALANCE_ANY_MORE;
  }

  private static boolean stillHaveUnbalancedNodes(@Nonnull ContainerBalancerTask task) {
    return !task.getUnBalancedNodes().isEmpty();
  }

  public static @Nonnull MockedSCM getMockedSCM(int datanodeCount) {
    TestableCluster cluster = new TestableCluster(datanodeCount, STORAGE_UNIT);
    return new MockedSCM(cluster, createBalancerConfig(datanodeCount));
  }

  private static @Nonnull ContainerBalancerConfiguration createBalancerConfig(int nodeCount) {
    ContainerBalancerConfiguration balancerCfg =
        new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);
    balancerCfg.setThreshold(10);
    balancerCfg.setIterations(1);
    if (nodeCount < DATANODE_COUNT_LIMIT_FOR_SMALL_CLUSTER) {
      balancerCfg.setMaxDatanodesPercentageToInvolvePerIteration(100);
    }
    balancerCfg.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    balancerCfg.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);
    return balancerCfg;
  }
}
