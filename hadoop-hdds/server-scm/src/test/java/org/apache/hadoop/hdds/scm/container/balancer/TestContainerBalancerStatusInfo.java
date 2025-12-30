/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.balancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ContainerBalancerStatusInfo}.
 */
class TestContainerBalancerStatusInfo {

  @Test
  void testGetIterationStatistics() {
    MockedSCM mockedScm = new MockedSCM(new TestableCluster(20, OzoneConsts.GB));

    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);

    config.setIterations(2);
    config.setBalancingInterval(0);
    config.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);

    ContainerBalancerTask task = mockedScm.startBalancerTask(config);
    List<ContainerBalancerTaskIterationStatusInfo> iterationStatistics = task.getCurrentIterationsStatistic();
    assertEquals(2, iterationStatistics.size());

    ContainerBalancerTaskIterationStatusInfo iterationHistory1 = iterationStatistics.get(0);
    verifyCompletedIteration(iterationHistory1, 1);

    ContainerBalancerTaskIterationStatusInfo iterationHistory2 = iterationStatistics.get(1);
    verifyCompletedIteration(iterationHistory2, 2);
  }

  @Test
  void testReRequestIterationStatistics() throws Exception {
    MockedSCM mockedScm = new MockedSCM(new TestableCluster(20, OzoneConsts.GB));

    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);

    config.setIterations(2);
    config.setBalancingInterval(0);
    config.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);

    ContainerBalancerTask task = mockedScm.startBalancerTask(config);
    List<ContainerBalancerTaskIterationStatusInfo> firstRequestIterationStatistics =
        task.getCurrentIterationsStatistic();
    Thread.sleep(1000L);
    List<ContainerBalancerTaskIterationStatusInfo> secondRequestIterationStatistics =
        task.getCurrentIterationsStatistic();
    assertEquals(firstRequestIterationStatistics.get(0), secondRequestIterationStatistics.get(0));
    assertEquals(firstRequestIterationStatistics.get(1), secondRequestIterationStatistics.get(1));
  }

  @Test
  void testGetCurrentStatisticsRequestInPeriodBetweenIterations() throws Exception {
    MockedSCM mockedScm = new MockedSCM(new TestableCluster(20, OzoneConsts.GB));

    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);

    config.setIterations(2);
    config.setBalancingInterval(10000);
    config.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);

    ContainerBalancerTask task = mockedScm.startBalancerTaskAsync(config, false);
    LambdaTestUtils.await(5000, 10,
        () -> task.getCurrentIterationsStatistic().size() == 1 &&
              "ITERATION_COMPLETED".equals(task.getCurrentIterationsStatistic().get(0).getIterationResult()));
    List<ContainerBalancerTaskIterationStatusInfo> iterationsStatic = task.getCurrentIterationsStatistic();
    assertEquals(1, iterationsStatic.size());

    ContainerBalancerTaskIterationStatusInfo firstIteration = iterationsStatic.get(0);
    verifyCompletedIteration(firstIteration, 1);
  }

  @Test
  void testCurrentStatisticsDoesntChangeWhenReRequestInPeriodBetweenIterations() throws InterruptedException {
    MockedSCM mockedScm = new MockedSCM(new TestableCluster(20, OzoneConsts.GB));

    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);

    config.setIterations(2);
    config.setBalancingInterval(10000);
    config.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);

    ContainerBalancerTask task = mockedScm.startBalancerTaskAsync(config, false);
    // Delay in finishing the first iteration
    Thread.sleep(1000L);
    List<ContainerBalancerTaskIterationStatusInfo> firstRequestIterationStatistics =
        task.getCurrentIterationsStatistic();
    // Delay occurred for some time during the period between iterations.
    Thread.sleep(1000L);
    List<ContainerBalancerTaskIterationStatusInfo> secondRequestIterationStatistics =
        task.getCurrentIterationsStatistic();
    assertEquals(1, firstRequestIterationStatistics.size());
    assertEquals(1, secondRequestIterationStatistics.size());
    assertEquals(firstRequestIterationStatistics.get(0), secondRequestIterationStatistics.get(0));
  }

  @Test
  void testGetCurrentStatisticsWithDelay() throws Exception {
    MockedSCM mockedScm = new MockedSCM(new TestableCluster(20, OzoneConsts.GB));

    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);

    config.setIterations(2);
    config.setBalancingInterval(0);
    config.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "1");
    ContainerBalancerTask task = mockedScm.startBalancerTaskAsync(config, configuration, true);
    // Delay in finishing the first iteration
    LambdaTestUtils.await(1100, 1, () -> task.getCurrentIterationsStatistic().size() == 1);
    List<ContainerBalancerTaskIterationStatusInfo> iterationsStatic = task.getCurrentIterationsStatistic();
    assertEquals(1, iterationsStatic.size());
    ContainerBalancerTaskIterationStatusInfo currentIteration = iterationsStatic.get(0);
    verifyStartedEmptyIteration(currentIteration);
  }

  @Test
  void testGetCurrentStatisticsWhileBalancingInProgress() throws Exception {
    MockedSCM mockedScm = new MockedSCM(new TestableCluster(20, OzoneConsts.GB));

    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);

    config.setIterations(3);
    config.setBalancingInterval(0);
    config.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);

    ContainerBalancerTask task = mockedScm.startBalancerTaskAsync(config, false);
    // Get the current iteration statistics when it has information about the containers moving.
    LambdaTestUtils.await(5000, 1,
        () -> task.getCurrentIterationsStatistic().size() == 2 &&
              task.getCurrentIterationsStatistic().get(1).getContainerMovesScheduled() > 0);
    List<ContainerBalancerTaskIterationStatusInfo> iterationsStatic = task.getCurrentIterationsStatistic();
    assertEquals(2, iterationsStatic.size());
    ContainerBalancerTaskIterationStatusInfo currentIteration = iterationsStatic.get(1);
    assertCurrentIterationStatisticWhileBalancingInProgress(currentIteration);
  }

  private static void assertCurrentIterationStatisticWhileBalancingInProgress(
      ContainerBalancerTaskIterationStatusInfo iterationsStatic
  ) {
    // No need to check others iterationsStatic fields(e.x. '*ContainerMoves*'), because it can lead to flaky results.
    assertEquals(2, iterationsStatic.getIterationNumber());
    assertNull(iterationsStatic.getIterationResult());
    assertEquals(0, iterationsStatic.getContainerMovesFailed());
    assertEquals(0, iterationsStatic.getContainerMovesTimeout());
    iterationsStatic.getSizeEnteringNodes().forEach((id, size) -> {
      assertNotNull(id);
      assertTrue(size > 0);
    });
    iterationsStatic.getSizeLeavingNodes().forEach((id, size) -> {
      assertNotNull(id);
      assertTrue(size > 0);
    });
  }

  private void verifyCompletedIteration(
      ContainerBalancerTaskIterationStatusInfo iteration,
      Integer expectedIterationNumber
  ) {
    assertEquals(expectedIterationNumber, iteration.getIterationNumber());
    assertEquals("ITERATION_COMPLETED", iteration.getIterationResult());
    assertNotNull(iteration.getIterationDuration());
    assertTrue(iteration.getContainerMovesScheduled() > 0);
    assertTrue(iteration.getContainerMovesCompleted() > 0);
    assertEquals(0, iteration.getContainerMovesFailed());
    assertEquals(0, iteration.getContainerMovesTimeout());
    assertTrue(iteration.getSizeScheduledForMove() > 0);
    assertTrue(iteration.getDataSizeMoved() > 0);
    assertFalse(iteration.getSizeEnteringNodes().isEmpty());
    assertFalse(iteration.getSizeLeavingNodes().isEmpty());
    iteration.getSizeEnteringNodes().forEach((id, size) -> {
      assertNotNull(id);
      assertTrue(size > 0);
    });
    iteration.getSizeLeavingNodes().forEach((id, size) -> {
      assertNotNull(id);
      assertTrue(size > 0);
    });
    Long enteringDataSum = getTotalMovedData(iteration.getSizeEnteringNodes());
    Long leavingDataSum = getTotalMovedData(iteration.getSizeLeavingNodes());
    assertEquals(enteringDataSum, leavingDataSum);
  }

  private void verifyStartedEmptyIteration(
      ContainerBalancerTaskIterationStatusInfo iteration
  ) {
    assertEquals(1, iteration.getIterationNumber());
    assertNull(iteration.getIterationResult());
    assertNotNull(iteration.getIterationDuration());
    assertEquals(0, iteration.getContainerMovesScheduled());
    assertEquals(0, iteration.getContainerMovesCompleted());
    assertEquals(0, iteration.getContainerMovesFailed());
    assertEquals(0, iteration.getContainerMovesTimeout());
    assertEquals(0, iteration.getSizeScheduledForMove());
    assertEquals(0, iteration.getDataSizeMoved());
    assertTrue(iteration.getSizeEnteringNodes().isEmpty());
    assertTrue(iteration.getSizeLeavingNodes().isEmpty());
  }

  private static Long getTotalMovedData(Map<DatanodeID, Long> iteration) {
    return iteration.values().stream().reduce(0L, ArithmeticUtils::addAndCheck);
  }

  /**
   * @see <a href="https://issues.apache.org/jira/browse/HDDS-11350">HDDS-11350</a>
   */
  @Test
  void testGetCurrentIterationsStatisticDoesNotThrowNullPointerExceptionWhenBalancingThreadIsSleeping() {
    MockedSCM mockedScm = new MockedSCM(new TestableCluster(10, OzoneConsts.GB));
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ContainerBalancerConfiguration config = ozoneConfig.getObject(ContainerBalancerConfiguration.class);

    config.setIterations(2);
    // the following config makes the balancing thread go to sleep while waiting for DU to be triggered in DNs and
    // updated storage reports to arrive via DN heartbeats - of course, this is a unit test and NodeManager, DNs etc.
    // are all mocked
    config.setTriggerDuEnable(true);
    mockedScm.init(config, ozoneConfig);

    // run ContainerBalancerTask in a new thread and have the current thread call getCurrentIterationsStatistic
    StorageContainerManager scm = mockedScm.getStorageContainerManager();
    ContainerBalancer cb = new ContainerBalancer(scm);
    ContainerBalancerTask task = new ContainerBalancerTask(scm, 0, cb, cb.getMetrics(), config, false);
    Thread thread = new Thread(task);
    thread.setDaemon(true);
    thread.start();
    Assertions.assertDoesNotThrow(task::getCurrentIterationsStatistic);
  }
}
