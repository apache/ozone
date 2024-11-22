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

import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    assertEquals(3, iterationStatistics.size());

    ContainerBalancerTaskIterationStatusInfo iterationHistory1 = iterationStatistics.get(0);
    verifyCompletedIteration(iterationHistory1, 1);

    ContainerBalancerTaskIterationStatusInfo iterationHistory2 = iterationStatistics.get(1);
    verifyCompletedIteration(iterationHistory2, 2);

    ContainerBalancerTaskIterationStatusInfo currentIteration = iterationStatistics.get(2);
    verifyStartedEmptyIteration(currentIteration);
  }

  @Test
  void testReRequestIterationStatistics() throws InterruptedException {
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

    assertEquals(firstRequestIterationStatistics.get(2).getIterationNumber(),
        secondRequestIterationStatistics.get(2).getIterationNumber());
    assertNotEquals(firstRequestIterationStatistics.get(2).getIterationDuration(),
        secondRequestIterationStatistics.get(2).getIterationDuration());
  }

  @Test
  void testGetCurrentStatisticsRequestInPeriodBetweenIterations() throws Exception {
    MockedSCM mockedScm = new MockedSCM(new TestableCluster(20, OzoneConsts.GB));

    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);

    config.setIterations(2);
    config.setBalancingInterval(10000);
    config.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);

    ContainerBalancerTask task = mockedScm.startBalancerTaskAsync(config, false);
    // Delay in finishing the first iteration
    LambdaTestUtils.await(1000, 500, () -> task.getCurrentIterationsStatistic().size() == 2);
    List<ContainerBalancerTaskIterationStatusInfo> iterationsStatic = task.getCurrentIterationsStatistic();
    assertEquals(2, iterationsStatic.size());

    ContainerBalancerTaskIterationStatusInfo firstIteration = iterationsStatic.get(0);
    verifyCompletedIteration(firstIteration, 1);

    assertEquals(0, iterationsStatic.get(1).getIterationNumber());
    assertTrue(iterationsStatic.get(1).getIterationDuration() >= 0);
    assertNull(iterationsStatic.get(1).getIterationResult());
    assertEquals(0, iterationsStatic.get(1).getContainerMovesScheduled());
    assertEquals(0, iterationsStatic.get(1).getContainerMovesCompleted());
    assertEquals(0, iterationsStatic.get(1).getContainerMovesFailed());
    assertEquals(0, iterationsStatic.get(1).getContainerMovesTimeout());
    assertEquals(0, iterationsStatic.get(1).getSizeScheduledForMove());
    assertEquals(0, iterationsStatic.get(1).getDataSizeMoved());
    assertEquals(0, iterationsStatic.get(1).getSizeEnteringNodes().size());
    assertEquals(0, iterationsStatic.get(1).getSizeLeavingNodes().size());
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
    assertEquals(2, firstRequestIterationStatistics.size());
    assertEquals(2, secondRequestIterationStatistics.size());
    assertEquals(firstRequestIterationStatistics.get(0), secondRequestIterationStatistics.get(0));

    ContainerBalancerTaskIterationStatusInfo currentIterationFromFirstRequest =
        firstRequestIterationStatistics.get(1);
    ContainerBalancerTaskIterationStatusInfo currentIterationFromSecondRequest =
        secondRequestIterationStatistics.get(1);
    assertEquals(
        currentIterationFromFirstRequest.getIterationNumber(),
        currentIterationFromSecondRequest.getIterationNumber());
    assertNotEquals(
        currentIterationFromFirstRequest.getIterationDuration(),
        currentIterationFromSecondRequest.getIterationDuration());
    assertEquals(
        currentIterationFromFirstRequest.getIterationResult(),
        currentIterationFromSecondRequest.getIterationResult());
    assertEquals(
        currentIterationFromFirstRequest.getContainerMovesScheduled(),
        currentIterationFromSecondRequest.getContainerMovesScheduled());
    assertEquals(
        currentIterationFromFirstRequest.getContainerMovesCompleted(),
        currentIterationFromSecondRequest.getContainerMovesCompleted());
    assertEquals(
        currentIterationFromFirstRequest.getContainerMovesFailed(),
        currentIterationFromSecondRequest.getContainerMovesFailed());
    assertEquals(
        currentIterationFromFirstRequest.getContainerMovesTimeout(),
        currentIterationFromSecondRequest.getContainerMovesTimeout());
    assertEquals(
        currentIterationFromFirstRequest.getSizeScheduledForMove(),
        currentIterationFromSecondRequest.getSizeScheduledForMove());
    assertEquals(currentIterationFromFirstRequest.getDataSizeMoved(),
        currentIterationFromSecondRequest.getDataSizeMoved());
    assertEquals(currentIterationFromFirstRequest.getSizeEnteringNodes().size(),
        currentIterationFromSecondRequest.getSizeEnteringNodes().size());
    assertEquals(
        currentIterationFromFirstRequest.getSizeLeavingNodes().size(),
        currentIterationFromSecondRequest.getSizeLeavingNodes().size());
  }

  @Test
  void testGetCurrentStatisticsWithDelay() throws Exception {
    MockedSCM mockedScm = new MockedSCM(new TestableCluster(20, OzoneConsts.GB));

    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);

    config.setIterations(2);
    config.setBalancingInterval(0);
    config.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);

    ContainerBalancerTask task = mockedScm.startBalancerTaskAsync(config, true);
    // Delay in finishing the first iteration
    LambdaTestUtils.await(1000, 500, () -> task.getCurrentIterationsStatistic().size() == 1);
    List<ContainerBalancerTaskIterationStatusInfo> iterationsStatic = task.getCurrentIterationsStatistic();
    assertEquals(1, iterationsStatic.size());
    ContainerBalancerTaskIterationStatusInfo currentIteration = iterationsStatic.get(0);
    verifyUnstartedIteration(currentIteration);
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
    LambdaTestUtils.await(1000, 10,
        () -> task.getCurrentIterationsStatistic().size() == 2 &&
              task.getCurrentIterationsStatistic().get(1).getContainerMovesScheduled() > 0);
    List<ContainerBalancerTaskIterationStatusInfo> iterationsStatic = task.getCurrentIterationsStatistic();
    assertEquals(2, iterationsStatic.size());
    ContainerBalancerTaskIterationStatusInfo currentIteration = iterationsStatic.get(1);
    assertCurrentIterationStatisticWhileBalancingInProgress(currentIteration);
  }


  private static void verifyUnstartedIteration(ContainerBalancerTaskIterationStatusInfo iterationsStatic) {
    assertEquals(0, iterationsStatic.getIterationNumber());
    assertEquals(-1, iterationsStatic.getIterationDuration());
    assertNull(iterationsStatic.getIterationResult());
    assertEquals(0, iterationsStatic.getContainerMovesScheduled());
    assertEquals(0, iterationsStatic.getContainerMovesCompleted());
    assertEquals(0, iterationsStatic.getContainerMovesFailed());
    assertEquals(0, iterationsStatic.getContainerMovesTimeout());
    assertEquals(0, iterationsStatic.getSizeScheduledForMove());
    assertEquals(0, iterationsStatic.getDataSizeMoved());
    assertTrue(iterationsStatic.getSizeEnteringNodes().isEmpty());
    assertTrue(iterationsStatic.getSizeLeavingNodes().isEmpty());
  }

  private static void assertCurrentIterationStatisticWhileBalancingInProgress(
      ContainerBalancerTaskIterationStatusInfo iterationsStatic
  ) {

    assertEquals(2, iterationsStatic.getIterationNumber());
    assertEquals(0, iterationsStatic.getIterationDuration());
    assertNull(iterationsStatic.getIterationResult());
    assertTrue(iterationsStatic.getContainerMovesScheduled() > 0);
    assertTrue(iterationsStatic.getContainerMovesCompleted() > 0);
    assertEquals(0, iterationsStatic.getContainerMovesFailed());
    assertEquals(0, iterationsStatic.getContainerMovesTimeout());
    assertTrue(iterationsStatic.getSizeScheduledForMove() > 0);
    assertTrue(iterationsStatic.getDataSizeMoved() > 0);
    assertFalse(iterationsStatic.getSizeEnteringNodes().isEmpty());
    assertFalse(iterationsStatic.getSizeLeavingNodes().isEmpty());
    iterationsStatic.getSizeEnteringNodes().forEach((id, size) -> {
      assertNotNull(id);
      assertTrue(size > 0);
    });
    iterationsStatic.getSizeLeavingNodes().forEach((id, size) -> {
      assertNotNull(id);
      assertTrue(size > 0);
    });
    Long enteringDataSum = getTotalMovedData(iterationsStatic.getSizeEnteringNodes());
    Long leavingDataSum = getTotalMovedData(iterationsStatic.getSizeLeavingNodes());
    assertEquals(enteringDataSum, leavingDataSum);
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
    assertEquals(0, iteration.getIterationNumber());
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

  private static Long getTotalMovedData(Map<UUID, Long> iteration) {
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
