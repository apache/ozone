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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link ContainerBalancerTask} records failure breakdown and details
 * in iteration statistics for common move failure reasons.
 */
class TestContainerBalancerMoveFailureBreakdown {

  private static final int NODE_COUNT = 5;
  private static final long STORAGE_UNIT = OzoneConsts.GB;

  @Test
  void testReplicationTimeoutRecordedInFailureBreakdown()
      throws NodeNotFoundException, ContainerReplicaNotFoundException, ContainerNotFoundException {
    MockedSCM mockedScm = createMockedScm();
    mockMoveFailureOnce(mockedScm, MoveManager.MoveResult.REPLICATION_FAIL_TIME_OUT);

    ContainerBalancerTask task = mockedScm.startBalancerTask(buildConfig(mockedScm));
    ContainerBalancerTaskIterationStatusInfo iteration = getCompletedIteration(task);

    assertThat(iteration.getContainerMovesTimeout()).isGreaterThan(0);
    assertFailureBreakdown(iteration, MoveManager.MoveResult.REPLICATION_FAIL_TIME_OUT.name());
  }

  @Test
  void testReplicationNotHealthyAfterMoveRecordedInFailureBreakdown()
      throws NodeNotFoundException, ContainerReplicaNotFoundException, ContainerNotFoundException {
    MockedSCM mockedScm = createMockedScm();
    mockMoveFailureOnce(mockedScm, MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_AFTER_MOVE);

    ContainerBalancerTask task = mockedScm.startBalancerTask(buildConfig(mockedScm));
    ContainerBalancerTaskIterationStatusInfo iteration = getCompletedIteration(task);

    assertThat(iteration.getContainerMovesFailed()).isGreaterThan(0);
    assertFailureBreakdown(iteration, MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_AFTER_MOVE.name());
  }

  @Test
  void testDeletionTimeoutRecordedInFailureBreakdown()
      throws NodeNotFoundException, ContainerReplicaNotFoundException, ContainerNotFoundException {
    MockedSCM mockedScm = createMockedScm();
    mockMoveFailureOnce(mockedScm, MoveManager.MoveResult.DELETION_FAIL_TIME_OUT);

    ContainerBalancerTask task = mockedScm.startBalancerTask(buildConfig(mockedScm));
    ContainerBalancerTaskIterationStatusInfo iteration = getCompletedIteration(task);

    assertThat(iteration.getContainerMovesTimeout()).isGreaterThan(0);
    assertFailureBreakdown(iteration, MoveManager.MoveResult.DELETION_FAIL_TIME_OUT.name());
  }

  @Test
  void testIterationMoveTimeoutRecordedInFailureBreakdown()
      throws NodeNotFoundException, ContainerNotFoundException, ContainerReplicaNotFoundException {
    MockedSCM mockedScm = createMockedScm();
    ContainerBalancerConfiguration config = buildConfig(mockedScm);
    config.setMoveTimeout(Duration.ofMillis(50));

    when(mockedScm.getMoveManager().move(any(ContainerID.class),
        any(DatanodeDetails.class), any(DatanodeDetails.class)))
        .thenAnswer(invocation -> slowMoveFuture(150));

    ContainerBalancerTask task = mockedScm.startBalancerTask(config);
    ContainerBalancerTaskIterationStatusInfo iteration = getCompletedIteration(task);

    assertThat(iteration.getContainerMovesTimeout()).isGreaterThanOrEqualTo(1);
    assertFailureBreakdown(iteration,
        ContainerBalancerTask.ContainerMoveFailureReason.ITERATION_MOVE_TIMEOUT.name());
  }

  @Test
  void testFailureBreakdownTotalsMatchHeadlineCountersInControlledScenario()
      throws NodeNotFoundException, ContainerReplicaNotFoundException, ContainerNotFoundException {
    MockedSCM mockedScm = createMockedScm();
    when(mockedScm.getMoveManager().move(any(ContainerID.class),
        any(DatanodeDetails.class), any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.REPLICATION_FAIL_TIME_OUT))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_AFTER_MOVE))
        .thenReturn(CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED));

    ContainerBalancerTask task = mockedScm.startBalancerTask(buildConfig(mockedScm));
    ContainerBalancerTaskIterationStatusInfo iteration = getCompletedIteration(task);

    assertThat(iteration.getContainerMovesTimeout()).isEqualTo(1);
    assertThat(iteration.getContainerMovesFailed()).isEqualTo(1);
    assertBreakdownTotalsMatchHeadlineCounters(iteration);
  }

  @Test
  void testPreMoveContainerNotFoundRecordedInFailureBreakdown()
      throws NodeNotFoundException, ContainerReplicaNotFoundException, ContainerNotFoundException {
    MockedSCM mockedScm = createMockedScm();
    when(mockedScm.getMoveManager().move(any(ContainerID.class),
        any(DatanodeDetails.class), any(DatanodeDetails.class)))
        .thenThrow(ContainerNotFoundException.newInstanceForTesting())
        .thenReturn(CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED));

    ContainerBalancerTask task = mockedScm.startBalancerTask(buildConfig(mockedScm));
    ContainerBalancerTaskIterationStatusInfo iteration = getCompletedIteration(task);

    assertThat(iteration.getContainerMovesFailed()).isGreaterThan(0);
    assertFailureBreakdown(iteration,
        ContainerBalancerTask.ContainerMoveFailureReason.PRE_MOVE_CONTAINER_NOT_FOUND.name());
  }

  private static MockedSCM createMockedScm() {
    return new MockedSCM(new MockCluster(NODE_COUNT, STORAGE_UNIT));
  }

  private static void mockMoveFailureOnce(MockedSCM mockedScm, MoveManager.MoveResult failureResult)
      throws NodeNotFoundException, ContainerReplicaNotFoundException, ContainerNotFoundException {
    when(mockedScm.getMoveManager().move(any(ContainerID.class),
        any(DatanodeDetails.class), any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.completedFuture(failureResult))
        .thenReturn(CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED));
  }

  private static ContainerBalancerConfiguration buildConfig(MockedSCM mockedScm) {
    ContainerBalancerConfiguration config = new ContainerBalancerConfigBuilder(mockedScm.getNodeCount()).build();
    config.setMaxSizeToMovePerIteration(5 * STORAGE_UNIT);
    config.setMaxSizeEnteringTarget(5 * STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);
    return config;
  }

  private static ContainerBalancerTaskIterationStatusInfo getCompletedIteration(
      ContainerBalancerTask task) {
    List<ContainerBalancerTaskIterationStatusInfo> iterations =
        task.getCurrentIterationsStatistic();
    assertEquals(1, iterations.size());
    ContainerBalancerTaskIterationStatusInfo iteration = iterations.get(0);
    assertEquals("ITERATION_COMPLETED", iteration.getIterationResult());
    return iteration;
  }

  private static CompletableFuture<MoveManager.MoveResult> slowMoveFuture(int sleepMillis) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        Thread.sleep(sleepMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return MoveManager.MoveResult.COMPLETED;
    });
  }

  private static void assertBreakdownTotalsMatchHeadlineCounters(
      ContainerBalancerTaskIterationStatusInfo iteration) {
    long breakdownTotal = iteration.getFailures().stream()
        .mapToLong(ContainerMoveFailureDetail::getCount)
        .sum();
    long headlineTotal = iteration.getContainerMovesFailed() + iteration.getContainerMovesTimeout();
    assertEquals(headlineTotal, breakdownTotal,
        "sum(failure breakdown) should equal failed + timeout counters");
  }

  private static void assertFailureBreakdown(
      ContainerBalancerTaskIterationStatusInfo iteration, String expectedReason) {
    List<ContainerMoveFailureDetail> failures = iteration.getFailures();
    assertThat(failures).as("failure details").isNotEmpty();
    ContainerMoveFailureDetail detail = failures.stream()
        .filter(f -> expectedReason.equals(f.getReason()))
        .findFirst()
        .orElse(null);
    assertThat(detail).as("failure detail for reason " + expectedReason).isNotNull();
    assertThat(detail.getCount()).isGreaterThanOrEqualTo(1L);
    assertThat(detail.getSourceFailureCounts()).isNotEmpty();
    assertThat(detail.getTargetFailureCounts()).isNotEmpty();
  }
}
