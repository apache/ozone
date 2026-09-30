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
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaNotFoundException;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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

    assertThat(iteration.getContainerMovesTimeout()).isEqualTo(1);
    assertFailureBreakdown(mockedScm, iteration, MoveManager.MoveResult.REPLICATION_FAIL_TIME_OUT.name(), 0);
  }

  @Test
  void testReplicationNotHealthyAfterMoveRecordedInFailureBreakdown()
      throws NodeNotFoundException, ContainerReplicaNotFoundException, ContainerNotFoundException {
    MockedSCM mockedScm = createMockedScm();
    mockMoveFailureOnce(mockedScm, MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_AFTER_MOVE);

    ContainerBalancerTask task = mockedScm.startBalancerTask(buildConfig(mockedScm));
    ContainerBalancerTaskIterationStatusInfo iteration = getCompletedIteration(task);

    assertThat(iteration.getContainerMovesFailed()).isEqualTo(1);
    assertFailureBreakdown(mockedScm, iteration,
            MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_AFTER_MOVE.name(), 0);
  }

  @Test
  void testDeletionTimeoutRecordedInFailureBreakdown()
      throws NodeNotFoundException, ContainerReplicaNotFoundException, ContainerNotFoundException {
    MockedSCM mockedScm = createMockedScm();
    mockMoveFailureOnce(mockedScm, MoveManager.MoveResult.DELETION_FAIL_TIME_OUT);

    ContainerBalancerTask task = mockedScm.startBalancerTask(buildConfig(mockedScm));
    ContainerBalancerTaskIterationStatusInfo iteration = getCompletedIteration(task);

    assertThat(iteration.getContainerMovesTimeout()).isEqualTo(1);
    assertFailureBreakdown(mockedScm, iteration, MoveManager.MoveResult.DELETION_FAIL_TIME_OUT.name(), 0);
  }

  @Test
  void testIterationMoveTimeoutRecordedInFailureBreakdown()
      throws NodeNotFoundException, ContainerNotFoundException, ContainerReplicaNotFoundException {
    MockedSCM mockedScm = createMockedScm();
    ContainerBalancerConfiguration config = buildConfig(mockedScm);
    config.setMoveTimeout(Duration.ofMillis(50));
    mockFirstMoveNeverCompletes(mockedScm);

    ContainerBalancerTask task = mockedScm.startBalancerTask(config);
    ContainerBalancerTaskIterationStatusInfo iteration = getCompletedIteration(task);

    assertThat(iteration.getContainerMovesTimeout()).isEqualTo(1);
    assertFailureBreakdown(mockedScm, iteration,
            ContainerBalancerTask.ContainerMoveFailureReason.ITERATION_MOVE_TIMEOUT.name(), 0);
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
    verify(mockedScm.getMoveManager(), atLeast(2)).move(
            any(ContainerID.class), any(DatanodeDetails.class), any(DatanodeDetails.class));
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

    assertThat(iteration.getContainerMovesFailed()).isEqualTo(1);
    assertFailureBreakdown(mockedScm, iteration,
            ContainerBalancerTask.ContainerMoveFailureReason.PRE_MOVE_CONTAINER_NOT_FOUND.name(), 0);
  }

  @Test
  void testNodeNotFoundRecordedInFailureBreakdown()
      throws NodeNotFoundException, ContainerReplicaNotFoundException, ContainerNotFoundException {
    MockedSCM mockedScm = createMockedScm();
    when(mockedScm.getMoveManager().move(any(ContainerID.class),
        any(DatanodeDetails.class), any(DatanodeDetails.class)))
        .thenThrow(new NodeNotFoundException())
        .thenReturn(CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED));
    ContainerBalancerTask task = mockedScm.startBalancerTask(buildConfig(mockedScm));
    ContainerBalancerTaskIterationStatusInfo iteration = getCompletedIteration(task);
    assertThat(iteration.getContainerMovesFailed()).isEqualTo(1);
    assertFailureBreakdown(mockedScm, iteration,
        ContainerBalancerTask.ContainerMoveFailureReason.PRE_MOVE_NODE_NOT_FOUND.name(), 0);
  }

  @Test
  void testReplicaNotFoundRecordedInFailureBreakdown()
      throws NodeNotFoundException, ContainerReplicaNotFoundException, ContainerNotFoundException {
    MockedSCM mockedScm = createMockedScm();
    when(mockedScm.getMoveManager().move(any(ContainerID.class),
        any(DatanodeDetails.class), any(DatanodeDetails.class)))
        .thenThrow(new ContainerReplicaNotFoundException("test"))
        .thenReturn(CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED));
    ContainerBalancerTask task = mockedScm.startBalancerTask(buildConfig(mockedScm));
    ContainerBalancerTaskIterationStatusInfo iteration = getCompletedIteration(task);
    assertThat(iteration.getContainerMovesFailed()).isEqualTo(1);
    assertFailureBreakdown(mockedScm, iteration,
        ContainerBalancerTask.ContainerMoveFailureReason.PRE_MOVE_REPLICA_NOT_FOUND.name(), 0);
  }

  @Test
  void testSameReasonAggregatesSourceFailureCountsFromMultipleSources() {
    ContainerMoveFailureTracker tracker = new ContainerMoveFailureTracker();
    DatanodeDetails source1 = MockDatanodeDetails.createDatanodeDetails("1.1.1.1", "/r1");
    DatanodeDetails source2 = MockDatanodeDetails.createDatanodeDetails("2.2.2.2", "/r1");
    DatanodeDetails target = MockDatanodeDetails.createDatanodeDetails("3.3.3.3", "/r2");

    String reason = MoveManager.MoveResult.REPLICATION_FAIL_TIME_OUT.name();
    tracker.recordFailure(reason, source1, target);
    tracker.recordFailure(reason, source2, target);

    ContainerMoveFailureDetail detail = tracker.getFailures().stream()
        .filter(f -> reason.equals(f.getReason()))
        .findFirst()
        .orElse(null);
    assertThat(detail).as("failure detail for reason " + reason).isNotNull();
    assertThat(detail.getCount()).isEqualTo(2L);
    assertThat(detail.getSourceFailureCounts())
        .hasSize(2)
        .containsEntry(source1.getUuidString(), 1L)
        .containsEntry(source2.getUuidString(), 1L);
    assertThat(detail.getTargetFailureCounts())
        .hasSize(1)
        .containsEntry(target.getUuidString(), 2L);
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

  private static void mockFirstMoveNeverCompletes(MockedSCM mockedScm)
          throws NodeNotFoundException, ContainerReplicaNotFoundException, ContainerNotFoundException {
    AtomicInteger moveInvocations = new AtomicInteger(0);
    when(mockedScm.getMoveManager().move(any(ContainerID.class),
            any(DatanodeDetails.class), any(DatanodeDetails.class)))
            .thenAnswer(invocation -> {
              if (moveInvocations.getAndIncrement() == 0) {
                return new CompletableFuture<>();
              }
              return CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED);
            });
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

  private static void assertBreakdownTotalsMatchHeadlineCounters(
      ContainerBalancerTaskIterationStatusInfo iteration) {
    long breakdownTotal = iteration.getFailures().stream()
        .mapToLong(ContainerMoveFailureDetail::getCount)
        .sum();
    long headlineTotal = iteration.getContainerMovesFailed() + iteration.getContainerMovesTimeout();
    assertEquals(headlineTotal, breakdownTotal,
        "sum(failure breakdown) should equal failed + timeout counters");
  }

  private static void assertFailureBreakdown(MockedSCM mockedScm, ContainerBalancerTaskIterationStatusInfo iteration,
      String expectedReason, int moveIndex) throws NodeNotFoundException, ContainerReplicaNotFoundException,
      ContainerNotFoundException {
    ArgumentCaptor<DatanodeDetails> sourceCaptor = ArgumentCaptor.forClass(DatanodeDetails.class);
    ArgumentCaptor<DatanodeDetails> targetCaptor = ArgumentCaptor.forClass(DatanodeDetails.class);
    verify(mockedScm.getMoveManager(), atLeastOnce()).move(
            any(ContainerID.class), sourceCaptor.capture(), targetCaptor.capture());
    assertThat(sourceCaptor.getAllValues().size()).isGreaterThan(moveIndex);
    assertThat(targetCaptor.getAllValues().size()).isGreaterThan(moveIndex);
    String sourceUuid = sourceCaptor.getAllValues().get(moveIndex).getUuidString();
    String targetUuid = targetCaptor.getAllValues().get(moveIndex).getUuidString();

    List<ContainerMoveFailureDetail> failures = iteration.getFailures();
    assertThat(failures).as("failure details").isNotEmpty();
    ContainerMoveFailureDetail detail = failures.stream()
        .filter(f -> expectedReason.equals(f.getReason()))
        .findFirst()
        .orElse(null);
    assertThat(detail).as("failure detail for reason " + expectedReason).isNotNull();
    assertThat(detail.getCount()).isEqualTo(1L);
    assertThat(detail.getSourceFailureCounts()).hasSize(1).containsEntry(sourceUuid, 1L);
    assertThat(detail.getTargetFailureCounts()).hasSize(1).containsEntry(targetUuid, 1L);
  }
}
