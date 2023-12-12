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

package org.apache.hadoop.hdds.scm.container.balancer.iteration;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancer;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerConfiguration;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerMetrics;
import org.apache.hadoop.hdds.scm.container.balancer.ContainerBalancerTask;
import org.apache.hadoop.hdds.scm.container.balancer.MockedSCM;
import org.apache.hadoop.hdds.scm.container.balancer.MoveManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

/**
 * Tests for {@link ContainerBalancer}.
 */
public class TestContainerBalancerIteration {
  /**
   * Tests the situation where some container moves time out because they take longer than "move.timeout".
   */
  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource(
      "org.apache.hadoop.hdds.scm.container.balancer.TestContainerBalancerTask#createMockedSCMWithDatanodeLimits")
  public void checkIterationResultTimeout(@Nonnull MockedSCM mockedSCM, boolean useDatanodeLimits)
      throws NodeNotFoundException, IOException, TimeoutException {
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
    ContainerBalancer balancer = mockedSCM.createContainerBalancer();
    ContainerBalancerTask task = mockedSCM.startBalancerTask(balancer, config);

    // According to the setup and configurations, this iteration's result should be ITERATION_COMPLETED.
    assertEquals(IterationResult.ITERATION_COMPLETED, task.getIterationResult());

    ContainerBalancerMetrics metrics = balancer.getMetrics();
    assertEquals(1, metrics.getNumContainerMovesCompleted());
    assertTrue(metrics.getNumContainerMovesTimeout() >= 1);

    // Test the same but use MoveManager instead of LegacyReplicationManager.
    // The first move being 10ms falls within the timeout duration of 500ms.
    // It should be successful. The rest should fail.
    mockedSCM.disableLegacyReplicationManager();
    Mockito
        .when(mockedSCM.getMoveManager()
            .move(
                any(ContainerID.class),
                any(DatanodeDetails.class),
                any(DatanodeDetails.class)))
        .thenReturn(completedFuture)
        .thenAnswer(invocation -> genCompletableFuture(2000, false));

    assertEquals(IterationResult.ITERATION_COMPLETED, task.getIterationResult());
    assertEquals(1, metrics.getNumContainerMovesCompleted());
    assertTrue(metrics.getNumContainerMovesTimeout() >= 1);
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource(
      "org.apache.hadoop.hdds.scm.container.balancer.TestContainerBalancerTask#createMockedSCMWithDatanodeLimits")
  public void checkIterationResultTimeoutFromReplicationManager(@Nonnull MockedSCM mockedSCM, boolean ignored)
      throws NodeNotFoundException, IOException, TimeoutException {
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

    IterationMetrics metrics = task.getIterationMetrics();
    assertTrue(metrics.getTimeoutContainerMovesCount() > 0);
    assertEquals(0, metrics.getCompletedContainerMovesCount());

    // Try the same test with MoveManager instead of LegacyReplicationManager.
    Mockito
        .when(mockedSCM.getMoveManager()
            .move(
                any(ContainerID.class),
                any(DatanodeDetails.class),
                any(DatanodeDetails.class)))
        .thenReturn(future)
        .thenAnswer(invocation -> future2);

    mockedSCM.disableLegacyReplicationManager();

    assertTrue(metrics.getTimeoutContainerMovesCount() > 0);
    assertEquals(0, metrics.getCompletedContainerMovesCount());
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource(
      "org.apache.hadoop.hdds.scm.container.balancer.TestContainerBalancerTask#createMockedSCMWithDatanodeLimits")
  public void checkIterationResultException(@Nonnull MockedSCM mockedSCM, boolean useDatanodeLimits)
      throws NodeNotFoundException, IOException, TimeoutException {
    CompletableFuture<MoveManager.MoveResult> future = new CompletableFuture<>();
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
    ContainerBalancer balancer = mockedSCM.createContainerBalancer();
    ContainerBalancerTask task = mockedSCM.startBalancerTask(balancer, config);
    assertEquals(IterationResult.ITERATION_COMPLETED, task.getIterationResult());

    int nodeCount = mockedSCM.getCluster().getNodeCount();
    int expectedMovesFailed = (nodeCount > 6) ? 3 : 1;
    assertTrue(balancer.getMetrics().getNumContainerMovesFailed() >= expectedMovesFailed);

    // Try the same test but with MoveManager instead of ReplicationManager.
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

    assertEquals(IterationResult.ITERATION_COMPLETED, task.getIterationResult());
    assertTrue(balancer.getMetrics().getNumContainerMovesFailed() >= expectedMovesFailed);
  }

  @ParameterizedTest(name = "MockedSCM #{index}: {0}; use datanode limits: {1}")
  @MethodSource(
      "org.apache.hadoop.hdds.scm.container.balancer.TestContainerBalancerTask#createMockedSCMWithDatanodeLimits")
  public void checkIterationResult(@Nonnull MockedSCM mockedSCM, boolean ignored)
      throws NodeNotFoundException, IOException, TimeoutException {
    ContainerBalancerConfiguration config = mockedSCM.getBalancerConfig();
    config.setThreshold(10);
    config.setIterations(1);
    config.setMaxSizeEnteringTarget(10 * MockedSCM.STORAGE_UNIT);
    config.setMaxSizeToMovePerIteration(100 * MockedSCM.STORAGE_UNIT);
    config.setMaxDatanodesPercentageToInvolvePerIteration(100);

    mockedSCM.enableLegacyReplicationManager();
    ContainerBalancerTask task = mockedSCM.startBalancerTask(config);

    // According to the setup and configurations, this iteration's result should be ITERATION_COMPLETED.
    assertEquals(IterationResult.ITERATION_COMPLETED, task.getIterationResult());

    // Now, limit maxSizeToMovePerIteration but fail all container moves.
    // The result should still be ITERATION_COMPLETED.
    Mockito
        .when(mockedSCM.getReplicationManager()
            .move(
                any(ContainerID.class),
                any(DatanodeDetails.class),
                any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.completedFuture(MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY));

    config.setMaxSizeToMovePerIteration(10 * MockedSCM.STORAGE_UNIT);
    mockedSCM.startBalancerTask(config);
    assertEquals(IterationResult.ITERATION_COMPLETED, task.getIterationResult());

    // Try the same but use MoveManager for container move instead of legacy RM.
    mockedSCM.disableLegacyReplicationManager();
    mockedSCM.startBalancerTask(config);
    assertEquals(IterationResult.ITERATION_COMPLETED, task.getIterationResult());
  }

  private static @Nonnull CompletableFuture<MoveManager.MoveResult> genCompletableFuture(
      int sleepMilSec,
      boolean doThrowException
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
}
