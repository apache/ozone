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
package org.apache.hadoop.ozone.container.ec.reconstruction;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.OptionalLong;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Tests the ECReconstructionSupervisor.
 */
public class TestECReconstructionSupervisor {

  private TestClock clock;

  @BeforeEach
  public void setup() {
    clock = new TestClock(Instant.now(), ZoneId.systemDefault());
  }


  @Test
  public void testAddTaskShouldExecuteTheGivenTask()
      throws InterruptedException, TimeoutException, IOException {
    final CountDownLatch runnableInvoked = new CountDownLatch(1);
    final CountDownLatch holdProcessing = new CountDownLatch(1);
    ECReconstructionSupervisor supervisor =
        new ECReconstructionSupervisor(null, null, 5,
            new ECReconstructionCoordinator(new OzoneConfiguration(), null,
                null, ECReconstructionMetrics.create()) {
              @Override
              public void reconstructECContainerGroup(long containerID,
                  ECReplicationConfig repConfig,
                  SortedMap<Integer, DatanodeDetails> sourceNodeMap,
                  SortedMap<Integer, DatanodeDetails> targetNodeMap)
                  throws IOException {
                runnableInvoked.countDown();
                try {
                  holdProcessing.await();
                } catch (InterruptedException e) {
                }
                super.reconstructECContainerGroup(containerID, repConfig,
                    sourceNodeMap, targetNodeMap);
              }
            }, clock) {
        };
    ReconstructECContainersCommand command = createCommand(1L);
    supervisor.addTask(new ECReconstructionCommandInfo(command));
    runnableInvoked.await();
    Assertions.assertEquals(1, supervisor.getInFlightReplications());
    holdProcessing.countDown();
    GenericTestUtils
        .waitFor(() -> supervisor.getInFlightReplications() == 0, 100, 15000);
  }

  @Test
  public void testTasksWithDeadlineExceededAreNotRun() throws IOException {
    ECReconstructionCoordinator coordinator =
        Mockito.mock(ECReconstructionCoordinator.class);
    ECReconstructionSupervisor supervisor =
        new ECReconstructionSupervisor(null, null,
            newDirectExecutorService(), coordinator, clock);

    ReconstructECContainersCommand command = createCommand(1);
    ECReconstructionCommandInfo task1 =
        new ECReconstructionCommandInfo(command);

    command = createCommand(2);
    command.setDeadline(clock.millis() + 10000);
    ECReconstructionCommandInfo task2 =
        new ECReconstructionCommandInfo(command);

    command = createCommand(3);
    command.setDeadline(clock.millis() + 20000);
    ECReconstructionCommandInfo task3 =
        new ECReconstructionCommandInfo(command);

    clock.fastForward(15000);
    supervisor.addTask(task1);
    supervisor.addTask(task2);
    supervisor.addTask(task3);

    // No deadline for container 1, it should run.
    Mockito.verify(coordinator, times(1))
        .reconstructECContainerGroup(eq(1L), any(), any(), any());
    // Deadline passed for container 2, it should not run.
    Mockito.verify(coordinator, times(0))
        .reconstructECContainerGroup(eq(2L), any(), any(), any());
    // Deadline not passed for container 3, it should run.
    Mockito.verify(coordinator, times(1))
        .reconstructECContainerGroup(eq(3L), any(), any(), any());
  }

  @Test
  void dropsTaskWithObsoleteTerm() throws IOException {
    final long commandTerm = 1;
    final long currentTerm = 2;
    ECReconstructionCoordinator coordinator =
        Mockito.mock(ECReconstructionCoordinator.class);
    when(coordinator.getTermOfLeaderSCM())
        .thenReturn(OptionalLong.of(currentTerm));
    ECReconstructionSupervisor supervisor =
        new ECReconstructionSupervisor(null, null,
            newDirectExecutorService(), coordinator, clock);

    ReconstructECContainersCommand command = createCommand(1);
    command.setTerm(commandTerm);
    supervisor.addTask(new ECReconstructionCommandInfo(command));

    Mockito.verify(coordinator, times(0))
        .reconstructECContainerGroup(eq(1L), any(), any(), any());
  }

  private static ReconstructECContainersCommand createCommand(
      long containerID) {
    return new ReconstructECContainersCommand(
        containerID, ImmutableList.of(), ImmutableList.of(), new byte[0],
        new ECReplicationConfig(3, 2));
  }

}
