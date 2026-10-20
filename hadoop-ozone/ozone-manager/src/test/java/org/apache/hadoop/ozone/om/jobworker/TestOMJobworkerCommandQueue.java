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

package org.apache.hadoop.ozone.om.jobworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test for the OMJobworkerCommandQueue class.
 */
public class TestOMJobworkerCommandQueue {

  private OMJobworkerCommandQueue commandQueue;

  @BeforeEach
  public void setUp() {
    commandQueue = new OMJobworkerCommandQueue();
  }

  @Test
  public void testSummaryUpdated() {
    OMJobworkerCommand reregisterCommand = createMockCommand(
        Type.reregisterCommand);
    OMJobworkerCommand unknownCommand = createMockCommand(
        Type.unknownCommand);
    UUID jobworker1UUID = UUID.randomUUID();
    UUID jobworker2UUID = UUID.randomUUID();

    commandQueue.addCommand(jobworker1UUID, unknownCommand);
    commandQueue.addCommand(jobworker1UUID, reregisterCommand);
    commandQueue.addCommand(jobworker1UUID, reregisterCommand);

    commandQueue.addCommand(jobworker2UUID, unknownCommand);
    commandQueue.addCommand(jobworker2UUID, reregisterCommand);

    // Check zero returned for unknown job worker
    assertEquals(0, commandQueue.getJobworkerCommandCount(
        UUID.randomUUID(), OMJobworkerCommandProto.Type.reregisterCommand));

    // Check command counts
    assertEquals(2, commandQueue.getJobworkerCommandCount(
        jobworker1UUID, OMJobworkerCommandProto.Type.reregisterCommand));
    assertEquals(1, commandQueue.getJobworkerCommandCount(
        jobworker1UUID, OMJobworkerCommandProto.Type.unknownCommand));
    assertEquals(1, commandQueue.getJobworkerCommandCount(
        jobworker2UUID, OMJobworkerCommandProto.Type.reregisterCommand));
    assertEquals(1, commandQueue.getJobworkerCommandCount(
        jobworker2UUID, OMJobworkerCommandProto.Type.unknownCommand));
    // Verify total commands in queue
    assertEquals(5, commandQueue.getCommandsInQueue());

    // Ensure the counts are cleared when the commands are retrieved
    assertEquals(3, commandQueue.pollCommand(jobworker1UUID).size());
    // Check jobworker1's commands are now empty
    assertEquals(0, commandQueue.getJobworkerCommandCount(
        jobworker1UUID, OMJobworkerCommandProto.Type.reregisterCommand));
    assertEquals(0, commandQueue.getJobworkerCommandCount(
        jobworker1UUID, OMJobworkerCommandProto.Type.unknownCommand));
    // jobworker2 is not affected
    assertEquals(1, commandQueue.getJobworkerCommandCount(
        jobworker2UUID, OMJobworkerCommandProto.Type.reregisterCommand));
    assertEquals(1, commandQueue.getJobworkerCommandCount(
        jobworker2UUID, OMJobworkerCommandProto.Type.unknownCommand));

    // Ensure the commands are zeroed when the queue is cleared
    commandQueue.clear();
    assertEquals(0, commandQueue.getJobworkerCommandCount(
        jobworker1UUID, OMJobworkerCommandProto.Type.reregisterCommand));
    assertEquals(0, commandQueue.getJobworkerCommandCount(
        jobworker1UUID, OMJobworkerCommandProto.Type.unknownCommand));
    assertEquals(0, commandQueue.getJobworkerCommandCount(
        jobworker2UUID, OMJobworkerCommandProto.Type.reregisterCommand));
    assertEquals(0, commandQueue.getJobworkerCommandCount(
        jobworker2UUID, OMJobworkerCommandProto.Type.unknownCommand));
    assertEquals(0, commandQueue.getCommandsInQueue());
  }

  @Test
  @Timeout(60)
  public void testConcurrentAddCommands() throws InterruptedException {
    final int numThreads = 10;
    final int commandsPerThread = 100;
    final CountDownLatch startSignal = new CountDownLatch(1);
    final CountDownLatch doneSignal = new CountDownLatch(numThreads);
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    try {
      final UUID sharedJobworkerUuid = UUID.randomUUID();

      // Launch multiple threads to add commands concurrently
      for (int i = 0; i < numThreads; i++) {
        final int threadId = i;
        executorService.submit(() -> {
          try {
            startSignal.await();

            for (int j = 0; j < commandsPerThread; j++) {
              OMJobworkerCommand command;
              if (j % 2 == 0) {
                command = createMockCommand(Type.unknownCommand);
              } else {
                command = createMockCommand(Type.reregisterCommand);
              }
              commandQueue.addCommand(sharedJobworkerUuid, command);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            doneSignal.countDown();
          }
        });
      }
      startSignal.countDown();
      doneSignal.await();

      // Verify that all commands were added correctly
      int totalExpectedCommands = numThreads * commandsPerThread;
      assertEquals(totalExpectedCommands, commandQueue.getCommandsInQueue());
      int createBucketCommands = commandQueue.getJobworkerCommandCount(
          sharedJobworkerUuid, Type.unknownCommand);
      int deleteBucketCommands = commandQueue.getJobworkerCommandCount(
          sharedJobworkerUuid, Type.reregisterCommand);

      assertEquals(totalExpectedCommands / 2, createBucketCommands);
      assertEquals(totalExpectedCommands / 2, deleteBucketCommands);
    } finally {
      executorService.shutdown();
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @Test
  @Timeout(60)
  public void testConcurrentPollCommands() throws InterruptedException {
    final UUID jobworkerUuid = UUID.randomUUID();
    final int totalCommands = 1000;

    // Add commands to the queue
    for (int i = 0; i < totalCommands; i++) {
      OMJobworkerCommand command;
      if (i % 2 == 0) {
        command = createMockCommand(Type.unknownCommand);
      } else {
        command = createMockCommand(Type.reregisterCommand);
      }
      commandQueue.addCommand(jobworkerUuid, command);
    }

    assertEquals(totalCommands, commandQueue.getCommandsInQueue());
    final int numThreads = 5;
    final CountDownLatch startSignal = new CountDownLatch(1);
    final CountDownLatch doneSignal = new CountDownLatch(numThreads);
    final AtomicInteger totalRetrievedCommands = new AtomicInteger(0);
    final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    try {
      // Launch multiple threads to retrieve commands concurrently
      for (int i = 0; i < numThreads; i++) {
        executorService.submit(() -> {
          try {
            startSignal.await(); // Wait for start signal
            List<OMJobworkerCommand> commands = commandQueue.pollCommand(jobworkerUuid);
            totalRetrievedCommands.addAndGet(commands.size());
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            doneSignal.countDown();
          }
        });
      }

      startSignal.countDown();
      doneSignal.await();

      // Verify that only one thread got all the commands (thread-safety check)
      assertEquals(totalCommands, totalRetrievedCommands.get());
      assertEquals(0, commandQueue.getCommandsInQueue());
    } finally {
      executorService.shutdown();
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  /**
   * Helper method to create mock OMJobworkerCommand with specified type.
   */
  private OMJobworkerCommand createMockCommand(OMJobworkerCommandProto.Type type) {
    OMJobworkerCommand command = mock(OMJobworkerCommand.class);
    when(command.getType()).thenReturn(type);
    return command;
  }
}
