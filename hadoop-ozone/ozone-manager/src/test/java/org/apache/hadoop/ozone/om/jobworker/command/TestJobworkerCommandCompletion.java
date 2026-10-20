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

package org.apache.hadoop.ozone.om.jobworker.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verifyNoInteractions;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.junit.jupiter.api.Test;

/**
 * Checks assignment validation and atomic completion versus timeout resolution.
 */
class TestJobworkerCommandCompletion {
  private static final String SERVICE = "service";

  private static OMJobworkerCommandManager newManager(JobworkerNodeManager nodes) {
    return new OMJobworkerCommandManager(nodes, SERVICE, () -> OptionalLong.of(1));
  }

  @Test
  void testDuplicateSubmissionKeepsOriginalTracking() throws Exception {
    JobworkerCommandListener listener = mock(JobworkerCommandListener.class);
    UUID workerId = UUID.randomUUID();
    try (OMJobworkerCommandManager manager = newManager(mock(JobworkerNodeManager.class))) {
      manager.registerHandler(Type.mockCommand, listener);
      manager.sendCommand(workerId, new MockOMJobworkerCommand(1L, Type.mockCommand));
      assertThrows(IOException.class,
          () -> manager.sendCommand(workerId, new MockOMJobworkerCommand(1L, Type.mockCommand)));
      assertEquals(1, manager.getInFlightCommandCount(Type.mockCommand));
    }
  }

  @Test
  void testNotReadyLeaderCannotSendCommand() throws Exception {
    JobworkerNodeManager nodes = mock(JobworkerNodeManager.class);
    try (OMJobworkerCommandManager manager = new OMJobworkerCommandManager(nodes, SERVICE, OptionalLong::empty)) {
      manager.registerHandler(Type.mockCommand, mock(JobworkerCommandListener.class));
      assertThrows(IOException.class,
          () -> manager.sendCommand(UUID.randomUUID(), new MockOMJobworkerCommand(1L, Type.mockCommand)));
      assertEquals(0, manager.getInFlightCommandCount(Type.mockCommand));
      verifyNoInteractions(nodes);
    }
  }

  @Test
  void testLeadershipChangeWhileQueueingRollsBack() throws Exception {
    long[] term = {1};
    JobworkerNodeManager nodes = mock(JobworkerNodeManager.class);
    doAnswer(invocation -> {
      term[0] = 2;
      return null;
    }).when(nodes).addOMJobworkerCommand(any(), any());
    try (OMJobworkerCommandManager manager =
        new OMJobworkerCommandManager(nodes, SERVICE, () -> OptionalLong.of(term[0]))) {
      manager.registerHandler(Type.mockCommand, mock(JobworkerCommandListener.class));
      assertThrows(IOException.class,
          () -> manager.sendCommand(UUID.randomUUID(), new MockOMJobworkerCommand(1L, Type.mockCommand)));
      assertEquals(0, manager.getInFlightCommandCount(Type.mockCommand));
    }
  }

  @Test
  void testEnqueueFailureDoesNotLeavePhantomTracking() throws Exception {
    JobworkerNodeManager nodes = mock(JobworkerNodeManager.class);
    doThrow(new IllegalStateException("enqueue failed")).when(nodes).addOMJobworkerCommand(any(), any());
    try (OMJobworkerCommandManager manager = newManager(nodes)) {
      manager.registerHandler(Type.mockCommand, mock(JobworkerCommandListener.class));
      assertThrows(IllegalStateException.class,
          () -> manager.sendCommand(UUID.randomUUID(), new MockOMJobworkerCommand(1L, Type.mockCommand)));
      assertEquals(0, manager.getInFlightCommandCount(Type.mockCommand));
    }
  }

  @Test
  void testRejectsStatusForAnotherWorkerOrService() throws Exception {
    JobworkerDetails owner = MockJobworkerDetails.randomJobworkerDetails();
    JobworkerDetails other = MockJobworkerDetails.randomJobworkerDetails();
    JobworkerCommandListener listener = mock(JobworkerCommandListener.class);
    try (OMJobworkerCommandManager manager = newManager(mock(JobworkerNodeManager.class))) {
      manager.registerHandler(Type.mockCommand, listener);
      manager.sendCommand(owner.getUuid(), new MockOMJobworkerCommand(1L, Type.mockCommand));
      manager.processStatusUpdate(other, success(1L, SERVICE));
      manager.processStatusUpdate(owner, success(1L, "other-service"));
      assertTrue(manager.isCommandInFlight(Type.mockCommand, 1L));
      verifyNoInteractions(listener);
    }
  }

  @Test
  void testCompletionAndTimeoutResolveEachAttemptOnce() throws Exception {
    JobworkerDetails owner = MockJobworkerDetails.randomJobworkerDetails();
    UUID workerId = owner.getUuid();
    JobworkerCommandListener listener = mock(JobworkerCommandListener.class);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try (OMJobworkerCommandManager manager = newManager(mock(JobworkerNodeManager.class))) {
      manager.registerHandler(Type.mockCommand, listener);
      OMJobworkerCommandManager.CommandTimeoutChecker checker = manager.getCommandTimeoutChecker();
      checker.setClock(() -> Long.MAX_VALUE);
      for (long id = 1; id <= 100; id++) {
        manager.sendCommand(workerId, new MockOMJobworkerCommand(id, Type.mockCommand));
        CommandStatus status = success(id, SERVICE);
        CountDownLatch start = new CountDownLatch(1);
        Future<?> completion = executor.submit(() -> {
          start.await();
          manager.processStatusUpdate(owner, status);
          return null;
        });
        Future<?> timeout = executor.submit(() -> {
          start.await();
          checker.checkTimeoutsForProcessor(Type.mockCommand);
          return null;
        });
        start.countDown();
        completion.get();
        timeout.get();
        // A repeated report cannot deliver another terminal callback.
        manager.processStatusUpdate(owner, status);
        assertEquals(0, manager.getInFlightCommandCount(Type.mockCommand));
      }
      long terminalCalls = mockingDetails(listener).getInvocations().stream()
          .filter(call -> call.getMethod().getName().equals("onCommandSucceeded")
              || call.getMethod().getName().equals("onStatusUpdateTimeout"))
          .count();
      assertEquals(100, terminalCalls);
    } finally {
      executor.shutdownNow();
    }
  }

  private static CommandStatus success(long id, String service) {
    return CommandStatus.newBuilder()
        .setCmdId(id)
        .setType(Type.mockCommand)
        .setOmServiceId(service)
        .setStatus(CommandStatus.Status.SUCCEEDED)
        .build();
  }
}
