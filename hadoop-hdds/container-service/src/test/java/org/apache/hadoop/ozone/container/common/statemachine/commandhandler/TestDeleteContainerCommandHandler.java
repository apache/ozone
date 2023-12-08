/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.OptionalLong;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Test for the DeleteContainerCommandHandler.
 */
public class TestDeleteContainerCommandHandler {

  private TestClock clock;
  private OzoneContainer ozoneContainer;
  private ContainerController controller;
  private StateContext context;

  @BeforeEach
  public void setup() {
    clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    ozoneContainer = mock(OzoneContainer.class);
    controller = mock(ContainerController.class);
    when(ozoneContainer.getController()).thenReturn(controller);
    context = mock(StateContext.class);
    when(context.getTermOfLeaderSCM())
        .thenReturn(OptionalLong.of(0));
  }

  @Test
  public void testExpiredCommandsAreNotProcessed() throws IOException {
    DeleteContainerCommandHandler handler = createSubject(clock, 1000);

    DeleteContainerCommand command1 = new DeleteContainerCommand(1L);
    command1.setDeadline(clock.millis() + 10000);
    DeleteContainerCommand command2 = new DeleteContainerCommand(2L);
    command2.setDeadline(clock.millis() + 20000);
    DeleteContainerCommand command3 = new DeleteContainerCommand(3L);
    // No deadline on the 3rd command

    clock.fastForward(15000);
    handler.handle(command1, ozoneContainer, null, null);
    Assertions.assertEquals(1, handler.getTimeoutCount());
    handler.handle(command2, ozoneContainer, null, null);
    handler.handle(command3, ozoneContainer, null, null);
    Assertions.assertEquals(1, handler.getTimeoutCount());
    Assertions.assertEquals(3, handler.getInvocationCount());
    Mockito.verify(controller, times(0))
        .deleteContainer(1L, false);
    Mockito.verify(controller, times(1))
        .deleteContainer(2L, false);
    Mockito.verify(controller, times(1))
        .deleteContainer(3L, false);
  }

  @Test
  public void testCommandForCurrentTermIsExecuted() throws IOException {
    // GIVEN
    DeleteContainerCommand command = new DeleteContainerCommand(1L);
    command.setTerm(1);

    when(context.getTermOfLeaderSCM())
        .thenReturn(OptionalLong.of(command.getTerm()));

    DeleteContainerCommandHandler subject = createSubject();

    // WHEN
    subject.handle(command, ozoneContainer, context, null);

    // THEN
    Mockito.verify(controller, times(1))
        .deleteContainer(1L, false);
  }

  @Test
  public void testCommandForOldTermIsDropped() throws IOException {
    // GIVEN
    DeleteContainerCommand command = new DeleteContainerCommand(1L);
    command.setTerm(1);

    when(context.getTermOfLeaderSCM())
        .thenReturn(OptionalLong.of(command.getTerm() + 1));

    DeleteContainerCommandHandler subject = createSubject();

    // WHEN
    subject.handle(command, ozoneContainer, context, null);

    // THEN
    Mockito.verify(controller, never())
        .deleteContainer(1L, false);
  }

  @Test
  public void testQueueSize() throws IOException {
    DeleteContainerCommandHandler handler = createSubjectWithPoolSize(
        clock, 1);
    DeleteContainerCommand command1 = new DeleteContainerCommand(1L);
    Lock lock = new ReentrantLock();
    Mockito.doAnswer(invocation -> {
      try {
        lock.lock();
      } finally {
        lock.unlock();
      }
      return null;
    }).when(controller).deleteContainer(1, false);

    lock.lock();
    try {
      for (int i = 0; i < 50; ++i) {
        handler.handle(command1, ozoneContainer, null, null);
      }
    
      // one is waiting in execution as thread count 1, so count 1
      // and one in queue, others ignored
      Mockito.verify(controller, times(1))
          .deleteContainer(1L, false);
    } finally {
      lock.unlock();
    }
  }

  private static DeleteContainerCommandHandler createSubject() {
    TestClock clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    return createSubject(clock, 1000);
  }

  private static DeleteContainerCommandHandler createSubject(
      TestClock clock, int queueSize) {
    return new DeleteContainerCommandHandler(clock,
        newDirectExecutorService(), queueSize);
  }

  private static DeleteContainerCommandHandler createSubjectWithPoolSize(
      TestClock clock, int queueSize) {
    return new DeleteContainerCommandHandler(1, clock, queueSize, "");
  }

}
