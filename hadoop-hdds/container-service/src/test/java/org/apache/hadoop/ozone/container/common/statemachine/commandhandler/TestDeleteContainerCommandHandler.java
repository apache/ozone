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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
  public void testExpiredCommandsAreNotProcessed()
      throws IOException, InterruptedException {
    CountDownLatch latch1 = new CountDownLatch(1);
    ThreadFactory threadFactory = new ThreadFactoryBuilder().build();
    ThreadPoolWithLockExecutor executor = new ThreadPoolWithLockExecutor(
        threadFactory, latch1);
    DeleteContainerCommandHandler handler = new DeleteContainerCommandHandler(
        clock, executor, 100);

    DeleteContainerCommand command1 = new DeleteContainerCommand(1L);
    command1.setDeadline(clock.millis() + 10000);
    DeleteContainerCommand command2 = new DeleteContainerCommand(2L);
    command2.setDeadline(clock.millis() + 20000);
    DeleteContainerCommand command3 = new DeleteContainerCommand(3L);
    // No deadline on the 3rd command

    clock.fastForward(15000);
    handler.handle(command1, ozoneContainer, null, null);
    latch1.await();
    assertEquals(1, handler.getTimeoutCount());
    CountDownLatch latch2 = new CountDownLatch(2);
    executor.setLatch(latch2);
    handler.handle(command2, ozoneContainer, null, null);
    handler.handle(command3, ozoneContainer, null, null);
    latch2.await();

    assertEquals(1, handler.getTimeoutCount());
    assertEquals(3, handler.getInvocationCount());
    verify(controller, times(0))
        .deleteContainer(1L, false);
    verify(controller, times(1))
        .deleteContainer(2L, false);
    verify(controller, times(1))
        .deleteContainer(3L, false);
  }

  @Test
  public void testCommandForCurrentTermIsExecuted()
      throws IOException, InterruptedException {
    // GIVEN
    DeleteContainerCommand command = new DeleteContainerCommand(1L);
    command.setTerm(1);

    when(context.getTermOfLeaderSCM())
        .thenReturn(OptionalLong.of(command.getTerm()));

    TestClock testClock = new TestClock(Instant.now(), ZoneId.systemDefault());
    CountDownLatch latch = new CountDownLatch(1);
    ThreadFactory threadFactory = new ThreadFactoryBuilder().build();
    ThreadPoolWithLockExecutor executor = new ThreadPoolWithLockExecutor(
        threadFactory, latch);
    DeleteContainerCommandHandler subject = new DeleteContainerCommandHandler(
        testClock, executor, 100);

    // WHEN
    subject.handle(command, ozoneContainer, context, null);
    latch.await();

    // THEN
    verify(controller, times(1))
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
    verify(controller, never())
        .deleteContainer(1L, false);
  }

  @Test
  public void testQueueSize() throws IOException {
    DeleteContainerCommandHandler handler = createSubjectWithPoolSize(
        clock, 1);
    DeleteContainerCommand command1 = new DeleteContainerCommand(1L);
    Lock lock = new ReentrantLock();
    doAnswer(invocation -> {
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
      verify(controller, times(1))
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
    ThreadFactory threadFactory = new ThreadFactoryBuilder().build();
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.
        newFixedThreadPool(1, threadFactory);
    return new DeleteContainerCommandHandler(clock, executor, queueSize);
  }

  private static DeleteContainerCommandHandler createSubjectWithPoolSize(
      TestClock clock, int queueSize) {
    return new DeleteContainerCommandHandler(1, clock, queueSize, "");
  }

  static class ThreadPoolWithLockExecutor extends ThreadPoolExecutor {
    private CountDownLatch countDownLatch;

    ThreadPoolWithLockExecutor(ThreadFactory threadFactory, CountDownLatch latch) {
      super(1, 1, 0, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>(), threadFactory);
      this.countDownLatch = latch;
    }

    void setLatch(CountDownLatch latch) {
      this.countDownLatch = latch;
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
      countDownLatch.countDown();
    }
  }
}
