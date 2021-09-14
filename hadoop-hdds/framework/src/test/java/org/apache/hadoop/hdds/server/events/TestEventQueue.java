/**
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
package org.apache.hadoop.hdds.server.events;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Testing the basic functionality of the event queue.
 */
public class TestEventQueue {

  private static final Event<Long> EVENT1 =
      new TypedEvent<>(Long.class, "SCM_EVENT1");
  private static final Event<Long> EVENT2 =
      new TypedEvent<>(Long.class, "SCM_EVENT2");

  private EventQueue queue;

  private AtomicLong eventTotal = new AtomicLong();

  @Before
  public void startEventQueue() {
    DefaultMetricsSystem.initialize(getClass().getSimpleName());
    queue = new EventQueue();
  }

  @After
  public void stopEventQueue() {
    DefaultMetricsSystem.shutdown();
    queue.close();
  }

  @Test
  public void simpleEvent() {

    final long[] result = new long[2];

    queue.addHandler(EVENT1, (payload, publisher) -> result[0] = payload);

    queue.fireEvent(EVENT1, 11L);
    queue.processAll(1000);
    Assert.assertEquals(11, result[0]);

  }

  @Test
  public void simpleEventWithFixedThreadPoolExecutor() {

    TestHandler testHandler = new TestHandler();

    queue.addHandler(EVENT1, new FixedThreadPoolExecutor<>(EVENT1.getName(),
            EventQueue.getExecutorName(EVENT1, testHandler)), testHandler);

    queue.fireEvent(EVENT1, 11L);
    queue.fireEvent(EVENT1, 11L);
    queue.fireEvent(EVENT1, 11L);
    queue.fireEvent(EVENT1, 11L);
    queue.fireEvent(EVENT1, 11L);
    queue.fireEvent(EVENT1, 11L);
    queue.fireEvent(EVENT1, 11L);
    queue.fireEvent(EVENT1, 11L);
    queue.fireEvent(EVENT1, 11L);
    queue.fireEvent(EVENT1, 11L);

    EventExecutor eventExecutor =
        queue.getExecutorAndHandler(EVENT1).keySet().iterator().next();

    // As it is fixed threadpool executor with 10 threads, all should be
    // scheduled.
    Assert.assertEquals(10, eventExecutor.queuedEvents());

    // As we don't see all 10 events scheduled.
    Assert.assertTrue(eventExecutor.scheduledEvents() > 1 &&
        eventExecutor.scheduledEvents() <= 10);

    queue.processAll(60000);
    Assert.assertEquals(110, eventTotal.intValue());

    Assert.assertEquals(10, eventExecutor.successfulEvents());
    eventTotal.set(0);

  }

  /**
   * Event handler used in tests.
   */
  public class TestHandler implements EventHandler {
    @Override
    public void onMessage(Object payload, EventPublisher publisher) {
      try {
        Thread.sleep(2000);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
      eventTotal.getAndAdd((long) payload);
    }
  }

  @Test
  public void multipleSubscriber() {
    final long[] result = new long[2];
    queue.addHandler(EVENT2, (payload, publisher) -> result[0] = payload);

    queue.addHandler(EVENT2, (payload, publisher) -> result[1] = payload);

    queue.fireEvent(EVENT2, 23L);
    queue.processAll(1000);
    Assert.assertEquals(23, result[0]);
    Assert.assertEquals(23, result[1]);

  }

}