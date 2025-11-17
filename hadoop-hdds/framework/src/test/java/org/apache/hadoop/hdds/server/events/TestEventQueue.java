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

package org.apache.hadoop.hdds.server.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

  @BeforeEach
  public void startEventQueue() {
    DefaultMetricsSystem.initialize(getClass().getSimpleName());
    queue = new EventQueue();
  }

  @AfterEach
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
    assertEquals(11, result[0]);

  }

  @Test
  public void simpleEventWithFixedThreadPoolExecutor()
      throws Exception {

    TestHandler testHandler = new TestHandler();
    BlockingQueue<Long> eventQueue = new LinkedBlockingQueue<>();
    List<BlockingQueue<Long>> queues = new ArrayList<>();
    queues.add(eventQueue);
    Map<String, FixedThreadPoolWithAffinityExecutor> reportExecutorMap
        = new ConcurrentHashMap<>();
    queue.addHandler(EVENT1,
        new FixedThreadPoolWithAffinityExecutor<>(
            EventQueue.getExecutorName(EVENT1, testHandler),
            testHandler, queues, queue, Long.class,
            FixedThreadPoolWithAffinityExecutor.initializeExecutorPool(
            queues), reportExecutorMap), testHandler);

    queue.fireEvent(EVENT1, 11L);
    queue.fireEvent(EVENT1, 11L);
    queue.fireEvent(EVENT1, 12L);
    queue.fireEvent(EVENT1, 13L);
    queue.fireEvent(EVENT1, 14L);
    queue.fireEvent(EVENT1, 15L);
    queue.fireEvent(EVENT1, 16L);
    queue.fireEvent(EVENT1, 17L);
    queue.fireEvent(EVENT1, 18L);
    queue.fireEvent(EVENT1, 19L);
    queue.fireEvent(EVENT1, 20L);

    EventExecutor eventExecutor =
        queue.getExecutorAndHandler(EVENT1).keySet().iterator().next();

    // As it is fixed threadpool executor with 10 threads, all should be
    // scheduled.
    assertEquals(11, eventExecutor.queuedEvents());
    Thread.currentThread().sleep(500);

    // As we don't see all 10 events scheduled.
    assertThat(eventExecutor.scheduledEvents()).isGreaterThanOrEqualTo(1)
        .isLessThanOrEqualTo(10);

    queue.processAll(60000);

    assertEquals(11, eventExecutor.scheduledEvents());

    assertEquals(166, eventTotal.intValue());

    assertEquals(11, eventExecutor.successfulEvents());
    eventTotal.set(0);
    eventExecutor.close();
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
    assertEquals(23, result[0]);
    assertEquals(23, result[1]);

  }
}
