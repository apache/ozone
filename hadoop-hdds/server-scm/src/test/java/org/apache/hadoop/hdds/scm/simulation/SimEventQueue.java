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

package org.apache.hadoop.hdds.scm.simulation;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventExecutor;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.FixedThreadPoolWithAffinityExecutor;

/**
 * EventQueue whose handlers run on {@link SimScheduler} lanes instead of executor threads.
 * <p>
 * Every handler gets its own lane, like the single thread executor production gives it, except the container report
 * handlers SCM registers with a {@link FixedThreadPoolWithAffinityExecutor}: like that executor, full and incremental
 * reports share a fixed set of lanes, chosen by the payload hash (the datanode ID), so reports of one datanode are
 * handled in the order they arrived. The executors SCM passes in are closed unused.
 */
final class SimEventQueue extends EventQueue {

  private final SimScheduler scheduler;
  private final Function<Object, String> describer;
  private final List<SimScheduler.Lane> reportLanes = new ArrayList<>();
  private final List<String> handlerFailures = new ArrayList<>();

  SimEventQueue(SimScheduler scheduler, Function<Object, String> describer, int reportLaneCount) {
    this.scheduler = scheduler;
    this.describer = describer;
    for (int i = 0; i < reportLaneCount; i++) {
      reportLanes.add(scheduler.newLane("ContainerReportLane-" + i));
    }
    setSilent(true);
  }

  @Override
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void addHandler(EVENT_TYPE event, EventHandler<PAYLOAD> handler) {
    String name = getExecutorName(event, handler);
    SimScheduler.Lane lane = scheduler.newLane(name);
    super.addHandler(event, new LaneExecutor<>(name, event, payload -> lane), handler);
  }

  @Override
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void addHandler(
      EVENT_TYPE event, EventExecutor<PAYLOAD> executor, EventHandler<PAYLOAD> handler) {
    try {
      executor.close();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to close " + executor.getName(), e);
    }
    if (executor instanceof FixedThreadPoolWithAffinityExecutor) {
      super.addHandler(event, new LaneExecutor<>(executor.getName(), event,
          payload -> reportLanes.get(Math.floorMod(payload.hashCode(), reportLanes.size()))), handler);
    } else {
      addHandler(event, handler);
    }
  }

  private static String firstFrame(Throwable e) {
    StackTraceElement[] trace = e.getStackTrace();
    return trace.length == 0 ? "?" : trace[0].toString();
  }

  /** Exceptions thrown by handlers so far; production only logs them. */
  List<String> getHandlerFailures() {
    return handlerFailures;
  }

  /** Executor which queues each delivery on a scheduler lane. */
  private final class LaneExecutor<P> implements EventExecutor<P> {
    private final String name;
    private final String eventName;
    private final Function<P, SimScheduler.Lane> laneSelector;
    private long queued;
    private long scheduled;
    private long done;
    private long failed;

    private LaneExecutor(String name, Event<P> event, Function<P, SimScheduler.Lane> laneSelector) {
      this.name = name;
      this.eventName = event.getName();
      this.laneSelector = laneSelector;
    }

    @Override
    public void onMessage(EventHandler<P> handler, P payload, EventPublisher publisher) {
      queued++;
      String detail = eventName + " -> " + handler.getClass().getSimpleName() + " " + describer.apply(payload);
      laneSelector.apply(payload).add(detail, () -> {
        scheduled++;
        try {
          handler.onMessage(payload, publisher);
          done++;
        } catch (RuntimeException | AssertionError e) {
          failed++;
          handlerFailures.add(detail + ": " + e + " at " + firstFrame(e));
        }
      });
    }

    @Override
    public long failedEvents() {
      return failed;
    }

    @Override
    public long successfulEvents() {
      return done;
    }

    @Override
    public long queuedEvents() {
      return queued;
    }

    @Override
    public long scheduledEvents() {
      return scheduled;
    }

    @Override
    public void close() {
    }

    @Override
    public String getName() {
      return name;
    }
  }
}
