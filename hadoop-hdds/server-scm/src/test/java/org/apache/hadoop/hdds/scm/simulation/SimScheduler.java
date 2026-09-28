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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.Predicate;
import org.apache.ozone.test.MockClock;

/**
 * Single-threaded scheduler for the SCM simulation.
 * <p>
 * Work comes from two sources:
 * <ul>
 *   <li>{@link Lane}s: FIFO queues standing in for the threads that run
 *   event handlers. Each lane keeps the order of its own tasks, like the
 *   single thread behind an event executor does.</li>
 *   <li>Timers: periodic SCM services, datanode heartbeats, delayed
 *   datanode work and injected faults.</li>
 * </ul>
 * Each step picks one runnable item (a non-empty lane or a due timer) using the seeded random source, which explores
 * the orderings that separate threads allow. Time only moves when nothing can run, to the next timer or to the end of a
 * slow handler: a picked handler is sometimes slow, which delays it and everything behind it on its lane while time
 * goes on.
 */
final class SimScheduler {

  private final MockClock clock;
  private final long startMillis;
  private final Random random;
  private final SimTrace trace;
  private final List<Lane> lanes = new ArrayList<>();
  private final TreeSet<Timer> timers = new TreeSet<>(
      Comparator.comparingLong(Timer::getTime).thenComparingLong(Timer::getSeq));
  private final double slowHandlerProbability;
  private final long maxSlowHandlerMs;
  private final List<String> timerFailures = new ArrayList<>();
  private long nextSeq;
  private long steps;

  SimScheduler(MockClock clock, Random random, SimTrace trace, double slowHandlerProbability, long maxSlowHandlerMs) {
    this.clock = clock;
    this.startMillis = clock.millis();
    this.random = random;
    this.trace = trace;
    this.slowHandlerProbability = slowHandlerProbability;
    this.maxSlowHandlerMs = maxSlowHandlerMs;
  }

  /** Drops every lane and the tasks queued on them, as when the process running the handlers dies. */
  void clearLanes() {
    lanes.clear();
  }

  Lane newLane(String name) {
    Lane lane = new Lane(name);
    lanes.add(lane);
    return lane;
  }

  Timer schedule(String name, long delayMs, Runnable action) {
    return add(new Timer(name, clock.millis() + delayMs, 0, action));
  }

  Timer scheduleEvery(String name, long initialDelayMs, long periodMs, Runnable action) {
    return add(new Timer(name, clock.millis() + initialDelayMs, periodMs, action));
  }

  void cancel(Timer timer) {
    if (timer != null) {
      timers.remove(timer);
    }
  }

  /** A task that is requested now and then, and runs once for all requests made before it runs. */
  WakeUp newWakeUp(String name, Runnable action) {
    return new WakeUp(name, action);
  }

  private Timer add(Timer timer) {
    timers.add(timer);
    return timer;
  }

  long now() {
    return clock.millis();
  }

  long elapsed() {
    return clock.millis() - startMillis;
  }

  long steps() {
    return steps;
  }

  Random random() {
    return random;
  }

  /** Exceptions thrown by timer actions so far. */
  List<String> getTimerFailures() {
    return timerFailures;
  }

  SimTrace trace() {
    return trace;
  }

  boolean hasPendingHandlerWork() {
    return hasPendingHandlerWork(name -> true);
  }

  /** Whether any lane whose name matches the filter has queued tasks. */
  boolean hasPendingHandlerWork(Predicate<String> laneFilter) {
    for (Lane lane : lanes) {
      if (!lane.tasks.isEmpty() && laneFilter.test(lane.name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Runs one item, or advances the clock to the next timer when nothing can run at the current time.
   *
   * @return false if there is neither handler work nor any timer left.
   */
  boolean step() {
    long now = clock.millis();
    long nextTime = timers.isEmpty() ? Long.MAX_VALUE : timers.first().time;
    List<Lane> ready = new ArrayList<>();
    for (Lane lane : lanes) {
      if (!lane.tasks.isEmpty()) {
        if (lane.busyUntil <= now) {
          ready.add(lane);
        } else {
          nextTime = Math.min(nextTime, lane.busyUntil);
        }
      }
    }
    List<Timer> due = new ArrayList<>();
    for (Timer timer : timers) {
      if (timer.time > now) {
        break;
      }
      due.add(timer);
    }
    if (ready.isEmpty() && due.isEmpty()) {
      if (nextTime == Long.MAX_VALUE) {
        return false;
      }
      advanceTo(nextTime);
      return true;
    }
    steps++;
    int pick = random.nextInt(ready.size() + due.size());
    if (pick < ready.size()) {
      Lane lane = ready.get(pick);
      Task task = lane.tasks.peekFirst();
      if (!task.slow && random.nextDouble() < slowHandlerProbability) {
        // This invocation is slow: it takes effect later, and holds up the tasks behind it.
        task.slow = true;
        lane.busyUntil = now + 1 + (long) (random.nextDouble() * maxSlowHandlerMs);
        trace.record(elapsed(), "slow", lane.name + " " + task.detail + " until +" + (lane.busyUntil - now));
        return true;
      }
      lane.tasks.pollFirst();
      trace.record(elapsed(), "handle", lane.name + " " + task.detail);
      task.action.run();
    } else {
      Timer timer = due.get(pick - ready.size());
      timers.remove(timer);
      if (timer.period > 0) {
        // Re-arm before running, so the action can cancel its own timer.
        timer.time = clock.millis() + timer.period;
        timers.add(timer);
      }
      trace.record(elapsed(), "timer", timer.name);
      try {
        timer.action.run();
      } catch (RuntimeException | AssertionError e) {
        // A failing periodic SCM service keeps running in production; report it and go on.
        StackTraceElement[] frames = e.getStackTrace();
        timerFailures.add(timer.name + ": " + e + " at " + (frames.length == 0 ? "?" : frames[0]));
      }
    }
    return true;
  }

  /** Runs handler work queued at the current time, without firing timers. */
  void drainHandlers(long maxSteps) {
    long count = 0;
    while (hasPendingHandlerWork()) {
      if (++count > maxSteps) {
        throw new IllegalStateException("Event handlers did not settle after " + maxSteps + " steps");
      }
      List<Lane> ready = new ArrayList<>();
      for (Lane lane : lanes) {
        if (!lane.tasks.isEmpty()) {
          ready.add(lane);
        }
      }
      Lane lane = ready.get(random.nextInt(ready.size()));
      Task task = lane.tasks.pollFirst();
      steps++;
      trace.record(elapsed(), "handle", lane.name + " " + task.detail);
      task.action.run();
    }
  }

  private void advanceTo(long time) {
    if (time > clock.millis()) {
      clock.fastForward(time - clock.millis());
    }
  }

  /** FIFO queue of tasks, standing in for the thread of one event executor. */
  final class Lane {
    private final String name;
    private final ArrayDeque<Task> tasks = new ArrayDeque<>();
    private long busyUntil;

    private Lane(String name) {
      this.name = name;
    }

    void add(String detail, Runnable action) {
      tasks.addLast(new Task(detail, action));
    }

    String name() {
      return name;
    }
  }

  private static final class Task {
    private final String detail;
    private final Runnable action;
    private boolean slow;

    private Task(String detail, Runnable action) {
      this.detail = detail;
      this.action = action;
    }
  }

  /** Stands in for waking up a thread that waits for work: requests made while one is pending are merged. */
  final class WakeUp {
    private final String name;
    private final Runnable action;
    private Timer pending;

    private WakeUp(String name, Runnable action) {
      this.name = name;
      this.action = action;
    }

    void request() {
      request(0);
    }

    void request(long delayMs) {
      if (pending == null) {
        pending = schedule(name, delayMs, () -> {
          pending = null;
          action.run();
        });
      }
    }

    void cancel() {
      SimScheduler.this.cancel(pending);
      pending = null;
    }
  }

  /** A one-shot or periodic action at a simulated time. */
  final class Timer {
    private final String name;
    private long time;
    private final long period;
    private final long seq;
    private final Runnable action;

    private Timer(String name, long time, long period, Runnable action) {
      this.name = name;
      this.time = time;
      this.period = period;
      this.action = action;
      this.seq = nextSeq++;
    }

    private long getTime() {
      return time;
    }

    private long getSeq() {
      return seq;
    }
  }
}
