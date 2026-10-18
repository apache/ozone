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

package org.apache.hadoop.hdds.server;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import javax.management.ObjectName;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Time;

/**
 * Tracks the phases of a service's startup, so that its progress can be observed while it is starting.
 * <p>
 * Phases are the constants of an enum, in the order they are executed. {@link Enum#toString()} is used as the
 * name of the phase. A phase may be divided into steps that run concurrently, e.g. one per volume. Each step
 * counts the work items done out of a known total, which is used to estimate the remaining time.
 *
 * @param <P> the enum defining the phases
 */
public class StartupProgress<P extends Enum<P>> implements StartupProgressMXBean {

  private final Map<P, Phase> phases;
  private final LongSupplier clock;
  private ObjectName jmxBean;

  public StartupProgress(Class<P> phaseType) {
    this(phaseType, Time::monotonicNow);
  }

  @VisibleForTesting
  StartupProgress(Class<P> phaseType, LongSupplier clock) {
    this.clock = clock;
    phases = new EnumMap<>(phaseType);
    for (P phase : phaseType.getEnumConstants()) {
      phases.put(phase, new Phase(phase.toString(), clock));
    }
  }

  /**
   * Begins the phase, ending all earlier phases. Has no effect if the phase has already begun.
   */
  public void beginPhase(P phase) {
    for (Map.Entry<P, Phase> entry : phases.entrySet()) {
      if (entry.getKey() == phase) {
        entry.getValue().begin();
        return;
      }
      entry.getValue().end();
    }
  }

  /**
   * Ends the phase and all earlier phases. A phase that has not begun is ended with zero elapsed time.
   */
  public void endPhase(P phase) {
    beginPhase(phase);
    phases.get(phase).end();
  }

  /**
   * Marks the running phases as failed.
   */
  public void fail() {
    for (Phase phase : phases.values()) {
      if (phase.getStatus() == Status.RUNNING) {
        phase.fail();
      }
    }
  }

  /**
   * @return a new running step of the phase.
   */
  public Step addStep(P phase, String name) {
    return phases.get(phase).addStep(name);
  }

  public void registerMXBean(String serviceName) {
    jmxBean = MBeans.register(serviceName, "StartupProgress", this);
  }

  public void unregisterMXBean() {
    if (jmxBean != null) {
      MBeans.unregister(jmxBean);
      jmxBean = null;
    }
  }

  @Override
  public boolean isComplete() {
    return phases.values().stream().allMatch(phase -> phase.getStatus() == Status.COMPLETE);
  }

  @Override
  public boolean isFailed() {
    return phases.values().stream().anyMatch(phase -> phase.getStatus() == Status.FAILED);
  }

  @Override
  public long getElapsedTime() {
    long start = -1;
    long end = -1;
    for (Tracked phase : phases.values()) {
      if (phase.startTime >= 0 && (start < 0 || phase.startTime < start)) {
        start = phase.startTime;
      }
      end = Math.max(end, phase.endTime);
    }
    if (start < 0) {
      return 0;
    }
    return (isComplete() || isFailed() ? end : clock.getAsLong()) - start;
  }

  @Override
  public List<PhaseInfo> getPhases() {
    return new ArrayList<>(phases.values());
  }

  /**
   * Start and end time of a phase or step.
   */
  private abstract static class Tracked {
    private final String name;
    private final LongSupplier clock;
    private volatile long startTime = -1;
    private volatile long endTime = -1;
    private volatile boolean failed;

    Tracked(String name, LongSupplier clock) {
      this.name = name;
      this.clock = clock;
    }

    synchronized void begin() {
      if (startTime < 0) {
        startTime = clock.getAsLong();
      }
    }

    public synchronized void end() {
      begin();
      if (endTime < 0) {
        endTime = clock.getAsLong();
      }
    }

    public synchronized void fail() {
      if (endTime < 0) {
        failed = true;
        end();
      }
    }

    public String getName() {
      return name;
    }

    public Status getStatus() {
      if (endTime >= 0) {
        return failed ? Status.FAILED : Status.COMPLETE;
      }
      return startTime >= 0 ? Status.RUNNING : Status.PENDING;
    }

    public long getElapsedTime() {
      long start = startTime;
      if (start < 0) {
        return 0;
      }
      long end = endTime;
      return (end >= 0 ? end : clock.getAsLong()) - start;
    }
  }

  private static final class Phase extends Tracked implements PhaseInfo {
    private final List<Step> steps = new CopyOnWriteArrayList<>();

    Phase(String name, LongSupplier clock) {
      super(name, clock);
    }

    Step addStep(String name) {
      Step step = new Step(name, ((Tracked) this).clock);
      step.begin();
      steps.add(step);
      return step;
    }

    /**
     * Steps run concurrently, so the phase completes with its slowest step. Unknown if no step is running,
     * since the phase may still have work to do outside of its steps.
     */
    @Override
    public long getRemainingTime() {
      Status status = getStatus();
      if (status == Status.COMPLETE) {
        return 0;
      }
      long remaining = -1;
      if (status == Status.RUNNING) {
        for (Step step : steps) {
          if (step.getStatus() == Status.RUNNING) {
            long stepRemaining = step.getRemainingTime();
            if (stepRemaining < 0) {
              return -1;
            }
            remaining = Math.max(remaining, stepRemaining);
          }
        }
      }
      return remaining;
    }

    @Override
    public long getDone() {
      return steps.stream().mapToLong(Step::getDone).sum();
    }

    @Override
    public long getTotal() {
      return steps.stream().mapToLong(Step::getTotal).sum();
    }

    @Override
    public List<StepInfo> getSteps() {
      return new ArrayList<>(steps);
    }
  }

  /**
   * A part of a phase which processes a known number of work items.
   */
  public static final class Step extends Tracked implements StepInfo {
    private final AtomicLong done = new AtomicLong();
    private volatile long total;

    private Step(String name, LongSupplier clock) {
      super(name, clock);
    }

    public void setTotal(long total) {
      this.total = total;
    }

    public void increment() {
      done.incrementAndGet();
    }

    /**
     * Estimates the remaining time assuming that the remaining work items are processed at the same rate.
     */
    @Override
    public long getRemainingTime() {
      Status status = getStatus();
      if (status == Status.COMPLETE) {
        return 0;
      }
      long doneCount = done.get();
      long totalCount = total;
      if (status != Status.RUNNING || doneCount <= 0 || totalCount <= 0) {
        return -1;
      }
      return Math.max(0, getElapsedTime() * (totalCount - doneCount) / doneCount);
    }

    @Override
    public long getDone() {
      return done.get();
    }

    @Override
    public long getTotal() {
      return total;
    }
  }
}
