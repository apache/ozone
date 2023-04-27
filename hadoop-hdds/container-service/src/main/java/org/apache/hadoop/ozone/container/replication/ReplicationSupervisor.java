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
package org.apache.hadoop.ozone.container.replication;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask.Status;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single point to schedule the downloading tasks based on priorities.
 */
public class ReplicationSupervisor {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicationSupervisor.class);

  private static final Comparator<TaskRunner> TASK_RUNNER_COMPARATOR =
      Comparator.comparing(TaskRunner::getTaskPriority)
          .thenComparing(TaskRunner::getTaskQueueTime);

  private final ExecutorService executor;
  private final StateContext context;
  private final Clock clock;

  private final AtomicLong requestCounter = new AtomicLong();
  private final AtomicLong successCounter = new AtomicLong();
  private final AtomicLong failureCounter = new AtomicLong();
  private final AtomicLong timeoutCounter = new AtomicLong();
  private final AtomicLong skippedCounter = new AtomicLong();

  /**
   * A set of container IDs that are currently being downloaded
   * or queued for download. Tracked so we don't schedule > 1
   * concurrent download for the same container. Note that the uniqueness of a
   * task is defined by the tasks equals and hashCode methods.
   */
  private final Set<AbstractReplicationTask> inFlight;

  private final Map<Class, AtomicInteger> taskCounter =
      new ConcurrentHashMap<>();
  private int maxQueueSize;

  @VisibleForTesting
  ReplicationSupervisor(
      StateContext context, ExecutorService executor, Clock clock,
      int maxQueueSize) {
    this.inFlight = ConcurrentHashMap.newKeySet();
    this.executor = executor;
    this.context = context;
    this.clock = clock;
    this.maxQueueSize = maxQueueSize;
  }

  public ReplicationSupervisor(
      StateContext context, ReplicationConfig replicationConfig, Clock clock,
      int maxQueueSize) {
    this(context,
        new ThreadPoolExecutor(
            replicationConfig.getReplicationMaxStreams(),
            replicationConfig.getReplicationMaxStreams(), 60, TimeUnit.SECONDS,
            new PriorityBlockingQueue<>(),
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("ContainerReplicationThread-%d")
                .build()),
        clock, maxQueueSize);
  }

  /**
   * Queue an asynchronous download of the given container.
   */
  public void addTask(AbstractReplicationTask task) {
    if (getTotalInFlightReplications() >= maxQueueSize) {
      LOG.warn("Ignored {} command for container {} in Replication Supervisor"
              + "as queue reached max size of {}.",
          task.getClass(), task.getContainerId(), maxQueueSize);
      return;
    }
    if (inFlight.add(task)) {
      if (task.getPriority() != ReplicationCommandPriority.LOW) {
        // Low priority tasks are not included in the replication queue sizes
        // returned to SCM in the heartbeat, so we only update the count for
        // priorities other than low.
        taskCounter.computeIfAbsent(task.getClass(),
            k -> new AtomicInteger()).incrementAndGet();
      }
      executor.execute(new TaskRunner(task));
    }
  }

  private void decrementTaskCounter(AbstractReplicationTask task) {
    if (task.getPriority() == ReplicationCommandPriority.LOW) {
      // LOW tasks are not included in the counter, so skip decrementing the
      // counter.
      return;
    }
    AtomicInteger counter = taskCounter.get(task.getClass());
    if (counter != null) {
      counter.decrementAndGet();
    }
  }

  @VisibleForTesting
  public void shutdownAfterFinish() throws InterruptedException {
    executor.shutdown();
    executor.awaitTermination(1L, TimeUnit.DAYS);
  }

  public void stop() {
    try {
      executor.shutdown();
      if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException ie) {
      // Ignore, we don't really care about the failure.
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Given the Class of a AbstractReplicationTask, return the count of tasks
   * currently inflight (queued or running) for that type of task.
   *
   * @param taskClass The Class of the tasks to get a count for.
   * @return Count of in-flight replications for the type of task.
   */
  public int getInFlightReplications(
      Class<? extends AbstractReplicationTask> taskClass) {
    AtomicInteger counter = taskCounter.get(taskClass);
    return counter == null ? 0 : counter.get();
  }

  /**
   * Returns a count of all inflight replication tasks across all task types.
   * Note that `getInFlightReplications(Class taskClass) allows for the .count
   * of replications for a given class to be retrieved.
   * @return Total replication tasks queued or running in the supervisor
   */
  public int getTotalInFlightReplications() {
    return inFlight.size();
  }

  /**
   * An executable form of a replication task with status handling.
   */
  public final class TaskRunner implements Comparable, Runnable {
    private final AbstractReplicationTask task;

    public TaskRunner(AbstractReplicationTask task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        requestCounter.incrementAndGet();

        final long now = clock.millis();
        final long deadline = task.getDeadline();
        if (deadline > 0 && now > deadline) {
          LOG.info("Ignoring {} since the deadline has passed ({} < {})",
              this, Instant.ofEpochMilli(deadline), Instant.ofEpochMilli(now));
          timeoutCounter.incrementAndGet();
          return;
        }

        if (context != null) {
          DatanodeDetails dn = context.getParent().getDatanodeDetails();
          if (dn != null && dn.getPersistedOpState() !=
              HddsProtos.NodeOperationalState.IN_SERVICE
              && task.shouldOnlyRunOnInServiceDatanodes()) {
            LOG.info("Ignoring {} since datanode is not in service ({})",
                this, dn.getPersistedOpState());
            return;
          }

          final OptionalLong currentTerm = context.getTermOfLeaderSCM();
          final long taskTerm = task.getTerm();
          if (currentTerm.isPresent() && taskTerm < currentTerm.getAsLong()) {
            LOG.info("Ignoring {} since SCM leader has new term ({} < {})",
                this, taskTerm, currentTerm.getAsLong());
            return;
          }
        }

        task.setStatus(Status.IN_PROGRESS);
        task.runTask();
        if (task.getStatus() == Status.FAILED) {
          LOG.warn("Failed {}", this);
          failureCounter.incrementAndGet();
        } else if (task.getStatus() == Status.DONE) {
          LOG.info("Successful {}", this);
          successCounter.incrementAndGet();
        } else if (task.getStatus() == Status.SKIPPED) {
          LOG.info("Skipped {}", this);
          skippedCounter.incrementAndGet();
        }
      } catch (Exception e) {
        task.setStatus(Status.FAILED);
        LOG.warn("Failed {}", this, e);
        failureCounter.incrementAndGet();
      } finally {
        inFlight.remove(task);
        decrementTaskCounter(task);
      }
    }

    @Override
    public String toString() {
      return task.toString();
    }

    public ReplicationCommandPriority getTaskPriority() {
      return task.getPriority();
    }

    public long getTaskQueueTime() {
      return task.getQueued().toEpochMilli();
    }

    @Override
    public int compareTo(Object o) {
      return TASK_RUNNER_COMPARATOR.compare(this, (TaskRunner) o);
    }

    @Override
    public int hashCode() {
      return Objects.hash(task);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TaskRunner that = (TaskRunner) o;
      return task.equals(that.task);
    }
  }

  public long getReplicationRequestCount() {
    return requestCounter.get();
  }

  public long getQueueSize() {
    if (executor instanceof ThreadPoolExecutor) {
      return ((ThreadPoolExecutor)executor).getQueue().size();
    } else {
      return 0;
    }
  }

  public long getReplicationSuccessCount() {
    return successCounter.get();
  }

  public long getReplicationFailureCount() {
    return failureCounter.get();
  }

  public long getReplicationTimeoutCount() {
    return timeoutCounter.get();
  }

  public long getReplicationSkippedCount() {
    return skippedCounter.get();
  }

}
