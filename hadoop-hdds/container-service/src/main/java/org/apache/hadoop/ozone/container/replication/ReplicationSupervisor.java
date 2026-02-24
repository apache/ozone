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

package org.apache.hadoop.ozone.container.replication;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.isDecommission;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.isMaintenance;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.container.checksum.ReconcileContainerTask;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinatorTask;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask.Status;
import org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single point to schedule the downloading tasks based on priorities.
 */
public final class ReplicationSupervisor {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicationSupervisor.class);

  private static final Comparator<TaskRunner> TASK_RUNNER_COMPARATOR =
      Comparator.comparing(TaskRunner::getTaskPriority)
          .thenComparing(TaskRunner::getTaskQueueTime);

  private final ExecutorService executor;
  private final StateContext context;
  private final Clock clock;

  private final Map<String, AtomicLong> requestCounter = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> successCounter = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> failureCounter = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> timeoutCounter = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> skippedCounter = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> queuedCounter = new ConcurrentHashMap<>();

  private final MetricsRegistry registry;
  private final Map<String, MutableRate> opsLatencyMs = new ConcurrentHashMap<>();

  private static final Map<String, String> METRICS_MAP;

  static {
    METRICS_MAP = new HashMap<>();
  }

  /**
   * A set of container IDs that are currently being downloaded
   * or queued for download. Tracked so we don't schedule > 1
   * concurrent download for the same container. Note that the uniqueness of a
   * task is defined by the tasks equals and hashCode methods.
   */
  private final Set<AbstractReplicationTask> inFlight;

  private final Map<Class<?>, AtomicInteger> taskCounter =
      new ConcurrentHashMap<>();
  private int maxQueueSize;

  private final AtomicReference<HddsProtos.NodeOperationalState> state
      = new AtomicReference<>();
  private final IntConsumer executorThreadUpdater;
  private final ReplicationConfig replicationConfig;
  private final DatanodeConfiguration datanodeConfig;

  /**
   * Builder for {@link ReplicationSupervisor}.
   */
  public static class Builder {
    private StateContext context;
    private ReplicationConfig replicationConfig;
    private DatanodeConfiguration datanodeConfig;
    private ExecutorService executor;
    private Clock clock;
    private IntConsumer executorThreadUpdater = threadCount -> {
    };

    public Builder clock(Clock newClock) {
      clock = newClock;
      return this;
    }

    public Builder executor(ExecutorService newExecutor) {
      executor = newExecutor;
      return this;
    }

    public Builder replicationConfig(ReplicationConfig newReplicationConfig) {
      replicationConfig = newReplicationConfig;
      return this;
    }

    public Builder datanodeConfig(DatanodeConfiguration newDatanodeConfig) {
      datanodeConfig = newDatanodeConfig;
      return this;
    }

    public Builder stateContext(StateContext newContext) {
      context = newContext;
      return this;
    }

    public Builder executorThreadUpdater(IntConsumer newUpdater) {
      executorThreadUpdater = newUpdater;
      return this;
    }

    public ReplicationSupervisor build() {
      if (replicationConfig == null || datanodeConfig == null) {
        ConfigurationSource conf = new OzoneConfiguration();
        if (replicationConfig == null) {
          replicationConfig =
              conf.getObject(ReplicationServer.ReplicationConfig.class);
        }
        if (datanodeConfig == null) {
          datanodeConfig = conf.getObject(DatanodeConfiguration.class);
        }
      }

      if (clock == null) {
        clock = Clock.system(ZoneId.systemDefault());
      }

      if (executor == null) {
        LOG.info("Initializing replication supervisor with thread count = {}",
            replicationConfig.getReplicationMaxStreams());
        String threadNamePrefix = context != null ? context.getThreadNamePrefix() : "";
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(threadNamePrefix + "ContainerReplicationThread-%d")
            .build();
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(
            replicationConfig.getReplicationMaxStreams(),
            replicationConfig.getReplicationMaxStreams(),
            60, TimeUnit.SECONDS,
            new PriorityBlockingQueue<>(),
            threadFactory);
        executor = tpe;
        executorThreadUpdater = threadCount -> {
          if (threadCount < tpe.getCorePoolSize()) {
            tpe.setCorePoolSize(threadCount);
            tpe.setMaximumPoolSize(threadCount);
          } else {
            tpe.setMaximumPoolSize(threadCount);
            tpe.setCorePoolSize(threadCount);
          }
        };
      }

      return new ReplicationSupervisor(context, executor, replicationConfig,
          datanodeConfig, clock, executorThreadUpdater);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Map<String, String> getMetricsMap() {
    return Collections.unmodifiableMap(METRICS_MAP);
  }

  private ReplicationSupervisor(StateContext context, ExecutorService executor,
      ReplicationConfig replicationConfig, DatanodeConfiguration datanodeConfig,
      Clock clock, IntConsumer executorThreadUpdater) {
    this.inFlight = ConcurrentHashMap.newKeySet();
    this.context = context;
    this.executor = executor;
    this.replicationConfig = replicationConfig;
    this.datanodeConfig = datanodeConfig;
    maxQueueSize = datanodeConfig.getCommandQueueLimit();
    this.clock = clock;
    this.executorThreadUpdater = executorThreadUpdater;

    // set initial state
    if (context != null) {
      DatanodeDetails dn = context.getParent().getDatanodeDetails();
      if (dn != null) {
        nodeStateUpdated(dn.getPersistedOpState());
      }
    }
    registry = new MetricsRegistry(ReplicationSupervisor.class.getSimpleName());
    initAllTaskCounters();
  }

  /**
   * Queue an asynchronous download of the given container.
   */
  public void addTask(AbstractReplicationTask task) {
    if (queueHasRoomFor(task)) {
      initCounters(task);
      addToQueue(task);
    }
  }

  private boolean queueHasRoomFor(AbstractReplicationTask task) {
    final int max = maxQueueSize;
    if (getTotalInFlightReplications() >= max) {
      LOG.warn("Ignored {} command for container {} in Replication Supervisor"
              + "as queue reached max size of {}.",
          task.getClass(), task.getContainerId(), max);
      return false;
    }
    return true;
  }

  private void initAllTaskCounters() {
    initCounters(ReplicationTask.METRIC_NAME,
        ReplicationTask.METRIC_DESCRIPTION_SEGMENT,
        ReplicationTask.class.getSimpleName());
    initCounters(ECReconstructionCoordinatorTask.METRIC_NAME,
        ECReconstructionCoordinatorTask.METRIC_DESCRIPTION_SEGMENT,
        ECReconstructionCoordinatorTask.class.getSimpleName());
    initCounters(ReconcileContainerTask.METRIC_NAME,
        ReconcileContainerTask.METRIC_DESCRIPTION_SEGMENT,
        ReconcileContainerTask.class.getSimpleName());
  }

  public void initCounters(AbstractReplicationTask task) {
    initCounters(task.getMetricName(), task.getMetricDescriptionSegment(),
        task.getClass().getSimpleName());
  }

  public void initCounters(String metricName, String metricDescriptionSegment,
      String taskSimpleName) {
    if (requestCounter.get(metricName) == null) {
      synchronized (this) {
        if (requestCounter.get(metricName) == null) {
          requestCounter.put(metricName, new AtomicLong(0));
          successCounter.put(metricName, new AtomicLong(0));
          failureCounter.put(metricName, new AtomicLong(0));
          timeoutCounter.put(metricName, new AtomicLong(0));
          skippedCounter.put(metricName, new AtomicLong(0));
          queuedCounter.put(metricName, new AtomicLong(0));
          opsLatencyMs.put(metricName, registry.newRate(taskSimpleName + "Ms"));
          METRICS_MAP.put(metricName, metricDescriptionSegment);
        }
      }
    }
  }

  private void addToQueue(AbstractReplicationTask task) {
    if (inFlight.add(task)) {
      if (task.getPriority() != ReplicationCommandPriority.LOW) {
        // Low priority tasks are not included in the replication queue sizes
        // returned to SCM in the heartbeat, so we only update the count for
        // priorities other than low.
        taskCounter.computeIfAbsent(task.getClass(),
            k -> new AtomicInteger()).incrementAndGet();
      }
      queuedCounter.get(task.getMetricName()).incrementAndGet();
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

  public Map<String, Integer> getInFlightReplicationSummary() {
    Map<String, Integer> result = new HashMap<>();
    for (Map.Entry<Class<?>, AtomicInteger> entry : taskCounter.entrySet()) {
      result.put(entry.getKey().getSimpleName(), entry.getValue().get());
    }
    return result;
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

  public int getMaxQueueSize() {
    return maxQueueSize;
  }

  public void nodeStateUpdated(HddsProtos.NodeOperationalState newState) {
    if (state.getAndSet(newState) != newState) {
      int threadCount = replicationConfig.getReplicationMaxStreams();
      int newMaxQueueSize = datanodeConfig.getCommandQueueLimit();

      if (isMaintenance(newState) || isDecommission(newState)) {
        threadCount = replicationConfig.scaleOutOfServiceLimit(threadCount);
        newMaxQueueSize =
            replicationConfig.scaleOutOfServiceLimit(newMaxQueueSize);
      }

      LOG.info("Node state updated to {}, scaling executor pool size to {}",
          newState, threadCount);

      maxQueueSize = newMaxQueueSize;
      executorThreadUpdater.accept(threadCount);
    }
  }

  /**
   * An executable form of a replication task with status handling.
   */
  public final class TaskRunner implements Comparable<TaskRunner>, Runnable {
    private final AbstractReplicationTask task;

    public TaskRunner(AbstractReplicationTask task) {
      this.task = task;
    }

    @Override
    public void run() {
      final long startTime = Time.monotonicNow();
      try {
        requestCounter.get(task.getMetricName()).incrementAndGet();

        final long now = clock.millis();
        final long deadline = task.getDeadline();
        if (deadline > 0 && now > deadline) {
          LOG.info("Ignoring {} since the deadline has passed ({} < {})",
              this, Instant.ofEpochMilli(deadline), Instant.ofEpochMilli(now));
          timeoutCounter.get(task.getMetricName()).incrementAndGet();
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
          failureCounter.get(task.getMetricName()).incrementAndGet();
        } else if (task.getStatus() == Status.DONE) {
          LOG.info("Successful {}", this);
          successCounter.get(task.getMetricName()).incrementAndGet();
        } else if (task.getStatus() == Status.SKIPPED) {
          LOG.info("Skipped {}", this);
          skippedCounter.get(task.getMetricName()).incrementAndGet();
        }
      } catch (Exception e) {
        task.setStatus(Status.FAILED);
        LOG.warn("Failed {}", this, e);
        failureCounter.get(task.getMetricName()).incrementAndGet();
      } finally {
        queuedCounter.get(task.getMetricName()).decrementAndGet();
        opsLatencyMs.get(task.getMetricName()).add(Time.monotonicNow() - startTime);
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
    public int compareTo(TaskRunner o) {
      return TASK_RUNNER_COMPARATOR.compare(this, o);
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
    return getCount(requestCounter);
  }

  public long getReplicationRequestCount(String metricsName) {
    AtomicLong counter = requestCounter.get(metricsName);
    return counter != null ? counter.get() : 0;
  }

  public long getQueueSize() {
    if (executor instanceof ThreadPoolExecutor) {
      return ((ThreadPoolExecutor)executor).getQueue().size();
    } else {
      return 0;
    }
  }

  public long getMaxReplicationStreams() {
    if (executor instanceof ThreadPoolExecutor) {
      return ((ThreadPoolExecutor) executor).getMaximumPoolSize();
    } else {
      return 1;
    }
  }

  private long getCount(Map<String, AtomicLong> counter) {
    long total = 0;
    for (Map.Entry<String, AtomicLong> entry : counter.entrySet()) {
      total += entry.getValue().get();
    }
    return total;
  }

  public long getReplicationSuccessCount() {
    return getCount(successCounter);
  }

  public long getReplicationSuccessCount(String metricsName) {
    AtomicLong counter = successCounter.get(metricsName);
    return counter != null ? counter.get() : 0;
  }

  public long getReplicationFailureCount() {
    return getCount(failureCounter);
  }

  public long getReplicationFailureCount(String metricsName) {
    AtomicLong counter = failureCounter.get(metricsName);
    return counter != null ? counter.get() : 0;
  }

  public long getReplicationTimeoutCount() {
    return getCount(timeoutCounter);
  }

  public long getReplicationTimeoutCount(String metricsName) {
    AtomicLong counter = timeoutCounter.get(metricsName);
    return counter != null ? counter.get() : 0;
  }

  public long getReplicationSkippedCount() {
    return getCount(skippedCounter);
  }

  public long getReplicationSkippedCount(String metricsName) {
    AtomicLong counter = skippedCounter.get(metricsName);
    return counter != null ? counter.get() : 0;
  }

  public long getReplicationQueuedCount() {
    return getCount(queuedCounter);
  }

  public long getReplicationQueuedCount(String metricsName) {
    AtomicLong counter = queuedCounter.get(metricsName);
    return counter != null ? counter.get() : 0;
  }

  public long getReplicationRequestAvgTime(String metricsName) {
    MutableRate rate = opsLatencyMs.get(metricsName);
    return rate != null ? (long) Math.ceil(rate.lastStat().mean()) : 0;
  }

  public long getReplicationRequestTotalTime(String metricsName) {
    MutableRate rate = opsLatencyMs.get(metricsName);
    return rate != null ? (long) Math.ceil(rate.lastStat().total()) : 0;
  }
}
