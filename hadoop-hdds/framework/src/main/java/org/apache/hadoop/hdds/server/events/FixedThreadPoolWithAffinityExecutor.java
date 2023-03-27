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
package org.apache.hadoop.hdds.server.events;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.utils.MetricsUtil;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_REPORT_EXEC_WAIT_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_REPORT_QUEUE_WAIT_THRESHOLD_DEFAULT;

/**
 * Fixed thread pool EventExecutor to call all the event handler one-by-one.
 * Payloads with the same hashcode will be mapped to the same thread.
 *
 * @param <P> the payload type of events
 */
@Metrics(context = "EventQueue")
public class FixedThreadPoolWithAffinityExecutor<P, Q>
    implements EventExecutor<P> {

  private static final String EVENT_QUEUE = "EventQueue";

  private static final Logger LOG =
      LoggerFactory.getLogger(FixedThreadPoolWithAffinityExecutor.class);

  private final Map<String, FixedThreadPoolWithAffinityExecutor> executorMap;

  private final String name;

  private final EventHandler<P> eventHandler;

  private final EventPublisher eventPublisher;

  private final List<BlockingQueue<Q>> workQueues;

  private final List<ThreadPoolExecutor> executors;

  // MutableCounterLong is thread safe.
  @Metric
  private MutableCounterLong queued;

  @Metric
  private MutableCounterLong done;

  @Metric
  private MutableCounterLong failed;

  @Metric
  private MutableCounterLong scheduled;

  @Metric
  private MutableCounterLong dropped;
  
  @Metric
  private MutableCounterLong longWaitInQueue;
  
  @Metric
  private MutableCounterLong longTimeExecution;

  private final AtomicBoolean isRunning = new AtomicBoolean(true);
  private long queueWaitThreshold
      = OZONE_SCM_EVENT_REPORT_QUEUE_WAIT_THRESHOLD_DEFAULT;
  private long execWaitThreshold
      = OZONE_SCM_EVENT_REPORT_EXEC_WAIT_THRESHOLD_DEFAULT;

  /**
   * Create FixedThreadPoolExecutor with affinity.
   * Based on the payload's hash code, the payload will be scheduled to the
   * same thread.
   *
   * @param name Unique name used in monitoring and metrics.
   */
  public FixedThreadPoolWithAffinityExecutor(
      String name, EventHandler<P> eventHandler,
      List<BlockingQueue<Q>> workQueues, EventPublisher eventPublisher,
      Class<P> clazz, List<ThreadPoolExecutor> executors,
      Map<String, FixedThreadPoolWithAffinityExecutor> executorMap) {
    this.name = name;
    this.eventHandler = eventHandler;
    this.workQueues = workQueues;
    this.eventPublisher = eventPublisher;
    this.executors = executors;
    this.executorMap = executorMap;
    executorMap.put(clazz.getName(), this);

    // Add runnable which will wait for task over another queue
    // This needs terminate canceling each task in shutdown
    int i = 0;
    for (BlockingQueue<Q> queue : workQueues) {
      ThreadPoolExecutor threadPoolExecutor = executors.get(i);
      if (threadPoolExecutor.getActiveCount() == 0) {
        threadPoolExecutor.submit(new ContainerReportProcessTask<>(queue,
            isRunning, executorMap));
      }
      ++i;
    }

    MetricsUtil.registerDynamic(this, EVENT_QUEUE + name,
        "Event Executor metrics ", "EventQueue");
  }
  
  public void setQueueWaitThreshold(long queueWaitThreshold) {
    this.queueWaitThreshold = queueWaitThreshold;
  }

  public void setExecWaitThreshold(long execWaitThreshold) {
    this.execWaitThreshold = execWaitThreshold;
  }

  public static <Q> List<ThreadPoolExecutor> initializeExecutorPool(
      List<BlockingQueue<Q>> workQueues) {
    List<ThreadPoolExecutor> executors = new ArrayList<>();
    for (int i = 0; i < workQueues.size(); ++i) {
      LinkedBlockingQueue<Runnable> poolQueue = new LinkedBlockingQueue<>(1);
      ThreadFactory threadFactory = new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("FixedThreadPoolWithAffinityExecutor-" + i + "-%d")
          .build();
      executors.add(new
          ThreadPoolExecutor(
          1,
          1,
          0,
          TimeUnit.SECONDS,
          poolQueue,
          threadFactory));
    }
    return executors;
  }

  @Override
  public void onMessage(EventHandler<P> handler, P message, EventPublisher
      publisher) {
    queued.incr();
    // For messages that need to be routed to the same thread need to
    // implement hashCode to match the messages. This should be safe for
    // other messages that implement the native hash.
    int index = message.hashCode() & (workQueues.size() - 1);
    BlockingQueue<Q> queue = workQueues.get(index);
    queue.add((Q) message);
    if (queue instanceof IQueueMetrics) {
      dropped.incr(((IQueueMetrics) queue).getAndResetDropCount(
          message.getClass().getSimpleName()));
    }
  }

  @Override
  public long failedEvents() {
    return failed.value();
  }

  @Override
  public long successfulEvents() {
    return done.value();
  }

  @Override
  public long queuedEvents() {
    return queued.value();
  }

  @Override
  public long scheduledEvents() {
    return scheduled.value();
  }

  @Override
  public long droppedEvents() {
    return dropped.value();
  }

  public long longWaitInQueueEvents() {
    return longWaitInQueue.value();
  }
  
  public long longTimeExecutionEvents() {
    return longTimeExecution.value();
  }

  @Override
  public void close() {
    isRunning.set(false);
    for (ThreadPoolExecutor executor : executors) {
      executor.shutdown();
    }
    executorMap.clear();
    DefaultMetricsSystem.instance().unregisterSource(EVENT_QUEUE + name);
  }

  @Override
  public String getName() {
    return name;
  }

  /**
   * Runnable class to perform execution of payload.
   */
  public static class ContainerReportProcessTask<P> implements Runnable {
    private BlockingQueue<P> queue;
    private AtomicBoolean isRunning;
    private Map<String, FixedThreadPoolWithAffinityExecutor> executorMap;

    public ContainerReportProcessTask(BlockingQueue<P> queue,
        AtomicBoolean isRunning,
        Map<String, FixedThreadPoolWithAffinityExecutor> executorMap) {
      this.queue = queue;
      this.isRunning = isRunning;
      this.executorMap = executorMap;
    }

    @Override
    public void run() {
      while (isRunning.get()) {
        try {
          Object report = queue.poll(1, TimeUnit.MILLISECONDS);
          if (report == null) {
            continue;
          }

          FixedThreadPoolWithAffinityExecutor executor = executorMap.get(
              report.getClass().getName());
          if (null == executor) {
            LOG.warn("Executor for report is not found");
            continue;
          }
          
          long createTime = 0;
          String eventId = "";
          if (report instanceof IEventInfo) {
            createTime = ((IEventInfo) report).getCreateTime();
            eventId = ((IEventInfo) report).getEventId();
          }
          
          long curTime = Time.monotonicNow();
          if (createTime != 0
              && ((curTime - createTime) > executor.queueWaitThreshold)) {
            executor.longWaitInQueue.incr();
            LOG.warn("Event remained in queue for long time {} millisec, {}",
                (curTime - createTime), eventId);
          }

          executor.scheduled.incr();
          try {
            executor.eventHandler.onMessage(report,
                executor.eventPublisher);
            executor.done.incr();
            curTime = Time.monotonicNow();
            if (createTime != 0
                && (curTime - createTime) > executor.execWaitThreshold) {
              executor.longTimeExecution.incr();
              LOG.warn("Event taken long execution time {} millisec, {}",
                  (curTime - createTime), eventId);
            }
          } catch (Exception ex) {
            LOG.error("Error on execution message {}", report, ex);
            executor.failed.incr();
          }
          if (Thread.currentThread().isInterrupted()) {
            LOG.warn("Interrupt of execution of Reports");
            return;
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupt of execution of Reports");
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  /**
   * Capture the metrics specific to customized queue.
   */
  public interface IQueueMetrics {
    int getAndResetDropCount(String type);
  }
}
