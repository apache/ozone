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
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

  private static final Map<String, FixedThreadPoolWithAffinityExecutor>
      EXECUTOR_MAP = new ConcurrentHashMap<>();

  private final String name;

  private EventHandler<P> eventHandler;

  private EventPublisher eventPublisher;

  private List<BlockingQueue<Q>> workQueues;

  private List<ThreadPoolExecutor> executors;

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
      Class<P> clazz, List<ThreadPoolExecutor> executors) {
    this.name = name;
    this.eventHandler = eventHandler;
    this.workQueues = workQueues;
    this.eventPublisher = eventPublisher;
    this.executors = executors;

    EXECUTOR_MAP.put(clazz.getName(), this);

    DefaultMetricsSystem.instance()
        .register(EVENT_QUEUE + name,
            "Event Executor metrics ",
            this);
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
    int i = 0;
    for (BlockingQueue<Q> queue : workQueues) {
      ThreadPoolExecutor threadPoolExecutor = executors.get(i);
      threadPoolExecutor.execute(new ReportExecutor<>(queue));
      ++i;
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
    if (queue instanceof FixedThreadPoolWithAffinityExecutor.IQueueMetrics) {
      dropped.incr(((IQueueMetrics) queue).getAndResetDropCount());
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
  public void close() {
    for (ThreadPoolExecutor executor : executors) {
      executor.shutdown();
    }
  }

  @Override
  public String getName() {
    return name;
  }

  /**
   * Runnable class to perform execution of payload.
   */
  public static class ReportExecutor<P> implements Runnable {
    private BlockingQueue<P> queue;
    public ReportExecutor(BlockingQueue<P> queue) {
      this.queue = queue;
    }

    @Override
    public void run() {
      while (true) {
        try {
          Object report = queue.take();
          if (report == null) {
            continue;
          }

          FixedThreadPoolWithAffinityExecutor executor = EXECUTOR_MAP.get(
              report.getClass().getName());
          if (null == executor) {
            continue;
          }

          executor.scheduled.incr();
          try {
            executor.eventHandler.onMessage(report,
                executor.eventPublisher);
            executor.done.incr();
          } catch (Exception ex) {
            LOG.error("Error on execution message {}", report, ex);
            executor.failed.incr();
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupt of execution of Reports");
          return;
        }
      }
    }
  }

  /**
   * Capture the metrics specific to customized queue.
   */
  public interface IQueueMetrics {
    int getAndResetDropCount();
  }
}
