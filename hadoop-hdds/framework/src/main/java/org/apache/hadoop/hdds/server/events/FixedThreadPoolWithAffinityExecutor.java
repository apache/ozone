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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_PREFIX;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_THREAD_POOL_SIZE_DEFAULT;

/**
 * Fixed thread pool EventExecutor to call all the event handler one-by-one.
 * Payloads with the same hashcode will be mapped to the same thread.
 *
 * @param <P> the payload type of events
 */
@Metrics(context = "EventQueue")
public class FixedThreadPoolWithAffinityExecutor<P>
    implements EventExecutor<P> {

  private static final String EVENT_QUEUE = "EventQueue";

  private static final Logger LOG =
      LoggerFactory.getLogger(FixedThreadPoolWithAffinityExecutor.class);

  private final String name;

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

  /**
   * Create FixedThreadPoolExecutor with affinity.
   * Based on the payload's hash code, the payload will be scheduled to the
   * same thread.
   *
   * @param name Unique name used in monitoring and metrics.
   */
  public FixedThreadPoolWithAffinityExecutor(
      String name,
      List<ThreadPoolExecutor> executors) {
    this.name = name;
    DefaultMetricsSystem.instance()
        .register(EVENT_QUEUE + name,
            "Event Executor metrics ",
            this);
    this.executors = executors;
  }

  public static List<ThreadPoolExecutor> initializeExecutorPool(
      String eventName) {
    List<ThreadPoolExecutor> executors = new LinkedList<>();
    OzoneConfiguration configuration = new OzoneConfiguration();
    int threadPoolSize = configuration.getInt(OZONE_SCM_EVENT_PREFIX +
            StringUtils.camelize(eventName) + ".thread.pool.size",
        OZONE_SCM_EVENT_THREAD_POOL_SIZE_DEFAULT);
    BlockingQueue<Runnable> workQueue = new LinkedBlockingDeque<>();
    for (int i = 0; i < threadPoolSize; i++) {
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
          workQueue,
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
    int index = message.hashCode() & (executors.size() - 1);
    executors.get(index).execute(() -> {
      scheduled.incr();
      try {
        handler.onMessage(message, publisher);
        done.incr();
      } catch (Exception ex) {
        LOG.error("Error on execution message {}", message, ex);
        failed.incr();
      }
    });
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
}
