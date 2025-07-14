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

package org.apache.hadoop.hdds.server.http;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

/**
 * Metrics related to HttpServer threadPool.
 */
@InterfaceAudience.Private
public final class HttpServer2Metrics implements MetricsSource {

  public static final String SOURCE_NAME = HttpServer2Metrics.class.getSimpleName();

  public static final String NAME = HttpServer2Metrics.class.getSimpleName();

  private final QueuedThreadPool threadPool;
  private final String name;

  private HttpServer2Metrics(QueuedThreadPool threadPool, String name) {
    this.threadPool = threadPool;
    this.name = name;
  }

  public static HttpServer2Metrics create(
      QueuedThreadPool threadPool, String name) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(NAME, "HttpServer2 Metrics",
        new HttpServer2Metrics(threadPool, name));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME)
        .setContext("HttpServer2")
        .tag(HttpServer2MetricsInfo.SERVER_NAME, name);

    recordBuilder.addGauge(HttpServer2MetricsInfo.HttpServerThreadCount,
            threadPool.getThreads())
        .addGauge(HttpServer2MetricsInfo.HttpServerIdleThreadCount,
            threadPool.getIdleThreads())
        .addGauge(HttpServer2MetricsInfo.HttpServerMaxThreadCount,
            threadPool.getMaxThreads())
        .addGauge(HttpServer2MetricsInfo.HttpServerThreadQueueWaitingTaskCount,
            threadPool.getQueueSize());
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(NAME);
  }

  enum HttpServer2MetricsInfo implements MetricsInfo {
    SERVER_NAME("HttpServer2 Metrics."),
    HttpServerThreadCount("Number of threads in the pool."),
    HttpServerIdleThreadCount("Number of idle threads but not reserved."),
    HttpServerMaxThreadCount("Maximum number of threads in the pool."),
    HttpServerThreadQueueWaitingTaskCount(
        "The number of jobs in the queue waiting for a thread");

    private final String desc;

    HttpServer2MetricsInfo(String desc) {
      this.desc = desc;
    }

    @Override
    public String description() {
      return desc;
    }
  }
}
