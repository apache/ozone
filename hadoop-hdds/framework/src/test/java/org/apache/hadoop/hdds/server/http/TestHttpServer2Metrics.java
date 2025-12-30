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

import static org.apache.hadoop.hdds.server.http.HttpServer2Metrics.HttpServer2MetricsInfo.HttpServerIdleThreadCount;
import static org.apache.hadoop.hdds.server.http.HttpServer2Metrics.HttpServer2MetricsInfo.HttpServerMaxThreadCount;
import static org.apache.hadoop.hdds.server.http.HttpServer2Metrics.HttpServer2MetricsInfo.HttpServerThreadCount;
import static org.apache.hadoop.hdds.server.http.HttpServer2Metrics.HttpServer2MetricsInfo.HttpServerThreadQueueWaitingTaskCount;
import static org.apache.hadoop.hdds.server.http.HttpServer2Metrics.HttpServer2MetricsInfo.SERVER_NAME;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Testing HttpServer2Metrics.
 */
public class TestHttpServer2Metrics {

  private QueuedThreadPool threadPool;
  private MetricsCollector metricsCollector;
  private MetricsRecordBuilder recorder;

  @BeforeEach
  public void setup() {
    threadPool = mock(QueuedThreadPool.class);
    metricsCollector = mock(MetricsCollector.class);
    recorder = mock(MetricsRecordBuilder.class);
  }

  @Test
  public void testMetrics() {
    // crate mock metrics
    int threadCount = RandomUtils.secure().randomInt();
    int maxThreadCount = RandomUtils.secure().randomInt();
    int idleThreadCount = RandomUtils.secure().randomInt();
    int threadQueueWaitingTaskCount = RandomUtils.secure().randomInt();
    String name = "s3g";

    when(threadPool.getThreads()).thenReturn(threadCount);
    when(threadPool.getMaxThreads()).thenReturn(maxThreadCount);
    when(threadPool.getIdleThreads()).thenReturn(idleThreadCount);
    when(threadPool.getQueueSize())
            .thenReturn(threadQueueWaitingTaskCount);
    when(recorder.addGauge(any(MetricsInfo.class), anyInt()))
        .thenReturn(recorder);
    when(recorder.setContext(anyString())).thenReturn(recorder);
    when(recorder.tag(any(MetricsInfo.class), anyString()))
        .thenReturn(recorder);
    when(metricsCollector.addRecord(anyString())).thenReturn(recorder);

    // get metrics
    HttpServer2Metrics server2Metrics =
        HttpServer2Metrics.create(threadPool, name);
    server2Metrics.getMetrics(metricsCollector, true);

    // verify
    verify(recorder).tag(SERVER_NAME, name);
    verify(metricsCollector).addRecord(HttpServer2Metrics.SOURCE_NAME);
    verify(recorder).addGauge(HttpServerThreadCount, threadCount);
    verify(recorder).addGauge(HttpServerMaxThreadCount, maxThreadCount);
    verify(recorder).addGauge(HttpServerIdleThreadCount, idleThreadCount);
    verify(recorder).addGauge(HttpServerThreadQueueWaitingTaskCount,
        threadQueueWaitingTaskCount);
  }
}
