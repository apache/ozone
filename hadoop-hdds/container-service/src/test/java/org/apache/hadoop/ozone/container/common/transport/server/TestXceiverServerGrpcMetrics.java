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

package org.apache.hadoop.ozone.container.common.transport.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.ratis.thirdparty.io.grpc.Attributes;
import org.apache.ratis.thirdparty.io.grpc.ServerTransportFilter;
import org.junit.jupiter.api.Test;

class TestXceiverServerGrpcMetrics {

  @Test
  void reportsReadExecutorAndClientConnectionMetrics() throws Exception {
    ThreadPoolExecutor readExecutor = new ThreadPoolExecutor(1, 3, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>());
    CountDownLatch taskStarted = new CountDownLatch(1);
    CountDownLatch releaseTask = new CountDownLatch(1);
    readExecutor.execute(() -> {
      taskStarted.countDown();
      try {
        releaseTask.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    taskStarted.await();
    readExecutor.execute(() -> { });
    readExecutor.execute(() -> { });

    XceiverServerGrpcMetrics metrics = XceiverServerGrpcMetrics.create(readExecutor, "test-datanode");
    try {
      ServerTransportFilter transportFilter = metrics.newTransportFilter();
      transportFilter.transportReady(Attributes.EMPTY);
      transportFilter.transportReady(Attributes.EMPTY);
      transportFilter.transportTerminated(Attributes.EMPTY);

      MetricsCollectorImpl collector = new MetricsCollectorImpl();
      metrics.getMetrics(collector, true);

      Iterable<AbstractMetric> all = collector.getRecords().get(0).metrics();
      assertEquals(1L, findLong(all, "ActiveReadExecutorThreads"));
      assertEquals(3L, findLong(all, "MaxReadExecutorThreads"));
      assertEquals(2L, findLong(all, "ReadExecutorQueueLength"));
      assertEquals(1L, findLong(all, "ActiveClientConnections"));
    } finally {
      metrics.unregister();
      releaseTask.countDown();
      readExecutor.shutdownNow();
    }
  }

  private static long findLong(Iterable<AbstractMetric> metrics, String name) {
    for (AbstractMetric metric : metrics) {
      if (metric.name().equals(name)) {
        return metric.value().longValue();
      }
    }
    throw new AssertionError("Metric not found: " + name);
  }
}
