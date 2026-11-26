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

import static org.apache.ozone.test.MetricsAsserts.assertCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.util.MetricUtil;
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
    XceiverServerGrpcMetrics metrics = new XceiverServerGrpcMetrics(readExecutor, "test-datanode");
    boolean registered = false;
    try {
      readExecutor.execute(() -> {
        taskStarted.countDown();
        try {
          releaseTask.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      assertTrue(taskStarted.await(10, TimeUnit.SECONDS));
      readExecutor.execute(() -> { });
      readExecutor.execute(() -> { });

      String sourceName = MetricUtil.getMetricsSourceName(XceiverServerGrpcMetrics.SOURCE_NAME, "test-datanode");
      assertNull(DefaultMetricsSystem.instance().getSource(sourceName));
      metrics.register();
      registered = true;
      ServerTransportFilter transportFilter = metrics.newTransportFilter();
      transportFilter.transportTerminated(null);
      transportFilter.transportReady(Attributes.EMPTY);
      transportFilter.transportReady(Attributes.EMPTY);
      transportFilter.transportTerminated(Attributes.EMPTY);

      MetricsCollectorImpl collector = new MetricsCollectorImpl();
      MetricsSource source = DefaultMetricsSystem.instance().getSource(sourceName);
      assertNotNull(source);
      source.getMetrics(collector, true);

      assertEquals(OzoneConsts.OZONE, collector.getRecords().get(0).context());
      Iterable<AbstractMetric> all = collector.getRecords().get(0).metrics();
      assertEquals(1L, findLong(all, "ActiveReadExecutorThreads"));
      assertEquals(3L, findLong(all, "MaxReadExecutorThreads"));
      assertEquals(2L, findLong(all, "ReadExecutorQueueLength"));
      assertEquals(1L, findLong(all, "ActiveClientConnections"));
      metrics.unregister();
      registered = false;
      assertNull(DefaultMetricsSystem.instance().getSource(sourceName));
    } finally {
      releaseTask.countDown();
      readExecutor.shutdownNow();
      if (registered) {
        metrics.unregister();
      }
    }
  }

  @Test
  void recordsReadExecutorQueueTime() throws Exception {
    ThreadPoolExecutor readExecutor = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    XceiverServerGrpcMetrics metrics = new XceiverServerGrpcMetrics(readExecutor, "test-datanode");
    try {
      Executor executor = metrics.timedExecutor();
      CountDownLatch done = new CountDownLatch(2);
      executor.execute(done::countDown);
      executor.execute(done::countDown);
      assertTrue(done.await(10, TimeUnit.SECONDS));

      assertCounter("ReadExecutorQueueTimeNsNumOps", 2L, getMetrics(metrics));
    } finally {
      readExecutor.shutdownNow();
    }
  }

  @Test
  void unregistersMetricsWhenServerStops() throws Exception {
    DatanodeDetails datanode = MockDatanodeDetails.randomDatanodeDetails();
    String sourceName = MetricUtil.getMetricsSourceName(XceiverServerGrpcMetrics.SOURCE_NAME, datanode.getUuidString());
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_IPC_RANDOM_PORT, true);
    ThreadPoolExecutor readExecutor = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>());
    assertNull(DefaultMetricsSystem.instance().getSource(sourceName));
    XceiverServerGrpc server = new XceiverServerGrpc(datanode, conf, readExecutor,
        mock(ContainerDispatcher.class), null);
    try {
      assertNotNull(DefaultMetricsSystem.instance().getSource(sourceName));
      server.start();
    } finally {
      try {
        server.stop();
      } finally {
        readExecutor.shutdownNow();
      }
    }
    assertNull(DefaultMetricsSystem.instance().getSource(sourceName));
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
