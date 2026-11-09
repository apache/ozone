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

package org.apache.hadoop.ipc_.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RpcMetrics}.
 *
 * The latency rates are held outside the {@link org.apache.hadoop.metrics2.lib.MetricsRegistry}
 * as lock-free {@link org.apache.hadoop.ozone.util.ConcurrentMutableRate}s, so these tests pin the
 * emitted metric names and the hybrid {@link MetricsSource} wiring that keeps the registry-backed
 * metrics in the same record.
 */
public class TestRpcMetrics {

  private static final int PORT = 9999;
  private static final String SERVER_NAME = "TestProtocol";

  private RpcMetrics metrics;
  private MetricsSource source;

  @BeforeEach
  public void setUp() {
    Server server = mock(Server.class);
    when(server.getListenerAddress()).thenReturn(new InetSocketAddress(PORT));
    when(server.getServerName()).thenReturn(SERVER_NAME);
    when(server.getNumOpenConnections()).thenReturn(7);
    when(server.getNumOpenConnectionsPerUser()).thenReturn("{\"alice\":7}");
    when(server.getCallQueueLen()).thenReturn(3);
    when(server.getNumDroppedConnections()).thenReturn(5L);

    metrics = new RpcMetrics(server, new Configuration());
    // Same reflection DefaultMetricsSystem.register() performs: it populates the
    // @Metric counter fields and the @Metric methods into the registry
    source = MetricsAnnotations.makeSource(metrics);
  }

  @Test
  public void testRpcMetricsIsUsedAsItsOwnSource() {
    assertSame(metrics, source);
  }

  @Test
  public void testLatencyRateNamesAreUnchanged() {
    metrics.addRpcQueueTime(10);
    metrics.addRpcQueueTime(20);
    metrics.addRpcLockWaitTime(4);
    metrics.addRpcProcessingTime(100);
    metrics.addRpcProcessingTime(300);

    Map<String, Number> values = snapshotMetrics();

    assertEquals(2L, values.get("RpcQueueTimeNumOps").longValue());
    assertEquals(15.0, values.get("RpcQueueTimeAvgTime").doubleValue(), 0.001);
    assertEquals(1L, values.get("RpcLockWaitTimeNumOps").longValue());
    assertEquals(4.0, values.get("RpcLockWaitTimeAvgTime").doubleValue(), 0.001);
    assertEquals(2L, values.get("RpcProcessingTimeNumOps").longValue());
    assertEquals(200.0, values.get("RpcProcessingTimeAvgTime").doubleValue(), 0.001);

    // the rates are not extended, matching @Metric.always() defaulting to false
    assertThat(values).doesNotContainKeys("RpcQueueTimeStdevTime",
        "RpcProcessingTimeStdevTime");
  }

  @Test
  public void testRegistryMetricsAndTagsStayInTheSameRecord() {
    metrics.incrSentBytes(42);
    metrics.incrReceivedBytes(24);
    metrics.incrSlowRpc();

    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    source.getMetrics(collector, true);
    assertEquals(1, collector.getRecords().size());
    MetricsRecord record = collector.getRecords().get(0);
    assertEquals("rpc", record.name());

    Map<String, String> tags = new HashMap<>();
    for (MetricsTag tag : record.tags()) {
      tags.put(tag.name(), tag.value());
    }
    assertEquals(String.valueOf(PORT), tags.get("port"));
    assertEquals(SERVER_NAME, tags.get("serverName"));
    assertEquals("rpc", tags.get("Context"));
    // a String valued @Metric method is emitted as a tag, not as a gauge
    assertEquals("{\"alice\":7}", tags.get("NumOpenConnectionsPerUser"));

    Map<String, Number> values = new HashMap<>();
    for (AbstractMetric metric : record.metrics()) {
      values.put(metric.name(), metric.value());
    }
    assertEquals(42L, values.get("SentBytes").longValue());
    assertEquals(24L, values.get("ReceivedBytes").longValue());
    assertEquals(1L, values.get("RpcSlowCalls").longValue());
    assertEquals(7, values.get("NumOpenConnections").intValue());
    assertEquals(3, values.get("CallQueueLength").intValue());
    assertEquals(5L, values.get("NumDroppedConnections").longValue());
    assertThat(values).containsKey("RpcProcessingTimeNumOps");
  }

  @Test
  public void testProcessingStatsFeedSlowCallDetection() {
    metrics.addRpcProcessingTime(100);
    metrics.addRpcProcessingTime(200);
    metrics.addRpcProcessingTime(300);

    assertEquals(3, metrics.getProcessingSampleCount());
    assertEquals(200.0, metrics.getProcessingMean(), 0.001);
    assertTrue(metrics.getProcessingStdDev() > 0.0,
        "slow call detection needs a positive deviation");
  }

  @Test
  public void testConcurrentProcessingTimeAddsAreCountedExactly()
      throws InterruptedException {
    int threads = 14;
    int addsPerThread = 1000;
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (int t = 0; t < threads; t++) {
        pool.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < addsPerThread; i++) {
              metrics.addRpcProcessingTime(1);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        });
      }
      start.countDown();
      assertTrue(done.await(30, TimeUnit.SECONDS));
    } finally {
      pool.shutdown();
    }

    assertEquals((long) threads * addsPerThread,
        snapshotMetrics().get("RpcProcessingTimeNumOps").longValue());
  }

  private Map<String, Number> snapshotMetrics() {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    source.getMetrics(collector, true);
    Map<String, Number> values = new HashMap<>();
    for (MetricsRecord record : collector.getRecords()) {
      for (AbstractMetric metric : record.metrics()) {
        values.put(metric.name(), metric.value());
      }
    }
    return values;
  }
}
