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

package org.apache.hadoop.ozone.grpc.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link GrpcMetrics}.
 *
 * The latency rates are held outside the {@link org.apache.hadoop.metrics2.lib.MetricsRegistry}
 * as lock-free {@link org.apache.hadoop.ozone.util.ConcurrentMutableRate}s, so these tests pin the
 * emitted metric names and that they stay in the same record as the registry-backed metrics.
 */
public class TestGrpcMetrics {

  private GrpcMetrics metrics;
  private MetricsSource source;

  @BeforeEach
  public void setUp() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_GPRC_METRICS_PERCENTILES_INTERVALS_KEY, "60");
    metrics = new GrpcMetrics(conf);
    // Same reflection DefaultMetricsSystem.register() performs: it populates the
    // @Metric counter fields into the registry
    source = MetricsAnnotations.makeSource(metrics);
  }

  @AfterEach
  public void tearDown() {
    metrics.unRegister();
  }

  @Test
  public void testGrpcMetricsIsUsedAsItsOwnSource() {
    assertSame(metrics, source);
  }

  @Test
  public void testLatencyRateNamesAreUnchanged() {
    metrics.addGrpcQueueTime(10);
    metrics.addGrpcQueueTime(20);
    metrics.addGrpcProcessingTime(100);
    metrics.addGrpcProcessingTime(300);

    Map<String, Number> values = snapshotMetrics();

    assertEquals(2L, values.get("GrpcQueueTimeNumOps").longValue());
    assertEquals(15.0, values.get("GrpcQueueTimeAvgTime").doubleValue(), 0.001);
    assertEquals(2L, values.get("GrpcProcessingTimeNumOps").longValue());
    assertEquals(200.0, values.get("GrpcProcessingTimeAvgTime").doubleValue(), 0.001);

    // the rates are not extended, matching @Metric.always() defaulting to false
    assertThat(values).doesNotContainKeys("GrpcQueueTimeStdevTime", "GrpcProcessingTimeStdevTime");
  }

  @Test
  public void testRatesAreInTheRegistryRecord() {
    metrics.incrSentBytes(42);
    metrics.incrReceivedBytes(24);
    metrics.inrcNumOpenClientConnections();
    metrics.addGrpcProcessingTime(5);
    metrics.setRequestType("CreateKey");

    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    source.getMetrics(collector, true);
    assertEquals(2, collector.getRecords().size());

    MetricsRecord record = collector.getRecords().get(0);
    assertEquals("grpc", record.name());
    Map<String, Number> values = new HashMap<>();
    for (AbstractMetric metric : record.metrics()) {
      values.put(metric.name(), metric.value());
    }
    assertEquals(42L, values.get("SentBytes").longValue());
    assertEquals(24L, values.get("ReceivedBytes").longValue());
    assertEquals(1L, values.get("NumOpenClientConnections").longValue());
    assertEquals(1L, values.get("GrpcProcessingTimeNumOps").longValue());
    assertThat(values).containsKeys("GrpcQueueTimeNumOps", "GrpcProcessingTime60sNumOps");

    MetricsRecord tagRecord = collector.getRecords().get(1);
    assertEquals(GrpcMetrics.class.getSimpleName(), tagRecord.name());
    assertThat(tagRecord.tags()).anyMatch(tag ->
        tag.name().equals("LatestRequestType") && tag.value().equals("CreateKey"));
  }

  @Test
  public void testConcurrentProcessingTimeAddsAreCountedExactly() throws InterruptedException {
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
              metrics.addGrpcProcessingTime(1);
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

    assertEquals((long) threads * addsPerThread, snapshotMetrics().get("GrpcProcessingTimeNumOps").longValue());
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
