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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_CLIENT_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY;
import static org.apache.ozone.test.MetricsAsserts.getDoubleGauge;
import static org.apache.ozone.test.MetricsAsserts.getLongCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.om.OmClientMetrics.Handle;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests per-command statistics and shared OM client metrics ownership. */
class TestOmClientMetrics {
  @Test
  void recordsStatisticsForObservedCommands() {
    try (Handle handle = OmClientMetrics.acquireHandle(configuration(""))) {
      OmClientMetrics metrics = handle.metrics();
      assertThat(metricNames(metrics)).isEmpty();

      metrics.addLatency(Type.GetKeyInfo, 100_000);
      metrics.addLatency(Type.GetKeyInfo, 300_000);
      metrics.addLatency(Type.CommitKey, 700_000);

      MetricsRecordBuilder snapshot = getMetrics(metrics);
      assertThat(getLongCounter("GetKeyInfoLatencyNsNumOps", snapshot)).isEqualTo(2);
      assertThat(getDoubleGauge("GetKeyInfoLatencyNsAvgTime", snapshot)).isEqualTo(200_000);
      assertThat(getDoubleGauge("GetKeyInfoLatencyNsIMinTime", snapshot)).isEqualTo(100_000);
      assertThat(getDoubleGauge("GetKeyInfoLatencyNsIMaxTime", snapshot)).isEqualTo(300_000);
      assertThat(getLongCounter("CommitKeyLatencyNsNumOps", snapshot)).isEqualTo(1);
      assertThat(getDoubleGauge("CommitKeyLatencyNsAvgTime", snapshot)).isEqualTo(700_000);
      assertThat(metricNames(metrics)).noneMatch(name -> name.startsWith("ListKeys"));
    }
  }

  @Test
  void sharedSourceSurvivesOneOwnerAndCanBeReacquired() {
    OmClientMetrics previous;
    Handle first = OmClientMetrics.acquireHandle(configuration("60"));
    try (Handle second = OmClientMetrics.acquireHandle(configuration("300"))) {
      previous = first.metrics();
      assertThat(second.metrics()).isSameAs(previous);
      first.close();
      first.close();
      assertThat(DefaultMetricsSystem.instance().getSource(OmClientMetrics.SOURCE_NAME)).isSameAs(previous);
      second.metrics().addLatency(Type.ListKeys, 1);
      assertThat(metricNames(previous)).contains("ListKeysLatencyNs60sNumOps")
          .doesNotContain("ListKeysLatencyNs300sNumOps");
    } finally {
      first.close();
    }
    assertThat(DefaultMetricsSystem.instance().getSource(OmClientMetrics.SOURCE_NAME)).isNull();

    try (Handle next = OmClientMetrics.acquireHandle(configuration("300"))) {
      assertThat(next.metrics()).isNotSameAs(previous);
      first.close();
      assertThat(DefaultMetricsSystem.instance().getSource(OmClientMetrics.SOURCE_NAME)).isSameAs(next.metrics());
      next.metrics().addLatency(Type.ListKeys, 2);
      assertThat(getLongCounter("ListKeysLatencyNsNumOps", getMetrics(next.metrics()))).isEqualTo(1);
      assertThat(metricNames(next.metrics())).contains("ListKeysLatencyNs300sNumOps")
          .doesNotContain("ListKeysLatencyNs60sNumOps");
    }
  }

  @Test
  void supportsMultipleIntervalsAndDisablingQuantiles() {
    try (Handle handle = OmClientMetrics.acquireHandle(configuration("60, 300"))) {
      handle.metrics().addLatency(Type.ListKeys, 1);
      assertThat(metricNames(handle.metrics())).contains("ListKeysLatencyNs60sNumOps", "ListKeysLatencyNs300sNumOps");
    }
    try (Handle handle = OmClientMetrics.acquireHandle(configuration("  "))) {
      handle.metrics().addLatency(Type.ListKeys, 1);
      assertThat(metricNames(handle.metrics())).contains("ListKeysLatencyNsNumOps")
          .noneMatch(name -> name.contains("Percentile"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "-1", "60,60", "invalid", "60,invalid", "60,0", ","})
  void invalidIntervalsUseDefaultBeforeRecording(String intervals) {
    try (Handle handle = OmClientMetrics.acquireHandle(configuration(intervals))) {
      handle.metrics().addLatency(Type.CommitKey, 1);
      assertThat(metricNames(handle.metrics())).contains("CommitKeyLatencyNs60sNumOps");
    }
  }

  @Test
  void concurrentFirstUseAndSnapshotsRetainEverySample() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(5);
    CountDownLatch start = new CountDownLatch(1);
    try (Handle handle = OmClientMetrics.acquireHandle(configuration("60"))) {
      List<Future<?>> writers = new ArrayList<>();
      for (int thread = 0; thread < 4; thread++) {
        writers.add(executor.submit(() -> {
          start.await();
          for (int sample = 0; sample < 500; sample++) {
            handle.metrics().addLatency(Type.GetKeyInfo, 1);
          }
          return null;
        }));
      }
      Future<?> reader = executor.submit(() -> {
        start.await();
        for (int snapshot = 0; snapshot < 20; snapshot++) {
          metricNames(handle.metrics());
        }
        return null;
      });
      start.countDown();
      for (Future<?> writer : writers) {
        writer.get(10, TimeUnit.SECONDS);
      }
      reader.get(10, TimeUnit.SECONDS);
      assertThat(getLongCounter("GetKeyInfoLatencyNsNumOps", getMetrics(handle.metrics()))).isEqualTo(2_000);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void completionAfterLastCloseCannotCreateQuantiles() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    Handle handle = OmClientMetrics.acquireHandle(configuration("60"));
    try {
      Future<?> writer = executor.submit(() -> {
        start.await();
        handle.metrics().addLatency(Type.GetKeyInfo, 1);
        return null;
      });
      Future<?> closer = executor.submit(() -> {
        start.await();
        handle.close();
        return null;
      });
      start.countDown();
      writer.get(10, TimeUnit.SECONDS);
      closer.get(10, TimeUnit.SECONDS);
      Set<String> retiredMetrics = metricNames(handle.metrics());
      handle.metrics().addLatency(Type.CommitKey, 1);
      assertThat(metricNames(handle.metrics())).isEqualTo(retiredMetrics);
      assertThat(DefaultMetricsSystem.instance().getSource(OmClientMetrics.SOURCE_NAME)).isNull();
    } finally {
      handle.close();
      executor.shutdownNow();
    }
  }

  private static OzoneConfiguration configuration(String intervals) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_CLIENT_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY, intervals);
    return conf;
  }

  private static Set<String> metricNames(OmClientMetrics metrics) {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    metrics.getMetrics(collector, true);
    Set<String> names = new HashSet<>();
    collector.getRecords().forEach(record -> record.metrics().forEach(metric -> names.add(metric.name())));
    return names;
  }
}
