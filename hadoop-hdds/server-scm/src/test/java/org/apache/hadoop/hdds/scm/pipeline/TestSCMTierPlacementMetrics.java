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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.server.http.PrometheusMetricsSink;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecordImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies the published tier-allocation counters alongside existing pipeline metrics.
 */
class TestSCMTierPlacementMetrics {
  private SCMPipelineMetrics metrics;

  @BeforeEach
  void setUp() {
    SCMPipelineMetrics.unRegister();
    metrics = SCMPipelineMetrics.create();
  }

  @AfterEach
  void tearDown() {
    SCMPipelineMetrics.unRegister();
  }

  @ParameterizedTest
  @MethodSource("failureReasons")
  void classifiesResultCodesWithoutParsingMessages(IOException failure, String reason) throws Exception {
    metrics.incTierPlacementFailure(StorageTier.SSD, failure);
    String output = prometheusOutput();
    assertThat(output).contains(series("SSD", reason, 1));
    assertEquals(1, totalFailures());
  }

  static Stream<Arguments> failureReasons() {
    return Stream.of(
        Arguments.of(new SCMException(ResultCodes.FAILED_TO_FIND_NODES_WITH_SPACE), "no_space"),
        Arguments.of(new SCMException(ResultCodes.FAILED_TO_FIND_CONTAINER_WITH_SPACE), "no_space"),
        Arguments.of(new SCMException(ResultCodes.FAILED_TO_FIND_HEALTHY_NODES), "no_suitable_node"),
        Arguments.of(new SCMException(ResultCodes.FAILED_TO_FIND_SUITABLE_NODE), "no_suitable_node"),
        Arguments.of(new SCMException(ResultCodes.FAILED_TO_FIND_NODE_IN_POOL), "no_suitable_node"),
        Arguments.of(new SCMException(ResultCodes.INTERNAL_ERROR), "other"),
        Arguments.of(new SCMException("no space and no suitable node"), "other"),
        Arguments.of(new IOException("FAILED_TO_FIND_NODES_WITH_SPACE"), "other"),
        Arguments.of(null, "other"));
  }

  @ParameterizedTest
  @EnumSource(StorageTier.class)
  void keepsTierSeriesSeparate(StorageTier tier) throws Exception {
    metrics.incTierPlacementFailure(tier, null);
    assertThat(prometheusOutput()).contains(series(tier == StorageTier.EMPTY ? "UNKNOWN" : tier.name(), "other", 1));
    assertEquals(1, totalFailures());
  }

  @Test
  void missingTierIsUnknownEvenWhenDefaultIsDisk() throws Exception {
    metrics.incTierPlacementFailure(null, new SCMException(ResultCodes.FAILED_TO_FIND_NODES_WITH_SPACE));
    assertThat(prometheusOutput()).contains(series("UNKNOWN", "no_space", 1), series("DISK", "no_space", 0));
  }

  @Test
  void exportsBoundedCounterLabelsIncludingZeroSeriesAndPreservesAggregates() throws Exception {
    metrics.incNumPipelineCreationFailed();
    metrics.incNumPipelineCreationFailed();
    metrics.incTierPlacementFailure(StorageTier.SSD, new SCMException(ResultCodes.FAILED_TO_FIND_NODES_WITH_SPACE));
    metrics.incTierPlacementFailure(StorageTier.SSD, new SCMException(ResultCodes.FAILED_TO_FIND_SUITABLE_NODE));
    metrics.incTierPlacementFailure(StorageTier.DISK, new IOException("key=/volume/bucket/private-key"));

    String output = prometheusOutput();
    assertThat(output).contains("# TYPE scm_pipeline_metrics_tier_placement_failures counter",
        series("SSD", "no_space", 1), series("SSD", "no_suitable_node", 1), series("SSD", "other", 0),
        series("DISK", "other", 1), series("ARCHIVE", "no_space", 0), series("UNKNOWN", "other", 0),
        "scm_pipeline_metrics_num_pipeline_creation_failed{} 2");
    assertThat(output).doesNotContain("private-key", "volume=", "bucket=", "pipelineid=");

    int labelledRecords = 0;
    for (MetricsRecordImpl record : snapshot().getRecords()) {
      for (AbstractMetric metric : record.metrics()) {
        if (metric.name().equals("TierPlacementFailures")) {
          labelledRecords++;
          assertEquals(MetricType.COUNTER, metric.type());
          assertThat(record.tags()).extracting(tag -> tag.name()).containsExactly("RequestedTier", "Reason");
        } else {
          assertThat(record.tags()).isEmpty();
        }
      }
    }
    assertEquals(12, labelledRecords);
    assertEquals(3, totalFailures());
    // Unchanged counters must still be exported on successive scrapes.
    assertThat(prometheusOutput()).contains(series("SSD", "no_space", 1));
  }

  @Test
  void concurrentUpdatesAndScrapesDoNotLoseIncrements() throws Exception {
    int writers = 8;
    int increments = 1000;
    ExecutorService executor = Executors.newFixedThreadPool(writers + 1);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < writers; i++) {
        futures.add(executor.submit(() -> {
          for (int j = 0; j < increments; j++) {
            metrics.incTierPlacementFailure(StorageTier.SSD, null);
          }
        }));
      }
      futures.add(executor.submit(() -> {
        for (int i = 0; i < increments; i++) {
          snapshot();
        }
      }));
      for (Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
    assertEquals((long) writers * increments, totalFailures());
    assertThat(prometheusOutput()).contains(series("SSD", "other", (long) writers * increments));
  }

  @Test
  void sharesRegistrationAndResetsWithExistingPipelineMetricsLifecycle() {
    assertSame(metrics, SCMPipelineMetrics.create());
    assertSame(metrics, DefaultMetricsSystem.instance().getSource("SCMPipelineMetrics"));
    metrics.incTierPlacementFailure(StorageTier.SSD, null);
    assertEquals(1, totalFailures());
    SCMPipelineMetrics.unRegister();
    assertNull(DefaultMetricsSystem.instance().getSource("SCMPipelineMetrics"));
    SCMPipelineMetrics restarted = SCMPipelineMetrics.create();
    assertNotSame(metrics, restarted);
    metrics = restarted;
    assertEquals(0, totalFailures());
  }

  private MetricsCollectorImpl snapshot() {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    metrics.getMetrics(collector, true);
    return collector;
  }

  private long totalFailures() {
    long count = 0;
    for (MetricsRecordImpl record : snapshot().getRecords()) {
      for (AbstractMetric metric : record.metrics()) {
        if (metric.name().equals("TierPlacementFailures")) {
          count += metric.value().longValue();
        }
      }
    }
    return count;
  }

  private String prometheusOutput() throws IOException {
    PrometheusMetricsSink sink = new PrometheusMetricsSink("scm");
    snapshot().getRecords().forEach(sink::putMetrics);
    sink.flush();
    StringWriter writer = new StringWriter();
    sink.writeMetrics(writer);
    return writer.toString();
  }

  private static String series(String tier, String reason, long value) {
    return "scm_pipeline_metrics_tier_placement_failures{requestedtier=\"" + tier
        + "\",reason=\"" + reason + "\"} " + value;
  }
}
