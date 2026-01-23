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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test prometheus Metrics.
 */
public class TestPrometheusMetricsIntegration {

  private MetricsSystem metrics;
  private PrometheusMetricsSink sink;

  private static final MetricsInfo PORT_INFO = new MetricsInfo() {
    @Override
    public String name() {
      return "PORT";
    }

    @Override
    public String description() {
      return "port";
    }
  };

  private static final MetricsInfo COUNTER_INFO = new MetricsInfo() {
    @Override
    public String name() {
      return "COUNTER";
    }

    @Override
    public String description() {
      return "counter";
    }
  };

  private static final int COUNTER_1 = 123;
  private static final int COUNTER_2 = 234;

  @BeforeEach
  public void init() {
    metrics = DefaultMetricsSystem.instance();

    metrics.init("test");
    sink = new PrometheusMetricsSink("random");
    metrics.register("Prometheus", "Prometheus", sink);
  }

  @AfterEach
  public void tearDown() {
    metrics.stop();
    metrics.shutdown();
  }

  @Test
  public void testPublish()
      throws InterruptedException, TimeoutException {
    //GIVEN
    TestMetrics testMetrics = metrics
        .register("TestMetrics", "Testing metrics", new TestMetrics());

    testMetrics.numBucketCreateFails.incr();

    String writtenMetrics = waitForMetricsToPublish("test_metrics_num");

    //THEN
    assertThat(writtenMetrics)
        .withFailMessage("The expected metric line is missing from prometheus metrics output")
        .contains("test_metrics_num_bucket_create_fails{context=\"dfs\"");

    metrics.unregisterSource("TestMetrics");
  }

  @Test
  public void testPublishWithSameName()
      throws InterruptedException, TimeoutException {
    // GIVEN
    metrics.register("FooBar", "fooBar", (MetricsSource) (collector, all) -> {
      collector.addRecord("RpcMetrics").add(new MetricsTag(PORT_INFO, "1234"))
          .addGauge(COUNTER_INFO, COUNTER_1).endRecord();

      collector.addRecord("RpcMetrics").add(new MetricsTag(
          PORT_INFO, "2345")).addGauge(COUNTER_INFO, COUNTER_2).endRecord();
    });

    String writtenMetrics = waitForMetricsToPublish("rpc_metrics_counter");

    // THEN
    assertThat(writtenMetrics)
        .withFailMessage("The expected metric line is missing from prometheus metrics output")
        .contains("rpc_metrics_counter{port=\"2345\"");

    assertThat(writtenMetrics)
        .withFailMessage("The expected metric line is missing from prometheus metrics output")
        .contains("rpc_metrics_counter{port=\"1234\"");

    metrics.unregisterSource("FooBar");
  }

  @Test
  public void testTypeWithSameNameButDifferentLabels()
      throws InterruptedException, TimeoutException {
    // GIVEN
    metrics.register("SameName", "sameName",
        (MetricsSource) (collector, all) -> {
          collector.addRecord("SameName").add(new MetricsTag(PORT_INFO, "1234"))
              .addGauge(COUNTER_INFO, COUNTER_1).endRecord();
          collector.addRecord("SameName").add(new MetricsTag(PORT_INFO, "2345"))
              .addGauge(COUNTER_INFO, COUNTER_2).endRecord();
        });

    // WHEN
    String writtenMetrics = waitForMetricsToPublish("same_name_counter");

    // THEN
    assertEquals(1, StringUtils.countMatches(writtenMetrics,
        "# TYPE same_name_counter"));

    // both metrics should be present
    assertThat(writtenMetrics)
        .withFailMessage("The expected metric line is present in prometheus metrics output")
        .contains("same_name_counter{port=\"1234\"");
    assertThat(writtenMetrics)
        .withFailMessage("The expected metric line is present in prometheus metrics output")
        .contains("same_name_counter{port=\"2345\"");

    metrics.unregisterSource("SameName");
  }

  /**
   * Make sure Prometheus metrics start fresh after each flush.
   * Publish the metrics and flush them,
   * then unregister one of them and register another.
   * Publish and flush the metrics again
   * and then check that the unregistered metric is not present.
   */
  @Test
  public void testRemovingStaleMetricsOnFlush()
      throws InterruptedException, TimeoutException {
    // GIVEN
    metrics.register("StaleMetric", "staleMetric",
        (MetricsSource) (collector, all) ->
            collector.addRecord("StaleMetric")
                .add(new MetricsTag(PORT_INFO, "1234"))
                .addGauge(COUNTER_INFO, COUNTER_1).endRecord());

    waitForMetricsToPublish("stale_metric_counter");

    // unregister the metric
    metrics.unregisterSource("StaleMetric");

    metrics.register("SomeMetric", "someMetric",
        (MetricsSource) (collector, all) ->
            collector.addRecord("SomeMetric")
                .add(new MetricsTag(PORT_INFO, "4321"))
                .addGauge(COUNTER_INFO, COUNTER_2).endRecord());

    String writtenMetrics = waitForMetricsToPublish("some_metric_counter");

    // THEN
    // The first metric shouldn't be present
    assertThat(writtenMetrics)
        .withFailMessage("The expected metric line is present in prometheus metrics output")
        .doesNotContain("stale_metric_counter{port=\"1234\"");
    assertThat(writtenMetrics)
        .withFailMessage("The expected metric line is present in prometheus metrics output")
        .contains("some_metric_counter{port=\"4321\"");

    metrics.unregisterSource("SomeMetric");
  }

  private String publishMetricsAndGetOutput() throws IOException {
    metrics.publishMetricsNow();

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(stream, UTF_8);

    sink.writeMetrics(writer);
    writer.flush();
    return stream.toString(UTF_8.name());
  }

  /**
   * metrics.publishMetricsNow() might not finish in a reasonable
   * amount of time leading to a full queue and any further attempt
   * for publishing to fail. Wrapping the call with
   * GenericTestUtils.waitFor() to retry until the queue has been
   * cleared and publish is a success.
   *
   * @param registeredMetric to check if it's published
   * @return all published metrics
   */
  private String waitForMetricsToPublish(String registeredMetric)
      throws InterruptedException, TimeoutException {

    final String[] writtenMetrics = new String[1];

    GenericTestUtils.waitFor(() -> {
      try {
        writtenMetrics[0] = publishMetricsAndGetOutput();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return writtenMetrics[0].contains(registeredMetric);
    }, 1000, 120000);

    return writtenMetrics[0];
  }

  /**
   * Example metric pojo.
   */
  @Metrics(about = "Test Metrics", context = "dfs")
  private static class TestMetrics {

    @Metric
    private MutableCounterLong numBucketCreateFails;
  }

}
