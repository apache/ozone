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
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BuildInfoMetrics}.
 */
public class TestBuildInfoMetrics {

  private MetricsSystem metricsSystem;
  private PrometheusMetricsSink sink;

  @BeforeEach
  public void setUp() {
    metricsSystem = DefaultMetricsSystem.instance();
    metricsSystem.init("test");
    sink = new PrometheusMetricsSink("testserver");
    metricsSystem.register("Prometheus", "Prometheus", sink);
  }

  @AfterEach
  public void tearDown() {
    metricsSystem.unregisterSource(BuildInfoMetrics.METRICS_SOURCE_NAME);
    metricsSystem.stop();
    metricsSystem.shutdown();
  }

  @Test
  public void testBuildInfoMetricPublished()
      throws IOException, InterruptedException, TimeoutException {
    BuildInfoMetrics.create("OM");

    String output = waitForMetricsToPublish("ozone_build_info");

    assertThat(output).contains("# TYPE ozone_build_info gauge");
    assertThat(output).contains("ozone_build_info{");
    assertThat(output).contains("component=\"OM\"");
    assertThat(output).contains("version=");
    assertThat(output).contains("revision=");
    // Info metrics always have value 1
    assertThat(output).containsPattern("ozone_build_info\\{.*\\} 1");
  }

  @Test
  public void testBuildInfoMetricOnlyOneTypeComment()
      throws IOException, InterruptedException, TimeoutException {
    BuildInfoMetrics.create("SCM");

    String output = waitForMetricsToPublish("ozone_build_info");

    assertEquals(1, countOccurrences(output, "# TYPE ozone_build_info gauge"),
        "Expected exactly one TYPE comment for ozone_build_info");
  }

  private String publishAndGet() throws IOException {
    metricsSystem.publishMetricsNow();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(bos, UTF_8);
    sink.writeMetrics(writer);
    writer.flush();
    return bos.toString(UTF_8.name());
  }

  private String waitForMetricsToPublish(String metric)
      throws InterruptedException, TimeoutException {
    String[] result = new String[1];
    GenericTestUtils.waitFor(() -> {
      try {
        result[0] = publishAndGet();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return result[0].contains(metric);
    }, 500, 30000);
    return result[0];
  }

  private static int countOccurrences(String text, String substring) {
    int count = 0;
    int idx = 0;
    while ((idx = text.indexOf(substring, idx)) != -1) {
      count++;
      idx += substring.length();
    }
    return count;
  }
}
