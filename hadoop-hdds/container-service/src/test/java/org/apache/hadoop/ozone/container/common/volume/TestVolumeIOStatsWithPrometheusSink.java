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

package org.apache.hadoop.ozone.container.common.volume;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_DATANODE_IO_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.PrometheusMetricsSink;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test PrometheusMetricSink regarding VolumeIOStats.
 */
public class TestVolumeIOStatsWithPrometheusSink {
  private MetricsSystem metrics;
  private PrometheusMetricsSink sink;

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
  public void testMultipleVolumeIOMetricsExist() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    int[] intervals = conf.getInts(OZONE_DATANODE_IO_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY);

    //GIVEN
    VolumeIOStats volumeIOStats1 = new VolumeIOStats("VolumeIOStat1",
        "vol1/dir", intervals);
    VolumeIOStats volumeIOStat2 = new VolumeIOStats("VolumeIOStat2",
        "vol2/dir", intervals);

    //WHEN
    String writtenMetrics = publishMetricsAndGetOutput();

    //THEN
    assertThat(writtenMetrics)
        .withFailMessage("The expected metric line is missing from prometheus metrics output")
        .contains("storagedirectory=\"" + volumeIOStats1.getStorageDirectory() + "\"");
    assertThat(writtenMetrics)
        .withFailMessage("The expected metric line is missing from prometheus metrics output")
        .contains("storagedirectory=\"" + volumeIOStat2.getStorageDirectory() + "\"");
  }

  private String publishMetricsAndGetOutput() throws IOException {
    metrics.publishMetricsNow();

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(stream, UTF_8);

    sink.writeMetrics(writer);
    writer.flush();

    return stream.toString(UTF_8.name());
  }
}
