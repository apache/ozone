/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.server.http;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test prometheus Sink.
 */
public class TestPrometheusMetricsSink {

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

  @Before
  public void init() {
    metrics = DefaultMetricsSystem.instance();

    metrics.init("test");
    sink = new PrometheusMetricsSink();
    metrics.register("Prometheus", "Prometheus", sink);
  }

  @After
  public void tearDown() {
    metrics.stop();
    metrics.shutdown();
  }

  @Test
  public void testPublish() throws IOException {
    //GIVEN
    TestMetrics testMetrics = metrics
        .register("TestMetrics", "Testing metrics", new TestMetrics());

    testMetrics.numBucketCreateFails.incr();

    //WHEN
    String writtenMetrics = publishMetricsAndGetOutput();

    //THEN
    Assert.assertTrue(
        "The expected metric line is missing from prometheus metrics output",
        writtenMetrics.contains(
            "test_metrics_num_bucket_create_fails{context=\"dfs\"")
    );
  }

  @Test
  public void testPublishWithSameName() throws IOException {
    //GIVEN
    metrics.register("FooBar", "fooBar", (MetricsSource) (collector, all) -> {
      collector.addRecord("RpcMetrics").add(new MetricsTag(PORT_INFO, "1234"))
          .addGauge(COUNTER_INFO, COUNTER_1).endRecord();

      collector.addRecord("RpcMetrics").add(new MetricsTag(
          PORT_INFO, "2345")).addGauge(COUNTER_INFO, COUNTER_2).endRecord();
    });

    // WHEN
    String writtenMetrics = publishMetricsAndGetOutput();

    // THEN
    Assert.assertTrue(
        "The expected metric line is missing from prometheus metrics output",
        writtenMetrics.contains(
            "rpc_metrics_counter{port=\"2345\""));

    Assert.assertTrue(
        "The expected metric line is missing from prometheus metrics "
            + "output",
        writtenMetrics.contains(
            "rpc_metrics_counter{port=\"1234\""));
  }

  @Test
  public void testTypeWithSameNameButDifferentLabels() throws IOException {
    //GIVEN
    metrics.register("SameName", "sameName",
        (MetricsSource) (collector, all) -> {
          collector.addRecord("SameName").add(new MetricsTag(PORT_INFO, "1234"))
              .addGauge(COUNTER_INFO, COUNTER_1).endRecord();
          collector.addRecord("SameName").add(new MetricsTag(PORT_INFO, "2345"))
              .addGauge(COUNTER_INFO, COUNTER_2).endRecord();
        });

    // WHEN
    String writtenMetrics = publishMetricsAndGetOutput();

    // THEN
    Assert.assertEquals(1, StringUtils.countMatches(writtenMetrics,
        "# TYPE same_name_counter"));
  }

  @Test
  public void testNamingCamelCase() {
    //THEN
    Assert.assertEquals("rpc_time_some_metrics",
        sink.prometheusName("RpcTime", "SomeMetrics"));

    Assert.assertEquals("om_rpc_time_om_info_keys",
        sink.prometheusName("OMRpcTime", "OMInfoKeys"));

    Assert.assertEquals("rpc_time_small",
        sink.prometheusName("RpcTime", "small"));
  }

  @Test
  public void testNamingRocksDB() {
    //RocksDB metrics are handled differently.
    // THEN
    Assert.assertEquals("rocksdb_om_db_num_open_connections",
        sink.prometheusName("Rocksdb_om.db", "num_open_connections"));
  }

  @Test
  public void testNamingPipeline() {
    // GIVEN
    String recordName = "SCMPipelineMetrics";
    String metricName = "NumBlocksAllocated-"
        + "RATIS-THREE-47659e3d-40c9-43b3-9792-4982fc279aba";

    // THEN
    Assert.assertEquals(
        "scm_pipeline_metrics_"
            + "num_blocks_allocated_"
            + "ratis_three_47659e3d_40c9_43b3_9792_4982fc279aba",
        sink.prometheusName(recordName, metricName));
  }

  @Test
  public void testNamingSpaces() {
    //GIVEN
    String recordName = "JvmMetrics";
    String metricName = "GcTimeMillisG1 Young Generation";

    // THEN
    Assert.assertEquals(
        "jvm_metrics_gc_time_millis_g1_young_generation",
        sink.prometheusName(recordName, metricName));
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
   * Example metric pojo.
   */
  @Metrics(about = "Test Metrics", context = "dfs")
  private static class TestMetrics {

    @Metric
    private MutableCounterLong numBucketCreateFails;
  }

}
