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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test prometheus Sink.
 */
public class TestPrometheusMetricsSink {

  private static PrometheusMetricsSink sink;

  @BeforeAll
  public static void setUp() {
    sink = new PrometheusMetricsSink();
  }

  @Test
  public void testNamingCamelCase() {
    //THEN
    Assertions.assertEquals("rpc_time_some_metrics",
        sink.prometheusName("RpcTime", "SomeMetrics"));

    Assertions.assertEquals("om_rpc_time_om_info_keys",
        sink.prometheusName("OMRpcTime", "OMInfoKeys"));

    Assertions.assertEquals("rpc_time_small",
        sink.prometheusName("RpcTime", "small"));
  }

  @Test
  public void testNamingRocksDB() {
    //RocksDB metrics are handled differently.
    // THEN
    Assertions.assertEquals("rocksdb_om_db_num_open_connections",
        sink.prometheusName("Rocksdb_om.db", "num_open_connections"));
  }

  @Test
  public void testNamingPipeline() {
    // GIVEN
    String recordName = "SCMPipelineMetrics";
    String metricName = "NumBlocksAllocated-"
        + "RATIS-THREE-47659e3d-40c9-43b3-9792-4982fc279aba";

    // THEN
    Assertions.assertEquals(
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
    Assertions.assertEquals(
        "jvm_metrics_gc_time_millis_g1_young_generation",
        sink.prometheusName(recordName, metricName));
  }
}
