/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Class for unit tests for {@link PrometheusMetricsSinkUtil}.
 */
class TestPrometheusMetricsSinkUtil {

  private static final String USERNAME_TAG_NAME = "username";
  private static final String USERNAME_TAG_DESCRIPTION = "caller username";
  private static final String SERVERNAME_TAG_NAME = "servername";
  private static final String SERVERNAME_TAG_DESCRIPTION = "name of the server";

  @Test
  void testAddTagsAddUsernameTagWithNullUsername() {
    // GIVEN
    String key = "key";
    String username = null;
    Collection<MetricsTag> unmodifiableMetricTags =
        Collections.unmodifiableList(new ArrayList<>());

    // WHEN
    List<MetricsTag> metricsTags = PrometheusMetricsSinkUtil.addTags(key,
        username,
        unmodifiableMetricTags);

    // THEN
    Assertions.assertFalse(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(USERNAME_TAG_NAME) ||
            metricsTag.description().equals(USERNAME_TAG_DESCRIPTION)));
  }

  @Test
  void testAddTagsAddUsernameTagWithEmptyUsername() {
    // GIVEN
    String key = "key";
    String username = "";
    Collection<MetricsTag> unmodifiableMetricTags =
        Collections.unmodifiableList(new ArrayList<>());

    // WHEN
    List<MetricsTag> metricsTags = PrometheusMetricsSinkUtil.addTags(key,
        username,
        unmodifiableMetricTags);

    // THEN
    Assertions.assertFalse(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(USERNAME_TAG_NAME) ||
            metricsTag.description().equals(USERNAME_TAG_DESCRIPTION)));
  }

  @Test
  void testAddTagsAddUsernameTagWithUsername() {
    // GIVEN
    String key = "key";
    String username = "username";
    Collection<MetricsTag> unmodifiableMetricTags =
        Collections.unmodifiableList(new ArrayList<>());

    // WHEN
    List<MetricsTag> metricsTags = PrometheusMetricsSinkUtil.addTags(key,
        username,
        unmodifiableMetricTags);

    // THEN
    Assertions.assertTrue(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(USERNAME_TAG_NAME) &&
            metricsTag.description().equals(USERNAME_TAG_DESCRIPTION)));
  }

  @Test
  void testAddTagsAddServernameTagWithNoUgiMetricsKey() {
    // GIVEN
    String key = "key";
    String username = null;
    Collection<MetricsTag> unmodifiableMetricTags =
        Collections.unmodifiableList(new ArrayList<>());

    // WHEN
    List<MetricsTag> metricsTags = PrometheusMetricsSinkUtil.addTags(key,
        username,
        unmodifiableMetricTags);

    // THEN
    Assertions.assertFalse(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(SERVERNAME_TAG_NAME) ||
            metricsTag.description().equals(SERVERNAME_TAG_DESCRIPTION)));
  }

  @Test
  void testAddTagsAddServernameTagWithUgiMetricsKey() {
    // GIVEN
    String key = "ugi_metrics";
    String username = null;
    Collection<MetricsTag> unmodifiableMetricTags =
        Collections.unmodifiableList(new ArrayList<>());

    // WHEN
    List<MetricsTag> metricsTags = PrometheusMetricsSinkUtil.addTags(key,
        username,
        unmodifiableMetricTags);

    // THEN
    Assertions.assertTrue(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(SERVERNAME_TAG_NAME) &&
            metricsTag.description().equals(SERVERNAME_TAG_DESCRIPTION)));
  }

  @Test
  void testAddTags() {
    // GIVEN
    String key = "ugi_metrics";
    String username = "username";
    Collection<MetricsTag> unmodifiableMetricTags =
        Collections.unmodifiableList(new ArrayList<>());

    // WHEN
    List<MetricsTag> metricsTags = PrometheusMetricsSinkUtil.addTags(key,
        username,
        unmodifiableMetricTags);

    // THEN
    Assertions.assertTrue(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(USERNAME_TAG_NAME)));
    Assertions.assertTrue(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(SERVERNAME_TAG_NAME)));
  }

  @Test
  void testReplaceServerNameTagValueWithNonUgiMetrics() {
    // GIVEN
    String key = "non_ugi";
    String metricsKey = "{\"servername\"=\"unknown\"}";
    int serverPort = 0;

    // WHEN
    String newMetricsKey =
        PrometheusMetricsSinkUtil.replaceServerNameTagValue(key, metricsKey,
            serverPort);

    // THEN
    Assertions.assertEquals(metricsKey, newMetricsKey);
  }

  @Test
  void testReplaceServerNameTagValueWithUgiMetricsAndNonOMOrSCMPort() {
    // GIVEN
    String key = "ugi_metrics";
    String metricsKey = "{\"servername\"=\"unknown\"}";
    int serverPort = 0;

    // WHEN
    String newMetricsKey =
        PrometheusMetricsSinkUtil.replaceServerNameTagValue(key, metricsKey,
            serverPort);

    // THEN
    Assertions.assertEquals(metricsKey, newMetricsKey);
  }

  @Test
  void testReplaceServerNameTagValueWithUgiMetricsAndOMPort() {
    // GIVEN
    String key = "ugi_metrics";
    String metricsKey = "{\"servername\"=\"unknown\"}";
    int serverPort = OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT;

    // WHEN
    String newMetricsKey =
        PrometheusMetricsSinkUtil.replaceServerNameTagValue(key, metricsKey,
            serverPort);

    // THEN
    Assertions.assertEquals("{\"servername\"=\"OM\"}", newMetricsKey);
  }

  @Test
  void testReplaceServerNameTagValueWithUgiMetricsAndSCMPort() {
    // GIVEN
    String key = "ugi_metrics";
    String metricsKey = "{\"servername\"=\"unknown\"}";
    int serverPort = ScmConfigKeys.OZONE_SCM_HTTP_BIND_PORT_DEFAULT;

    // WHEN
    String newMetricsKey =
        PrometheusMetricsSinkUtil.replaceServerNameTagValue(key, metricsKey,
            serverPort);

    // THEN
    Assertions.assertEquals("{\"servername\"=\"SCM\"}", newMetricsKey);
  }

  @Test
  public void testNamingCamelCase() {
    //THEN
    Assertions.assertEquals("rpc_time_some_metrics",
        PrometheusMetricsSinkUtil.prometheusName("RpcTime", "SomeMetrics"));

    Assertions.assertEquals("om_rpc_time_om_info_keys",
        PrometheusMetricsSinkUtil.prometheusName("OMRpcTime", "OMInfoKeys"));

    Assertions.assertEquals("rpc_time_small",
        PrometheusMetricsSinkUtil.prometheusName("RpcTime", "small"));
  }

  @Test
  public void testNamingRocksDB() {
    //RocksDB metrics are handled differently.
    // THEN
    Assertions.assertEquals("rocksdb_om_db_num_open_connections",
        PrometheusMetricsSinkUtil.prometheusName("Rocksdb_om.db",
            "num_open_connections"));
  }

  @Test
  public void testNamingPipeline() {
    // GIVEN
    String recordName = "SCMPipelineMetrics";
    String metricName = "NumBlocksAllocated-"
        + "RATIS-THREE-47659e3d-40c9-43b3-9792-4982fc279aba";

    // THEN
    Assertions.assertEquals(
        "scm_pipeline_metrics_" + "num_blocks_allocated_"
            + "ratis_three_47659e3d_40c9_43b3_9792_4982fc279aba",
        PrometheusMetricsSinkUtil.prometheusName(recordName, metricName));
  }

  @Test
  public void testNamingSpaces() {
    //GIVEN
    String recordName = "JvmMetrics";
    String metricName = "GcTimeMillisG1 Young Generation";

    // THEN
    Assertions.assertEquals(
        "jvm_metrics_gc_time_millis_g1_young_generation",
        PrometheusMetricsSinkUtil.prometheusName(recordName, metricName));
  }

}