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

package org.apache.hadoop.hdds.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.metrics2.MetricsTag;
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
    final String key = "key";
    final String username = null;
    final String servername = null;
    Collection<MetricsTag> unmodifiableMetricTags =
        Collections.unmodifiableList(new ArrayList<>());

    // WHEN
    List<MetricsTag> metricsTags = PrometheusMetricsSinkUtil.addTags(key,
        username, servername, unmodifiableMetricTags);

    // THEN
    assertFalse(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(USERNAME_TAG_NAME) ||
            metricsTag.description().equals(USERNAME_TAG_DESCRIPTION)));
  }

  @Test
  void testAddTagsAddUsernameTagWithEmptyUsername() {
    // GIVEN
    final String key = "key";
    final String username = "";
    final String servername = null;
    Collection<MetricsTag> unmodifiableMetricTags =
        Collections.unmodifiableList(new ArrayList<>());

    // WHEN
    List<MetricsTag> metricsTags = PrometheusMetricsSinkUtil.addTags(key,
        username, servername, unmodifiableMetricTags);

    // THEN
    assertFalse(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(USERNAME_TAG_NAME) ||
            metricsTag.description().equals(USERNAME_TAG_DESCRIPTION)));
  }

  @Test
  void testAddTagsAddUsernameTagWithUsername() {
    // GIVEN
    final String key = "key";
    final String username = "username";
    final String servername = null;
    Collection<MetricsTag> unmodifiableMetricTags =
        Collections.unmodifiableList(new ArrayList<>());

    // WHEN
    List<MetricsTag> metricsTags = PrometheusMetricsSinkUtil.addTags(key,
        username, servername, unmodifiableMetricTags);

    // THEN
    assertTrue(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(USERNAME_TAG_NAME) &&
            metricsTag.description().equals(USERNAME_TAG_DESCRIPTION)));
  }

  @Test
  void testAddTagsAddServernameTagWithNoUgiMetricsKey() {
    // GIVEN
    final String key = "key";
    final String username = null;
    final String servername = "SERVERNAME";
    Collection<MetricsTag> unmodifiableMetricTags =
        Collections.unmodifiableList(new ArrayList<>());

    // WHEN
    List<MetricsTag> metricsTags = PrometheusMetricsSinkUtil.addTags(key,
        username, servername, unmodifiableMetricTags);

    // THEN
    assertFalse(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(SERVERNAME_TAG_NAME) ||
            metricsTag.description().equals(SERVERNAME_TAG_DESCRIPTION)));
  }

  @Test
  void testAddTagsAddServernameTagWithUgiMetricsKey() {
    // GIVEN
    final String key = "ugi_metrics";
    final String username = null;
    final String servername = "SERVERNAME";
    Collection<MetricsTag> unmodifiableMetricTags =
        Collections.unmodifiableList(new ArrayList<>());

    // WHEN
    List<MetricsTag> metricsTags = PrometheusMetricsSinkUtil.addTags(key,
        username, servername, unmodifiableMetricTags);

    // THEN
    assertTrue(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(SERVERNAME_TAG_NAME) &&
            metricsTag.description().equals(SERVERNAME_TAG_DESCRIPTION)));
  }

  @Test
  void testAddTags() {
    // GIVEN
    final String key = "ugi_metrics";
    final String username = "username";
    final String servername = "SERVERNAME";
    Collection<MetricsTag> unmodifiableMetricTags =
        Collections.unmodifiableList(new ArrayList<>());

    // WHEN
    List<MetricsTag> metricsTags = PrometheusMetricsSinkUtil.addTags(key,
        username, servername, unmodifiableMetricTags);

    // THEN
    assertTrue(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(USERNAME_TAG_NAME)));
    assertTrue(metricsTags.stream()
        .anyMatch(metricsTag -> metricsTag.name().equals(SERVERNAME_TAG_NAME)));
  }

  @Test
  void testNamingCamelCase() {
    //THEN
    assertEquals("rpc_time_some_metrics",
        PrometheusMetricsSinkUtil.prometheusName("RpcTime", "SomeMetrics"));

    assertEquals("om_rpc_time_om_info_keys",
        PrometheusMetricsSinkUtil.prometheusName("OMRpcTime", "OMInfoKeys"));

    assertEquals("rpc_time_small",
        PrometheusMetricsSinkUtil.prometheusName("RpcTime", "small"));
  }

  @Test
  void testNamingRocksDB() {
    //RocksDB metrics are handled differently.
    // THEN
    assertEquals("rocksdb_om_db_num_open_connections",
        PrometheusMetricsSinkUtil.prometheusName("Rocksdb_om.db",
            "num_open_connections"));
  }

  @Test
  void testNamingPipeline() {
    // GIVEN
    String recordName = "SCMPipelineMetrics";
    String metricName = "NumBlocksAllocated-"
        + "RATIS-THREE-47659e3d-40c9-43b3-9792-4982fc279aba";

    // THEN
    assertEquals(
        "scm_pipeline_metrics_" + "num_blocks_allocated_"
            + "ratis_three_47659e3d_40c9_43b3_9792_4982fc279aba",
        PrometheusMetricsSinkUtil.prometheusName(recordName, metricName));
  }

  @Test
  void testNamingSpaces() {
    //GIVEN
    String recordName = "JvmMetrics";
    String metricName = "GcTimeMillisG1 Young Generation";

    // THEN
    assertEquals(
        "jvm_metrics_gc_time_millis_g1_young_generation",
        PrometheusMetricsSinkUtil.prometheusName(recordName, metricName));
  }

  @Test
  void testGetMetricName() {
    // GIVEN
    final String recordName = "record_name";
    final String metricName = "metric_name";

    // WHEN
    String newMetricName = PrometheusMetricsSinkUtil.getMetricName(recordName,
        metricName);

    // THEN
    assertEquals(metricName, newMetricName);
  }

  @Test
  void testGetUsername() {
    // GIVEN
    final String recordName = "record_name";
    final String metricName = "metric_name";

    // WHEN
    String username = PrometheusMetricsSinkUtil.getUsername(recordName,
        metricName);

    // THEN
    assertNull(username);
  }

}
