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

import static org.apache.hadoop.hdds.utils.RocksDBStoreMetrics.ROCKSDB_CONTEXT_PREFIX;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.metrics2.MetricsTag;

/**
 * Util class for
 * {@link org.apache.hadoop.hdds.server.http.PrometheusMetricsSink}.
 */
public final class PrometheusMetricsSinkUtil {
  private static final Pattern SPLIT_PATTERN =
      Pattern.compile("(?<!(^|[A-Z_]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])");
  private static final Pattern REPLACE_PATTERN =
      Pattern.compile("[^a-zA-Z0-9]+");

  /**
   * Never constructed.
   */
  private PrometheusMetricsSinkUtil() {
  }

  /**
   * Adds necessary tags.
   *
   * @param key              metrics entry key
   * @param username         caller username
   * @param servername       servername
   * @param unmodifiableTags list of metrics tags
   * @return modifiable list of metrics tags
   */
  public static List<MetricsTag> addTags(String key, String username,
      String servername, Collection<MetricsTag> unmodifiableTags) {
    List<MetricsTag> metricTags = new ArrayList<>(unmodifiableTags);

    Stream.of(DecayRpcSchedulerUtil.createUsernameTag(username),
            UgiMetricsUtil.createServernameTag(key, servername))
        .forEach(
            metricsTag -> metricsTag.ifPresent(mt -> addTag(mt, metricTags)));

    return metricTags;
  }

  /**
   * Adds metric tag to a metrics tags.
   * @param metricsTag metrics tag to be added
   * @param metricsTags metrics tags where metrics tag needs to be added
   */
  private static void addTag(MetricsTag metricsTag,
      List<MetricsTag> metricsTags) {
    metricsTags.add(metricsTag);
  }

  /**
   * Convert CamelCase based names to lower-case names where the separator
   * is the underscore, to follow prometheus naming conventions.
   */
  public static String prometheusName(String recordName,
      String metricName) {

    // RocksDB metric names already have underscores as delimiters,
    // but record name is from DB file name and '.' (as in 'om.db') is invalid
    if (StringUtils.isNotEmpty(recordName) &&
        recordName.startsWith(ROCKSDB_CONTEXT_PREFIX)) {
      return normalizeName(recordName) + "_" + metricName.toLowerCase();
    }

    String baseName = StringUtils.capitalize(recordName)
        + StringUtils.capitalize(metricName);
    return normalizeName(baseName);
  }

  /**
   * Normalizes metrics tag key name.
   * @param baseName
   * @return normalized name.
   */
  private static String normalizeName(String baseName) {
    String[] parts = SPLIT_PATTERN.split(baseName);
    String result = String.join("_", parts).toLowerCase();
    return REPLACE_PATTERN.matcher(result).replaceAll("_");
  }

  public static String getMetricName(String recordName, String metricName) {
    return DecayRpcSchedulerUtil.splitMetricNameIfNeeded(recordName,
        metricName);
  }

  public static String getUsername(String recordName, String metricName) {
    return DecayRpcSchedulerUtil.checkMetricNameForUsername(recordName,
        metricName);
  }
}
