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

import static org.apache.hadoop.hdds.utils.RocksDBStoreMBean.ROCKSDB_CONTEXT_PREFIX;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.ozone.om.OMConfigKeys;

/**
 * Util class for
 * {@link org.apache.hadoop.hdds.server.http.PrometheusMetricsSink}.
 */
public final class PrometheusMetricsSinkUtil {

  private static final String UGI_METRICS = "ugi_metrics";
  private static final String SERVER_NAME = "unknown";
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
   * @param key              metric entry key
   * @param username         caller username
   * @param unmodifiableTags list of metric tags
   * @return modifiable list of metric tags
   */
  public static List<MetricsTag> addTags(String key, String username,
      Collection<MetricsTag> unmodifiableTags) {
    List<MetricsTag> metricTags = new ArrayList<>(unmodifiableTags);

    addUsernameTag(username, metricTags);
    addServerNameTag(key, metricTags);

    return metricTags;
  }

  /**
   * Adds a <tt>servername</tt> metric tag.
   *
   * @param key        metrics entry key
   * @param metricTags list of metric tags
   */
  private static void addServerNameTag(String key,
      List<MetricsTag> metricTags) {
    if (!key.contains(UGI_METRICS)) {
      return;
    }

    final String name = "servername";
    final String description = "name of the server";
    addTag(name, description, SERVER_NAME, metricTags);
  }

  /**
   * Adds a <tt>username</tt> metric tag.
   *
   * @param username   caller username
   * @param metricTags list of metric tags
   */
  private static void addUsernameTag(String username,
      List<MetricsTag> metricTags) {

    if (Strings.isNullOrEmpty(username)) {
      return;
    }

    final String name = "username";
    final String description = "caller username";
    addTag(name, description, username, metricTags);
  }

  /**
   * Adds a tag.
   *
   * @param name        tag name
   * @param description tag description
   * @param value       tag value
   * @param metricsTags list of metric tags
   */
  private static void addTag(String name, String description, String value,
      List<MetricsTag> metricsTags) {
    MetricsInfo metricsInfo = createMetricsInfo(name, description);
    MetricsTag metricTag = new MetricsTag(metricsInfo, value);
    metricsTags.add(metricTag);
  }

  /**
   * Creates MetricsInfo.
   *
   * @param name        metric name
   * @param description metric description
   * @return <tt>MetricsInfo</tt>
   */
  private static MetricsInfo createMetricsInfo(String name,
      String description) {
    return new MetricsInfo() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public String description() {
        return description;
      }
    };
  }

  /**
   * Replaces default servername tag value with actual server name.
   *
   * @param metricEntryKey metric entry key
   * @param metricKey      metric key
   * @param serverPort     server port
   * @return <tt>metricKey</tt> with replaced servername tag value.
   */
  public static String replaceServerNameTagValue(String metricEntryKey,
      String metricKey, int serverPort) {
    if (!metricEntryKey.contains(UGI_METRICS)) {
      return metricKey;
    }

    String serverName = null;
    switch (serverPort) {
    case OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT:
      serverName = "OM";
      break;
    case ScmConfigKeys.OZONE_SCM_HTTP_BIND_PORT_DEFAULT:
      serverName = "SCM";
      break;
    default:
      break;
    }

    if (serverName != null) {
      metricKey = metricKey.replace(SERVER_NAME, serverName);
    }

    return metricKey;
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

  private static String normalizeName(String baseName) {
    String[] parts = SPLIT_PATTERN.split(baseName);
    String result = String.join("_", parts).toLowerCase();
    return REPLACE_PATTERN.matcher(result).replaceAll("_");
  }

}
