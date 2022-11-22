/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.utils;

import com.google.common.base.Strings;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper functions for DecayRpcScheduler
 * metrics for Prometheus.
 */
public final class DecayRpcSchedulerUtil {

  private DecayRpcSchedulerUtil() {
  }

  private static final MetricsInfo USERNAME_INFO = new MetricsInfo() {
    @Override
    public String name() {
      return "username";
    }

    @Override
    public String description() {
      return "caller username";
    }
  };

  /**
   * For Decay_Rpc_Scheduler, the metric name is in format
   * "Caller(<callers_username>).Volume"
   * or
   * "Caller(<callers_username>).Priority"
   * Split it and return the metric.
   *
   * If the recordName doesn't belong to Decay_Rpc_Scheduler,
   * then return the metricName as it is without making
   * any changes to it.
   *
   * @param recordName
   * @param metricName "Caller(xyz).Volume" or "Caller(xyz).Priority"
   * @return "Volume" or "Priority" or metricName(unchanged)
   */
  public static String splitMetricNameIfNeeded(String recordName,
                                               String metricName) {
    if (recordName.toLowerCase().contains("decayrpcscheduler") &&
        metricName.toLowerCase().contains("caller(")) {
      // names will contain ["Caller(xyz)", "Volume" / "Priority"]
      String[] names = metricName.split("[.]");

      // "Volume" or "Priority"
      return names[1];
    }
    return metricName;
  }

  /**
   * For Decay_Rpc_Scheduler, split the metric name
   * and then get the part that is in the format "Caller(<callers_username>)"
   * and split it to return the username.
   * @param recordName
   * @param metricName
   * @return caller username or null if not present
   */
  public static String checkMetricNameForUsername(String recordName,
                                                  String metricName) {
    if (recordName.toLowerCase().contains("decayrpcscheduler") &&
        metricName.toLowerCase().contains("caller(")) {
      // names will contain ["Caller(xyz)", "Volume" / "Priority"]
      String[] names = metricName.split("[.]");

      // Caller(xyz)
      String caller = names[0];

      // subStrings will contain ["Caller", "xyz"]
      String[] subStrings = caller.split("[()]");

      String username = subStrings[1];

      return username;
    }
    return null;
  }

  /**
   * MetricRecord.tags() is an unmodifiable collection of tags.
   * Store it in a list, to modify it and add a username tag.
   * @param metricsRecord
   * @return the new list with the metric tags and the username tag
   */
  public static List<MetricsTag> tagListWithUsernameIfNeeded(
      MetricsRecord metricsRecord, String username) {
    List<MetricsTag> list = new ArrayList<>(metricsRecord.tags());

    if (!Strings.isNullOrEmpty(username)) {
      MetricsTag tag = new MetricsTag(USERNAME_INFO, username);
      list.add(tag);
    }
    return list;
  }
}
