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

import java.util.Optional;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;

/**
 * Util class for UGI metrics.
 */
public final class UgiMetricsUtil {

  private static final String UGI_METRICS = "ugi_metrics";

  /**
   * Never constructed.
   */
  private UgiMetricsUtil() {
  }

  /**
   * Creates servername metrics tag.
   *
   * @param key        metrics entry key
   * @param servername server name
   * @return empty optional if no metrics tag was created, otherwise
   * optional of metrics tag.
   */
  public static Optional<MetricsTag> createServernameTag(String key,
      String servername) {
    if (!key.contains(UGI_METRICS)) {
      return Optional.empty();
    }

    final String name = "servername";
    final String description = "name of the server";
    final MetricsInfo metricsInfo = new MetricsInfo() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public String description() {
        return description;
      }
    };
    MetricsTag metricsTag = new MetricsTag(metricsInfo, servername);
    return Optional.of(metricsTag);
  }

}
