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

package org.apache.hadoop.hdds.conf;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.tracing.TracingConfig;
import org.apache.hadoop.hdds.tracing.TracingUtil;

/**
 * Reconfiguration hook for ozone.tracing settings.
 * init runs TracingUtil.initTracing once and returns this callback for registerCompleteCallback.
 * onPropertiesChanged calls TracingUtil.reconfigureTracing when any changed key starts with ozone.tracing.
 */

public final class TracingReconfigurationCallback implements ReconfigurationChangeCallback {

  private static final String TRACING_KEY_PREFIX = "ozone.tracing.";

  private final String serviceName;
  private final TracingConfig tracingConfig;

  private TracingReconfigurationCallback(String serviceName, TracingConfig tracingConfig) {
    this.serviceName = serviceName;
    this.tracingConfig = tracingConfig;
  }

  /**
   * Initializes tracing and returns a callback for runtime reconfiguration.
   */
  public static TracingReconfigurationCallback init(
      String serviceName, TracingConfig tracingConfig) {
    TracingUtil.initTracing(serviceName, tracingConfig);
    return new TracingReconfigurationCallback(serviceName, tracingConfig);
  }

  @Override
  public void onPropertiesChanged(Map<String, Boolean> changedKeys, Configuration newConf) {
    if (changedKeys.keySet().stream().anyMatch(k -> k.startsWith(TRACING_KEY_PREFIX))) {
      TracingUtil.reconfigureTracing(serviceName, tracingConfig);
    }
  }
}
