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

package org.apache.hadoop.hdds.tracing;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.ReconfigurableConfig;

/**
 * OpenTelemetry tracing configuration for Ozone services.
 * Priority is given as such:
 * 1. Explicit configuration keys
 * 2. Environment variables
 * 3. Default values defined in this class
 */
@ConfigGroup(prefix = "ozone.tracing")
public class TracingConfig extends ReconfigurableConfig {

  private static final String OTEL_EXPORTER_OTLP_ENDPOINT = "OTEL_EXPORTER_OTLP_ENDPOINT";
  private static final String OTEL_TRACES_SAMPLER_ARG = "OTEL_TRACES_SAMPLER_ARG";
  private static final String OTEL_SPAN_SAMPLING_ARG = "OTEL_SPAN_SAMPLING_ARG";

  @Config(
      key = "ozone.tracing.enabled",
      defaultValue = "false",
      type = ConfigType.BOOLEAN,
      tags = { ConfigTag.OZONE, ConfigTag.HDDS },
      description = "If true, tracing is initialized and spans may be exported (subject to sampling)."
  )
  private boolean tracingEnabled;

  @Config(
      key = "ozone.tracing.endpoint",
      defaultValue = "http://localhost:4317",
      type = ConfigType.STRING,
      tags = { ConfigTag.OZONE, ConfigTag.HDDS },
      description = "OTLP gRPC receiver endpoint URL."
  )
  private String tracingEndpoint;

  @Config(
      key = "ozone.tracing.sampler",
      defaultValue = "1.0",
      type = ConfigType.DOUBLE,
      tags = { ConfigTag.OZONE, ConfigTag.HDDS },
      description = "Root trace sampling ratio (0.0 to 1.0)."
  )
  private double traceSamplerRatio;

  @Config(
      key = "ozone.tracing.span.sampling",
      defaultValue = "",
      type = ConfigType.STRING,
      tags = { ConfigTag.OZONE, ConfigTag.HDDS },
      description = "Optional per-span sampling: comma-separated spanName:rate entries."
  )
  private String spanSampling;

  public boolean isTracingEnabled() {
    return tracingEnabled;
  }

  /**
   * Checks if configuration value is set for endpoint.
   * Checks environment variable if config is default value.
   * Returns default value if environment variable is not set
   */
  public String getTracingEndpoint(ConfigurationSource conf) {
    String configEndpoint = conf.get("ozone.tracing.endpoint");
    if (configEndpoint != null && !configEndpoint.equals("http://localhost:4317")) {
      return configEndpoint.trim();
    }
    String envEndpoint = System.getenv(OTEL_EXPORTER_OTLP_ENDPOINT);
    if (envEndpoint != null && !envEndpoint.equals("http://localhost:4317")) {
      return envEndpoint.trim();
    }
    return tracingEndpoint;
  }

  /**
   * Checks if configuration value is set for trace ratio.
   * Checks environment variable if config is default value.
   * Returns default value if environment variable is not set
   */
  public double getTraceSamplerRatio(ConfigurationSource conf) {
    String configTraceRatio = conf.get("ozone.tracing.sampler");
    //if config value is set to something other than default and null then return it
    if (configTraceRatio != null && !configTraceRatio.equals("1.0")) {
      return Double.parseDouble(configTraceRatio.trim());
    }
    String envTraceRatio = System.getenv(OTEL_TRACES_SAMPLER_ARG);
    //if environment variable is set ot something other than default and null then return it
    if (envTraceRatio != null && !envTraceRatio.equals("1.0")) {
      return Double.parseDouble(envTraceRatio.trim());
    }
    return traceSamplerRatio;
  }

  /**
   * Checks if configuration value is set for span ratio.
   * Checks environment variable if config is default value.
   * Returns default value if environment variable is not set
   */
  public String getSpanSampling(ConfigurationSource conf) {
    String configSpanRatio = conf.get("ozone.tracing.span.sampling");
    if (configSpanRatio != null && !configSpanRatio.isEmpty()) {
      return configSpanRatio.trim();
    }
    String envSpanRatio = System.getenv(OTEL_SPAN_SAMPLING_ARG);
    if (envSpanRatio != null && !envSpanRatio.isEmpty()) {
      return envSpanRatio;
    }
    return spanSampling;
  }

}
