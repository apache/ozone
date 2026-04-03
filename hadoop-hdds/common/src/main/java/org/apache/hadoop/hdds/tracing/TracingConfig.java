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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.PostConstruct;
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
      defaultValue = "",
      type = ConfigType.STRING,
      tags = { ConfigTag.OZONE, ConfigTag.HDDS },
      description = "OTLP gRPC receiver endpoint URL."
  )
  private String tracingEndpoint;

  @Config(
      key = "ozone.tracing.sampler",
      defaultValue = "-1",
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

  @PostConstruct
  void validate() {
    if (tracingEndpoint.isEmpty()) {
      tracingEndpoint = System.getenv(OTEL_EXPORTER_OTLP_ENDPOINT);
    }
    if (StringUtils.isBlank(tracingEndpoint)) {
      tracingEndpoint = "http://localhost:4317";
    }

    if (traceSamplerRatio < 0) {
      String envTraceRatio = System.getenv(OTEL_TRACES_SAMPLER_ARG);
      if (envTraceRatio != null) {
        try {
          traceSamplerRatio = Double.parseDouble(envTraceRatio.trim());
        } catch (NumberFormatException ignored) {
        }
      }
    }
    if (traceSamplerRatio < 0 || traceSamplerRatio > 1) {
      traceSamplerRatio = 1.0;
    }

    if (spanSampling.isEmpty()) {
      String envSampling = System.getenv(OTEL_SPAN_SAMPLING_ARG);
      if (!StringUtils.isBlank(envSampling)) {
        spanSampling = envSampling;
      }
    }
  }

  public String getTracingEndpoint() {
    return tracingEndpoint;
  }

  public double getTraceSamplerRatio() {
    return traceSamplerRatio;
  }

  public String getSpanSampling() {
    return spanSampling;
  }

}
