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

  /**
   * Tracing configuration in ozone.
   */
  public enum TracingMode {
    /** No Ozone tracer; do not create spans from application context. */
    TRACING_OFF,
    /** No Ozone tracer; may create child spans under an application trace. */
    TRACING_CLIENT,
    /** Ozone initializes its OTLP tracer and exports spans (subject to sampling). */
    TRACING_OZONE;

    static TracingMode fromConfig(String raw) {
      if (raw == null || raw.isEmpty()) {
        return TRACING_CLIENT;
      }
      return valueOf(raw.trim().toUpperCase(java.util.Locale.ROOT));
    }
  }

  @Config(
      key = "ozone.tracing.mode",
      defaultValue = "TRACING_CLIENT",
      type = ConfigType.STRING,
      reconfigurable = true,
      tags = { ConfigTag.OZONE, ConfigTag.HDDS },
      description = "Tracing modes: " +
          "TRACING_OZONE: Ozone's internal tracer is used to export spans to OTLP. " +
          "TRACING_CLIENT: If context is present then The application's tracer is used to create child spans. " +
          "TRACING_OFF: no tracing."
  )
  private String tracingModeRaw;

  private TracingMode tracingMode = TracingMode.TRACING_CLIENT;

  @Config(
      key = "ozone.tracing.endpoint",
      defaultValue = "",
      type = ConfigType.STRING,
      reconfigurable = true,
      tags = { ConfigTag.OZONE, ConfigTag.HDDS },
      description = "OTLP gRPC receiver endpoint URL."
  )
  private String tracingEndpoint;

  @Config(
      key = "ozone.tracing.sampler",
      defaultValue = "-1",
      type = ConfigType.DOUBLE,
      reconfigurable = true,
      tags = { ConfigTag.OZONE, ConfigTag.HDDS },
      description = "Root trace sampling ratio (0.0 to 1.0)."
  )
  private double traceSamplerRatio;

  @Config(
      key = "ozone.tracing.span.sampling",
      defaultValue = "",
      type = ConfigType.STRING,
      reconfigurable = true,
      tags = { ConfigTag.OZONE, ConfigTag.HDDS },
      description = "Optional per-span sampling: comma-separated spanName:rate entries."
  )
  private String spanSampling;

  public boolean isClientApplicationAware() {
    return tracingMode != TracingMode.TRACING_OFF;
  }

  public boolean isTracingEnabled() {
    return tracingMode == TracingMode.TRACING_OZONE;
  }

  private static TracingMode resolveTracingMode(String value) {
    if (value == null || value.isEmpty()) {
      return TracingMode.TRACING_CLIENT;
    }
    if (TracingMode.TRACING_OFF.name().equals(value)) {
      return TracingMode.TRACING_OFF;
    }
    if (TracingMode.TRACING_CLIENT.name().equals(value)) {
      return TracingMode.TRACING_CLIENT;
    }
    if (TracingMode.TRACING_OZONE.name().equals(value)) {
      return TracingMode.TRACING_OZONE;
    }
    return TracingMode.TRACING_CLIENT;
  }

  @PostConstruct
  public void validate() {
    tracingMode = resolveTracingMode(tracingModeRaw);

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
