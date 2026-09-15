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

package org.apache.hadoop.ozone.conf;

import static org.apache.hadoop.hdds.conf.ConfigTag.MANAGEMENT;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.S3GATEWAY;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * Config for the S3 Gateway health check endpoints served on the web admin
 * server (default port 19878).
 */
@ConfigGroup(prefix = "ozone.s3g.health-check")
public class S3GatewayHealthCheckConfig {

  @Config(key = "ozone.s3g.health-check.enabled",
      defaultValue = "true",
      type = ConfigType.BOOLEAN,
      tags = {OZONE, S3GATEWAY, MANAGEMENT},
      description = "If enabled, the S3 Gateway web admin server exposes " +
          "unauthenticated health endpoints that a load balancer can poll: " +
          "a liveness endpoint at /health/live (process is up) and a " +
          "readiness endpoint at /health/ready (the gateway can reach OM). " +
          "Disable to remove the endpoints entirely.")
  private boolean enabled = true;

  @Config(key = "ozone.s3g.health-check.probe.interval",
      defaultValue = "10s",
      type = ConfigType.TIME,
      timeUnit = TimeUnit.MILLISECONDS,
      tags = {OZONE, S3GATEWAY, MANAGEMENT},
      description = "How often the readiness endpoint refreshes its cached " +
          "view of OM reachability in the background. The probe runs off the " +
          "request path so /health/ready always responds immediately.")
  private long probeInterval = 10 * 1000;

  @Config(key = "ozone.s3g.health-check.probe.timeout",
      defaultValue = "10s",
      type = ConfigType.TIME,
      timeUnit = TimeUnit.MILLISECONDS,
      tags = {OZONE, S3GATEWAY, MANAGEMENT},
      description = "Upper bound on a single background readiness probe to " +
          "OM. If a probe does not complete within this time the gateway is " +
          "reported as not ready, so a stuck OM cannot leave readiness " +
          "stuck at ready.")
  private long probeTimeout = 10 * 1000;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public long getProbeInterval() {
    return probeInterval;
  }

  public void setProbeInterval(long probeInterval) {
    this.probeInterval = probeInterval;
  }

  public long getProbeTimeout() {
    return probeTimeout;
  }

  public void setProbeTimeout(long probeTimeout) {
    this.probeTimeout = probeTimeout;
  }
}
