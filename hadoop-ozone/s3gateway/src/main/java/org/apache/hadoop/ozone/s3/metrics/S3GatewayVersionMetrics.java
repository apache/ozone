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

package org.apache.hadoop.ozone.s3.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Exposes the current S3 Gateway client-protocol version as a Prometheus gauge.
 *
 * <p>S3G does not use the layout-version framework (no ComponentVersionManager),
 * so this metric fills the equivalent role for the ZDU dashboard: an operator
 * can query {@code s3_gateway_client_version} to see which client protocol
 * version each S3G instance is running.</p>
 *
 * <pre>
 * s3_gateway_client_version{} 3
 * </pre>
 */
@Metrics(about = "S3 Gateway Version Metrics", context = OzoneConsts.OZONE)
public final class S3GatewayVersionMetrics implements MetricsSource {

  public static final String METRICS_SOURCE_NAME = "S3GatewayVersion";

  private static final MetricsInfo CLIENT_VERSION = Interns.info(
      "ClientVersion",
      "Current S3 Gateway client-protocol version in serialized int form.");

  S3GatewayVersionMetrics() {
  }

  public static S3GatewayVersionMetrics create() {
    S3GatewayVersionMetrics metrics = (S3GatewayVersionMetrics) DefaultMetricsSystem.instance()
        .getSource(METRICS_SOURCE_NAME);
    if (metrics == null) {
      return DefaultMetricsSystem.instance().register(
          METRICS_SOURCE_NAME,
          "S3 Gateway client-protocol version metrics.",
          new S3GatewayVersionMetrics());
    }
    return metrics;
  }

  public void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    // Record name "S3Gateway" + gauge "ClientVersion" → s3_gateway_client_version
    MetricsRecordBuilder builder = collector.addRecord("S3Gateway");
    builder.addGauge(CLIENT_VERSION, ClientVersion.CURRENT.serialize());
  }
}
