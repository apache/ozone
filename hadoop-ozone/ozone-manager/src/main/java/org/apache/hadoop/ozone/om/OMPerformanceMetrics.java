/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * Including OM performance related metrics.
 */
public class OMPerformanceMetrics {
  private static final String SOURCE_NAME =
      OMPerformanceMetrics.class.getSimpleName();

  public static OMPerformanceMetrics register() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
            "OzoneManager Request Performance",
            new OMPerformanceMetrics());
  }

  public static void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Metric(about = "Overall lookupKey in nanoseconds")
  private MutableRate lookupKeyLatencyNs;

  @Metric(about = "Read key info from meta in nanoseconds")
  private MutableRate readKeyInfoLatencyNs;

  @Metric(about = "Block token generation latency in nanoseconds")
  private MutableRate generateBlockTokenLatencyNs;

  @Metric(about = "Refresh location nanoseconds")
  private MutableRate refreshContainerLocationLatencyNs;

  @Metric(about = "ACLs check nanoseconds")
  private MutableRate aclCheckLatencyNs;

  @Metric(about = "s3VolumeInfo latency nanoseconds")
  private MutableRate s3VolumeContextLatencyNs;

  @Metric(about = "resolveBucketLink latency nanoseconds")
  private MutableRate resolveBucketLinkLatencyNs;

  public void addLookupKeyLatency(long latencyInNs) {
    lookupKeyLatencyNs.add(latencyInNs);
  }

  public void addBlockTokenLatency(long latencyInNs) {
    blockTokenLatencyNs.add(latencyInNs);
  }

  public void addRefreshLatency(long latencyInNs) {
    refreshLatencyNs.add(latencyInNs);
  }

  public void addAckCheckLatency(long latencyInNs) {
    aclCheckLatencyNs.add(latencyInNs);
  }

  public void addReadKeyInfoLatency(long latencyInNs) {
    readKeyInfoLatencyNs.add(latencyInNs);
  }

  public void addS3VolumeContextLatencyNs(long latencyInNs) {
    s3VolumeContextLatencyNs.add(latencyInNs);
  }

  public void addResolveBucketLinkLatencyNs(long latencyInNs) {
    resolveBucketLinkLatencyNs.add(latencyInNs);
  }
}
