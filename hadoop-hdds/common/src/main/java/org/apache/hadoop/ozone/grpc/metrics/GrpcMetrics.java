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

package org.apache.hadoop.ozone.grpc.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which maintains metrics related to using GRPC.
 */
@Metrics(about = "GRPC Metrics", context = OzoneConsts.OZONE)
public class GrpcMetrics {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcMetrics.class);
  private static final String SOURCE_NAME =
      GrpcMetrics.class.getSimpleName();

  private final MetricsRegistry registry;
  private final boolean grpcQuantileEnable;

  public GrpcMetrics(Configuration conf) {
    registry = new MetricsRegistry("grpc");
    int[] intervals = conf.getInts(
        OzoneConfigKeys.OZONE_GPRC_METRICS_PERCENTILES_INTERVALS_KEY);
    grpcQuantileEnable = (intervals.length > 0);
    if (grpcQuantileEnable) {
      grpcQueueTimeMillisQuantiles =
          new MutableQuantiles[intervals.length];
      grpcProcessingTimeMillisQuantiles =
          new MutableQuantiles[intervals.length];
      for (int i = 0; i < intervals.length; i++) {
        int interval = intervals[i];
        grpcProcessingTimeMillisQuantiles[i] = registry
            .newQuantiles("grpcQueueTime" + interval
                    + "s", "grpc queue time in milli second", "ops",
                "latency", interval);
        grpcProcessingTimeMillisQuantiles[i] = registry.newQuantiles(
            "grpcProcessingTime" + interval + "s",
            "grpc processing time in milli second",
            "ops", "latency", interval);
      }
    }
    LOG.debug("Initialized " + registry);
  }

  /**
   * Create and return GrpcMetrics instance.
   * @param conf
   * @return GrpcMetrics
   */
  public static synchronized GrpcMetrics create(Configuration conf) {
    GrpcMetrics metrics = new GrpcMetrics(conf);
    return DefaultMetricsSystem.instance().register(SOURCE_NAME,
        "Metrics for using gRPC", metrics);
  }

  /**
   * Unregister the metrics instance.
   */
  public void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  @Metric("Number of sent bytes")
  private MutableGaugeLong sentBytes;

  @Metric("Number of received bytes")
  private MutableGaugeLong receivedBytes;

  @Metric("Queue time")
  private MutableRate grpcQueueTime;

  // There should be no getter method to avoid
  // exposing internal representation. FindBugs error raised.
  private MutableQuantiles[] grpcQueueTimeMillisQuantiles;

  @Metric("Processsing time")
  private MutableRate grpcProcessingTime;

  // There should be no getter method to avoid
  // exposing internal representation. FindBugs error raised.
  private MutableQuantiles[] grpcProcessingTimeMillisQuantiles;

  @Metric("Number of active clients connected")
  private MutableGaugeInt numOpenClientConnections;

  public void setSentBytes(long byteCount) {
    sentBytes.set(byteCount);
  }

  public void setReceivedBytes(long byteCount) {
    receivedBytes.set(byteCount);
  }

  public void addGrpcQueueTime(int queueTime) {
    grpcQueueTime.add(queueTime);
    if (grpcQuantileEnable) {
      for (MutableQuantiles q : grpcQueueTimeMillisQuantiles) {
        if (q != null) {
          q.add(queueTime);
        }
      }
    }
  }

  public void addGrpcProcessingTime(int processingTime) {
    grpcProcessingTime.add(processingTime);
    if (grpcQuantileEnable) {
      for (MutableQuantiles q : grpcProcessingTimeMillisQuantiles) {
        if (q != null) {
          q.add(processingTime);
        }
      }
    }
  }

  public void setNumOpenClientConnections(int activeClients) {
    numOpenClientConnections.set(activeClients);
  }

  public MutableGaugeLong getSentBytes() {
    return sentBytes;
  }

  public MutableGaugeLong getReceivedBytes() {
    return receivedBytes;
  }

  public MutableRate getGrpcQueueTime() {
    return grpcQueueTime;
  }

  public MutableRate getGrpcProcessingTime() {
    return grpcProcessingTime;
  }

  public MutableGaugeInt getNumActiveClientConnections() {
    return numOpenClientConnections;
  }
}