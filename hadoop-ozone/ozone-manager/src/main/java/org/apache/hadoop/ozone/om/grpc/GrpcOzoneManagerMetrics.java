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
package org.apache.hadoop.ozone.om.grpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which maintains metrics related to using GRPC with OzoneManager.
 */
@Metrics(about = "GRPC OM Metrics", context = OzoneConsts.OZONE)
public class GrpcOzoneManagerMetrics {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcOzoneManagerMetrics.class);
  private static final String SOURCE_NAME =
      GrpcOzoneManagerMetrics.class.getSimpleName();

  private final GrpcOzoneManagerServer grpcOmServer;
  private final MetricsRegistry registry;
  private final boolean grpcOmQuantileEnable;

  public GrpcOzoneManagerMetrics(GrpcOzoneManagerServer grpcOmServer,
                                 Configuration conf) {
    this.grpcOmServer = grpcOmServer;
    String port = String.valueOf(grpcOmServer.getPort());
    registry = new MetricsRegistry("grpc").tag("port", "gRPC port", port);
    int[] intervals = conf.getInts(
        OMConfigKeys.OZONE_OM_S3_GPRC_METRICS_PERCENTILES_INTERVALS_KEY);
    grpcOmQuantileEnable = (intervals.length > 0) && conf.getBoolean(
        OMConfigKeys.OZONE_OM_S3_GPRC_METRICS_QUANTILE_ENABLED,
        OMConfigKeys.OZONE_OM_S3_GPRC_METRICS_QUANTILE_ENABLED_DEFAULT);
    if (grpcOmQuantileEnable) {
      grpcOmQueueTimeMillisQuantiles =
          new MutableQuantiles[intervals.length];
      grpcOmProcessingTimeMillisQuantiles =
          new MutableQuantiles[intervals.length];
      for (int i = 0; i < intervals.length; i++) {
        int interval = intervals[i];
        grpcOmProcessingTimeMillisQuantiles[i] = registry
            .newQuantiles("grpcOmQueueTime" + interval
                    + "s", "grpc om queue time in milli second", "ops",
            "latency", interval);
        grpcOmProcessingTimeMillisQuantiles[i] = registry.newQuantiles(
            "grpcOmProcessingTime" + interval + "s",
            "grpc om processing time in milli second",
            "ops", "latency", interval);
      }
    }
    LOG.debug("Initialized " + registry);
  }

  /**
   * Create and return GrpcOzoneManagerMetrics instance.
   * @param grpcOmServer
   * @param conf
   * @return GrpcOzoneManagerMetrics
   */
  public static synchronized GrpcOzoneManagerMetrics create(
      GrpcOzoneManagerServer grpcOmServer, Configuration conf) {
    GrpcOzoneManagerMetrics metrics =
        new GrpcOzoneManagerMetrics(grpcOmServer, conf);
    return DefaultMetricsSystem.instance().register(SOURCE_NAME,
        "Metrics for using gRPC with OzoneManager", metrics);
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
  private MutableRate grpcOmQueueTime;

  private MutableQuantiles[] grpcOmQueueTimeMillisQuantiles;

  @Metric("Processsing time")
  private MutableRate grpcOmProcessingTime;

  private MutableQuantiles[] grpcOmProcessingTimeMillisQuantiles;

  @Metric("Number of active client connections")
  private MutableGaugeInt numActiveClientConnections;

  public void setSentBytes(long byteCount) {
    sentBytes.set(byteCount);
  }

  public void setReceivedBytes(long byteCount) {
    receivedBytes.set(byteCount);
  }

  public void addGrpcOmQueueTime(int queueTime) {
    grpcOmQueueTime.add(queueTime);
    if (grpcOmQuantileEnable) {
      for (MutableQuantiles q : grpcOmQueueTimeMillisQuantiles) {
        if (q != null) {
          q.add(queueTime);
        }
      }
    }
  }

  public void addGrpcOmProcessingTime(int processingTime) {
    grpcOmProcessingTime.add(processingTime);
    if (grpcOmQuantileEnable) {
      for (MutableQuantiles q : grpcOmProcessingTimeMillisQuantiles) {
        if (q != null) {
          q.add(processingTime);
        }
      }
    }
  }

  public void setNumActiveClientConnections(int activeClients) {
    numActiveClientConnections.set(activeClients);
  }

  public MutableGaugeLong getSentBytes() {
    return sentBytes;
  }

  public MutableGaugeLong getReceivedBytes() {
    return receivedBytes;
  }

  public MutableRate getGrpcOmQueueTime() {
    return grpcOmQueueTime;
  }

  public MutableQuantiles[] getGrpcOmQueueTimeMillisQuantiles() {
    return grpcOmQueueTimeMillisQuantiles;
  }

  public MutableRate getGrpcOmProcessingTime() {
    return grpcOmProcessingTime;
  }

  public MutableQuantiles[] getGrpcOmProcessingTimeMillisQuantiles() {
    return grpcOmProcessingTimeMillisQuantiles;
  }

  public MutableGaugeInt getNumActiveClientConnections() {
    return numActiveClientConnections;
  }

  public GrpcOzoneManagerServer getGrpcOmServer() {
    return grpcOmServer;
  }
}