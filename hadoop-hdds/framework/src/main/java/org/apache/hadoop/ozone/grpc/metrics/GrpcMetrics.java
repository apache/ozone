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

package org.apache.hadoop.ozone.grpc.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which maintains metrics related to using GRPC.
 */
@Metrics(about = "GRPC Metrics", context = OzoneConsts.OZONE)
public class GrpcMetrics implements MetricsSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcMetrics.class);

  private static final MetricsInfo LATEST_REQUEST_TYPE = Interns
      .info(
          "LatestRequestType",
          "Latest type of request for " +
              "which metrics were captured");

  private static final String SOURCE_NAME =
      GrpcMetrics.class.getSimpleName();

  private final MetricsRegistry registry;
  private final boolean grpcQuantileEnable;
  private String requestType;

  @Metric("Number of sent bytes")
  private MutableCounterLong sentBytes;

  @Metric("Number of received bytes")
  private MutableCounterLong receivedBytes;

  @Metric("Number of unknown messages sent")
  private MutableCounterLong unknownMessagesSent;

  @Metric("Number of unknown messages received")
  private MutableCounterLong unknownMessagesReceived;

  @Metric("Queue time")
  private MutableRate grpcQueueTime;

  // There should be no getter method to avoid
  // exposing internal representation. FindBugs error raised.
  private MutableQuantiles[] grpcQueueTimeMillisQuantiles;

  @Metric("Processing time")
  private MutableRate grpcProcessingTime;

  // There should be no getter method to avoid
  // exposing internal representation. FindBugs error raised.
  private MutableQuantiles[] grpcProcessingTimeMillisQuantiles;

  @Metric("Number of active clients connected")
  private MutableCounterLong numOpenClientConnections;

  public GrpcMetrics(Configuration conf) {
    this.registry = new MetricsRegistry("grpc");
    this.requestType = "NoRequest";
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
        grpcQueueTimeMillisQuantiles[i] = registry
            .newQuantiles("grpcQueueTime" + interval
                    + "s", "grpc queue time in millisecond", "ops",
                "latency", interval);
        grpcProcessingTimeMillisQuantiles[i] = registry.newQuantiles(
            "grpcProcessingTime" + interval + "s",
            "grpc processing time in millisecond",
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
    MetricUtil.stop(grpcProcessingTimeMillisQuantiles);
    MetricUtil.stop(grpcQueueTimeMillisQuantiles);
  }

  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(registry.info()), all);
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);
    recordBuilder.tag(LATEST_REQUEST_TYPE, requestType);
  }

  public void incrSentBytes(long byteCount) {
    sentBytes.incr(byteCount);
  }

  public void incrReceivedBytes(long byteCount) {
    receivedBytes.incr(byteCount);
  }

  public void incrUnknownMessagesSent() {
    unknownMessagesSent.incr();
  }

  public void incrUnknownMessagesReceived() {
    unknownMessagesReceived.incr();
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

  public void inrcNumOpenClientConnections() {
    numOpenClientConnections.incr();
  }

  public void decrNumOpenClientConnections() {
    numOpenClientConnections.incr(-1);
  }

  public long getSentBytes() {
    return sentBytes.value();
  }

  public long getReceivedBytes() {
    return receivedBytes.value();
  }

  public long getUnknownMessagesSent() {
    return unknownMessagesSent.value();
  }

  public long getUnknownMessagesReceived() {
    return unknownMessagesReceived.value();
  }
  
  MutableRate getGrpcQueueTime() {
    return grpcQueueTime;
  }

  MutableRate getGrpcProcessingTime() {
    return grpcProcessingTime;
  }

  public long getNumActiveClientConnections() {
    return numOpenClientConnections.value();
  }

  public void setRequestType(String requestType) {
    this.requestType = requestType;
  }

  public String getRequestType() {
    return requestType;
  }
}
