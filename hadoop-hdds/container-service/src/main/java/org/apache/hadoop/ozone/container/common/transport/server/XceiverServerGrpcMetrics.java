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

package org.apache.hadoop.ozone.container.common.transport.server;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.util.MetricUtil;
import org.apache.ratis.thirdparty.io.grpc.Attributes;
import org.apache.ratis.thirdparty.io.grpc.ServerTransportFilter;

/**
 * Metrics for the datanode read executor shared by gRPC and domain socket reads,
 * and for gRPC client connections.
 */
@Metrics(about = "Datanode read executor and gRPC client connection metrics", context = OzoneConsts.OZONE)
public final class XceiverServerGrpcMetrics implements MetricsSource {

  public static final String SOURCE_NAME = XceiverServerGrpcMetrics.class.getSimpleName();

  private final ThreadPoolExecutor readExecutor;
  private final AtomicLong activeClientConnections = new AtomicLong();
  private final String sourceName;

  XceiverServerGrpcMetrics(ThreadPoolExecutor readExecutor, String datanodeUuid) {
    this.readExecutor = readExecutor;
    this.sourceName = MetricUtil.getMetricsSourceName(SOURCE_NAME, datanodeUuid);
  }

  void register() {
    DefaultMetricsSystem.instance().register(sourceName,
        "Datanode read executor and gRPC client connection metrics", this);
  }

  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(sourceName);
  }

  ServerTransportFilter newTransportFilter() {
    return new ServerTransportFilter() {
      @Override
      public Attributes transportReady(Attributes transportAttrs) {
        activeClientConnections.incrementAndGet();
        return transportAttrs;
      }

      @Override
      public void transportTerminated(Attributes transportAttrs) {
        if (transportAttrs != null) {
          activeClientConnections.decrementAndGet();
        }
      }
    };
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(sourceName).setContext(OzoneConsts.OZONE);
    builder.addGauge(Interns.info("ActiveReadExecutorThreads",
        "Number of active threads in the shared read executor"), readExecutor.getActiveCount())
        .addGauge(Interns.info("MaxReadExecutorThreads",
            "Maximum number of threads in the shared read executor"), readExecutor.getMaximumPoolSize())
        .addGauge(Interns.info("ReadExecutorQueueLength",
            "Number of requests waiting in the shared read executor queue"), readExecutor.getQueue().size())
        .addGauge(Interns.info("ActiveClientConnections",
            "Number of active gRPC client connections"), activeClientConnections.get());
  }
}
