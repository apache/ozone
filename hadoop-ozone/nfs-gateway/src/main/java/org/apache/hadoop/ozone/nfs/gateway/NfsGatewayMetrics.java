/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.nfs.gateway;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.OzoneConsts;

/** Metrics for NFS backend RPC volume, latency, and errors. */
@Metrics(about = "NFS Gateway RPC Metrics", context = OzoneConsts.OZONE)
public final class NfsGatewayMetrics implements MetricsSource {
  public static final String SOURCE_NAME = NfsGatewayMetrics.class.getSimpleName();

  private static final List<String> OPERATIONS = List.of("Capabilities", "Getattr", "Lookup", "Create", "Mkdir",
      "Remove", "Rename", "Readdir", "Read", "Write", "Commit");
  private static int users;

  private final MetricsRegistry registry = new MetricsRegistry(SOURCE_NAME);
  private final Map<String, OperationMetrics> operations = new LinkedHashMap<>();

  private NfsGatewayMetrics() {
    for (String operation : OPERATIONS) {
      operations.put(operation, new OperationMetrics(operation));
    }
  }

  public static synchronized NfsGatewayMetrics create() {
    MetricsSystem metricsSystem = DefaultMetricsSystem.instance();
    MetricsSource registered = metricsSystem.getSource(SOURCE_NAME);
    if (registered instanceof NfsGatewayMetrics) {
      users++;
      return (NfsGatewayMetrics) registered;
    }
    NfsGatewayMetrics metrics = metricsSystem.register(SOURCE_NAME, "NFS Gateway backend RPC metrics",
        new NfsGatewayMetrics());
    users = 1;
    return metrics;
  }

  public static synchronized void unregister() {
    if (users > 0 && --users == 0) {
      DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
    }
  }

  void recordRequest(String operation) {
    OperationMetrics metrics = operations.get(operation);
    if (metrics != null) {
      metrics.requests.incr();
    }
  }

  void recordNfsStatus(String operation, int status) {
    OperationMetrics metrics = operations.get(operation);
    if (metrics != null && status != 0) {
      metrics.nfsErrors.incr();
    }
  }

  void recordCompletion(String operation, long latencyNanos, boolean rpcError) {
    OperationMetrics metrics = operations.get(operation);
    if (metrics != null) {
      metrics.latency.add(latencyNanos);
      if (rpcError) {
        metrics.rpcErrors.incr();
      }
    }
  }

  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(SOURCE_NAME), all);
  }

  private final class OperationMetrics {
    private final MutableCounterLong requests;
    private final MutableCounterLong nfsErrors;
    private final MutableCounterLong rpcErrors;
    private final MutableRate latency;

    private OperationMetrics(String operation) {
      requests = registry.newCounter(operation + "Requests", "Number of " + operation + " RPCs", 0L);
      nfsErrors = registry.newCounter(operation + "NfsErrors", "NFS error responses for " + operation, 0L);
      rpcErrors = registry.newCounter(operation + "RpcErrors", "gRPC failures for " + operation, 0L);
      latency = registry.newRate(operation + "LatencyNs", operation + " RPC latency in nanoseconds");
    }
  }
}
