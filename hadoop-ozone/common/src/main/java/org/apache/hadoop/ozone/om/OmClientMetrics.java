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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_CLIENT_METRICS_PERCENTILES_INTERVALS_SECONDS_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_CLIENT_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.util.PerformanceMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process-wide latency of OM calls made by initialized RpcClients, including failed calls and background requests.
 * Each sample measures one transport submission, including transport retries but excluding retries by its caller.
 * These are client-side nanoseconds, not OM server processing time or a count of physical RPC attempts.
 */
@InterfaceAudience.Private
public final class OmClientMetrics implements MetricsSource {
  public static final String SOURCE_NAME = OmClientMetrics.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(OmClientMetrics.class);
  private static OmClientMetrics instance;
  private static int referenceCount;

  private final MetricsRegistry registry = new MetricsRegistry(SOURCE_NAME);
  private final Map<Type, PerformanceMetrics> latencies = new ConcurrentHashMap<>();
  private final int[] intervals;
  private boolean closed;

  private OmClientMetrics(int[] intervals) {
    this.intervals = intervals;
  }

  /** The first active client supplies the percentile intervals until the last client closes. */
  public static synchronized Handle acquireHandle(ConfigurationSource conf) {
    int[] intervals = readIntervals(conf);
    if (instance == null) {
      instance = DefaultMetricsSystem.instance().register(SOURCE_NAME, "OM Client Metrics",
          new OmClientMetrics(intervals));
    } else if (!Arrays.equals(instance.intervals, intervals)) {
      LOG.debug("Using active OM client metrics intervals {} instead of {}",
          Arrays.toString(instance.intervals), Arrays.toString(intervals));
    }
    referenceCount++;
    return new Handle(instance);
  }

  private static int[] readIntervals(ConfigurationSource conf) {
    String configured = conf.getTrimmed(OZONE_OM_CLIENT_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY);
    if (configured != null) {
      if (configured.isEmpty()) {
        return new int[0];
      }
      int[] intervals;
      try {
        intervals = conf.getInts(OZONE_OM_CLIENT_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY);
      } catch (NumberFormatException e) {
        intervals = new int[0];
      }
      if (intervals.length > 0 && Arrays.stream(intervals).allMatch(interval -> interval > 0)
          && Arrays.stream(intervals).distinct().count() == intervals.length) {
        return intervals;
      }
      LOG.warn("Invalid {}: {}. The default interval is {} seconds.",
          OZONE_OM_CLIENT_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY,
          conf.get(OZONE_OM_CLIENT_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY),
          OZONE_OM_CLIENT_METRICS_PERCENTILES_INTERVALS_SECONDS_DEFAULT);
    }
    return new int[]{OZONE_OM_CLIENT_METRICS_PERCENTILES_INTERVALS_SECONDS_DEFAULT};
  }

  private static synchronized void release() {
    referenceCount--;
    if (referenceCount == 0) {
      instance.stop();
      // Do not hold the instance lock while unregistering from the metrics system.
      try {
        DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
      } finally {
        instance = null;
      }
    }
  }

  /** Record a completed transport submission, regardless of its outcome. */
  public void addLatency(Type type, long latencyNanos) {
    PerformanceMetrics metric = latencies.get(type);
    if (metric == null) {
      metric = createLatency(type);
    }
    if (metric != null) {
      metric.add(latencyNanos);
    }
  }

  private synchronized PerformanceMetrics createLatency(Type type) {
    // An in-flight call may finish after its client closes. It must not start a new quantile task after stop().
    if (closed) {
      return null;
    }
    PerformanceMetrics metric = latencies.get(type);
    if (metric == null) {
      metric = new PerformanceMetrics(registry, type + "LatencyNs", "OM " + type + " client latency in nanoseconds",
          "Ops", "Time", intervals);
      latencies.put(type, metric);
    }
    return metric;
  }

  private synchronized void stop() {
    closed = true;
    latencies.values().forEach(PerformanceMetrics::close);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(SOURCE_NAME).setContext(OzoneConsts.OZONE);
    latencies.values().forEach(metric -> metric.snapshot(builder, all));
  }

  /** One client's ownership of the shared metrics source. */
  public static final class Handle implements AutoCloseable {
    private final OmClientMetrics metrics;
    private final AtomicBoolean closed = new AtomicBoolean();

    private Handle(OmClientMetrics metrics) {
      this.metrics = metrics;
    }

    public OmClientMetrics metrics() {
      return metrics;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        release();
      }
    }
  }
}
