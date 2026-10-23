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

package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.EnumMap;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig.MetricsMode;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.util.PerformanceMetrics;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The client metrics for the Storage Container protocol.
 */
@InterfaceAudience.Private
@Metrics(about = "Storage Container Client Metrics", context = "dfs")
public class XceiverClientMetrics implements MetricsSource {
  public static final String SOURCE_NAME = XceiverClientMetrics.class
      .getSimpleName();

  private static final Logger LOG = LoggerFactory.getLogger(XceiverClientMetrics.class);
  /** [instance].[option] after "[prefix].sink.", as Hadoop's MetricsConfig matches sink instances. */
  private static final Pattern SINK_INSTANCE_OPTION = Pattern.compile("[^.*]+\\..+");

  private static volatile MetricsMode metricsMode = MetricsMode.AUTO;

  private @Metric MutableCounterLong pendingOps;
  private @Metric MutableCounterLong totalOps;
  private @Metric MutableCounterLong ecReconstructionTotal;
  private @Metric MutableCounterLong ecReconstructionFailsTotal;
  private EnumMap<ContainerProtos.Type, MutableCounterLong> pendingOpsArray;
  private EnumMap<ContainerProtos.Type, MutableCounterLong> opsArray;
  private EnumMap<ContainerProtos.Type, PerformanceMetrics> containerOpsLatency;

  // TODO: https://issues.apache.org/jira/browse/HDDS-13555
  @SuppressWarnings("PMD.SingularField")
  private MetricsRegistry registry;

  public XceiverClientMetrics() {
    init();
  }

  public void init() {
    OzoneConfiguration conf = new OzoneConfiguration();
    int[] intervals = conf.getInts(OzoneConfigKeys.OZONE_XCEIVER_CLIENT_METRICS_PERCENTILES_INTERVALS_SECONDS_KEY);

    this.registry = new MetricsRegistry(SOURCE_NAME);

    this.pendingOpsArray = new EnumMap<>(ContainerProtos.Type.class);
    this.opsArray = new EnumMap<>(ContainerProtos.Type.class);
    this.containerOpsLatency = new EnumMap<>(ContainerProtos.Type.class);
    for (ContainerProtos.Type type : ContainerProtos.Type.values()) {
      pendingOpsArray.put(type, registry.newCounter("numPending" + type,
          "number of pending" + type + " ops", (long) 0));
      opsArray.put(type, registry.newCounter("opCount" + type,
          "number of" + type + " ops", (long) 0));
      containerOpsLatency.put(type, new PerformanceMetrics(registry,
          type + "Latency", "latency of " + type, "Ops", "Time", intervals));
    }
  }

  /**
   * Set by each {@link XceiverClientCreator} from its configuration; read when the metrics are created.
   */
  public static void setMetricsMode(MetricsMode mode) {
    metricsMode = mode;
  }

  public static XceiverClientMetrics create() {
    if (startsMetricsSystem(metricsMode, classLoader())) {
      DefaultMetricsSystem.initialize(SOURCE_NAME);
    }
    // Registering is cheap without a started metrics system: the source goes live if the system starts later.
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "Storage Container Client Metrics",
        new XceiverClientMetrics());
  }

  static boolean startsMetricsSystem(MetricsMode mode, ClassLoader loader) {
    switch (mode) {
    case ALWAYS:
      return true;
    case NEVER:
      return false;
    default:
      return isSinkConfigured(SOURCE_NAME, loader);
    }
  }

  /**
   * Whether the metrics system would configure a sink for the prefix. Same lookup as Hadoop's MetricsConfig:
   * the first of hadoop-metrics2-[prefix].properties and hadoop-metrics2.properties found on the classpath, with
   * [prefix].sink.[instance].[option] keys; "*.sink" keys only supply defaults. Uses java.util.Properties to avoid
   * loading Commons Configuration.
   */
  static boolean isSinkConfigured(String prefix, ClassLoader loader) {
    final String lowerPrefix = StringUtils.toLowerCase(prefix);
    for (String name : new String[] {"hadoop-metrics2-" + lowerPrefix + ".properties", "hadoop-metrics2.properties"}) {
      final URL url = loader.getResource(name);
      if (url == null) {
        continue;
      }
      final Properties properties = new Properties();
      try (InputStream in = url.openStream()) {
        properties.load(in);
      } catch (IOException | IllegalArgumentException e) {
        LOG.warn("Failed to read {}, starting the metrics system", url, e);
        return true;
      }
      final String sinkPrefix = lowerPrefix + ".sink.";
      return properties.stringPropertyNames().stream()
          .anyMatch(key -> key.startsWith(sinkPrefix)
              && SINK_INSTANCE_OPTION.matcher(key.substring(sinkPrefix.length())).matches());
    }
    return false;
  }

  private static ClassLoader classLoader() {
    final ClassLoader loader = Thread.currentThread().getContextClassLoader();
    return loader != null ? loader : XceiverClientMetrics.class.getClassLoader();
  }

  public void incrPendingContainerOpsMetrics(ContainerProtos.Type type) {
    pendingOps.incr();
    totalOps.incr();
    opsArray.get(type).incr();
    pendingOpsArray.get(type).incr();
  }

  public void decrPendingContainerOpsMetrics(ContainerProtos.Type type) {
    pendingOps.incr(-1);
    pendingOpsArray.get(type).incr(-1);
  }

  public void addContainerOpsLatency(ContainerProtos.Type type,
      long latencyMillis) {
    containerOpsLatency.get(type).add(latencyMillis);
  }

  public long getPendingContainerOpCountMetrics(ContainerProtos.Type type) {
    return pendingOpsArray.get(type).value();
  }

  public void incECReconstructionTotal() {
    ecReconstructionTotal.incr();
  }

  public void incECReconstructionFailsTotal() {
    ecReconstructionFailsTotal.incr();
  }

  @VisibleForTesting
  public long getTotalOpCount() {
    return totalOps.value();
  }

  @VisibleForTesting
  public long getContainerOpCountMetrics(ContainerProtos.Type type) {
    return opsArray.get(type).value();
  }

  @VisibleForTesting
  public void reset() {
    init();
  }

  public void unRegister() {
    IOUtils.closeQuietly(containerOpsLatency.values());
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean b) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);

    pendingOps.snapshot(recordBuilder, true);
    totalOps.snapshot(recordBuilder, true);
    ecReconstructionTotal.snapshot(recordBuilder, true);
    ecReconstructionFailsTotal.snapshot(recordBuilder, true);

    for (ContainerProtos.Type type : ContainerProtos.Type.values()) {
      pendingOpsArray.get(type).snapshot(recordBuilder, b);
      opsArray.get(type).snapshot(recordBuilder, b);
      containerOpsLatency.get(type).snapshot(recordBuilder, b);
    }
  }
}
