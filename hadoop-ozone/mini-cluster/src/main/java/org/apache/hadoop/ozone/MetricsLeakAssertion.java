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

package org.apache.hadoop.ozone;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 * Asserts that no metrics sources remain registered in the
 * {@link DefaultMetricsSystem} after a mini cluster is shut down.
 *
 * <p>Hadoop's {@code MetricsSystemImpl} does not remove registered sources on
 * {@code stop()} or {@code shutdown()}; a metrics class that forgets to call
 * {@code unregisterSource(...)} leaks its registration silently.  This helper
 * inspects the private {@code allSources} map via reflection and fails the
 * test if anything is still registered.
 */
public final class MetricsLeakAssertion {

  private static final String ALL_SOURCES_FIELD = "allSources";

  /**
   * Metrics sources that are known to still be registered after a mini
   * cluster shuts down.  This is a transitional list: the JVM-level
   * singletons (first group) are never unregistered by design, while the rest
   * are genuine per-service leaks that are being burned down in follow-up
   * issues.  Do not add new entries; fix the leak instead and remove the
   * entry here.
   *
   * <p>Matching rules:
   * <ul>
   *   <li>An entry ending in {@code *} is a prefix match, used for sources
   *   whose registered name embeds a random id, an absolute path, or a port
   *   (e.g. {@code RpcActivityForPort15000}).</li>
   *   <li>An entry starting with {@code *} is a suffix match, used for sources
   *   whose registered name embeds a table-specific prefix (e.g.
   *   {@code keyTableCache-1}).</li>
   *   <li>Any other entry matches the source name exactly, or the name with a
   *   numeric suffix the metrics system appends for repeated registrations
   *   (e.g. {@code JvmMetrics-1}).</li>
   * </ul>
   */
  private static final List<String> EXPECTED_LEFTOVER_SOURCES = Arrays.asList(
      // JVM-level singletons, never unregistered by design.
      "JvmMetrics*", // per service, via HddsServerUtil.initializeMetrics
      "JvmMetricsCpu",
      "UgiMetrics", // Hadoop security UGI metrics
      "ManagedRocksObjectMetrics",

      // RPC / HTTP server metrics (name embeds the listening port).
      "RpcActivityForPort*",
      "RpcDetailedActivityForPort*",
      "HttpServer2*",
      "LocalJobRunnerMetrics*",

      // Datanode per-instance metrics.
      "StorageContainerMetrics",
      "ContainerDataScannerMetrics*", // name embeds the volume path
      "ContainerMetadataScannerMetrics",
      "On-demand container scanner metrics",
      "BackgroundVolumeScannerMetrics",
      "VolumeHealthMetrics-*",
      "VolumeIOStats-*", // name embeds the volume path
      "VolumeInfoMetrics-*", // name embeds the volume path
      "CommandHandlerMetrics",
      "HddsDispatcher",
      "ECReconstructionMetrics",
      "ReplicationSupervisorMetrics",
      "ContainerReplicator/push",
      "BlockDeletingService",
      "GrpcMetrics",

      // SCM metrics.
      "SCMNodeMetrics",
      "SCMContainerManagerMetrics",
      "SCMContainerMetrics",
      "SCMMetrics",
      "SafeModeMetrics",
      "ContainerBalancerMetrics",
      "NodeDecommissionMetrics",
      "SCMDatanodeProtocol",
      "ScmBlockLocationProtocol",
      "ScmContainerLocationProtocol",
      "ScmSecurityProtocol",
      "EventQueue*", // per event/handler pair, registered by SCM EventQueue

      // OM metrics.
      "OMMetrics",
      "OMPerformanceMetrics",
      "OMLockMetrics",
      "OMHAMetrics",
      "OMSnapshotDirectoryMetrics",
      "OmSnapshotInternalMetrics",
      "OmSnapshotMetrics",
      "OmClientProtocol",
      "DeletingServiceMetrics",
      "KeyLifecycleServiceMetrics",
      "BucketUtilizationMetrics",
      "DelegationTokenSecretManagerMetrics",

      // OM / SCM RocksDB table cache and DB metrics (name embeds path/uuid).
      "Rocksdb_*",
      "SSTFilePruningMetrics-*",
      "DBCheckpointMetrics",
      "*TableCache",

      // Ratis / third-party infrastructure metrics.
      "CSMMetricsgroup-*", // Ratis container state machine, embeds a random id
      "CacheMetrics-*", // Hadoop cache metrics (XceiverClientManager, etc.)
      "NettyMetrics*",
      "ContainerClientMetrics1",
      "ReconTaskMetrics",
      "ReconTaskControllerMetrics"
  );

  private MetricsLeakAssertion() {
  }

  /**
   * Throws an {@link AssertionError} if any metrics sources are still
   * registered with the default metrics system, or if the expected
   * {@code allSources} field cannot be found or read (e.g. a Hadoop version
   * change restructured {@code MetricsSystemImpl}), so that a broken or
   * missing check fails loudly instead of going unnoticed.
   */
  public static void assertNoLeaks() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    Field field = findAllSourcesField(ms.getClass());
    if (field == null) {
      throw new AssertionError("Cannot check for metrics leaks: '" + ALL_SOURCES_FIELD +
          "' field not found on " + ms.getClass().getName() +
          ". The metrics system implementation may have changed.");
    }
    final Map<String, MetricsSource> allSources;
    try {
      field.setAccessible(true);
      Object value = field.get(ms);
      if (!(value instanceof Map)) {
        throw new AssertionError("Cannot check for metrics leaks: '" + ALL_SOURCES_FIELD +
            "' on " + ms.getClass().getName() + " is not a Map.");
      }
      @SuppressWarnings("unchecked")
      Map<String, MetricsSource> sources = (Map<String, MetricsSource>) value;
      allSources = sources;
    } catch (IllegalAccessException e) {
      throw new AssertionError("Cannot check for metrics leaks: unable to access '" +
          ALL_SOURCES_FIELD + "' on " + ms.getClass().getName() + ".", e);
    }
    Set<String> leaked = new TreeSet<>(allSources.keySet());
    leaked.removeIf(MetricsLeakAssertion::isExpectedLeftover);
    if (!leaked.isEmpty()) {
      throw new AssertionError("Found " + leaked.size() +
          " metrics source(s) still registered after cluster shutdown: " + leaked);
    }
  }

  private static boolean isExpectedLeftover(String name) {
    for (String entry : EXPECTED_LEFTOVER_SOURCES) {
      if (entry.startsWith("*")) {
        // Suffix match, also tolerating a trailing numeric suffix (-N).
        String suffix = entry.substring(1);
        if (name.endsWith(suffix) || stripNumericSuffix(name).endsWith(suffix)) {
          return true;
        }
      } else if (entry.endsWith("*")) {
        if (name.startsWith(entry.substring(0, entry.length() - 1))) {
          return true;
        }
      } else if (name.equals(entry) || name.startsWith(entry + "-")) {
        return true;
      }
    }
    return false;
  }

  private static String stripNumericSuffix(String name) {
    int idx = name.lastIndexOf('-');
    if (idx > 0 && idx < name.length() - 1) {
      String tail = name.substring(idx + 1);
      boolean numeric = true;
      for (int i = 0; i < tail.length(); i++) {
        if (!Character.isDigit(tail.charAt(i))) {
          numeric = false;
          break;
        }
      }
      if (numeric) {
        return name.substring(0, idx);
      }
    }
    return name;
  }

  private static Field findAllSourcesField(Class<?> clazz) {
    Class<?> current = clazz;
    while (current != null) {
      try {
        return current.getDeclaredField(ALL_SOURCES_FIELD);
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      }
    }
    return null;
  }
}
