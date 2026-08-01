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

package org.apache.hadoop.hdds.utils;

import java.lang.management.ManagementFactory;
import java.util.List;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBufAllocatorMetricProvider;

/**
 * This class emits Netty metrics.
 */
public final class NettyMetrics implements MetricsSource {

  public static final String SOURCE_NAME = NettyMetrics.class.getSimpleName();

  private static final String MAX_DIRECT_MEMORY_FLAG =
      "-XX:MaxDirectMemorySize=";

  // JVM arguments are fixed for the process lifetime, so resolve the maximum
  // direct memory once instead of on every getMetrics() call.
  private static final long MAX_DIRECT_MEMORY = maxDirectMemory();

  public static NettyMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    NettyMetrics metrics = new NettyMetrics();
    return ms.register(SOURCE_NAME, "Netty metrics", metrics);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME)
        .setContext("Netty metrics");
    recordBuilder
        .addGauge(MetricsInfos.USED_DIRECT_MEM, usedDirectMemory())
        .addGauge(MetricsInfos.MAX_DIRECT_MEM, MAX_DIRECT_MEMORY);
  }

  public void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  /**
   * Direct memory currently used by the default Netty {@link ByteBufAllocator}
   * (the allocator Ratis and gRPC use). The previous implementation reported
   * Netty's process-wide direct memory counter; this reports the default
   * allocator's usage, which is close but not necessarily identical.
   *
   * @return used direct memory in bytes, or -1 if it cannot be determined
   */
  static long usedDirectMemory() {
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    if (allocator instanceof ByteBufAllocatorMetricProvider) {
      return ((ByteBufAllocatorMetricProvider) allocator).metric()
          .usedDirectMemory();
    }
    return -1L;
  }

  /**
   * Resolve the maximum direct memory using only public API, mirroring Netty's
   * resolution order: the netty {@code maxDirectMemory} system property, then
   * the {@code -XX:MaxDirectMemorySize} JVM flag, then the maximum heap size.
   * The JVM flag is read from the runtime input arguments (command line or
   * {@code JAVA_TOOL_OPTIONS}); a value set by other means falls back to the
   * maximum heap size.
   * <p>
   * The property is the shaded ({@code ratis-thirdparty}) one, since
   * {@link #usedDirectMemory()} reads usage from the shaded allocator; using the
   * unshaded {@code io.netty.maxDirectMemory} would mismatch the two.
   *
   * @return maximum direct memory in bytes
   */
  static long maxDirectMemory() {
    return resolveMaxDirectMemory(
        Long.getLong("org.apache.ratis.thirdparty.io.netty.maxDirectMemory", -1L),
        ManagementFactory.getRuntimeMXBean().getInputArguments(),
        Runtime.getRuntime().maxMemory());
  }

  static long resolveMaxDirectMemory(long nettyProperty, List<String> jvmArgs,
      long maxHeap) {
    if (nettyProperty >= 0) {
      return nettyProperty;
    }
    for (String arg : jvmArgs) {
      if (arg.startsWith(MAX_DIRECT_MEMORY_FLAG)) {
        long parsed =
            parseSize(arg.substring(MAX_DIRECT_MEMORY_FLAG.length()));
        if (parsed > 0) {
          return parsed;
        }
      }
    }
    return maxHeap;
  }

  /**
   * Parse a JVM memory size such as {@code "512m"}, {@code "1g"} or
   * {@code "1073741824"} (1024-based units).
   *
   * @return the size in bytes, or -1 if the value cannot be parsed
   */
  static long parseSize(String value) {
    String size = value.trim();
    if (size.isEmpty()) {
      return -1L;
    }
    long unit = 1L;
    switch (Character.toLowerCase(size.charAt(size.length() - 1))) {
    case 'k':
      unit = 1L << 10;
      break;
    case 'm':
      unit = 1L << 20;
      break;
    case 'g':
      unit = 1L << 30;
      break;
    case 't':
      unit = 1L << 40;
      break;
    default:
      break;
    }
    String digits = unit == 1L ? size : size.substring(0, size.length() - 1);
    try {
      long number = Long.parseLong(digits.trim());
      return number < 0 ? -1L : number * unit;
    } catch (NumberFormatException e) {
      return -1L;
    }
  }

  private enum MetricsInfos implements MetricsInfo {
    USED_DIRECT_MEM("Used direct memory."),
    MAX_DIRECT_MEM("Max direct memory.");

    private final String desc;

    MetricsInfos(String desc) {
      this.desc = desc;
    }

    @Override
    public String description() {
      return desc;
    }
  }
}
