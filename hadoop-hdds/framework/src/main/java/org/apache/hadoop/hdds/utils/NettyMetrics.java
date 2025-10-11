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

import static org.apache.ratis.thirdparty.io.netty.util.internal.PlatformDependent.maxDirectMemory;
import static org.apache.ratis.thirdparty.io.netty.util.internal.PlatformDependent.usedDirectMemory;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 * This class emits Netty metrics.
 */
public final class NettyMetrics implements MetricsSource {

  public static final String SOURCE_NAME = NettyMetrics.class.getSimpleName();

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
        .addGauge(MetricsInfos.MAX_DIRECT_MEM, maxDirectMemory());
  }

  public void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
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
