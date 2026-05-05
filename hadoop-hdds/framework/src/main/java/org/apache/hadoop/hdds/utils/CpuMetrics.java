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

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Expose the next JMX metrics.
 *  <p>jvm_metrics_cpu_available_processors</p>
 *  <p>jvm_metrics_cpu_system_load</p>
 *  <p>jvm_metrics_cpu_jvm_load</p>
 */
@Metrics(about = "CPU Metrics", context = OzoneConsts.OZONE)
public class CpuMetrics implements MetricsSource {

  public static final String SOURCE = "JvmMetricsCpu";

  private final OperatingSystemMXBean operatingSystemMXBean;

  public CpuMetrics() {
    operatingSystemMXBean =
        (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  }

  public static void create() {
    if (DefaultMetricsSystem.instance().getSource(SOURCE) == null) {
      DefaultMetricsSystem.instance().register(SOURCE,
          "CPU Metrics",
          new CpuMetrics());
    }
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(SOURCE);
    builder.addGauge(Interns.info("jvmLoad",
                "JVM CPU Load"),
            operatingSystemMXBean.getProcessCpuLoad())
        .addGauge(Interns.info("systemLoad",
                "System CPU Load"),
            operatingSystemMXBean.getSystemCpuLoad())
        .addGauge(Interns.info("availableProcessors",
                "Available processors"),
            operatingSystemMXBean.getAvailableProcessors());
  }

}
