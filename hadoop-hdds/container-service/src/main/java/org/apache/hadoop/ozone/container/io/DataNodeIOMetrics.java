/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.container.io;

import org.apache.hadoop.hdds.annotation.InterfaceAudience.Private;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

@Private
@Metrics(about = "Datanode io metrics", context = "io")
public class DataNodeIOMetrics {
  private final String name;
  private final MetricsSystem ms;

  @Metric
  private MutableGaugeLong ioWaitGauge;

  @Metric
  private MutableGaugeLong systemGauge;

  public DataNodeIOMetrics(String name, MetricsSystem ms) {
    this.name = name;
    this.ms = ms;
  }

  public void setDNIoWait(long ioWait) {
    ioWaitGauge.set(ioWait);
  }

  public void setDNSystem(long system) {
    systemGauge.set(system);
  }

  public static DataNodeIOMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    String name = "DataNodeIOMetrics";
    return ms.register(name, null, new DataNodeIOMetrics(name, ms));
  }

  public void unregister() {
    ms.unregisterSource(name);
  }
}
