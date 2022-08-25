/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 * This class captures the container meta-data scrubber metrics on the
 * data-node.
 **/
@InterfaceAudience.Private
@Metrics(about = "DataNode container data scrubber metrics", context = "dfs")
public final class ContainerMetadataScrubberMetrics
    extends AbstractContainerScannerMetric {

  private ContainerMetadataScrubberMetrics(String name, MetricsSystem ms) {
    super(name, ms);
  }

  public static ContainerMetadataScrubberMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    String name = "ContainerMetadataScrubberMetrics";
    return ms.register(name, null,
        new ContainerMetadataScrubberMetrics(name, ms));
  }
}
