/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.replication;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Metrics source to report number of replication tasks.
 */
@InterfaceAudience.Private
@Metrics(about = "Container Replication Supervisor Metrics",
    context = OzoneConsts.OZONE)
public class ReplicationSupervisorMetrics implements MetricsSource {

  private static final String SOURCE =
      ReplicationSupervisorMetrics.class.getSimpleName();
  private final ReplicationSupervisor supervisor;

  public ReplicationSupervisorMetrics(ReplicationSupervisor
      replicationSupervisor) {
    this.supervisor = replicationSupervisor;
  }

  public static ReplicationSupervisorMetrics create(ReplicationSupervisor
      supervisor) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE, "Container Replication Supervisor Metrics",
        new ReplicationSupervisorMetrics(supervisor));
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    collector.addRecord(SOURCE)
        .addGauge(Interns.info("numInFlightReplications",
            "Number of pending replications(including queued replications"),
            supervisor.getInFlightReplications())
        .addGauge(Interns.info("numQueuedReplications",
            "Number of replications in queue"),
            supervisor.getQueueSize())
        .addGauge(Interns.info("numRequestedReplications",
            "Number of requested replications"),
            supervisor.getReplicationRequestCount());
  }
}
