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

package org.apache.hadoop.ozone.container.replication;

import java.util.Map;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
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

  public static final String SOURCE =
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
    MetricsRecordBuilder builder = collector.addRecord(SOURCE);
    builder.addGauge(Interns.info("numInFlightReplications",
        "Total number of pending replications and reconstructions both low "
            + "and normal priority"),
            supervisor.getTotalInFlightReplications())
        .addGauge(Interns.info("numQueuedReplications",
            "Number of replications in queue"),
            supervisor.getReplicationQueuedCount())
        .addGauge(Interns.info("numRequestedReplications",
            "Number of requested replications"),
            supervisor.getReplicationRequestCount())
        .addGauge(Interns.info("numSuccessReplications",
            "Number of successful replications"),
            supervisor.getReplicationSuccessCount())
        .addGauge(Interns.info("numFailureReplications",
            "Number of failure replications"),
            supervisor.getReplicationFailureCount())
        .addGauge(Interns.info("numTimeoutReplications",
            "Number of replication requests timed out before being processed"),
            supervisor.getReplicationTimeoutCount())
        .addGauge(Interns.info("numSkippedReplications",
            "Number of replication requests skipped as the container is "
            + "already present"),
            supervisor.getReplicationSkippedCount())
        .addGauge(Interns.info("maxReplicationStreams", "Maximum number of "
            + "concurrent replication tasks which can run simultaneously"),
            supervisor.getMaxReplicationStreams());

    Map<String, String> metricsMap = ReplicationSupervisor.getMetricsMap();
    if (!metricsMap.isEmpty()) {
      metricsMap.forEach((metricsName, descriptionSegment) -> {
        if (!metricsName.equals("")) {
          builder.addGauge(Interns.info("numRequested" + metricsName,
              "Number of requested " + descriptionSegment),
                  supervisor.getReplicationRequestCount(metricsName))
              .addGauge(Interns.info("numSuccess" + metricsName,
                  "Number of successful " + descriptionSegment),
                  supervisor.getReplicationSuccessCount(metricsName))
              .addGauge(Interns.info("numFailure" + metricsName,
                  "Number of failure " + descriptionSegment),
                  supervisor.getReplicationFailureCount(metricsName))
              .addGauge(Interns.info("numTimeout" + metricsName,
                  "Number of " + descriptionSegment + " timed out before being processed"),
                  supervisor.getReplicationTimeoutCount(metricsName))
              .addGauge(Interns.info("numSkipped" + metricsName,
                  "Number of " + descriptionSegment + " skipped as the container is "
                  + "already present"),
                  supervisor.getReplicationSkippedCount(metricsName))
              .addGauge(Interns.info("numQueued" + metricsName,
                  "Number of " + descriptionSegment + " in queue"),
                  supervisor.getReplicationQueuedCount(metricsName));
        }
      });
    }

    Map<String, Integer> tasks = supervisor.getInFlightReplicationSummary();
    for (Map.Entry<String, Integer> entry : tasks.entrySet()) {
      builder.addGauge(Interns.info("numInflight" + entry.getKey(),
          "Number of normal priority" + entry.getKey() + " tasks pending"),
          entry.getValue());
    }
  }
}
