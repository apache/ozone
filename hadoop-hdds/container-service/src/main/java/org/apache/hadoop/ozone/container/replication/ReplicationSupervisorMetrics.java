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
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinatorTask;

import java.util.Map;

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
            supervisor.getQueueSize())
        .addGauge(Interns.info("numRequestedReplications",
            "Number of requested replications"),
            supervisor.getReplicationRequestCount())
        .addGauge(Interns.info("numRequestedECReconstructions",
            "Number of requested EC reconstructions"),
            supervisor.getReplicationRequestCount(ECReconstructionCoordinatorTask.class))
        .addGauge(Interns.info("numRequestedContainerReplications",
            "Number of requested container replications"),
            supervisor.getReplicationRequestCount(ReplicationTask.class))
        .addGauge(Interns.info("numSuccessReplications",
            "Number of successful replications"),
            supervisor.getReplicationSuccessCount())
        .addGauge(Interns.info("numSuccessECReconstructions",
            "Number of successful EC reconstructions"),
            supervisor.getReplicationSuccessCount(ECReconstructionCoordinatorTask.class))
        .addGauge(Interns.info("numSuccessContainerReplications",
            "Number of successful container replications"),
            supervisor.getReplicationSuccessCount(ReplicationTask.class))
        .addGauge(Interns.info("numFailureReplications",
            "Number of failure replications"),
            supervisor.getReplicationFailureCount())
        .addGauge(Interns.info("numFailureECReconstructions",
            "Number of failure EC reconstructions"),
            supervisor.getReplicationFailureCount(ECReconstructionCoordinatorTask.class))
        .addGauge(Interns.info("numFailureContainerReplications",
            "Number of failure container replications"),
            supervisor.getReplicationFailureCount(ReplicationTask.class))
        .addGauge(Interns.info("numTimeoutReplications",
            "Number of replication requests timed out before being processed"),
            supervisor.getReplicationTimeoutCount())
        .addGauge(Interns.info("numTimeoutECReconstructions",
            "Number of EC reconstructions timed out before being processed"),
            supervisor.getReplicationTimeoutCount(ECReconstructionCoordinatorTask.class))
        .addGauge(Interns.info("numTimeoutContainerReplications",
            "Number of container replications timed out before being processed"),
            supervisor.getReplicationTimeoutCount(ReplicationTask.class))
        .addGauge(Interns.info("numSkippedReplications",
            "Number of replication requests skipped as the container is "
            + "already present"),
            supervisor.getReplicationSkippedCount())
        .addGauge(Interns.info("numSkippedECReconstructions",
            "Number of EC reconstructions skipped as the container is "
            + "already present"),
            supervisor.getReplicationSkippedCount(ECReconstructionCoordinatorTask.class))
        .addGauge(Interns.info("numSkippedContainerReplications",
            "Number of container replications skipped as the container is "
            + "already present"),
            supervisor.getReplicationSkippedCount(ReplicationTask.class))
        .addGauge(Interns.info("maxReplicationStreams", "Maximum number of "
            + "concurrent replication tasks which can run simultaneously"),
            supervisor.getMaxReplicationStreams());

    Map<String, Integer> tasks = supervisor.getInFlightReplicationSummary();
    for (Map.Entry<String, Integer> entry : tasks.entrySet()) {
      builder.addGauge(Interns.info("numInflight" + entry.getKey(),
          "Number of normal priority" + entry.getKey() + " tasks pending"),
          entry.getValue());
    }
  }
}
