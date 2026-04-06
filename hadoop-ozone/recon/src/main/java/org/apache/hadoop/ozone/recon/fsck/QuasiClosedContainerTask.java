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

package org.apache.hadoop.ozone.recon.fsck;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.ozone.recon.persistence.QuasiClosedContainerSchemaManager;
import org.apache.hadoop.ozone.recon.persistence.QuasiClosedContainerSchemaManager.QuasiClosedContainerRecord;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconScmTask;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background task that periodically scans for QUASI_CLOSED containers and
 * persists them to the {@code QUASI_CLOSED_CONTAINERS} table.
 *
 * <p><b>Detection strategy:</b> calls
 * {@link ReconContainerManager#getContainers(LifeCycleState)} with
 * {@code QUASI_CLOSED} directly — O(q) where q is the number of QUASI_CLOSED
 * containers, not the entire cluster. This avoids running the full
 * ReplicationManager handler chain just to extract lifecycle state.
 *
 * <p><b>first_seen_time preservation:</b> the task reads the previous
 * first_seen_time for containers that were already tracked, and keeps that
 * value so operators can see how long a container has been stuck.
 */
public class QuasiClosedContainerTask extends ReconScmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(QuasiClosedContainerTask.class);

  /** Minimum gap between two consecutive task runs regardless of configured interval. */
  private static final long MIN_NEXT_RUN_INTERVAL_MS = 60_000L;

  private final ReconContainerManager containerManager;
  private final QuasiClosedContainerSchemaManager schemaManager;
  private final long intervalMs;

  public QuasiClosedContainerTask(
      ReconContainerManager containerManager,
      QuasiClosedContainerSchemaManager schemaManager,
      ReconTaskConfig reconTaskConfig,
      ReconTaskStatusUpdaterManager taskStatusUpdaterManager) {
    super(taskStatusUpdaterManager);
    this.containerManager = containerManager;
    this.schemaManager = schemaManager;
    // Reuse the missingContainer interval as the default; operators can
    // override via ozone.recon.task.missingcontainer.interval
    this.intervalMs = reconTaskConfig.getMissingContainerTaskInterval().toMillis();
    LOG.info("Initialized QuasiClosedContainerTask with interval={}ms", intervalMs);
  }

  @Override
  protected void run() {
    while (canRun()) {
      long cycleStart = Time.monotonicNow();
      try {
        initializeAndRunTask();
        long elapsed = Time.monotonicNow() - cycleStart;
        long sleepMs = Math.max(MIN_NEXT_RUN_INTERVAL_MS, intervalMs - elapsed);
        Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.info("QuasiClosedContainerTask interrupted");
        break;
      } catch (Exception e) {
        LOG.error("Error in QuasiClosedContainerTask", e);
      }
    }
  }

  @Override
  protected void runTask() throws Exception {
    long start = Time.monotonicNow();
    LOG.info("QuasiClosedContainerTask starting scan");

    // 1. Fetch all QUASI_CLOSED containers from the ContainerManager.
    //    This is O(q) — only containers in QUASI_CLOSED state, not the whole cluster.
    List<ContainerInfo> qcContainers =
        containerManager.getContainers(LifeCycleState.QUASI_CLOSED);

    LOG.info("Found {} containers in QUASI_CLOSED state", qcContainers.size());

    // 2. Collect container IDs for first_seen_time preservation
    List<Long> containerIds = new ArrayList<>(qcContainers.size());
    for (ContainerInfo ci : qcContainers) {
      containerIds.add(ci.getContainerID());
    }

    // 3. Load existing first_seen_times from DB before replace
    Map<Long, Long> existingFirstSeen = schemaManager.getExistingFirstSeenTimes(containerIds);
    long now = Time.now();

    // 4. Build records
    List<QuasiClosedContainerRecord> records = new ArrayList<>(qcContainers.size());
    for (ContainerInfo ci : qcContainers) {
      long containerId = ci.getContainerID();

      int datanodeCount = 0;
      try {
        Set<ContainerReplica> replicas =
            containerManager.getContainerReplicas(ContainerID.valueOf(containerId));
        datanodeCount = replicas.size();
      } catch (IOException e) {
        LOG.warn("Failed to get replicas for container {}", containerId, e);
      }

      long stateEnterTime = ci.getStateEnterTime() != null
          ? ci.getStateEnterTime().toEpochMilli() : now;

      // Preserve original first_seen_time if we already knew about this container
      long firstSeenTime = existingFirstSeen.getOrDefault(containerId, now);

      String pipelineId = ci.getPipelineID() != null
          ? ci.getPipelineID().getId().toString() : null;

      int requiredNodes;
      try {
        requiredNodes = ci.getReplicationConfig().getRequiredNodes();
      } catch (Exception e) {
        requiredNodes = 0;
      }

      records.add(new QuasiClosedContainerRecord(
          containerId,
          pipelineId,
          datanodeCount,
          ci.getNumberOfKeys(),
          ci.getUsedBytes(),
          ci.getReplicationType().name(),
          requiredNodes,
          stateEnterTime,
          firstSeenTime,
          now  // last_scan_time always updated
      ));
    }

    // 5. Atomically replace all records in DB
    schemaManager.replaceAll(records);

    long elapsed = Time.monotonicNow() - start;
    LOG.info("QuasiClosedContainerTask completed: {} containers in {} ms",
        records.size(), elapsed);
  }
}
