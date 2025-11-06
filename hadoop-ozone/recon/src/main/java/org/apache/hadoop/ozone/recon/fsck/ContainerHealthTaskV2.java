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
import java.util.Set;
import javax.inject.Inject;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport.HealthState;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2.UnhealthyContainerRecordV2;
import org.apache.hadoop.ozone.recon.scm.ReconScmTask;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.hadoop.util.Time;
import org.apache.ozone.recon.schema.ContainerSchemaDefinitionV2.UnHealthyContainerStates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V2 implementation of Container Health Task that uses SCM's ReplicationManager
 * as the single source of truth for container health status.
 *
 * This is an independent task (does NOT extend ContainerHealthTask) that:
 * 1. Uses UNHEALTHY_CONTAINERS_V2 table for storage
 * 2. Queries SCM for authoritative health status per container
 * 3. Performs two-way synchronization:
 *    a) Validates Recon's containers against SCM
 *    b) Ensures Recon has all containers that SCM knows about
 * 4. Implements REPLICA_MISMATCH detection locally (SCM doesn't track checksums)
 */
public class ContainerHealthTaskV2 extends ReconScmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerHealthTaskV2.class);

  // Batch size for database operations - balance between memory and DB roundtrips
  private static final int DB_BATCH_SIZE = 1000;

  private final StorageContainerServiceProvider scmClient;
  private final ContainerManager containerManager;
  private final ContainerHealthSchemaManagerV2 schemaManagerV2;
  private final long interval;

  @Inject
  @SuppressWarnings("checkstyle:ParameterNumber")
  public ContainerHealthTaskV2(
      ContainerManager containerManager,
      StorageContainerServiceProvider scmClient,
      ContainerHealthSchemaManagerV2 schemaManagerV2,
      PlacementPolicy placementPolicy,
      ReconContainerMetadataManager reconContainerMetadataManager,
      OzoneConfiguration conf,
      ReconTaskConfig reconTaskConfig,
      ReconTaskStatusUpdaterManager taskStatusUpdaterManager) {
    super(taskStatusUpdaterManager);
    this.scmClient = scmClient;
    this.containerManager = containerManager;
    this.schemaManagerV2 = schemaManagerV2;
    this.interval = reconTaskConfig.getMissingContainerTaskInterval().toMillis();
    LOG.info("Initialized ContainerHealthTaskV2 with SCM-based two-way sync, interval={}ms", interval);
  }

  @Override
  protected void run() {
    while (canRun()) {
      try {
        initializeAndRunTask();

        // Wait before next run using configured interval
        synchronized (this) {
          wait(interval);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.info("ContainerHealthTaskV2 interrupted");
        break;
      } catch (Exception e) {
        LOG.error("Error in ContainerHealthTaskV2", e);
      }
    }
  }

  /**
   * Main task execution - performs two-way synchronization with SCM.
   */
  @Override
  protected void runTask() throws Exception {
    LOG.info("ContainerHealthTaskV2 starting - two-way sync with SCM");

    long startTime = Time.monotonicNow();
    long currentTime = System.currentTimeMillis();

    try {
      // Part 1: For each container Recon has, check status with SCM
      processReconContainersAgainstSCM(currentTime);
      LOG.debug("Recon to SCM validation completed in {} ms",
          Time.monotonicNow() - startTime);

      // Part 2: Get all containers from SCM and ensure Recon has them
      startTime = Time.monotonicNow();
      processSCMContainersAgainstRecon(currentTime);
      LOG.debug("SCM to Recon synchronization completed in {} ms",
          Time.monotonicNow() - startTime);

    } catch (IOException e) {
      LOG.error("Failed during ContainerHealthTaskV2 execution", e);
      throw e;
    } catch (Exception e) {
      LOG.error("Unexpected error during ContainerHealthTaskV2 execution", e);
      throw e;
    }

    LOG.info("ContainerHealthTaskV2 completed successfully");
  }

  /**
   * Part 1: For each container Recon has, sync its health status with SCM.
   * This validates Recon's container superset against SCM's authoritative state.
   * Uses batch processing for efficient database operations.
   */
  private void processReconContainersAgainstSCM(long currentTime)
      throws IOException {

    LOG.info("Starting Recon to SCM container validation (batch processing)");

    int syncedCount = 0;
    int errorCount = 0;
    int batchSize = 100; // Process 100 containers at a time from Recon
    long startContainerID = 0;

    // Batch accumulator for DB operations
    BatchOperationAccumulator batchOps = new BatchOperationAccumulator();

    while (true) {
      // Get a batch of containers
      List<ContainerInfo> batch = containerManager.getContainers(
          ContainerID.valueOf(startContainerID), batchSize);

      if (batch.isEmpty()) {
        LOG.info("Containers not found in Recon beyond ID {}", startContainerID);
        break;
      }

      // Process this batch
      for (ContainerInfo container : batch) {
        // Only process CLOSED, QUASI_CLOSED, and CLOSING containers
        HddsProtos.LifeCycleState state = container.getState();
        if (state != HddsProtos.LifeCycleState.CLOSED &&
            state != HddsProtos.LifeCycleState.QUASI_CLOSED &&
            state != HddsProtos.LifeCycleState.CLOSING) {
          LOG.debug("Container {} in state {} - skipping (not CLOSED/QUASI_CLOSED/CLOSING)",
              container.getContainerID(), state);
          continue;
        }

        try {
          // Sync this container's health status with SCM (source of truth)
          // This collects operations instead of executing immediately
          syncContainerWithSCMBatched(container, currentTime, true, batchOps);
          syncedCount++;

          // Execute batch if it reached the threshold
          if (batchOps.shouldFlush()) {
            batchOps.flush();
          }

        } catch (ContainerNotFoundException e) {
          // Container exists in Recon but not in SCM
          LOG.warn("Container {} exists in Recon but not in SCM - removing from V2",
              container.getContainerID());
          schemaManagerV2.deleteAllStatesForContainer(container.getContainerID());
          errorCount++;
        } catch (Exception e) {
          LOG.error("Error syncing container {} with SCM",
              container.getContainerID(), e);
          errorCount++;
        }
      }

      // Move to next batch - start after the last container we saw
      long lastContainerID = batch.get(batch.size() - 1).getContainerID();
      startContainerID = lastContainerID + 1;
    }

    // Flush any remaining operations
    batchOps.flush();

    LOG.info("Recon to SCM validation complete: synced={}, errors={}",
        syncedCount, errorCount);
  }

  /**
   * Part 2: Get all CLOSED, QUASI_CLOSED, CLOSING containers from SCM and sync with V2 table.
   * For all containers (both in Recon and not in Recon), sync their health status with SCM.
   * This ensures Recon doesn't miss any unhealthy containers that SCM knows about.
   * Uses batch processing for efficient database operations.
   */
  private void processSCMContainersAgainstRecon(long currentTime)
      throws IOException {

    LOG.info("Starting SCM to Recon container synchronization (batch processing)");

    int syncedCount = 0;
    int missingInRecon = 0;
    int errorCount = 0;
    int totalProcessed = 0;
    long startId = 0;
    int batchSize = 1000;

    // Batch accumulator for DB operations
    BatchOperationAccumulator batchOps = new BatchOperationAccumulator();

    // Process CLOSED, QUASI_CLOSED, and CLOSING containers from SCM
    HddsProtos.LifeCycleState[] statesToProcess = {
        HddsProtos.LifeCycleState.CLOSED,
        HddsProtos.LifeCycleState.QUASI_CLOSED,
        HddsProtos.LifeCycleState.CLOSING
    };

    for (HddsProtos.LifeCycleState state : statesToProcess) {
      LOG.info("Processing {} containers from SCM", state);
      startId = 0;

      while (true) {
        // Get a batch of containers in this state from SCM
        List<ContainerInfo> batch = scmClient.getListOfContainers(
            startId, batchSize, state);

        if (batch.isEmpty()) {
          break;
        }

        // Process this batch
        for (ContainerInfo scmContainer : batch) {
          totalProcessed++;

          try {
            // Check if Recon has this container
            ContainerInfo reconContainer = null;
            boolean existsInRecon = true;
            try {
              reconContainer = containerManager.getContainer(scmContainer.containerID());
            } catch (ContainerNotFoundException e) {
              existsInRecon = false;
              missingInRecon++;
            }

            // Sync with SCM regardless of whether container exists in Recon
            // This collects operations instead of executing immediately
            if (existsInRecon) {
              // Container exists in Recon - sync using Recon's container info (has replicas)
              syncContainerWithSCMBatched(reconContainer, currentTime, true, batchOps);
            } else {
              // Container missing in Recon - sync using SCM's container info (no replicas available)
              syncContainerWithSCMBatched(scmContainer, currentTime, false, batchOps);
            }

            syncedCount++;

            // Execute batch if it reached the threshold
            if (batchOps.shouldFlush()) {
              batchOps.flush();
            }

          } catch (Exception ex) {
            LOG.error("Error syncing container {} from SCM",
                scmContainer.getContainerID(), ex);
            errorCount++;
          }
        }

        startId = batch.get(batch.size() - 1).getContainerID() + 1;
        LOG.debug("SCM to Recon sync processed {} {} containers, next startId: {}",
            totalProcessed, state, startId);
      }

      LOG.info("Completed processing {} containers from SCM", state);
    }

    // Flush any remaining operations
    batchOps.flush();

    LOG.info("SCM to Recon sync complete: totalProcessed={}, synced={}, " +
        "missingInRecon={}, errors={}",
        totalProcessed, syncedCount, missingInRecon, errorCount);
  }

  /**
   * Check if replicas have mismatched data checksums.
   */
  private boolean hasDataChecksumMismatch(Set<ContainerReplica> replicas) {
    if (replicas == null || replicas.size() <= 1) {
      return false; // Can't have mismatch with 0 or 1 replica
    }

    // Get first checksum as reference
    Long referenceChecksum = null;
    for (ContainerReplica replica : replicas) {
      long checksum = replica.getDataChecksum();
      if (checksum == 0) {
        continue; // Skip replicas without checksum
      }
      if (referenceChecksum == null) {
        referenceChecksum = checksum;
      } else if (referenceChecksum != checksum) {
        return true; // Found mismatch
      }
    }

    return false;
  }

  /**
   * Create an unhealthy container record.
   */
  private UnhealthyContainerRecordV2 createRecord(
      ContainerInfo container,
      UnHealthyContainerStates state,
      long currentTime,
      int expectedReplicaCount,
      int actualReplicaCount,
      String reason) {

    int replicaDelta = actualReplicaCount - expectedReplicaCount;

    return new UnhealthyContainerRecordV2(
        container.getContainerID(),
        state.toString(),
        currentTime,
        expectedReplicaCount,
        actualReplicaCount,
        replicaDelta,
        reason
    );
  }

  /**
   * Batched version of syncContainerWithSCM - collects operations instead of executing immediately.
   */
  private void syncContainerWithSCMBatched(
      ContainerInfo container,
      long currentTime,
      boolean canAccessReplicas,
      BatchOperationAccumulator batchOps) throws IOException {

    // Get SCM's authoritative health status for this container
    ReplicationManagerReport report = scmClient.checkContainerStatus(container);
    LOG.debug("Container {} health status from SCM: {}", container.getContainerID(), report);

    // Collect delete operation for this container's SCM states
    batchOps.addContainerForSCMStateDeletion(container.getContainerID());

    // Collect insert operations based on SCM's report
    if (canAccessReplicas) {
      Set<ContainerReplica> replicas =
          containerManager.getContainerReplicas(container.containerID());
      int actualReplicaCount = replicas.size();
      int expectedReplicaCount = container.getReplicationConfig().getRequiredNodes();

      collectRecordsFromReport(container, report, currentTime,
          expectedReplicaCount, actualReplicaCount, "Reported by SCM", batchOps);

      // Check REPLICA_MISMATCH locally and add to batch
      checkAndUpdateReplicaMismatchBatched(container, replicas, currentTime,
          expectedReplicaCount, actualReplicaCount, batchOps);
    } else {
      int expectedReplicaCount = container.getReplicationConfig().getRequiredNodes();
      int actualReplicaCount = 0;

      collectRecordsFromReport(container, report, currentTime,
          expectedReplicaCount, actualReplicaCount,
          "Reported by SCM (container not in Recon)", batchOps);
    }
  }

  /**
   * Check REPLICA_MISMATCH and collect batch operations (batched version).
   */
  private void checkAndUpdateReplicaMismatchBatched(
      ContainerInfo container,
      Set<ContainerReplica> replicas,
      long currentTime,
      int expectedReplicaCount,
      int actualReplicaCount,
      BatchOperationAccumulator batchOps) {

    try {
      // Check if replicas have mismatched checksums
      boolean hasMismatch = hasDataChecksumMismatch(replicas);

      if (hasMismatch) {
        // Add REPLICA_MISMATCH record to batch
        UnhealthyContainerRecordV2 record = createRecord(
            container,
            UnHealthyContainerStates.REPLICA_MISMATCH,
            currentTime,
            expectedReplicaCount,
            actualReplicaCount,
            "Checksum mismatch detected by Recon");
        batchOps.addRecordForInsertion(record);
      } else {
        // No mismatch - collect delete operation for REPLICA_MISMATCH
        batchOps.addContainerForReplicaMismatchDeletion(container.getContainerID());
      }

    } catch (Exception e) {
      LOG.warn("Error checking replica mismatch for container {}",
          container.getContainerID(), e);
    }
  }

  /**
   * Collect unhealthy container records from SCM's report.
   */
  private void collectRecordsFromReport(
      ContainerInfo container,
      ReplicationManagerReport report,
      long currentTime,
      int expectedReplicaCount,
      int actualReplicaCount,
      String reason,
      BatchOperationAccumulator batchOps) {

    if (report.getStat(HealthState.MISSING) > 0) {
      batchOps.addRecordForInsertion(createRecord(container,
          UnHealthyContainerStates.MISSING, currentTime,
          expectedReplicaCount, actualReplicaCount, reason));
    }

    if (report.getStat(HealthState.UNDER_REPLICATED) > 0) {
      batchOps.addRecordForInsertion(createRecord(container,
          UnHealthyContainerStates.UNDER_REPLICATED, currentTime,
          expectedReplicaCount, actualReplicaCount, reason));
    }

    if (report.getStat(HealthState.OVER_REPLICATED) > 0) {
      batchOps.addRecordForInsertion(createRecord(container,
          UnHealthyContainerStates.OVER_REPLICATED, currentTime,
          expectedReplicaCount, actualReplicaCount, reason));
    }

    if (report.getStat(HealthState.MIS_REPLICATED) > 0) {
      batchOps.addRecordForInsertion(createRecord(container,
          UnHealthyContainerStates.MIS_REPLICATED, currentTime,
          expectedReplicaCount, actualReplicaCount, reason));
    }
  }

  /**
   * Accumulator for batch database operations.
   * Collects delete and insert operations and flushes them when threshold is reached.
   */
  private class BatchOperationAccumulator {
    private final List<Long> containerIdsForSCMStateDeletion;
    private final List<Long> containerIdsForReplicaMismatchDeletion;
    private final List<UnhealthyContainerRecordV2> recordsForInsertion;

    BatchOperationAccumulator() {
      this.containerIdsForSCMStateDeletion = new ArrayList<>(DB_BATCH_SIZE);
      this.containerIdsForReplicaMismatchDeletion = new ArrayList<>(DB_BATCH_SIZE);
      this.recordsForInsertion = new ArrayList<>(DB_BATCH_SIZE);
    }

    void addContainerForSCMStateDeletion(long containerId) {
      containerIdsForSCMStateDeletion.add(containerId);
    }

    void addContainerForReplicaMismatchDeletion(long containerId) {
      containerIdsForReplicaMismatchDeletion.add(containerId);
    }

    void addRecordForInsertion(UnhealthyContainerRecordV2 record) {
      recordsForInsertion.add(record);
    }

    boolean shouldFlush() {
      // Flush when any list reaches batch size
      return containerIdsForSCMStateDeletion.size() >= DB_BATCH_SIZE ||
          containerIdsForReplicaMismatchDeletion.size() >= DB_BATCH_SIZE ||
          recordsForInsertion.size() >= DB_BATCH_SIZE;
    }

    void flush() {
      if (containerIdsForSCMStateDeletion.isEmpty() &&
          containerIdsForReplicaMismatchDeletion.isEmpty() &&
          recordsForInsertion.isEmpty()) {
        return; // Nothing to flush
      }

      try {
        // Execute batch delete for SCM states
        if (!containerIdsForSCMStateDeletion.isEmpty()) {
          schemaManagerV2.batchDeleteSCMStatesForContainers(containerIdsForSCMStateDeletion);
          LOG.info("Batch deleted SCM states for {} containers", containerIdsForSCMStateDeletion.size());
          containerIdsForSCMStateDeletion.clear();
        }

        // Execute batch delete for REPLICA_MISMATCH
        if (!containerIdsForReplicaMismatchDeletion.isEmpty()) {
          schemaManagerV2.batchDeleteReplicaMismatchForContainers(containerIdsForReplicaMismatchDeletion);
          LOG.info("Batch deleted REPLICA_MISMATCH for {} containers",
              containerIdsForReplicaMismatchDeletion.size());
          containerIdsForReplicaMismatchDeletion.clear();
        }

        // Execute batch insert
        if (!recordsForInsertion.isEmpty()) {
          schemaManagerV2.insertUnhealthyContainerRecords(recordsForInsertion);
          LOG.info("Batch inserted {} unhealthy container records", recordsForInsertion.size());
          recordsForInsertion.clear();
        }

      } catch (Exception e) {
        LOG.error("Failed to flush batch operations", e);
        // Clear lists to avoid retrying bad data
        containerIdsForSCMStateDeletion.clear();
        containerIdsForReplicaMismatchDeletion.clear();
        recordsForInsertion.clear();
        throw new RuntimeException("Batch operation failed", e);
      }
    }
  }
}
