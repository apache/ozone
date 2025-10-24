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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import javax.inject.Inject;

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

  private final StorageContainerServiceProvider scmClient;
  private final ContainerManager containerManager;
  private final ContainerHealthSchemaManagerV2 schemaManagerV2;
  private final ReconContainerMetadataManager reconContainerMetadataManager;
  private final PlacementPolicy placementPolicy;
  private final OzoneConfiguration conf;
  private final long interval;

  @Inject
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
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.placementPolicy = placementPolicy;
    this.conf = conf;
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
   * Part 1: For each container Recon has, check its health status with SCM.
   * This validates Recon's container superset against SCM's authoritative state.
   * Process containers in batches to avoid OOM.
   */
  private void processReconContainersAgainstSCM(long currentTime)
      throws IOException {

    LOG.info("Starting Recon to SCM container validation (batch processing)");

    int validatedCount = 0;
    int errorCount = 0;
    int batchSize = 100; // Process 100 containers at a time
    long startContainerID = 0;

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
          LOG.info("Container {} not in CLOSED, QUASI_CLOSED or CLOSING state.", container);
          continue;
        }

        try {
          // Ask SCM: What's the health status of this container?
          ReplicationManagerReport report =
              scmClient.checkContainerStatus(container);
          LOG.info("Container {} check status {}", container.getContainerID(), report);

          // Update Recon's V2 table based on SCM's answer
          syncContainerHealthToDatabase(container, report, currentTime);

          validatedCount++;

        } catch (ContainerNotFoundException e) {
          // Container exists in Recon but not in SCM
          LOG.warn("Container {} exists in Recon but not in SCM - removing from V2",
              container.getContainerID());
          schemaManagerV2.deleteAllStatesForContainer(container.getContainerID());
          errorCount++;
        } catch (Exception e) {
          LOG.error("Error checking container {} status with SCM",
              container.getContainerID(), e);
          errorCount++;
        }
      }

      // Move to next batch - start after the last container we saw
      long lastContainerID = batch.get(batch.size() - 1).getContainerID();
      startContainerID = lastContainerID + 1;
    }

    LOG.info("Recon to SCM validation complete: validated={}, errors={}",
        validatedCount, errorCount);
  }

  /**
   * Part 2: Get all CLOSED, QUASI_CLOSED, CLOSING containers from SCM and ensure Recon has them.
   * For containers missing in Recon, check their health status with SCM and update V2 table.
   * This ensures Recon doesn't miss any unhealthy containers that SCM knows about.
   * Process containers in batches to avoid OOM.
   */
  private void processSCMContainersAgainstRecon(long currentTime)
      throws IOException {

    LOG.info("Starting SCM to Recon container synchronization (batch processing)");

    int existsInBoth = 0;
    int missingInRecon = 0;
    int unhealthyInSCM = 0;
    int totalProcessed = 0;
    long startId = 0;
    int batchSize = 1000;

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
          try {
            // Check if Recon already has this container
            containerManager.getContainer(scmContainer.containerID());
            existsInBoth++;
            // Container exists in both - already handled in Part 1

          } catch (ContainerNotFoundException e) {
            // Container exists in SCM but not in Recon
            // Since SCM is the source of truth, check its health status
            LOG.info("Container {} exists in SCM ({}) but not in Recon - checking health status",
                scmContainer.getContainerID(), state);
            missingInRecon++;

            try {
              // Get health status from SCM for this container
              ReplicationManagerReport report =
                  scmClient.checkContainerStatus(scmContainer);
              LOG.info("Container {} (missing in Recon) health status from SCM: {}",
                  scmContainer.getContainerID(), report);

              // Check if this container is unhealthy according to SCM
              boolean isUnhealthy = report.getStat(HealthState.MISSING) > 0 ||
                  report.getStat(HealthState.UNDER_REPLICATED) > 0 ||
                  report.getStat(HealthState.OVER_REPLICATED) > 0 ||
                  report.getStat(HealthState.MIS_REPLICATED) > 0;

              if (isUnhealthy) {
                // Update V2 table with SCM's health status
                // Note: We cannot get replicas from Recon's containerManager since container doesn't exist
                // So we'll use SCM's report directly with placeholder values for replica counts
                List<UnhealthyContainerRecordV2> recordsToInsert = new ArrayList<>();
                int expectedReplicaCount = scmContainer.getReplicationConfig().getRequiredNodes();
                int actualReplicaCount = 0; // Unknown since container not in Recon

                if (report.getStat(HealthState.MISSING) > 0) {
                  recordsToInsert.add(createRecord(scmContainer, UnHealthyContainerStates.MISSING,
                      currentTime, expectedReplicaCount, actualReplicaCount,
                      "Reported by SCM (container not in Recon)"));
                }

                if (report.getStat(HealthState.UNDER_REPLICATED) > 0) {
                  recordsToInsert.add(createRecord(scmContainer, UnHealthyContainerStates.UNDER_REPLICATED,
                      currentTime, expectedReplicaCount, actualReplicaCount,
                      "Reported by SCM (container not in Recon)"));
                }

                if (report.getStat(HealthState.OVER_REPLICATED) > 0) {
                  recordsToInsert.add(createRecord(scmContainer, UnHealthyContainerStates.OVER_REPLICATED,
                      currentTime, expectedReplicaCount, actualReplicaCount,
                      "Reported by SCM (container not in Recon)"));
                }

                if (report.getStat(HealthState.MIS_REPLICATED) > 0) {
                  recordsToInsert.add(createRecord(scmContainer, UnHealthyContainerStates.MIS_REPLICATED,
                      currentTime, expectedReplicaCount, actualReplicaCount,
                      "Reported by SCM (container not in Recon)"));
                }

                if (!recordsToInsert.isEmpty()) {
                  schemaManagerV2.insertUnhealthyContainerRecords(recordsToInsert);
                  unhealthyInSCM++;
                  LOG.info("Updated V2 table with {} unhealthy states for container {} (missing in Recon)",
                      recordsToInsert.size(), scmContainer.getContainerID());
                }
              }

            } catch (Exception ex) {
              LOG.error("Error checking health status for container {} (missing in Recon)",
                  scmContainer.getContainerID(), ex);
            }
          }
        }

        totalProcessed += batch.size();
        startId = batch.get(batch.size() - 1).getContainerID() + 1;
        LOG.debug("SCM to Recon sync processed {} {} containers, next startId: {}",
            totalProcessed, state, startId);
      }

      LOG.info("Completed processing {} containers from SCM", state);
    }

    LOG.info("SCM to Recon sync complete: totalProcessed={}, existsInBoth={}, " +
        "onlyInSCM={}, unhealthyInSCM={}",
        totalProcessed, existsInBoth, missingInRecon, unhealthyInSCM);
  }

  /**
   * Sync container health state to V2 database based on SCM's ReplicationManager report.
   */
  private void syncContainerHealthToDatabase(
      ContainerInfo container,
      ReplicationManagerReport report,
      long currentTime) throws IOException {

    List<UnhealthyContainerRecordV2> recordsToInsert = new ArrayList<>();
    boolean isHealthy = true;

    // Get replicas for building records
    Set<ContainerReplica> replicas =
        containerManager.getContainerReplicas(container.containerID());
    int actualReplicaCount = replicas.size();
    int expectedReplicaCount = container.getReplicationConfig().getRequiredNodes();

    // Check each health state from SCM's report
    if (report.getStat(HealthState.MISSING) > 0) {
      recordsToInsert.add(createRecord(container, UnHealthyContainerStates.MISSING,
          currentTime, expectedReplicaCount, actualReplicaCount, "Reported by SCM"));
      isHealthy = false;
    }

    if (report.getStat(HealthState.UNDER_REPLICATED) > 0) {
      recordsToInsert.add(createRecord(container, UnHealthyContainerStates.UNDER_REPLICATED,
          currentTime, expectedReplicaCount, actualReplicaCount, "Reported by SCM"));
      isHealthy = false;
    }

    if (report.getStat(HealthState.OVER_REPLICATED) > 0) {
      recordsToInsert.add(createRecord(container, UnHealthyContainerStates.OVER_REPLICATED,
          currentTime, expectedReplicaCount, actualReplicaCount, "Reported by SCM"));
      isHealthy = false;
    }

    if (report.getStat(HealthState.MIS_REPLICATED) > 0) {
      recordsToInsert.add(createRecord(container, UnHealthyContainerStates.MIS_REPLICATED,
          currentTime, expectedReplicaCount, actualReplicaCount, "Reported by SCM"));
      isHealthy = false;
    }

    // Insert/update unhealthy records
    if (!recordsToInsert.isEmpty()) {
      schemaManagerV2.insertUnhealthyContainerRecords(recordsToInsert);
    }

    // Check REPLICA_MISMATCH locally (SCM doesn't track data checksums)
    checkAndUpdateReplicaMismatch(container, replicas, currentTime,
        expectedReplicaCount, actualReplicaCount);

    // If healthy according to SCM and no REPLICA_MISMATCH, remove from V2 table
    // (except REPLICA_MISMATCH which is handled separately)
    if (isHealthy) {
      // Remove SCM-tracked states, but keep REPLICA_MISMATCH if it exists
      schemaManagerV2.deleteUnhealthyContainer(container.getContainerID(),
          UnHealthyContainerStates.MISSING);
      schemaManagerV2.deleteUnhealthyContainer(container.getContainerID(),
          UnHealthyContainerStates.UNDER_REPLICATED);
      schemaManagerV2.deleteUnhealthyContainer(container.getContainerID(),
          UnHealthyContainerStates.OVER_REPLICATED);
      schemaManagerV2.deleteUnhealthyContainer(container.getContainerID(),
          UnHealthyContainerStates.MIS_REPLICATED);
    }
  }

  /**
   * Check for REPLICA_MISMATCH locally (SCM doesn't track data checksums).
   * This compares checksums across replicas to detect data inconsistencies.
   */
  private void checkAndUpdateReplicaMismatch(
      ContainerInfo container,
      Set<ContainerReplica> replicas,
      long currentTime,
      int expectedReplicaCount,
      int actualReplicaCount) {

    try {
      // Check if replicas have mismatched checksums
      boolean hasMismatch = hasDataChecksumMismatch(replicas);

      if (hasMismatch) {
        UnhealthyContainerRecordV2 record = createRecord(
            container,
            UnHealthyContainerStates.REPLICA_MISMATCH,
            currentTime,
            expectedReplicaCount,
            actualReplicaCount,
            "Checksum mismatch detected by Recon");

        List<UnhealthyContainerRecordV2> records = new ArrayList<>();
        records.add(record);
        schemaManagerV2.insertUnhealthyContainerRecords(records);
      } else {
        // No mismatch - remove REPLICA_MISMATCH state if it exists
        schemaManagerV2.deleteUnhealthyContainer(container.getContainerID(),
            UnHealthyContainerStates.REPLICA_MISMATCH);
      }

    } catch (Exception e) {
      LOG.warn("Error checking replica mismatch for container {}",
          container.getContainerID(), e);
    }
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
}