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
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport.HealthState;
import org.apache.hadoop.hdds.scm.container.replication.NullReplicationQueue;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationQueue;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2.UnhealthyContainerRecordV2;
import org.apache.ozone.recon.schema.ContainerSchemaDefinitionV2.UnHealthyContainerStates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon-specific extension of SCM's ReplicationManager.
 *
 * <p><b>Key Differences from SCM:</b></p>
 * <ol>
 *   <li>Uses NullContainerReplicaPendingOps stub (no pending operations tracking)</li>
 *   <li>Overrides processAll() to capture ALL container health states (no 100-sample limit)</li>
 *   <li>Stores results in Recon's UNHEALTHY_CONTAINERS_V2 table</li>
 *   <li>Does not issue replication commands (read-only monitoring)</li>
 * </ol>
 *
 * <p><b>Why This Works Without PendingOps:</b></p>
 * <p>SCM's health check logic uses a two-phase approach:
 * <ul>
 *   <li><b>Phase 1 (Health Determination):</b> Calls isSufficientlyReplicated(false)
 *       which ignores pending operations. This phase determines the health state.</li>
 *   <li><b>Phase 2 (Command Deduplication):</b> Calls isSufficientlyReplicated(true)
 *       which considers pending operations. This phase decides whether to enqueue
 *       new commands.</li>
 * </ul>
 * Since Recon only needs Phase 1 (health determination) and doesn't issue commands,
 * the stub PendingOps does not cause false positives.</p>
 *
 * @see NullContainerReplicaPendingOps
 * @see ReconReplicationManagerReport
 */
public class ReconReplicationManager extends ReplicationManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconReplicationManager.class);

  private final ContainerHealthSchemaManagerV2 healthSchemaManager;
  private final ContainerManager containerManager;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public ReconReplicationManager(
      ReplicationManagerConfiguration rmConf,
      ConfigurationSource conf,
      ContainerManager containerManager,
      PlacementPolicy ratisContainerPlacement,
      PlacementPolicy ecContainerPlacement,
      EventPublisher eventPublisher,
      SCMContext scmContext,
      NodeManager nodeManager,
      Clock clock,
      ContainerHealthSchemaManagerV2 healthSchemaManager) throws IOException {

    // Call parent with stub PendingOps (proven to not cause false positives)
    super(
        rmConf,
        conf,
        containerManager,
        ratisContainerPlacement,
        ecContainerPlacement,
        eventPublisher,
        scmContext,
        nodeManager,
        clock,
        new NullContainerReplicaPendingOps(clock, rmConf)
    );

    this.containerManager = containerManager;
    this.healthSchemaManager = healthSchemaManager;
  }

  /**
   * Override start() to prevent background threads from running.
   *
   * <p>In Recon, we don't want the ReplicationManager's background threads
   * (replicationMonitor, underReplicatedProcessor, overReplicatedProcessor)
   * to run continuously. Instead, we call processAll() manually from
   * ContainerHealthTaskV2 on a schedule.</p>
   *
   * <p>This prevents:
   * <ul>
   *   <li>Unnecessary CPU usage from continuous monitoring</li>
   *   <li>Initialization race conditions (start() being called before fields are initialized)</li>
   *   <li>Replication commands being generated (Recon is read-only)</li>
   * </ul>
   * </p>
   */
  @Override
  public synchronized void start() {
    LOG.info("ReconReplicationManager.start() called - no-op (manual invocation via processAll())");
    // Do nothing - we call processAll() manually from ContainerHealthTaskV2
  }

  /**
   * Override processAll() to capture ALL per-container health states,
   * not just aggregate counts and 100 samples.
   *
   * <p><b>Processing Flow:</b></p>
   * <ol>
   *   <li>Get all containers from ContainerManager</li>
   *   <li>Process each container using inherited health check chain</li>
   *   <li>Capture ALL unhealthy container IDs per health state (no sampling limit)</li>
   *   <li>Store results in Recon's UNHEALTHY_CONTAINERS_V2 table</li>
   * </ol>
   *
   * <p><b>Differences from SCM's processAll():</b></p>
   * <ul>
   *   <li>Uses ReconReplicationManagerReport (captures all containers)</li>
   *   <li>Uses NullReplicationQueue (doesn't enqueue commands)</li>
   *   <li>Stores results in database instead of just keeping in-memory report</li>
   * </ul>
   */
  @Override
  public synchronized void processAll() {
    LOG.info("ReconReplicationManager starting container health check");

    final long startTime = System.currentTimeMillis();

    // Use extended report that captures ALL containers, not just 100 samples
    final ReconReplicationManagerReport report = new ReconReplicationManagerReport();
    final ReplicationQueue nullQueue = new NullReplicationQueue();

    // Get all containers (same as parent)
    final List<ContainerInfo> containers = containerManager.getContainers();

    LOG.info("Processing {} containers", containers.size());

    // Process each container (reuses inherited processContainer and health check chain)
    int processedCount = 0;
    for (ContainerInfo container : containers) {
      report.increment(container.getState());
      try {
        // Call inherited processContainer - this runs the health check chain
        // readOnly=true ensures no commands are generated
        processContainer(container, nullQueue, report, true);
        processedCount++;

        if (processedCount % 10000 == 0) {
          LOG.info("Processed {}/{} containers", processedCount, containers.size());
        }
      } catch (ContainerNotFoundException e) {
        LOG.error("Container {} not found", container.getContainerID(), e);
      }
    }

    report.setComplete();

    // Store ALL per-container health states to database
    storeHealthStatesToDatabase(report, containers);

    long duration = System.currentTimeMillis() - startTime;
    LOG.info("ReconReplicationManager completed in {}ms for {} containers",
        duration, containers.size());
  }

  /**
   * Convert ReconReplicationManagerReport to database records and store.
   * This captures all unhealthy containers with detailed replica counts.
   *
   * @param report The report with all captured container health states
   * @param allContainers List of all containers for cleanup
   */
  private void storeHealthStatesToDatabase(
      ReconReplicationManagerReport report,
      List<ContainerInfo> allContainers) {

    long currentTime = System.currentTimeMillis();
    List<UnhealthyContainerRecordV2> recordsToInsert = new ArrayList<>();
    List<Long> containerIdsToDelete = new ArrayList<>();

    // Get all containers per health state (not just 100 samples)
    Map<HealthState, List<ContainerID>> containersByState =
        report.getAllContainersByState();

    LOG.info("Processing health states: MISSING={}, UNDER_REPLICATED={}, " +
            "OVER_REPLICATED={}, MIS_REPLICATED={}",
        report.getAllContainersCount(HealthState.MISSING),
        report.getAllContainersCount(HealthState.UNDER_REPLICATED),
        report.getAllContainersCount(HealthState.OVER_REPLICATED),
        report.getAllContainersCount(HealthState.MIS_REPLICATED));

    // Process MISSING containers
    List<ContainerID> missingContainers =
        containersByState.getOrDefault(HealthState.MISSING, Collections.emptyList());
    for (ContainerID cid : missingContainers) {
      try {
        ContainerInfo container = containerManager.getContainer(cid);
        int expected = container.getReplicationConfig().getRequiredNodes();
        recordsToInsert.add(createRecord(container,
            UnHealthyContainerStates.MISSING, currentTime, expected, 0,
            "No replicas available"));
      } catch (ContainerNotFoundException e) {
        LOG.warn("Container {} not found when processing MISSING state", cid);
      }
    }

    // Process UNDER_REPLICATED containers
    List<ContainerID> underRepContainers =
        containersByState.getOrDefault(HealthState.UNDER_REPLICATED, Collections.emptyList());
    for (ContainerID cid : underRepContainers) {
      try {
        ContainerInfo container = containerManager.getContainer(cid);
        Set<ContainerReplica> replicas = containerManager.getContainerReplicas(cid);
        int expected = container.getReplicationConfig().getRequiredNodes();
        int actual = replicas.size();
        recordsToInsert.add(createRecord(container,
            UnHealthyContainerStates.UNDER_REPLICATED, currentTime, expected, actual,
            "Insufficient replicas"));
      } catch (ContainerNotFoundException e) {
        LOG.warn("Container {} not found when processing UNDER_REPLICATED state", cid);
      }
    }

    // Process OVER_REPLICATED containers
    List<ContainerID> overRepContainers =
        containersByState.getOrDefault(HealthState.OVER_REPLICATED, Collections.emptyList());
    for (ContainerID cid : overRepContainers) {
      try {
        ContainerInfo container = containerManager.getContainer(cid);
        Set<ContainerReplica> replicas = containerManager.getContainerReplicas(cid);
        int expected = container.getReplicationConfig().getRequiredNodes();
        int actual = replicas.size();
        recordsToInsert.add(createRecord(container,
            UnHealthyContainerStates.OVER_REPLICATED, currentTime, expected, actual,
            "Excess replicas"));
      } catch (ContainerNotFoundException e) {
        LOG.warn("Container {} not found when processing OVER_REPLICATED state", cid);
      }
    }

    // Process MIS_REPLICATED containers
    List<ContainerID> misRepContainers =
        containersByState.getOrDefault(HealthState.MIS_REPLICATED, Collections.emptyList());
    for (ContainerID cid : misRepContainers) {
      try {
        ContainerInfo container = containerManager.getContainer(cid);
        Set<ContainerReplica> replicas = containerManager.getContainerReplicas(cid);
        int expected = container.getReplicationConfig().getRequiredNodes();
        int actual = replicas.size();
        recordsToInsert.add(createRecord(container,
            UnHealthyContainerStates.MIS_REPLICATED, currentTime, expected, actual,
            "Placement policy violated"));
      } catch (ContainerNotFoundException e) {
        LOG.warn("Container {} not found when processing MIS_REPLICATED state", cid);
      }
    }

    // Collect all container IDs for SCM state deletion
    for (ContainerInfo container : allContainers) {
      containerIdsToDelete.add(container.getContainerID());
    }

    // Batch delete old states, then batch insert new states
    LOG.info("Deleting SCM states for {} containers", containerIdsToDelete.size());
    healthSchemaManager.batchDeleteSCMStatesForContainers(containerIdsToDelete);

    LOG.info("Inserting {} unhealthy container records", recordsToInsert.size());
    healthSchemaManager.insertUnhealthyContainerRecords(recordsToInsert);

    LOG.info("Stored {} MISSING, {} UNDER_REPLICATED, {} OVER_REPLICATED, {} MIS_REPLICATED",
        missingContainers.size(), underRepContainers.size(),
        overRepContainers.size(), misRepContainers.size());
  }

  /**
   * Create an unhealthy container record for database insertion.
   *
   * @param container The container info
   * @param state The health state
   * @param timestamp The timestamp when this state was determined
   * @param expectedReplicaCount Expected number of replicas
   * @param actualReplicaCount Actual number of replicas
   * @param reason Human-readable reason for the health state
   * @return UnhealthyContainerRecordV2 ready for insertion
   */
  private UnhealthyContainerRecordV2 createRecord(
      ContainerInfo container,
      UnHealthyContainerStates state,
      long timestamp,
      int expectedReplicaCount,
      int actualReplicaCount,
      String reason) {
    return new UnhealthyContainerRecordV2(
        container.getContainerID(),
        state.toString(),
        timestamp,
        expectedReplicaCount,
        actualReplicaCount,
        actualReplicaCount - expectedReplicaCount,  // replicaDelta
        reason
    );
  }
}
