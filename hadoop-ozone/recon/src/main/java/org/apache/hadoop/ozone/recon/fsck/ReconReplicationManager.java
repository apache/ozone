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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.MonitoringReplicationQueue;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationQueue;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager.ContainerStateKey;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager.UnhealthyContainerRecord;
import org.apache.hadoop.util.Time;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon-specific extension of SCM's ReplicationManager.
 *
 * <p><b>Key Differences from SCM:</b></p>
 * <ol>
 *   <li>Uses NoOpsContainerReplicaPendingOps stub (no pending operations tracking)</li>
 *   <li>Overrides processAll() to capture ALL container health states (no 100-sample limit)</li>
 *   <li>Stores results in Recon's UNHEALTHY_CONTAINERS table</li>
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
 * @see NoOpsContainerReplicaPendingOps
 * @see ReconReplicationManagerReport
 */
public class ReconReplicationManager extends ReplicationManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconReplicationManager.class);
  private static final int PERSIST_CHUNK_SIZE = 50_000;

  private final ContainerHealthSchemaManager healthSchemaManager;
  private final ContainerManager containerManager;

  /**
   * Immutable wiring context for ReconReplicationManager initialization.
   */
  public static final class InitContext {
    private final ReplicationManagerConfiguration rmConf;
    private final ConfigurationSource conf;
    private final ContainerManager containerManager;
    private final PlacementPolicy ratisContainerPlacement;
    private final PlacementPolicy ecContainerPlacement;
    private final EventPublisher eventPublisher;
    private final SCMContext scmContext;
    private final NodeManager nodeManager;
    private final Clock clock;

    private InitContext(Builder builder) {
      this.rmConf = builder.rmConf;
      this.conf = builder.conf;
      this.containerManager = builder.containerManager;
      this.ratisContainerPlacement = builder.ratisContainerPlacement;
      this.ecContainerPlacement = builder.ecContainerPlacement;
      this.eventPublisher = builder.eventPublisher;
      this.scmContext = builder.scmContext;
      this.nodeManager = builder.nodeManager;
      this.clock = builder.clock;
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    /**
     * Builder for creating {@link InitContext} instances.
     */
    public static final class Builder {
      private ReplicationManagerConfiguration rmConf;
      private ConfigurationSource conf;
      private ContainerManager containerManager;
      private PlacementPolicy ratisContainerPlacement;
      private PlacementPolicy ecContainerPlacement;
      private EventPublisher eventPublisher;
      private SCMContext scmContext;
      private NodeManager nodeManager;
      private Clock clock;

      private Builder() {
      }

      public Builder setRmConf(ReplicationManagerConfiguration rmConf) {
        this.rmConf = rmConf;
        return this;
      }

      public Builder setConf(ConfigurationSource conf) {
        this.conf = conf;
        return this;
      }

      public Builder setContainerManager(ContainerManager containerManager) {
        this.containerManager = containerManager;
        return this;
      }

      public Builder setRatisContainerPlacement(PlacementPolicy ratisContainerPlacement) {
        this.ratisContainerPlacement = ratisContainerPlacement;
        return this;
      }

      public Builder setEcContainerPlacement(PlacementPolicy ecContainerPlacement) {
        this.ecContainerPlacement = ecContainerPlacement;
        return this;
      }

      public Builder setEventPublisher(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
        return this;
      }

      public Builder setScmContext(SCMContext scmContext) {
        this.scmContext = scmContext;
        return this;
      }

      public Builder setNodeManager(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
        return this;
      }

      public Builder setClock(Clock clock) {
        this.clock = clock;
        return this;
      }

      public InitContext build() {
        return new InitContext(this);
      }
    }
  }

  public ReconReplicationManager(
      InitContext initContext,
      ContainerHealthSchemaManager healthSchemaManager) throws IOException {

    // Call parent with stub PendingOps (proven to not cause false positives)
    super(
        initContext.rmConf,
        initContext.conf,
        initContext.containerManager,
        initContext.ratisContainerPlacement,
        initContext.ecContainerPlacement,
        initContext.eventPublisher,
        initContext.scmContext,
        initContext.nodeManager,
        initContext.clock,
        new NoOpsContainerReplicaPendingOps(initContext.clock, initContext.rmConf)
    );

    this.containerManager = initContext.containerManager;
    this.healthSchemaManager = healthSchemaManager;
  }

  /**
   * Override start() to prevent background threads from running.
   *
   * <p>In Recon, we don't want the ReplicationManager's background threads
   * (replicationMonitor, underReplicatedProcessor, overReplicatedProcessor)
   * to run continuously. Instead, we call processAll() manually from
   * ContainerHealthTask on a schedule.</p>
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
    // Do nothing - we call processAll() manually from ContainerHealthTask
  }

  /**
   * Checks if container replicas have mismatched data checksums.
   * This is a Recon-specific check not done by SCM's ReplicationManager.
   *
   * <p>REPLICA_MISMATCH detection is crucial for identifying:
   * <ul>
   *   <li>Bit rot (silent data corruption)</li>
   *   <li>Failed writes to some replicas</li>
   *   <li>Storage corruption on specific datanodes</li>
   *   <li>Network corruption during replication</li>
   * </ul>
   * </p>
   *
   * <p>This uses checksum mismatch logic:
   * {@code replicas.stream().map(ContainerReplica::getDataChecksum).distinct().count() != 1}
   * </p>
   *
   * @param replicas Set of container replicas to check
   * @return true if replicas have different data checksums
   */
  private boolean hasDataChecksumMismatch(Set<ContainerReplica> replicas) {
    if (replicas == null || replicas.isEmpty()) {
      return false;
    }

    // Count distinct checksums (filter out nulls)
    long distinctChecksums = replicas.stream()
        .map(ContainerReplica::getDataChecksum)
        .filter(Objects::nonNull)
        .distinct()
        .count();

    // More than 1 distinct checksum = data mismatch
    // 0 distinct checksums = all nulls, no mismatch
    return distinctChecksums > 1;
  }

  /**
   * Override processAll() to capture ALL per-container health states,
   * not just aggregate counts and 100 samples.
   *
   * <p><b>Processing Flow:</b></p>
   * <ol>
   *   <li>Get all containers from ContainerManager</li>
   *   <li>Process each container using inherited health check chain (SCM logic)</li>
   *   <li>Additionally check for REPLICA_MISMATCH (Recon-specific)</li>
   *   <li>Capture ALL unhealthy container IDs per health state (no sampling limit)</li>
   *   <li>Store results in Recon's UNHEALTHY_CONTAINERS table</li>
   * </ol>
   *
   * <p><b>Differences from SCM's processAll():</b></p>
   * <ul>
   *   <li>Uses ReconReplicationManagerReport (captures all containers)</li>
   *   <li>Uses MonitoringReplicationQueue (doesn't enqueue commands)</li>
   *   <li>Adds REPLICA_MISMATCH detection (not done by SCM)</li>
   *   <li>Stores results in database instead of just keeping in-memory report</li>
   * </ul>
   */
  @Override
  public synchronized void processAll() {
    LOG.info("ReconReplicationManager starting container health check");

    final long startTime = Time.monotonicNow();

    // Use extended report that captures ALL containers, not just 100 samples
    final ReconReplicationManagerReport report = new ReconReplicationManagerReport();
    final ReplicationQueue nullQueue = new MonitoringReplicationQueue();

    // Get all containers (same as parent)
    final List<ContainerInfo> containers = containerManager.getContainers();

    LOG.info("Processing {} containers", containers.size());

    // Process each container (reuses inherited processContainer and health check chain)
    int processedCount = 0;
    for (ContainerInfo container : containers) {
      report.increment(container.getState());
      try {
        ContainerID cid = container.containerID();
        Set<ContainerReplica> replicas = containerManager.getContainerReplicas(cid);
        List<ContainerReplicaOp> pendingOps = getPendingReplicationOps(cid);

        // Call inherited processContainer - this runs SCM's health check chain
        // readOnly=true ensures no commands are generated
        processContainer(container, replicas, pendingOps, nullQueue, report, true);

        // ADDITIONAL CHECK: Detect REPLICA_MISMATCH (Recon-specific, not in SCM)
        if (hasDataChecksumMismatch(replicas)) {
          report.addReplicaMismatchContainer(cid);
          LOG.debug("Container {} has data checksum mismatch across replicas", cid);
        }

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

    long duration = Time.monotonicNow() - startTime;
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
    ProcessingStats totalStats = new ProcessingStats();
    int totalReplicaMismatchCount = 0;
    Set<ContainerID> missingContainers = unionAsIdSet(report,
        ContainerHealthState.MISSING,
        ContainerHealthState.QUASI_CLOSED_STUCK_MISSING,
        ContainerHealthState.MISSING_UNDER_REPLICATED);
    Set<ContainerID> underReplicatedContainers = unionAsIdSet(report,
        ContainerHealthState.UNDER_REPLICATED,
        ContainerHealthState.UNHEALTHY_UNDER_REPLICATED,
        ContainerHealthState.QUASI_CLOSED_STUCK_UNDER_REPLICATED,
        ContainerHealthState.MISSING_UNDER_REPLICATED);
    Set<ContainerID> overReplicatedContainers = unionAsIdSet(report,
        ContainerHealthState.OVER_REPLICATED,
        ContainerHealthState.UNHEALTHY_OVER_REPLICATED,
        ContainerHealthState.QUASI_CLOSED_STUCK_OVER_REPLICATED);
    Set<ContainerID> misReplicatedContainers =
        asIdSet(report, ContainerHealthState.MIS_REPLICATED);
    logUnmappedScmStates(report);
    Set<ContainerID> replicaMismatchContainers =
        new HashSet<>(report.getReplicaMismatchContainers());

    for (int from = 0; from < allContainers.size(); from += PERSIST_CHUNK_SIZE) {
      int to = Math.min(from + PERSIST_CHUNK_SIZE, allContainers.size());
      List<Long> chunkContainerIds = collectContainerIds(allContainers, from, to);
      Map<ContainerStateKey, Long> existingInStateSinceByContainerAndState =
          healthSchemaManager.getExistingInStateSinceByContainerIds(chunkContainerIds);
      List<ContainerHealthSchemaManager.UnhealthyContainerRecord> recordsToInsert = new ArrayList<>();
      List<Long> existingContainerIdsToDelete =
          collectExistingContainerIds(existingInStateSinceByContainerAndState);
      ProcessingStats chunkStats = new ProcessingStats();
      Set<Long> negativeSizeRecorded = new HashSet<>();

      for (int i = from; i < to; i++) {
        ContainerInfo container = allContainers.get(i);
        ContainerID containerId = container.containerID();
        try {
          if (missingContainers.contains(containerId)) {
            handleMissingContainer(containerId, currentTime,
                existingInStateSinceByContainerAndState, recordsToInsert, chunkStats);
          }
          if (underReplicatedContainers.contains(containerId)) {
            chunkStats.incrementUnderRepCount();
            handleReplicaStateContainer(containerId, currentTime,
                UnHealthyContainerStates.UNDER_REPLICATED,
                existingInStateSinceByContainerAndState, recordsToInsert,
                negativeSizeRecorded, chunkStats);
          }
          if (overReplicatedContainers.contains(containerId)) {
            chunkStats.incrementOverRepCount();
            handleReplicaStateContainer(containerId, currentTime,
                UnHealthyContainerStates.OVER_REPLICATED,
                existingInStateSinceByContainerAndState, recordsToInsert,
                negativeSizeRecorded, chunkStats);
          }
          if (misReplicatedContainers.contains(containerId)) {
            chunkStats.incrementMisRepCount();
            handleReplicaStateContainer(containerId, currentTime,
                UnHealthyContainerStates.MIS_REPLICATED,
                existingInStateSinceByContainerAndState, recordsToInsert,
                negativeSizeRecorded, chunkStats);
          }
          if (replicaMismatchContainers.contains(containerId)) {
            processReplicaMismatchContainer(containerId, currentTime,
                existingInStateSinceByContainerAndState, recordsToInsert);
            totalReplicaMismatchCount++;
          }
        } catch (ContainerNotFoundException e) {
          LOG.warn("Container {} not found when processing unhealthy states",
              containerId, e);
        }
      }

      totalStats.add(chunkStats);
      persistUnhealthyRecords(existingContainerIdsToDelete, recordsToInsert);
    }

    LOG.info("Stored {} MISSING, {} EMPTY_MISSING, {} UNDER_REPLICATED, " +
            "{} OVER_REPLICATED, {} MIS_REPLICATED, {} NEGATIVE_SIZE, " +
            "{} REPLICA_MISMATCH",
        totalStats.missingCount, totalStats.emptyMissingCount, totalStats.underRepCount,
        totalStats.overRepCount, totalStats.misRepCount, totalStats.negativeSizeCount,
        totalReplicaMismatchCount);
  }

  private void handleMissingContainer(
      ContainerID containerId,
      long currentTime,
      Map<ContainerStateKey, Long> existingInStateSinceByContainerAndState,
      List<ContainerHealthSchemaManager.UnhealthyContainerRecord> recordsToInsert,
      ProcessingStats stats) throws ContainerNotFoundException {
    ContainerInfo container = containerManager.getContainer(containerId);
    int expected = container.getReplicationConfig().getRequiredNodes();
    if (isEmptyMissing(container)) {
      stats.incrementEmptyMissingCount();
      recordsToInsert.add(createRecord(container,
          UnHealthyContainerStates.EMPTY_MISSING,
          resolveInStateSince(container.getContainerID(),
              UnHealthyContainerStates.EMPTY_MISSING, currentTime,
              existingInStateSinceByContainerAndState),
          expected, 0,
          "Container has no replicas and no keys"));
      return;
    }

    stats.incrementMissingCount();
    recordsToInsert.add(createRecord(container,
        UnHealthyContainerStates.MISSING,
        resolveInStateSince(container.getContainerID(),
            UnHealthyContainerStates.MISSING, currentTime,
            existingInStateSinceByContainerAndState),
        expected, 0,
        "No replicas available"));
  }

  private void handleReplicaStateContainer(
      ContainerID containerId,
      long currentTime,
      UnHealthyContainerStates targetState,
      Map<ContainerStateKey, Long> existingInStateSinceByContainerAndState,
      List<ContainerHealthSchemaManager.UnhealthyContainerRecord> recordsToInsert,
      Set<Long> negativeSizeRecorded,
      ProcessingStats stats) throws ContainerNotFoundException {
    ContainerInfo container = containerManager.getContainer(containerId);
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(containerId);
    int expected = container.getReplicationConfig().getRequiredNodes();
    int actual = replicas.size();
    recordsToInsert.add(createRecord(container, targetState,
        resolveInStateSince(container.getContainerID(), targetState,
            currentTime, existingInStateSinceByContainerAndState),
        expected, actual, reasonForState(targetState)));
    addNegativeSizeRecordIfNeeded(container, currentTime, actual, recordsToInsert,
        existingInStateSinceByContainerAndState, negativeSizeRecorded, stats);
  }

  private void processReplicaMismatchContainer(
      ContainerID containerId,
      long currentTime,
      Map<ContainerStateKey, Long> existingInStateSinceByContainerAndState,
      List<ContainerHealthSchemaManager.UnhealthyContainerRecord> recordsToInsert) throws ContainerNotFoundException {
    ContainerInfo container = containerManager.getContainer(containerId);
    Set<ContainerReplica> replicas = containerManager.getContainerReplicas(containerId);
    int expected = container.getReplicationConfig().getRequiredNodes();
    int actual = replicas.size();
    recordsToInsert.add(createRecord(container,
        UnHealthyContainerStates.REPLICA_MISMATCH,
        resolveInStateSince(container.getContainerID(),
            UnHealthyContainerStates.REPLICA_MISMATCH, currentTime,
            existingInStateSinceByContainerAndState),
        expected, actual,
        "Data checksum mismatch across replicas"));
  }

  private List<Long> collectContainerIds(List<ContainerInfo> allContainers,
      int fromInclusive, int toExclusive) {
    List<Long> containerIds = new ArrayList<>(toExclusive - fromInclusive);
    for (int i = fromInclusive; i < toExclusive; i++) {
      containerIds.add(allContainers.get(i).getContainerID());
    }
    return containerIds;
  }

  private void persistUnhealthyRecords(
      List<Long> containerIdsToDelete,
      List<ContainerHealthSchemaManager.UnhealthyContainerRecord> recordsToInsert) {
    LOG.info("Replacing unhealthy container records atomically: deleteRowsFor={} containers, insert={}",
        containerIdsToDelete.size(), recordsToInsert.size());
    healthSchemaManager.replaceUnhealthyContainerRecordsAtomically(
        containerIdsToDelete, recordsToInsert);
  }

  private boolean isEmptyMissing(ContainerInfo container) {
    return container.getNumberOfKeys() == 0;
  }

  private boolean isNegativeSize(ContainerInfo container) {
    return container.getUsedBytes() < 0;
  }

  private void addNegativeSizeRecordIfNeeded(
      ContainerInfo container,
      long currentTime,
      int actualReplicaCount,
      List<ContainerHealthSchemaManager.UnhealthyContainerRecord> recordsToInsert,
      Map<ContainerStateKey, Long> existingInStateSinceByContainerAndState,
      Set<Long> negativeSizeRecorded,
      ProcessingStats stats) {
    if (isNegativeSize(container)
        && negativeSizeRecorded.add(container.getContainerID())) {
      int expected = container.getReplicationConfig().getRequiredNodes();
      recordsToInsert.add(createRecord(container,
          UnHealthyContainerStates.NEGATIVE_SIZE,
          resolveInStateSince(container.getContainerID(),
              UnHealthyContainerStates.NEGATIVE_SIZE, currentTime,
              existingInStateSinceByContainerAndState),
          expected, actualReplicaCount,
          "Container reports negative usedBytes"));
      stats.incrementNegativeSizeCount();
    }
  }

  private long resolveInStateSince(long containerId, UnHealthyContainerStates state,
      long currentTime, Map<ContainerStateKey, Long> existingInStateSinceByContainerAndState) {
    Long inStateSince = existingInStateSinceByContainerAndState.get(
        new ContainerStateKey(containerId, state.toString()));
    return inStateSince == null ? currentTime : inStateSince;
  }

  private String reasonForState(UnHealthyContainerStates state) {
    switch (state) {
    case UNDER_REPLICATED:
      return "Insufficient replicas";
    case OVER_REPLICATED:
      return "Excess replicas";
    case MIS_REPLICATED:
      return "Placement policy violated";
    default:
      return null;
    }
  }

  private Set<ContainerID> asIdSet(ReconReplicationManagerReport report,
      ContainerHealthState state) {
    List<ContainerID> containers = report.getAllContainers(state);
    if (containers.isEmpty()) {
      return Collections.emptySet();
    }
    return new HashSet<>(containers);
  }

  private Set<ContainerID> unionAsIdSet(ReconReplicationManagerReport report,
      ContainerHealthState... states) {
    Set<ContainerID> result = null;
    for (ContainerHealthState state : states) {
      List<ContainerID> containers = report.getAllContainers(state);
      if (containers.isEmpty()) {
        continue;
      }
      if (result == null) {
        result = new HashSet<>();
      }
      result.addAll(containers);
    }
    return result == null ? Collections.emptySet() : result;
  }

  private void logUnmappedScmStates(ReconReplicationManagerReport report) {
    for (Map.Entry<ContainerHealthState, List<ContainerID>> entry :
        report.getAllContainersByState().entrySet()) {
      ContainerHealthState state = entry.getKey();
      if (isMappedScmState(state)) {
        continue;
      }
      int count = entry.getValue().size();
      if (count > 0) {
        LOG.warn("SCM state {} has {} containers but is not mapped to "
                + "UNHEALTHY_CONTAINERS allowed states; skipping persistence "
                + "for this state in this run",
            state, count);
      }
    }
  }

  private boolean isMappedScmState(ContainerHealthState state) {
    switch (state) {
    case MISSING:
    case UNDER_REPLICATED:
    case OVER_REPLICATED:
    case MIS_REPLICATED:
    case UNHEALTHY_UNDER_REPLICATED:
    case UNHEALTHY_OVER_REPLICATED:
    case MISSING_UNDER_REPLICATED:
    case QUASI_CLOSED_STUCK_MISSING:
    case QUASI_CLOSED_STUCK_UNDER_REPLICATED:
    case QUASI_CLOSED_STUCK_OVER_REPLICATED:
      return true;
    default:
      return false;
    }
  }

  private List<Long> collectExistingContainerIds(
      Map<ContainerStateKey, Long> existingInStateSinceByContainerAndState) {
    if (existingInStateSinceByContainerAndState.isEmpty()) {
      return Collections.emptyList();
    }
    Set<Long> existingContainerIds = new HashSet<>();
    existingInStateSinceByContainerAndState.keySet()
        .forEach(key -> existingContainerIds.add(key.getContainerId()));
    return new ArrayList<>(existingContainerIds);
  }

  private static final class ProcessingStats {
    private int missingCount;
    private int underRepCount;
    private int overRepCount;
    private int misRepCount;
    private int emptyMissingCount;
    private int negativeSizeCount;

    void incrementMissingCount() {
      missingCount++;
    }

    void incrementUnderRepCount() {
      underRepCount++;
    }

    void incrementOverRepCount() {
      overRepCount++;
    }

    void incrementMisRepCount() {
      misRepCount++;
    }

    void incrementEmptyMissingCount() {
      emptyMissingCount++;
    }

    void incrementNegativeSizeCount() {
      negativeSizeCount++;
    }

    void add(ProcessingStats other) {
      missingCount += other.missingCount;
      underRepCount += other.underRepCount;
      overRepCount += other.overRepCount;
      misRepCount += other.misRepCount;
      emptyMissingCount += other.emptyMissingCount;
      negativeSizeCount += other.negativeSizeCount;
    }
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
   * @return UnhealthyContainerRecord ready for insertion
   */
  private ContainerHealthSchemaManager.UnhealthyContainerRecord createRecord(
      ContainerInfo container,
      UnHealthyContainerStates state,
      long timestamp,
      int expectedReplicaCount,
      int actualReplicaCount,
      String reason) {
    return new UnhealthyContainerRecord(
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
