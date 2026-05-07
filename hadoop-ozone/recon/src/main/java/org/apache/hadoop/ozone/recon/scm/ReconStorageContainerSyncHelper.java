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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.CLEANUP;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.CLOSE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.DELETE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.FORCE_CLOSE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.QUASI_CLOSE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_THRESHOLD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_PER_STATE_DRIFT_THRESHOLD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_PER_STATE_DRIFT_THRESHOLD_DEFAULT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.recon.metrics.ReconScmContainerSyncMetrics;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that performs targeted incremental sync between SCM and Recon
 * container metadata. Executes four passes per sync cycle, all completing in
 * a single cycle with local pagination — no cross-cycle cursor state except
 * for Pass 2 (see below):
 *
 * <ol>
 *   <li><b>Pass 1 — CLOSED (SCM-driven, add + correct):</b> paginates SCM's
 *       CLOSED container ID list; for each page, one batch RPC adds all absent
 *       containers, and local state-machine transitions correct containers stuck
 *       in OPEN, CLOSING, or QUASI_CLOSED in Recon but already CLOSED in SCM.</li>
 *   <li><b>Pass 2 — OPEN (SCM-driven, add only, incremental cursor):</b> scans
 *       only newly created OPEN containers starting from the last-seen ID
 *       ({@code pass2OpenStartContainerId}). OPEN container IDs are monotonic so
 *       rescanning from 1 every cycle would be wasteful; the cursor advances
 *       forward only and never needs to go backward.</li>
 *   <li><b>Pass 3 — QUASI_CLOSED (SCM-driven, add + correct):</b> paginates SCM's
 *       QUASI_CLOSED list; adds absent containers (fast path via pipeline-capable
 *       batch RPC; fallback via {@code addContainerInfoFallback} for containers
 *       with zero viable replicas whose pipeline cannot be resolved) and corrects
 *       containers stuck OPEN or CLOSING in Recon.</li>
   *   <li><b>Pass 4 — DELETED retirement (SCM-driven, transition only):</b>
   *       paginates SCM's DELETED list using {@code getListOfContainerIDs} (IDs
   *       only, 12 bytes each, no pipeline resolution). For each ID SCM reports
   *       as DELETED, Recon drives the container atomically from
   *       CLOSED/QUASI_CLOSED → DELETING → DELETED in a single call. The
   *       DELETING list is intentionally skipped to avoid leaving Recon in an
   *       intermediate DELETING state across cycles.</li>
 * </ol>
 *
 * <h3>Scalability at 100M containers</h3>
 * <ul>
 *   <li>{@link #decideSyncAction()} uses {@link
 *       org.apache.hadoop.hdds.scm.container.ContainerManager#getTotalContainerCount()}
 *       (O(number of states), all O(1) per-state lookups) — never calls
 *       {@code getContainers()} which would allocate a 20-40 GB
 *       {@code List<ContainerInfo>} on every 6h decision tick.</li>
 *   <li>Pass 1 and Pass 3 issue <b>one</b> {@code getExistContainerWithPipelinesInBatch}
 *       RPC per sub-batch of absent containers — not one per absent container.
 *       Sub-batch size is bounded by {@link #safeContainerWithPipelineBatchSize}
 *       to keep the CWP response within the 128 MB IPC limit.</li>
   *   <li>Pass 4 uses {@code getListOfContainerIDs} (IDs only, 12 bytes/container)
   *       against SCM's DELETED list only. Even 1 billion deleted containers
   *       require only ~100 page calls per cycle at the 128 MB IPC limit.</li>
 * </ul>
 */
class ReconStorageContainerSyncHelper {

  /**
   * Wire size of one {@code ContainerID} proto (varint tag + 8-byte long ≈ 12 bytes).
   * Used to compute the maximum number of IDs that fit in one
   * {@code getListOfContainerIDs} RPC call, where both the request (IDs sent
   * to SCM) and the response (IDs returned by SCM) carry only ContainerID entries.
   * Applies to Pass 1, Pass 2, Pass 3 pagination and Pass 4
   * (DELETED ID list).
   */
  private static final long CONTAINER_ID_PROTO_SIZE_BYTES = 12;

  /**
   * Conservative wire-size upper bound for one {@code ContainerInfo} proto
   * response entry (used by {@code getListOfContainerInfos}).
   *
   * <p>ContainerInfo carries only container metadata — no pipeline, no
   * DatanodeDetails. Measured estimate: containerID(8) + state(2) +
   * usedBytes(8) + numberOfKeys(8) + owner(~20) + replicationType(2) +
   * replicationFactor(2) + stateEnterTime(8) + sequenceId(8) +
   * pipelineID(~20) ≈ 86 bytes. This constant uses <b>128 bytes</b>
   * (~1.5× measured) as a safety margin.
   *
   * <p>Safe max batch for {@code getListOfContainerInfos} at 128 MB IPC limit:
   * <pre>
   *   128 MB / 128 bytes = 1,048,576 containers per call
   *   (actual bytes: 1,048,576 × 86 ≈ 86 MB — well within limit)
   * </pre>
   *
   * <p>This is ~8× larger than {@link #CONTAINER_WITH_PIPELINE_PROTO_SIZE_BYTES}
   * because ContainerInfo does not include the Pipeline and DatanodeDetails
   * entries that dominate the ContainerWithPipeline size.
   */
  private static final long CONTAINER_INFO_PROTO_SIZE_BYTES = 128;

  /**
   * Conservative wire-size upper bound for one {@code ContainerWithPipeline}
   * proto response entry.
   *
   * <p>Measured estimate: ContainerInfoProto ~120 bytes + PipelineProto with 3
   * DatanodeDetailsProto entries ~370 bytes ≈ 490 bytes. This constant uses
   * <b>1024 bytes</b> — approximately 2× the measured value — to provide a
   * comfortable safety margin against larger deployments where hostnames,
   * certificates, or additional port entries grow the proto beyond the estimate.
   *
   * <p>This constant is used exclusively to bound the <em>response</em> of
   * {@code getExistContainerWithPipelinesInBatch}. The <em>request</em> carries
   * only container IDs and is bounded by {@link #CONTAINER_ID_PROTO_SIZE_BYTES}.
   * The two constants are different because the request and response payloads
   * have vastly different sizes (12 bytes vs ~490 bytes per entry).
   *
   * <h3>Safe batch limits at the 128 MB default IPC ceiling</h3>
   * <p>{@code IPC_MAXIMUM_DATA_LENGTH_DEFAULT = 134,217,728 bytes = 128 MB}
   * (verified from Hadoop 3.x {@code CommonConfigurationKeys}).
   * <pre>
   *   Single-state CWP call (Pass 1/3 absent-container adds):
   *     128 MB / 1024 bytes = 131,072 containers per call
   *     (actual bytes: 131,072 × 490 ≈ 61 MB — well within limit)
   * </pre>
   *
   * @see #safeContainerWithPipelineBatchSize(int)
   */
  private static final long CONTAINER_WITH_PIPELINE_PROTO_SIZE_BYTES = 1024;

  /**
   * Monotonic cursor for Pass 2 (OPEN add-only sync). OPEN containers are
   * created with increasing container IDs, so each cycle only needs to scan
   * from the last-seen ID onward rather than rescanning the full OPEN set.
   *
   * <p>{@link AtomicLong} rather than {@code volatile long}: provides the same
   * visibility guarantee but expresses concurrent intent explicitly through the
   * type, following standard Java concurrency conventions. The CAS mutex in
   * {@link ReconStorageContainerManagerFacade} ensures a single writer, so
   * compound-atomic operations ({@code compareAndSet}, {@code getAndAdd}) are
   * not needed — only {@code get()} and {@code set()} are used.
   */
  private final AtomicLong pass2OpenStartContainerId = new AtomicLong(1L);

  private static final Logger LOG = LoggerFactory
      .getLogger(ReconStorageContainerSyncHelper.class);

  private final StorageContainerServiceProvider scmServiceProvider;
  private final OzoneConfiguration ozoneConfiguration;
  private final ReconContainerManager containerManager;
  private final ReconScmContainerSyncMetrics metrics;

  /**
   * Describes the action that the periodic scheduler should take based on the
   * observed drift between SCM and Recon container metadata.
   */
  public enum SyncAction {
    /**
     * No drift detected — no sync work needed this cycle.
     */
    NO_ACTION,

    /**
     * Small or per-state drift detected — run the four-pass targeted sync.
     * This is the normal steady-state response: cheaper than a full snapshot
     * and sufficient for the vast majority of drift scenarios.
     */
    TARGETED_SYNC,

    /**
     * Large non-OPEN drift exceeded the configured threshold. The periodic
     * scheduler records this condition through logs and metrics, but does not
     * automatically download an SCM DB snapshot.
     */
    LARGE_DRIFT_THRESHOLD_EXCEEDED
  }

  ReconStorageContainerSyncHelper(StorageContainerServiceProvider scmServiceProvider,
                                  OzoneConfiguration ozoneConfiguration,
                                  ReconContainerManager containerManager) {
    this(scmServiceProvider, ozoneConfiguration, containerManager, null);
  }

  ReconStorageContainerSyncHelper(StorageContainerServiceProvider scmServiceProvider,
                                  OzoneConfiguration ozoneConfiguration,
                                  ReconContainerManager containerManager,
                                  ReconScmContainerSyncMetrics metrics) {
    this.scmServiceProvider = scmServiceProvider;
    this.ozoneConfiguration = ozoneConfiguration;
    this.containerManager = containerManager;
    this.metrics = metrics;
  }

  /**
   * Decides what sync action the periodic scheduler should take based on the
   * observed drift between SCM and Recon.
   *
   * <p>Decision logic:
   * <ol>
   *   <li>If {@code |(SCM_total - SCM_open) - (Recon_total - Recon_open)| >
   *       ozone.recon.scm.container.threshold} (default 1,000,000): return
   *       {@link SyncAction#LARGE_DRIFT_THRESHOLD_EXCEEDED}. Large drift in
   *       non-OPEN containers is surfaced through logs and metrics instead of
   *       triggering an automatic SCM DB snapshot replacement.</li>
   *   <li>If total drift is positive but the non-OPEN drift is at or below the
   *       threshold: return {@link SyncAction#TARGETED_SYNC}. This keeps large
   *       OPEN-only gaps on the incremental path because missing OPEN
   *       containers can be repaired cheaply without replacing the full SCM DB.</li>
   *   <li>If total drift is zero, check per-state drift for OPEN, QUASI_CLOSED,
   *       and CLOSED against {@code ozone.recon.scm.per.state.drift.threshold}
   *       (default 5):
   *       <ul>
   *         <li><b>OPEN</b>: detects containers stuck OPEN in Recon after SCM
   *             has advanced them to QUASI_CLOSED or CLOSED.</li>
   *         <li><b>QUASI_CLOSED</b>: detects containers stuck QUASI_CLOSED in
   *             Recon after SCM has advanced them to CLOSED.</li>
   *         <li><b>CLOSED</b>: detects CLOSED count mismatch when total counts
   *             are equal (e.g., OPEN and QUASI_CLOSED counts are also equal but
   *             some containers are in the wrong bucket).</li>
   *       </ul>
   *       If drift in <em>any</em> checked state exceeds the threshold:
   *       return {@link SyncAction#TARGETED_SYNC}.</li>
   *   <li>Otherwise: return {@link SyncAction#NO_ACTION}.</li>
   * </ol>
   *
   * <p>Per-state drift deliberately routes to targeted sync, not a full
   * snapshot — the targeted sync's per-state passes correct each condition
   * efficiently without replacing the entire database.
   *
   * @return the recommended {@link SyncAction}
   * @throws IOException if SCM RPC calls to retrieve counts fail
   */
  public SyncAction decideSyncAction() throws IOException {
    int largeThreshold = ozoneConfiguration.getInt(
        OZONE_RECON_SCM_CONTAINER_THRESHOLD,
        OZONE_RECON_SCM_CONTAINER_THRESHOLD_DEFAULT);
    int perStateDriftThreshold = ozoneConfiguration.getInt(
        OZONE_RECON_SCM_PER_STATE_DRIFT_THRESHOLD,
        OZONE_RECON_SCM_PER_STATE_DRIFT_THRESHOLD_DEFAULT);

    // All per-state counts use O(1) getContainerStateCount() — no heap allocation,
    // no container list loading. OPEN and QUASI_CLOSED are read here because they
    // are needed for both the Check 1 non-OPEN drift and Check 2 per-state checks.
    // reconClosed is read later in Check 2 where it is first needed.
    // getTotalContainerCount() sums all LifeCycleState values via the same O(1)
    // per-state lookups.
    long reconOpen = containerManager.getContainerStateCount(HddsProtos.LifeCycleState.OPEN);
    long reconQuasiClosed =
        containerManager.getContainerStateCount(HddsProtos.LifeCycleState.QUASI_CLOSED);
    long reconTotal = containerManager.getTotalContainerCount();

    // --- Check 1: large non-OPEN drift exceeds threshold ---
    long scmTotal = scmServiceProvider.getContainerCount();
    long scmOpen = scmServiceProvider.getContainerCount(HddsProtos.LifeCycleState.OPEN);
    long totalDrift = Math.abs(scmTotal - reconTotal);
    long scmNonOpen = Math.max(0, scmTotal - scmOpen);
    long reconNonOpen = Math.max(0, reconTotal - reconOpen);
    long nonOpenDrift = Math.abs(scmNonOpen - reconNonOpen);

    if (nonOpenDrift > largeThreshold) {
      LOG.warn("Tiered sync decision: LARGE_DRIFT_THRESHOLD_EXCEEDED. "
              + "Non-OPEN container drift {} exceeds threshold {} "
              + "(SCM_non_OPEN={}, Recon_non_OPEN={}, SCM_total={}, Recon_total={}). "
              + "Periodic full SCM DB snapshot download is disabled; "
              + "skipping automatic SCM checkpoint replacement and recording "
              + "large-drift threshold-exceeded event. Check Recon metrics "
              + "fullScmDbSnapshotThresholdExceededCount, "
              + "lastFullScmDbSnapshotThresholdExceededNonOpenDrift, and "
              + "intervalSinceLastFullScmDbSnapshotThresholdExceededMs.",
          nonOpenDrift, largeThreshold, scmNonOpen, reconNonOpen, scmTotal, reconTotal);
      if (metrics != null) {
        metrics.recordFullSnapshotThresholdExceededEvent(nonOpenDrift);
      }
      return SyncAction.LARGE_DRIFT_THRESHOLD_EXCEEDED;
    }
    if (totalDrift > 0) {
      LOG.info("Total container drift {} detected (SCM={}, Recon={}). "
              + "Non-OPEN drift is {} (SCM_non_OPEN={}, Recon_non_OPEN={}), so "
              + "using targeted sync.",
          totalDrift, scmTotal, reconTotal, nonOpenDrift, scmNonOpen, reconNonOpen);
      return SyncAction.TARGETED_SYNC;
    }

    // --- Check 2: per-state drift (total drift = 0, lifecycle state may lag) ---
    //
    // All three counts are read directly via O(1) per-state lookups — no
    // derivation or subtraction. Each is an exact count from the per-state
    // NavigableMap on Recon and from SCM's getContainerStateCount on SCM.
    long scmQuasiClosed =
        scmServiceProvider.getContainerCount(HddsProtos.LifeCycleState.QUASI_CLOSED);
    long scmClosed =
        scmServiceProvider.getContainerCount(HddsProtos.LifeCycleState.CLOSED);
    long reconClosed =
        containerManager.getContainerStateCount(HddsProtos.LifeCycleState.CLOSED);

    for (Object[] entry : new Object[][]{
        {HddsProtos.LifeCycleState.OPEN, scmOpen, reconOpen},
        {HddsProtos.LifeCycleState.QUASI_CLOSED, scmQuasiClosed, reconQuasiClosed},
        {HddsProtos.LifeCycleState.CLOSED, scmClosed, reconClosed}}) {
      HddsProtos.LifeCycleState state = (HddsProtos.LifeCycleState) entry[0];
      long scmCount = (long) entry[1];
      long reconCount = (long) entry[2];
      long drift = Math.abs(scmCount - reconCount);
      if (drift > perStateDriftThreshold) {
        LOG.info("Per-state {} drift {} detected (SCM_{}={}, Recon_{}={}, threshold={}). "
                + "Total counts are equal — targeted sync will correct stale states.",
            state, drift, state, scmCount, state, reconCount, perStateDriftThreshold);
        if (metrics != null) {
          recordPerStateDriftMetric(state, drift);
        }
        return SyncAction.TARGETED_SYNC;
      }
    }

    LOG.info("No significant drift detected (total drift={}). No sync needed.", totalDrift);
    return SyncAction.NO_ACTION;
  }

  private void recordPerStateDriftMetric(HddsProtos.LifeCycleState state,
                                         long drift) {
    switch (state) {
    case OPEN:
      metrics.recordOpenContainerDrift(drift);
      break;
    case QUASI_CLOSED:
      metrics.recordQuasiClosedContainerDrift(drift);
      break;
    case CLOSED:
      metrics.recordClosedContainerDrift(drift);
      break;
    default:
      break;
    }
  }

  /**
   * Runs all four sync passes and returns {@code true} if all passes completed
   * without a fatal error.
   */
  public boolean syncWithSCMContainerInfo() {
    boolean pass1 = syncClosedContainers();
    boolean pass2 = syncOpenContainersIncrementally();
    boolean pass3 = syncQuasiClosedContainers();
    boolean pass4 = retireDeletedContainers();
    return pass1 && pass2 && pass3 && pass4;
  }

  // ---------------------------------------------------------------------------
  // Pass 1: CLOSED containers — add missing, correct stale OPEN/CLOSING/QUASI_CLOSED
  // ---------------------------------------------------------------------------

  /**
   * Fetches SCM's full CLOSED container ID list (paginated) and for each page:
   * <ol>
   *   <li>Splits the page into absent-from-Recon and present-in-Recon sets.</li>
   *   <li>One or more sub-batch RPCs ({@code getExistContainerWithPipelinesInBatch}),
   *       each bounded by {@link #safeContainerWithPipelineBatchSize}, add all
   *       absent containers. This is critical for large clusters where a per-container
   *       RPC would otherwise fire N times for N missing containers per page.</li>
   *   <li>For containers returned: {@code addNewContainer()}.</li>
   *   <li>For containers excluded (pipeline unresolvable): {@code addContainerInfoFallback()}
   *       — targeted, expected near-zero in healthy clusters.</li>
   *   <li>For present containers whose state is stale (OPEN/CLOSING/QUASI_CLOSED):
   *       state-machine corrections using local in-memory transitions only — no SCM RPCs.</li>
   * </ol>
   */
  private boolean syncClosedContainers() {
    try {
      long totalClosed = scmServiceProvider.getContainerCount(
          HddsProtos.LifeCycleState.CLOSED);
      if (totalClosed == 0) {
        LOG.debug("No CLOSED containers found in SCM.");
        return true;
      }

      // Compute batchSize once from totalClosed and reuse across all pages.
      // Calling getContainerCount(CLOSED) on every page would add one extra
      // RPC per page — e.g. 100 extra RPCs for 100M containers at 1M/page.
      // The count is stable enough for the duration of this sync pass.
      int batchSize = (int) getContainerCountPerCall(totalClosed);

      ContainerID startContainerId = ContainerID.valueOf(1);
      long retrieved = 0;
      int addedCount = 0;
      int correctedCount = 0;

      while (true) {
        List<ContainerID> batch = scmServiceProvider.getListOfContainerIDs(
            startContainerId, batchSize, HddsProtos.LifeCycleState.CLOSED);
        if (batch == null || batch.isEmpty()) {
          break; // no more CLOSED containers at or above startContainerId
        }

        LOG.info("Pass 1 (CLOSED): processing batch of {} containers.", batch.size());

        // ── Phase A: batch-add absent containers ──────────────────────────────────
        // Partition the page into absent (need to add) and present (may need
        // state correction). This single pass avoids repeated containerExist()
        // calls in Phase B.
        List<Long> absentIds = new ArrayList<>();
        List<ContainerID> presentIds = new ArrayList<>();
        for (ContainerID containerID : batch) {
          if (!containerManager.containerExist(containerID)) {
            absentIds.add(containerID.getId());
          } else {
            presentIds.add(containerID);
          }
        }

        if (!absentIds.isEmpty()) {
          addedCount += batchedAddMissingContainers(
              absentIds, HddsProtos.LifeCycleState.CLOSED, "Pass 1");
        }

        // ── Phase B: state-correct present containers ─────────────────────────────
        // All operations here are local state-machine transitions (no SCM RPCs).
        // Only stale containers (OPEN/CLOSING/QUASI_CLOSED in Recon but CLOSED in SCM)
        // require action; already-CLOSED containers are no-ops.
        for (ContainerID containerID : presentIds) {
          correctedCount += correctClosedContainerState(containerID);
        }

        long lastID = batch.get(batch.size() - 1).getId();
        startContainerId = ContainerID.valueOf(lastID + 1);
        retrieved += batch.size();
      }

      LOG.info("Pass 1 (CLOSED): sync complete, checked {}, added {}, corrected {}.",
          retrieved, addedCount, correctedCount);
      return true;
    } catch (Exception e) {
      LOG.error("Pass 1 (CLOSED): unexpected error during sync.", e);
      return false;
    }
  }

  /**
   * Corrects the lifecycle state of a container that is present in Recon but
   * whose state lags behind SCM's CLOSED state. All transitions are local
   * in-memory state-machine operations — no SCM RPCs are issued.
   *
   * <p>Valid correction paths:
   * <pre>
   *   OPEN        → CLOSING (FINALIZE)  → CLOSED (CLOSE)   returns 1
   *   CLOSING     → CLOSED  (CLOSE)                        returns 1
   *   QUASI_CLOSED → CLOSED (FORCE_CLOSE)                  returns 1
   *   CLOSED      → no-op                                  returns 0
   * </pre>
   *
   * @return 1 if any correction was applied, 0 if already CLOSED or correction failed
   */
  private int correctClosedContainerState(ContainerID containerID) {
    try {
      ContainerInfo reconContainer = containerManager.getContainer(containerID);
      HddsProtos.LifeCycleState reconState = reconContainer.getState();

      if (reconState == HddsProtos.LifeCycleState.OPEN) {
        LOG.info("Pass 1 (CLOSED): container {} is OPEN in Recon but CLOSED in SCM. "
            + "Correcting state.", containerID);
        // OPEN → CLOSING; also decrements pipelineToOpenContainer counter.
        containerManager.transitionOpenToClosing(containerID, reconContainer);
        reconState = HddsProtos.LifeCycleState.CLOSING;
      }

      if (reconState == HddsProtos.LifeCycleState.CLOSING) {
        containerManager.updateContainerState(containerID, CLOSE);
        LOG.info("Pass 1 (CLOSED): container {} corrected to CLOSED "
            + "(was CLOSING).", containerID);
        return 1;  // correction applied — was CLOSING or OPEN before this call
      }

      if (reconState == HddsProtos.LifeCycleState.QUASI_CLOSED) {
        containerManager.updateContainerState(containerID, FORCE_CLOSE);
        LOG.info("Pass 1 (CLOSED): container {} corrected from QUASI_CLOSED to CLOSED "
            + "via FORCE_CLOSE.", containerID);
        return 1;
      }

      return 0;  // already CLOSED or past CLOSED — no action needed
    } catch (ContainerNotFoundException e) {
      LOG.warn("Pass 1 (CLOSED): container {} vanished from Recon between partition "
          + "and state correction.", containerID, e);
    } catch (InvalidStateTransitionException | IOException e) {
      LOG.warn("Pass 1 (CLOSED): failed to correct state for container {}.", containerID, e);
    }
    return 0;
  }

  // ---------------------------------------------------------------------------
  // Pass 2: Incremental add-only sync for OPEN containers
  // Pass 3: Full add + state-correction sync for QUASI_CLOSED containers
  // ---------------------------------------------------------------------------

  /**
   * Fetches only the newly created OPEN containers from SCM, starting at the
   * last-seen OPEN container ID from the previous cycle, and adds any that are
   * absent from Recon.
   *
   * <p>This deliberately avoids rescanning the full OPEN set every cycle.
   * OPEN container IDs are monotonic, so once Recon has scanned through a
   * given ID range it can continue from the next ID in later cycles. This
   * keeps OPEN drift on an incremental path while CLOSED/QUASI_CLOSED still use
   * full state scans for correction.
   */
  private boolean syncOpenContainersIncrementally() {
    try {
      long totalOpen = scmServiceProvider.getContainerCount(HddsProtos.LifeCycleState.OPEN);
      if (totalOpen == 0) {
        LOG.debug("Pass 2 (OPEN): no containers found in SCM.");
        return true;
      }

      long retrieved = 0;
      int addedCount = 0;
      long batchSize = Math.min(totalOpen, getStatePaginationBatchSize());
      ContainerID startContainerId = ContainerID.valueOf(pass2OpenStartContainerId.get());

      while (true) {
        List<ContainerID> batch = scmServiceProvider.getListOfContainerIDs(
            startContainerId, (int) batchSize, HddsProtos.LifeCycleState.OPEN);
        if (batch == null || batch.isEmpty()) {
          LOG.info("Pass 2 (OPEN): sync complete from cursor {}, checked {}, added {}.",
              pass2OpenStartContainerId.get(), retrieved, addedCount);
          return true;
        }

        addedCount += addMissingContainersForState(batch, HddsProtos.LifeCycleState.OPEN);
        retrieved += batch.size();

        long lastID = batch.get(batch.size() - 1).getId();
        pass2OpenStartContainerId.set(lastID + 1);
        startContainerId = ContainerID.valueOf(pass2OpenStartContainerId.get());
      }
    } catch (Exception e) {
      LOG.error("Pass 2 (OPEN): unexpected error during sync.", e);
      return false;
    }
  }

  /**
   * Adds any containers in {@code batch} that are absent from Recon.
   * Called exclusively from Pass 2 (OPEN containers).
   *
   * <p>For OPEN containers, if {@code getExistContainerWithPipelinesInBatch}
   * excludes a container (pipeline not yet resolvable), it is skipped with no
   * fallback. Because the {@link #pass2OpenStartContainerId} cursor is monotonic
   * and never goes backward, the skipped container is only re-visited on Recon
   * restart (cursor resets to 1) or when the container transitions to
   * CLOSED/QUASI_CLOSED and Pass 1 or Pass 3 picks it up. No null-pipeline
   * fallback is safe for OPEN because {@link ReconContainerManager#addNewContainer}
   * requires a valid pipeline to register the container correctly.
   *
   * @param batch page of container IDs to check
   * @param state the lifecycle state — must be OPEN when called from Pass 2
   * @return number of containers successfully added
   */
  private int addMissingContainersForState(List<ContainerID> batch,
                                           HddsProtos.LifeCycleState state) {
    // NOTE: ContainerWithPipeline.pipeline is *required* in the proto2 schema
    // so getExistContainerWithPipelinesInBatch cannot return a null pipeline.
    // OPEN containers excluded because createPipelineForRead failed are skipped.
    // They will only be re-scanned if Recon restarts (resetting the monotonic
    // cursor to 1) or if the container transitions to CLOSED/QUASI_CLOSED and
    // is caught by Pass 1 or Pass 3 in a future TARGETED_SYNC cycle.
    List<Long> missingIds = new ArrayList<>();
    for (ContainerID containerID : batch) {
      if (!containerManager.containerExist(containerID)) {
        missingIds.add(containerID.getId());
      }
    }
    if (missingIds.isEmpty()) {
      return 0;
    }
    return batchedAddMissingContainers(missingIds, state, "Pass 2 (OPEN)");
  }

  // ---------------------------------------------------------------------------
  // Pass 3: QUASI_CLOSED — add missing containers and correct stale states
  // ---------------------------------------------------------------------------

  /**
   * Fetches SCM's full QUASI_CLOSED container ID list (paginated) and for
   * each entry:
   * <ul>
   *   <li>If absent from Recon: calls {@code addNewContainer()}.</li>
   *   <li>If present in Recon as OPEN: advances via FINALIZE → QUASI_CLOSE.</li>
   *   <li>If present in Recon as CLOSING: advances via QUASI_CLOSE.</li>
   *   <li>If already QUASI_CLOSED (or past): no action.</li>
   * </ul>
   *
   * <p>Correcting OPEN/CLOSING → QUASI_CLOSED handles the case where Recon
   * missed the QUASI_CLOSE transition while it was down or lagging. Without
   * this correction, {@link #decideSyncAction()} could detect QUASI_CLOSED count
   * drift but simply adding missing containers would not fix the issue — the
   * container already exists in Recon, just stuck in the wrong state.
   */
  private boolean syncQuasiClosedContainers() {
    try {
      long totalQuasiClosed = scmServiceProvider.getContainerCount(
          HddsProtos.LifeCycleState.QUASI_CLOSED);
      if (totalQuasiClosed == 0) {
        LOG.debug("Pass 3 (QUASI_CLOSED): no containers found in SCM.");
        return true;
      }

      // Compute batchSize once — same reasoning as in syncClosedContainers().
      int batchSize = (int) getContainerCountPerCall(totalQuasiClosed);

      ContainerID startContainerId = ContainerID.valueOf(1);
      long retrieved = 0;
      int addedCount = 0;
      int correctedCount = 0;

      while (true) {
        List<ContainerID> batch = scmServiceProvider.getListOfContainerIDs(
            startContainerId, batchSize, HddsProtos.LifeCycleState.QUASI_CLOSED);
        if (batch == null || batch.isEmpty()) {
          break; // no more QUASI_CLOSED containers at or above startContainerId
        }

        // ── Phase A: batch-add absent containers ─────────────────────────────────
        // Same pattern as Pass 1: use batchedAddMissingContainers for auto
        // sub-batching within safeContainerWithPipelineBatchSize.
        List<Long> absentIds = new ArrayList<>();
        List<ContainerID> presentIds = new ArrayList<>();
        for (ContainerID containerID : batch) {
          if (!containerManager.containerExist(containerID)) {
            absentIds.add(containerID.getId());
          } else {
            presentIds.add(containerID);
          }
        }

        if (!absentIds.isEmpty()) {
          addedCount += batchedAddMissingContainers(
              absentIds, HddsProtos.LifeCycleState.QUASI_CLOSED, "Pass 3");
        }

        // ── Phase B: state-correct present containers ─────────────────────────────
        // Local state-machine transitions only — no SCM RPCs.
        for (ContainerID containerID : presentIds) {
          try {
            ContainerInfo reconContainer = containerManager.getContainer(containerID);
            HddsProtos.LifeCycleState reconState = reconContainer.getState();

            if (reconState == HddsProtos.LifeCycleState.OPEN) {
              containerManager.transitionOpenToClosing(containerID, reconContainer);
              reconState = HddsProtos.LifeCycleState.CLOSING;
              LOG.info("Pass 3 (QUASI_CLOSED): container {} advanced OPEN → CLOSING.",
                  containerID);
            }
            if (reconState == HddsProtos.LifeCycleState.CLOSING) {
              containerManager.updateContainerState(containerID, QUASI_CLOSE);
              correctedCount++;
              LOG.info("Pass 3 (QUASI_CLOSED): container {} corrected to QUASI_CLOSED.",
                  containerID);
            }
            // Already QUASI_CLOSED (or past): no action needed.
          } catch (ContainerNotFoundException e) {
            LOG.warn("Pass 3 (QUASI_CLOSED): container {} vanished from Recon between "
                + "partition and state correction.", containerID, e);
          } catch (InvalidStateTransitionException | IOException e) {
            LOG.warn("Pass 3 (QUASI_CLOSED): failed to correct state for container {}.",
                containerID, e);
          }
        }

        long lastID = batch.get(batch.size() - 1).getId();
        startContainerId = ContainerID.valueOf(lastID + 1);
        retrieved += batch.size();
      }

      LOG.info("Pass 3 (QUASI_CLOSED): sync complete, checked {}, added {}, corrected {}.",
          retrieved, addedCount, correctedCount);
      return true;
    } catch (IOException e) {
      LOG.error("Pass 3 (QUASI_CLOSED): unexpected error during sync.", e);
      return false;
    }
  }

  // ---------------------------------------------------------------------------
  // Pass 4: DELETED retirement — SCM-driven, transition only, never "add"
  // ---------------------------------------------------------------------------

  /**
   * Retires containers that SCM has fully deleted (state = DELETED) but Recon
   * still holds as CLOSED or QUASI_CLOSED.
   *
   * <p>Only SCM's DELETED list is scanned — not DELETING. Reason: if we
   * processed DELETING, we would drive Recon to the intermediate DELETING state
   * and leave it there until the next cycle. In the next cycle, Recon would be
   * DELETING but the condition checks CLOSED || QUASI_CLOSED — causing the
   * container to be stuck at DELETING forever. By waiting for SCM to confirm
   * full deletion (DELETED), we transition Recon atomically from
   * CLOSED/QUASI_CLOSED → DELETING → DELETED in a single call with no
   * cross-cycle intermediate state.
   *
   * <p>Uses {@code getListOfContainerIDs} (IDs only, 12 bytes/container) rather
   * than {@code getExistContainerWithPipelinesInBatch} (~490 bytes/container with
   * DatanodeDetails). Pass 4 only needs to know which containers are DELETED.
   * At 128 MB IPC limit: up to ~10.9M IDs per page, so even 1 billion deleted
   * containers need only ~100 calls per cycle.
   *
   * @return {@code true} if all RPC calls completed without error
   */
  private boolean retireDeletedContainers() {
    try {
      // getListOfContainerInfos returns ContainerInfo objects (~86 bytes each
      // on wire). Use safeContainerInfoBatchSize (not safeContainerWithPipelineBatchSize)
      // because ContainerInfo does NOT include Pipeline or DatanodeDetails.
      // At batchSize=50K: 50K × 86 bytes ≈ 4 MB per page — well within 128 MB IPC limit.
      int configuredBatch = ozoneConfiguration.getInt(
          OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE,
          OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE_DEFAULT);
      int batchSize = safeContainerInfoBatchSize(configuredBatch);
      int retiredCount = 0;

      // Paginate SCM's DELETED list using getListOfContainerInfos (not IDs-only).
      // We need full ContainerInfo to handle two cases correctly:
      //   1. Containers absent from Recon: added via addNewContainer with their
      //      actual replication config (RATIS or EC) — using IDs only would
      //      require a placeholder config that could be wrong for EC containers.
      //   2. Containers present in Recon: retired using retireContainerToDeleted.
      //
      // Memory: one page (batchSize ContainerInfo objects) in memory at a time.
      // Each page is processed and GC'd before the next page is fetched — never
      // millions of objects simultaneously.
      //
      // We do NOT scan the DELETING list: processing DELETING would drive Recon
      // to an intermediate DELETING state across cycles (stuck). We wait for SCM
      // to confirm full deletion (DELETED) and then retire atomically.
      ContainerID start = ContainerID.valueOf(1);
      while (true) {
        List<ContainerInfo> page = scmServiceProvider.getListOfContainerInfos(
            start, batchSize, HddsProtos.LifeCycleState.DELETED);
        if (page == null || page.isEmpty()) {
          break;
        }
        retiredCount += processDeletedPage(page);
        start = ContainerID.valueOf(
            page.get(page.size() - 1).containerID().getId() + 1);
      }

      LOG.info("Pass 4 (DELETED retirement): sync complete, retired={}.", retiredCount);
      return true;
    } catch (Exception e) {
      LOG.error("Pass 4 (DELETED retirement): unexpected error.", e);
      return false;
    }
  }

  /**
   * Processes one page of DELETED containers from SCM.
   * For each container:
   * <ul>
   *   <li>If absent from Recon: adds it using the full {@link ContainerInfo}
   *       from SCM (preserving the actual replication config — RATIS or EC).</li>
   *   <li>If present in Recon in a non-terminal state: drives it to DELETED.</li>
   *   <li>If already DELETED in Recon: no-op.</li>
   * </ul>
   */
  private int processDeletedPage(List<ContainerInfo> page) {
    int retiredCount = 0;
    for (ContainerInfo scmInfo : page) {
      ContainerID containerID = scmInfo.containerID();
      if (!containerManager.containerExist(containerID)) {
        // Container entirely absent from Recon — add it with the correct
        // replication config from SCM (handles both RATIS and EC correctly).
        try {
          containerManager.addNewContainer(
              new ContainerWithPipeline(scmInfo, null));
          retiredCount++;
          LOG.info("Pass 4 (DELETED retirement): added missing DELETED container {} "
              + "(full lifecycle occurred while Recon was down).", containerID);
        } catch (IOException e) {
          LOG.warn("Pass 4 (DELETED retirement): failed to add missing DELETED "
              + "container {}.", containerID, e);
        }
        continue;
      }
      try {
        ContainerInfo reconInfo = containerManager.getContainer(containerID);
        if (reconInfo.getState() != HddsProtos.LifeCycleState.DELETED) {
          retireContainerToDeleted(containerID, reconInfo,
              HddsProtos.LifeCycleState.DELETED);
          retiredCount++;
        }
        // reconState == DELETED: already terminal, nothing to do.
      } catch (ContainerNotFoundException e) {
        LOG.debug("Pass 4 (DELETED retirement): container {} vanished from Recon "
            + "between existence check and retirement.", containerID);
      }
    }
    return retiredCount;
  }

  /**
   * Drives a container in Recon from any non-terminal lifecycle state to
   * DELETED by applying the minimum valid state machine transitions.
   *
   * <p>This handles all states that can arrive at Pass 4 after the three
   * preceding passes have already run:
   * <pre>
   *   OPEN         → CLOSING (FINALIZE via transitionOpenToClosing)
   *                → CLOSED  (CLOSE)
   *                → DELETING (DELETE)
   *                → DELETED  (CLEANUP)
   *
   *   CLOSING      → CLOSED  (CLOSE)
   *                → DELETING (DELETE)
   *                → DELETED  (CLEANUP)
   *
   *   QUASI_CLOSED → DELETING (DELETE)
   *                → DELETED  (CLEANUP)
   *
   *   CLOSED       → DELETING (DELETE)
   *                → DELETED  (CLEANUP)
   *
   *   DELETING     → DELETING (DELETE is idempotent — no-op, no exception)
   *                → DELETED  (CLEANUP)
   * </pre>
   *
   * <p>The idempotent transitions in the state machine (CLOSE is idempotent
   * from CLOSED/DELETING/DELETED; DELETE is idempotent from DELETING/DELETED)
   * ensure no {@link InvalidStateTransitionException} is thrown for states
   * that have already advanced past a particular transition.
   *
   * @param containerID the container to retire
   * @param reconInfo   current Recon snapshot of the container (used for
   *                    OPEN→CLOSING transition and log messages)
   * @param scmState    always DELETED (passed through to log messages)
   */
  private void retireContainerToDeleted(ContainerID containerID,
                                        ContainerInfo reconInfo,
                                        HddsProtos.LifeCycleState scmState) {
    try {
      HddsProtos.LifeCycleState reconState = reconInfo.getState();

      // OPEN → CLOSING: must use transitionOpenToClosing to also decrement
      // the pipelineToOpenContainer counter accurately.
      if (reconState == HddsProtos.LifeCycleState.OPEN) {
        containerManager.transitionOpenToClosing(containerID, reconInfo);
        reconState = HddsProtos.LifeCycleState.CLOSING;
      }
      // CLOSING → CLOSED (idempotent from CLOSED/DELETING/DELETED — safe for all).
      if (reconState == HddsProtos.LifeCycleState.CLOSING) {
        containerManager.updateContainerState(containerID, CLOSE);
      }
      // CLOSED/QUASI_CLOSED → DELETING; idempotent no-op from DELETING.
      containerManager.updateContainerState(containerID, DELETE);
      // DELETING → DELETED.
      containerManager.updateContainerState(containerID, CLEANUP);

      LOG.info("Pass 4 (DELETED retirement): container {} transitioned "
          + "{} → DELETED in Recon (SCM state: {}).",
          containerID, reconInfo.getState(), scmState);
    } catch (InvalidStateTransitionException | IOException e) {
      LOG.warn("Pass 4 (DELETED retirement): failed to retire container {} "
          + "from {} toward DELETED.", containerID, reconInfo.getState(), e);
    }
  }

  // ---------------------------------------------------------------------------
  // Batched add with automatic CWP-response size safety
  // ---------------------------------------------------------------------------

  /**
   * Adds containers whose IDs are in {@code absentIds} by calling
   * {@code getExistContainerWithPipelinesInBatch} in sub-batches that are
   * guaranteed to fit within the Hadoop IPC message size limit.
   *
   * <h3>Why sub-batching is required</h3>
   * {@code getExistContainerWithPipelinesInBatch} returns full
   * {@code ContainerWithPipeline} objects (conservatively bounded at
   * {@link #CONTAINER_WITH_PIPELINE_PROTO_SIZE_BYTES} = 1024 bytes each, actual
   * ~490 bytes, including DatanodeDetails for all replicas). A page fetched by
   * {@code getListOfContainerIDs} can contain up to ~10.9M IDs at the 128 MB
   * limit. Sending all absent IDs from such a page in one CWP call could produce
   * a response of 10.9M × 1024 ≈ 10 GB — far exceeding the IPC limit.
   *
   * <p>This method splits {@code absentIds} into sub-batches of at most
   * {@link #safeContainerWithPipelineBatchSize} entries and issues one
   * {@code getExistContainerWithPipelinesInBatch} RPC per sub-batch, ensuring
   * every response stays within the IPC ceiling regardless of how
   * {@code ozone.recon.scm.container.id.batch.size} is configured.
   *
   * <h3>Fast path vs fallback</h3>
   * <ul>
   *   <li>Containers returned by SCM: added via
   *       {@link ReconContainerManager#addNewContainer}.</li>
   *   <li>Containers <em>excluded</em> (pipeline unresolvable, 0 viable replicas):
   *       for non-OPEN states (CLOSED, QUASI_CLOSED), retried via
   *       {@link #addContainerInfoFallback} (one targeted
   *       {@code getListOfContainerInfos} RPC each, expected near-zero in healthy
   *       clusters). For OPEN, excluded containers are silently skipped — they
   *       are only re-visited on Recon restart or when they transition to a
   *       non-OPEN state caught by Pass 1 or Pass 3.</li>
   * </ul>
   *
   * @param absentIds IDs confirmed absent from Recon (may be up to 1M)
   * @param state     lifecycle state of all IDs in {@code absentIds}
   * @param passLabel log prefix (e.g. "Pass 1 (CLOSED)")
   * @return total number of containers successfully added
   */
  private int batchedAddMissingContainers(List<Long> absentIds,
                                          HddsProtos.LifeCycleState state,
                                          String passLabel) {
    int added = 0;
    int cwpSubBatch = safeContainerWithPipelineBatchSize(absentIds.size());

    for (int offset = 0; offset < absentIds.size(); offset += cwpSubBatch) {
      List<Long> subBatch = absentIds.subList(
          offset, Math.min(offset + cwpSubBatch, absentIds.size()));

      List<ContainerWithPipeline> cwpList =
          scmServiceProvider.getExistContainerWithPipelinesInBatch(subBatch);

      Set<Long> addedViaFastPath = new HashSet<>(cwpList.size() * 2);
      for (ContainerWithPipeline cwp : cwpList) {
        long cid = cwp.getContainerInfo().getContainerID();
        try {
          containerManager.addNewContainer(cwp);
          addedViaFastPath.add(cid);
          added++;
          LOG.info("{}: added missing container {}.", passLabel, cid);
        } catch (IOException e) {
          LOG.error("{}: could not add missing container {}.", passLabel, cid, e);
        }
      }

      // For non-OPEN states: fallback for containers excluded by the batch RPC
      // (pipeline unresolvable). OPEN containers are skipped — no null-pipeline
      // fallback is safe for OPEN (pipeline tracking required).
      if (state != HddsProtos.LifeCycleState.OPEN) {
        for (Long id : subBatch) {
          if (!addedViaFastPath.contains(id)) {
            if (addContainerInfoFallback(ContainerID.valueOf(id), state, passLabel)) {
              added++;
            }
          }
        }
      }
    }
    return added;
  }

  // ---------------------------------------------------------------------------
  // Pipeline-failed fallback: add non-OPEN containers without a pipeline
  // ---------------------------------------------------------------------------

  /**
   * Fallback for when {@code getExistContainerWithPipelinesInBatch} excludes a
   * container because {@code createPipelineForRead} failed (e.g., the container
   * has zero viable replicas or all replicas are UNHEALTHY).
   *
   * <p>Because {@code ContainerWithPipeline.pipeline} is {@code required} in
   * the protobuf schema, the batch RPC cannot return a container without a
   * valid pipeline. This fallback uses {@link
   * StorageContainerServiceProvider#getListOfContainerInfos} — which delegates
   * to SCM's {@code listContainer} and carries only {@code ContainerInfo} —
   * to obtain the metadata without triggering pipeline resolution.
   *
   * <p><b>Performance:</b> this method issues at most one lightweight RPC per
   * invocation and is called <em>only</em> for containers excluded from the
   * batch result. In healthy clusters that number is zero, so the overhead on
   * the hot path is negligible even at 30 million containers.
   *
   * <p><b>Safety:</b> {@link ReconContainerManager#addNewContainer} accepts a
   * {@code null} pipeline for non-OPEN containers and stores the container
   * in the state manager without pipeline tracking, which is correct for
   * CLOSED and QUASI_CLOSED containers.
   *
   * @param containerID the container to add
   * @param state       the expected SCM lifecycle state (CLOSED or QUASI_CLOSED)
   * @param passLabel   logging label (e.g. "Pass 1", "Pass 3")
   * @return {@code true} if the container was successfully added
   */
  private boolean addContainerInfoFallback(ContainerID containerID,
                                           HddsProtos.LifeCycleState state,
                                           String passLabel) {
    // Safety guard: null-pipeline add is only safe for non-OPEN states.
    // ReconContainerManager.addNewContainer only accesses the pipeline argument
    // in the OPEN branch; the else branch (CLOSED, QUASI_CLOSED, …) passes only
    // containerInfo.getProtobuf() to the state manager and never calls getPipeline().
    if (state == HddsProtos.LifeCycleState.OPEN) {
      LOG.error("{}: addContainerInfoFallback called with OPEN state for container {}. "
          + "Skipping — OPEN containers require a valid pipeline.", passLabel, containerID);
      return false;
    }
    try {
      List<ContainerInfo> infos = scmServiceProvider.getListOfContainerInfos(
          containerID, 1, state);
      if (infos.isEmpty() || !infos.get(0).containerID().equals(containerID)) {
        // Container no longer in this state (race: it may have transitioned or
        // been removed between the ID-list call and this fallback call).
        LOG.debug("{} ({}): container {} no longer in state {} in SCM; skipping.",
            passLabel, state, containerID, state);
        return false;
      }
      containerManager.addNewContainer(
          new ContainerWithPipeline(infos.get(0), null));
      LOG.info("{} ({}): added container {} via pipeline-failed fallback "
              + "(pipeline unresolvable — container has no viable replicas).",
          passLabel, state, containerID);
      return true;
    } catch (IOException e) {
      LOG.error("{} ({}): fallback add failed for container {}.",
          passLabel, state, containerID, e);
      return false;
    }
  }

  // ---------------------------------------------------------------------------
  // Batch size utility
  // ---------------------------------------------------------------------------

  /**
   * Returns the maximum number of container IDs that can be included in one
   * {@code getListOfContainerIDs} call without exceeding the Hadoop IPC message
   * size limit or the configured per-call batch cap.
   *
   * <p>Both the request (IDs sent to SCM) and the response (IDs returned by SCM)
   * carry {@code ContainerID} entries at {@link #CONTAINER_ID_PROTO_SIZE_BYTES}
   * bytes each, so a single size constant bounds both directions correctly.
   *
   * <p>Applies to: Pass 1, Pass 2, Pass 3 pagination, Pass 4
   * ({@code getListOfContainerIDs} for the DELETED state list).
   *
   * @param upperBound cap on the returned batch size; pass the total container
   *                   count in a state when paginating that state, or a
   *                   configured batch size limit when the caller owns the
   *                   upper bound (e.g. Pass 4 uses the configured deleted-check
   *                   batch size rather than the DELETED container total)
   * @return safe batch size ≤ {@code upperBound} and ≤ IPC / per-call limits
   */
  private long getContainerCountPerCall(long upperBound) {
    long hadoopRPCSize = ozoneConfiguration.getInt(
        IPC_MAXIMUM_DATA_LENGTH, IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    long countByRpcLimit = hadoopRPCSize / CONTAINER_ID_PROTO_SIZE_BYTES;
    long countByBatchLimit = ozoneConfiguration.getLong(
        OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE,
        OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE_DEFAULT);

    long batchSize = Math.min(countByRpcLimit, countByBatchLimit);
    return Math.min(upperBound, batchSize);
  }

  /**
   * Returns the maximum number of containers for one {@code getListOfContainerInfos}
   * call without exceeding the Hadoop IPC message size limit.
   *
   * <p>Uses {@link #CONTAINER_INFO_PROTO_SIZE_BYTES} (128 bytes) — ContainerInfo
   * carries only container metadata with no Pipeline or DatanodeDetails, making it
   * ~8× smaller than a full {@code ContainerWithPipeline}. The safe maximum at
   * 128 MB IPC limit is ~1M containers; the configured batch size is typically far
   * below this, so the IPC bound rarely restricts in practice.
   *
   * @param requested caller-requested batch size
   * @return safe maximum containers for one ContainerInfo response
   */
  private int safeContainerInfoBatchSize(int requested) {
    long hadoopRPCSize = ozoneConfiguration.getInt(
        IPC_MAXIMUM_DATA_LENGTH, IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    long responseLimit = hadoopRPCSize / CONTAINER_INFO_PROTO_SIZE_BYTES;
    return (int) Math.min(requested, responseLimit);
  }

  /**
   * Returns the maximum number of containers that can be sent in one
   * {@code getExistContainerWithPipelinesInBatch} call without causing the
   * response to exceed the Hadoop IPC message size limit.
   *
   * <p>{@code getContainerCountPerCall} is NOT appropriate here: it uses
   * {@link #CONTAINER_ID_PROTO_SIZE_BYTES} (12 bytes) which bounds the request
   * but ignores the response. The response carries full
   * {@code ContainerWithPipeline} objects at conservatively
   * {@link #CONTAINER_WITH_PIPELINE_PROTO_SIZE_BYTES} = 1024 bytes each — ~85×
   * larger than a bare ID. Using the ID-based limit would silently allow a
   * response 85× larger than the IPC ceiling.
   *
   * <p>At the default {@code ipc.maximum.data.length = 128 MB}:
   * <pre>
   *   128 MB / 1024 bytes = 131,072 containers per call (conservative estimate)
   *   actual bytes:  131,072 × 490 ≈ 61 MB — well within the 128 MB limit
   * </pre>
   *
   * @param requested caller-requested batch size
   * @return safe maximum containers for one CWP response
   */
  private int safeContainerWithPipelineBatchSize(int requested) {
    long hadoopRPCSize = ozoneConfiguration.getInt(
        IPC_MAXIMUM_DATA_LENGTH, IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    long responseLimit = hadoopRPCSize / CONTAINER_WITH_PIPELINE_PROTO_SIZE_BYTES;
    return (int) Math.min(requested, responseLimit);
  }

  private long getStatePaginationBatchSize() {
    return getContainerCountPerCall(Long.MAX_VALUE);
  }
}
