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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that performs targeted incremental sync between SCM and Recon
 * container metadata. Executes four passes per sync cycle:
 *
 * <ol>
 *   <li><b>Pass 1 — CLOSED (SCM-driven, add + correct):</b> fetches SCM's
 *       CLOSED container ID list, adds any absent from Recon, and corrects
 *       containers that are OPEN or CLOSING in Recon but CLOSED in SCM.</li>
 *   <li><b>Pass 2 — OPEN (SCM-driven, add only):</b> adds OPEN containers
 *       that are absent from Recon entirely (e.g., created while Recon was
 *       down).</li>
 *   <li><b>Pass 3 — QUASI_CLOSED (SCM-driven, add only):</b> adds
 *       QUASI_CLOSED containers absent from Recon. Requires that SCM returns
 *       container metadata with a null pipeline when pipeline lookup fails, and
 *       that Recon's {@code addNewContainer} handles a null pipeline gracefully;
 *       otherwise QUASI_CLOSED containers whose pipelines have been cleaned up
 *       will fail with {@code NullPointerException} or {@code IOException}.</li>
 *   <li><b>Pass 4 — DELETED retirement (Recon-driven, transition only):</b>
 *       scans Recon's CLOSED and QUASI_CLOSED containers in batches, queries
 *       SCM for each, and transitions any that SCM reports as DELETED.
 *       Intentionally Recon-driven (not SCM-driven) because SCM's DELETED
 *       list grows unboundedly; starting from Recon's bounded set of
 *       non-terminal containers is always more efficient.</li>
 * </ol>
 */
class ReconStorageContainerSyncHelper {

  // Serialized size of one ContainerID proto on the wire (varint tag + 8-byte long = ~12 bytes).
  // Used to derive the maximum batch size that fits within ipc.maximum.data.length.
  private static final long CONTAINER_ID_PROTO_SIZE_BYTES = 12;

  /**
   * Rotating cursor for Pass 4 (DELETED retirement). Tracks the list position
   * where the next sync cycle should begin so that all candidates are
   * eventually covered regardless of batch size. Volatile because it is
   * updated by the scheduler thread and read by tests.
   */
  private volatile int pass4BatchOffset = 0;
  /**
   * Monotonic cursor for Pass 2 (OPEN add-only sync). OPEN containers are
   * created with increasing container IDs, so each cycle only needs to scan
   * from the last-seen ID onward rather than rescanning the full OPEN set.
   */
  private volatile long pass2OpenStartContainerId = 1L;

  private static final Logger LOG = LoggerFactory
      .getLogger(ReconStorageContainerSyncHelper.class);

  private final StorageContainerServiceProvider scmServiceProvider;
  private final OzoneConfiguration ozoneConfiguration;
  private final ReconContainerManager containerManager;

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
     * Large total-count drift detected — replace Recon's entire SCM DB with a
     * fresh checkpoint from SCM. Reserved for cases where targeted sync would
     * be unreliable (e.g., Recon was down for hours and hundreds of containers
     * changed state).
     */
    FULL_SNAPSHOT
  }

  ReconStorageContainerSyncHelper(StorageContainerServiceProvider scmServiceProvider,
                                  OzoneConfiguration ozoneConfiguration,
                                  ReconContainerManager containerManager) {
    this.scmServiceProvider = scmServiceProvider;
    this.ozoneConfiguration = ozoneConfiguration;
    this.containerManager = containerManager;
  }

  /**
   * Decides what sync action the periodic scheduler should take based on the
   * observed drift between SCM and Recon.
   *
   * <p>Decision logic:
   * <ol>
   *   <li>If {@code |(SCM_total - SCM_open) - (Recon_total - Recon_open)| >
   *       ozone.recon.scm.container.threshold} (default 10,000): return
   *       {@link SyncAction#FULL_SNAPSHOT}. Large drift in non-OPEN containers
   *       means Recon is badly behind on stable SCM state and a full checkpoint
   *       replacement is cheaper and more reliable at that scale.</li>
   *   <li>If total drift is positive but the non-OPEN drift is at or below the
   *       threshold: return {@link SyncAction#TARGETED_SYNC}. This keeps large
   *       OPEN-only gaps on the incremental path because missing OPEN
   *       containers can be repaired cheaply without replacing the full SCM DB.</li>
   *   <li>If total drift is zero, check per-state drift for each active
   *       (non-terminal) lifecycle state against
   *       {@code ozone.recon.scm.per.state.drift.threshold} (default 5):
   *       <ul>
   *         <li><b>OPEN</b>: detects containers stuck OPEN in Recon after SCM
   *             has advanced them to QUASI_CLOSED or CLOSED.</li>
   *         <li><b>QUASI_CLOSED</b>: detects containers stuck QUASI_CLOSED in
   *             Recon after SCM has advanced them to CLOSED. This case produces
   *             zero OPEN drift and is invisible to an OPEN-only check.</li>
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
    List<ContainerInfo> reconContainers = containerManager.getContainers();
    long reconTotal = reconContainers.size();
    long reconOpen = reconContainers.stream()
        .filter(c -> c.getState() == HddsProtos.LifeCycleState.OPEN)
        .count();

    // --- Check 1: large non-OPEN drift escalates to full snapshot ---
    long scmTotal = scmServiceProvider.getContainerCount();
    long scmOpen = scmServiceProvider.getContainerCount(HddsProtos.LifeCycleState.OPEN);
    long totalDrift = Math.abs(scmTotal - reconTotal);
    long scmNonOpen = Math.max(0, scmTotal - scmOpen);
    long reconNonOpen = Math.max(0, reconTotal - reconOpen);
    long nonOpenDrift = Math.abs(scmNonOpen - reconNonOpen);

    if (nonOpenDrift > largeThreshold) {
      LOG.warn("Non-OPEN container drift {} exceeds threshold {} "
              + "(SCM_non_OPEN={}, Recon_non_OPEN={}, SCM_total={}, Recon_total={}). "
              + "Triggering full snapshot.",
          nonOpenDrift, largeThreshold, scmNonOpen, reconNonOpen, scmTotal, reconTotal);
      return SyncAction.FULL_SNAPSHOT;
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
    // These checks intentionally use the lightweight per-state count RPCs so
    // the decision path remains cheap. CLOSED is derived as the remainder after
    // subtracting OPEN and QUASI_CLOSED from the total on each side.
    long scmQuasiClosed =
        scmServiceProvider.getContainerCount(HddsProtos.LifeCycleState.QUASI_CLOSED);
    long reconQuasiClosed = reconContainers.stream()
        .filter(c -> c.getState() == HddsProtos.LifeCycleState.QUASI_CLOSED)
        .count();
    long scmClosed = Math.max(0, scmTotal - scmOpen - scmQuasiClosed);
    long reconClosed = Math.max(0, reconTotal - reconOpen - reconQuasiClosed);

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
        return SyncAction.TARGETED_SYNC;
      }
    }

    LOG.info("No significant drift detected (total drift={}). No sync needed.", totalDrift);
    return SyncAction.NO_ACTION;
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
  // Pass 1: CLOSED containers — add missing, correct stale OPEN/CLOSING state
  // ---------------------------------------------------------------------------

  /**
   * Fetches SCM's full CLOSED container ID list (paginated) and for each entry:
   * <ul>
   *   <li>If absent from Recon: calls {@code addNewContainer()}.</li>
   *   <li>If present in Recon as OPEN or CLOSING: advances to CLOSED
   *       via the appropriate lifecycle events.</li>
   *   <li>If already CLOSED (or past): no action.</li>
   * </ul>
   */
  private boolean syncClosedContainers() {
    try {
      long totalClosed = scmServiceProvider.getContainerCount(
          HddsProtos.LifeCycleState.CLOSED);
      if (totalClosed == 0) {
        LOG.debug("No CLOSED containers found in SCM.");
        return true;
      }

      ContainerID startContainerId = ContainerID.valueOf(1);
      long retrieved = 0;

      while (retrieved < totalClosed) {
        List<ContainerID> batch = getContainerIDsByState(
            startContainerId, HddsProtos.LifeCycleState.CLOSED);
        if (batch == null || batch.isEmpty()) {
          LOG.warn("Pass 1 (CLOSED): SCM reported {} CLOSED containers, but "
              + "returned an empty batch after {} were retrieved.", totalClosed, retrieved);
          return false;
        }

        LOG.info("Pass 1 (CLOSED): processing batch of {} containers.", batch.size());
        for (ContainerID containerID : batch) {
          processSyncedClosedContainer(containerID);
        }

        long lastID = batch.get(batch.size() - 1).getId();
        startContainerId = ContainerID.valueOf(lastID + 1);
        retrieved += batch.size();
      }

      LOG.info("Pass 1 (CLOSED): sync complete, checked {} containers.", retrieved);
      return true;
    } catch (Exception e) {
      LOG.error("Pass 1 (CLOSED): unexpected error during sync.", e);
      return false;
    }
  }

  /**
   * Processes a single container ID from SCM's CLOSED list:
   * adds it to Recon if absent, or corrects its state if stale.
   */
  private void processSyncedClosedContainer(ContainerID containerID) {
    if (!containerManager.containerExist(containerID)) {
      // Container completely absent from Recon — add it.
      // Use the batch API instead of the individual getContainerWithPipeline: the batch API
      // has a null-pipeline fallback that returns the container even when the pipeline lookup
      // fails (e.g., pipeline cleaned up on SCM side or createPipelineForRead fails for
      // containers with 0 replicas).  The individual call throws IOException in those cases
      // and silently skips the container, leaving it permanently absent from Recon.
      List<ContainerWithPipeline> cwpList =
          scmServiceProvider.getExistContainerWithPipelinesInBatch(
              Collections.singletonList(containerID.getId()));
      if (cwpList.isEmpty()) {
        LOG.warn("Pass 1 (CLOSED): container {} not returned by SCM; skipping.", containerID);
        return;
      }
      try {
        containerManager.addNewContainer(cwpList.get(0));
        LOG.info("Pass 1 (CLOSED): added missing container {}.", containerID);
      } catch (IOException e) {
        LOG.error("Pass 1 (CLOSED): could not add missing container {}.", containerID, e);
      }
      return;
    }

    // Container exists in Recon — check if its state is stale.
    try {
      ContainerInfo reconContainer = containerManager.getContainer(containerID);
      HddsProtos.LifeCycleState reconState = reconContainer.getState();

      if (reconState == HddsProtos.LifeCycleState.OPEN) {
        LOG.info("Pass 1 (CLOSED): container {} is OPEN in Recon but CLOSED in SCM. "
            + "Correcting state.", containerID);
        // OPEN → CLOSING; transitionOpenToClosing also decrements pipelineToOpenContainer
        // so the Node API's open-container-per-pipeline count stays accurate.
        containerManager.transitionOpenToClosing(containerID, reconContainer);
        reconState = HddsProtos.LifeCycleState.CLOSING;
      }

      if (reconState == HddsProtos.LifeCycleState.CLOSING) {
        // CLOSING → CLOSED (CLOSE is idempotent at CLOSED and beyond)
        containerManager.updateContainerState(containerID, CLOSE);
        LOG.info("Pass 1 (CLOSED): container {} corrected from CLOSING to CLOSED.", containerID);
        reconState = HddsProtos.LifeCycleState.CLOSED;
      }

      if (reconState == HddsProtos.LifeCycleState.QUASI_CLOSED) {
        // QUASI_CLOSED → CLOSED: SCM has already completed the quorum decision
        // (the container is definitively CLOSED in SCM), so Recon should
        // reflect that. FORCE_CLOSE is the only valid event for this transition.
        containerManager.updateContainerState(containerID, FORCE_CLOSE);
        LOG.info("Pass 1 (CLOSED): container {} corrected from QUASI_CLOSED to CLOSED "
            + "via FORCE_CLOSE.", containerID);
      }
    } catch (ContainerNotFoundException e) {
      LOG.warn("Pass 1 (CLOSED): container {} vanished from Recon between existence "
          + "check and state read.", containerID, e);
    } catch (InvalidStateTransitionException | IOException e) {
      LOG.warn("Pass 1 (CLOSED): failed to correct state for container {}.", containerID, e);
    }
  }

  // ---------------------------------------------------------------------------
  // Pass 2 / Pass 3: Add-only sync for OPEN and QUASI_CLOSED containers
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
      ContainerID startContainerId = ContainerID.valueOf(pass2OpenStartContainerId);

      while (true) {
        List<ContainerID> batch = scmServiceProvider.getListOfContainerIDs(
            startContainerId, (int) batchSize, HddsProtos.LifeCycleState.OPEN);
        if (batch == null || batch.isEmpty()) {
          LOG.info("Pass 2 (OPEN): sync complete from cursor {}, checked {}, added {}.",
              pass2OpenStartContainerId, retrieved, addedCount);
          return true;
        }

        addedCount += addMissingContainersForState(batch, HddsProtos.LifeCycleState.OPEN);
        retrieved += batch.size();

        long lastID = batch.get(batch.size() - 1).getId();
        pass2OpenStartContainerId = lastID + 1;
        startContainerId = ContainerID.valueOf(pass2OpenStartContainerId);
      }
    } catch (Exception e) {
      LOG.error("Pass 2 (OPEN): unexpected error during sync.", e);
      return false;
    }
  }

  private int addMissingContainersForState(List<ContainerID> batch,
                                           HddsProtos.LifeCycleState state) {
    // Collect all missing container IDs in this page and fetch them in one
    // batch RPC. The batch API has a null-pipeline fallback: if a pipeline
    // lookup fails (e.g., pipeline not yet OPEN or cleaned up), SCM still
    // returns the container with pipeline=null so Recon can record it.
    List<Long> missingIds = new ArrayList<>();
    for (ContainerID containerID : batch) {
      if (!containerManager.containerExist(containerID)) {
        missingIds.add(containerID.getId());
      }
    }
    if (missingIds.isEmpty()) {
      return 0;
    }

    int addedCount = 0;
    List<ContainerWithPipeline> cwpList =
        scmServiceProvider.getExistContainerWithPipelinesInBatch(missingIds);
    for (ContainerWithPipeline cwp : cwpList) {
      try {
        containerManager.addNewContainer(cwp);
        addedCount++;
        LOG.info("Pass ({}): added missing container {}.", state,
            cwp.getContainerInfo().getContainerID());
      } catch (IOException e) {
        LOG.error("Pass ({}): could not add missing container {}.", state,
            cwp.getContainerInfo().getContainerID(), e);
      }
    }
    return addedCount;
  }

  // ---------------------------------------------------------------------------
  // Pass 3 (extended): QUASI_CLOSED — add missing containers and correct
  // containers whose state has lagged behind SCM.
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
   * this correction the drift check in {@link #decideSyncAction()} could
   * detect QUASI_CLOSED count drift but the add-only pass would never fix it
   * (the container already exists in Recon, just in the wrong state).
   */
  private boolean syncQuasiClosedContainers() {
    try {
      long totalQuasiClosed = scmServiceProvider.getContainerCount(
          HddsProtos.LifeCycleState.QUASI_CLOSED);
      if (totalQuasiClosed == 0) {
        LOG.debug("Pass 3 (QUASI_CLOSED): no containers found in SCM.");
        return true;
      }

      ContainerID startContainerId = ContainerID.valueOf(1);
      long retrieved = 0;
      int addedCount = 0;
      int correctedCount = 0;

      while (retrieved < totalQuasiClosed) {
        List<ContainerID> batch = getContainerIDsByState(
            startContainerId, HddsProtos.LifeCycleState.QUASI_CLOSED);
        if (batch == null || batch.isEmpty()) {
          LOG.warn("Pass 3 (QUASI_CLOSED): SCM reported {} containers, but "
              + "returned an empty batch after {} were retrieved.",
              totalQuasiClosed, retrieved);
          return false;
        }

        for (ContainerID containerID : batch) {
          if (!containerManager.containerExist(containerID)) {
            // Use the batch API with null-pipeline fallback (see Pass 2 comment).
            List<ContainerWithPipeline> cwpList =
                scmServiceProvider.getExistContainerWithPipelinesInBatch(
                    Collections.singletonList(containerID.getId()));
            if (cwpList.isEmpty()) {
              LOG.warn("Pass 3 (QUASI_CLOSED): container {} not returned by SCM; skipping.",
                  containerID);
            } else {
              try {
                containerManager.addNewContainer(cwpList.get(0));
                addedCount++;
                LOG.info("Pass 3 (QUASI_CLOSED): added missing container {}.", containerID);
              } catch (IOException e) {
                LOG.error("Pass 3 (QUASI_CLOSED): could not add missing container {}.",
                    containerID, e);
              }
            }
          } else {
            // Container exists — correct if its state is behind QUASI_CLOSED.
            try {
              ContainerInfo reconContainer = containerManager.getContainer(containerID);
              HddsProtos.LifeCycleState reconState = reconContainer.getState();

              if (reconState == HddsProtos.LifeCycleState.OPEN) {
                // Use transitionOpenToClosing to keep pipelineToOpenContainer accurate.
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
                  + "existence check and state read.", containerID, e);
            } catch (InvalidStateTransitionException | IOException e) {
              LOG.warn("Pass 3 (QUASI_CLOSED): failed to correct state for container {}.",
                  containerID, e);
            }
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
  // Pass 4: DELETED retirement — Recon-driven, transition only, never "add"
  // ---------------------------------------------------------------------------

  /**
   * Retires containers that SCM has marked as DELETED but Recon still holds in
   * a non-terminal state (CLOSED or QUASI_CLOSED).
   *
   * <p><b>Why Recon-driven (not SCM-driven):</b> SCM's DELETED list grows
   * unboundedly over the lifetime of a cluster. Fetching the full DELETED list
   * and diffing against Recon would be O(SCM_DELETED_total) — potentially
   * millions of entries. Starting from Recon's bounded set of non-terminal
   * containers and querying SCM for each is always cheaper.
   *
   * <p><b>Batching:</b> containers are queried in batches of
   * {@code ozone.recon.scm.deleted.container.check.batch.size} (default 500)
   * to avoid overwhelming SCM with individual RPCs during a single sync cycle.
   * Containers not checked in this cycle are deferred to the next.
   *
   * <p><b>What this does NOT do:</b> this pass never adds new containers to
   * Recon. It only drives the lifecycle state forward to DELETED for containers
   * that Recon already knows about.
   *
   * @return {@code true} if the pass completed without fatal error
   */
  private boolean retireDeletedContainers() {
    try {
      // Collect Recon's non-terminal containers (CLOSED and QUASI_CLOSED).
      // These are the only states from which SCM can reach DELETED.
      List<ContainerInfo> candidates = containerManager.getContainers().stream()
          .filter(c -> c.getState() == HddsProtos.LifeCycleState.CLOSED
              || c.getState() == HddsProtos.LifeCycleState.QUASI_CLOSED)
          .collect(Collectors.toList());

      if (candidates.isEmpty()) {
        LOG.debug("Pass 4 (DELETED retirement): no CLOSED/QUASI_CLOSED containers in Recon.");
        return true;
      }

      int batchSize = ozoneConfiguration.getInt(
          OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE,
          OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE_DEFAULT);

      // --- Gap 4 fix: rotating offset ensures every candidate is eventually
      // visited even when candidates.size() >> batchSize. ---
      int total = candidates.size();
      int start = pass4BatchOffset % total;
      int end = Math.min(start + batchSize, total);
      List<ContainerInfo> batch = candidates.subList(start, end);
      // Advance the cursor; wrap to 0 when we have covered the full list.
      pass4BatchOffset = (end >= total) ? 0 : end;

      // --- Gap 6 fix: one batch RPC instead of N individual RPCs. ---
      // getExistContainerWithPipelinesInBatch() returns only containers that
      // still exist in SCM; containers absent from the result were purged.
      List<Long> batchIds = batch.stream()
          .map(c -> c.containerID().getId())
          .collect(Collectors.toList());
      List<ContainerWithPipeline> existingInSCM =
          scmServiceProvider.getExistContainerWithPipelinesInBatch(batchIds);
      if (existingInSCM == null) {
        LOG.warn("Pass 4 (DELETED retirement): SCM batch lookup returned null "
            + "for {} candidate containers. Skipping retirement this cycle.", batchIds.size());
        return true;
      }
      if (existingInSCM.isEmpty()) {
        LOG.warn("Pass 4 (DELETED retirement): SCM batch lookup returned an "
            + "empty result for {} candidate containers. Treating this as "
            + "ambiguous/unavailable and skipping retirement this cycle.", batchIds.size());
        return true;
      }

      // Build a lookup map: containerID (long) → SCM lifecycle state.
      Map<Long, HddsProtos.LifeCycleState> scmStateMap = new HashMap<>();
      for (ContainerWithPipeline cwp : existingInSCM) {
        scmStateMap.put(cwp.getContainerInfo().getContainerID(),
            cwp.getContainerInfo().getState());
      }

      int retiredCount = 0;
      int checked = 0;
      for (ContainerInfo container : batch) {
        ContainerID containerID = container.containerID();
        checked++;
        HddsProtos.LifeCycleState scmState = scmStateMap.get(containerID.getId());

        if (scmState == null) {
          // Container absent from SCM batch result — it was purged entirely.
          LOG.warn("Pass 4 (DELETED retirement): container {} not found in SCM "
              + "(may have been purged). Transitioning to DELETED in Recon.", containerID);
          retireContainerToDeleted(containerID, container.getState(),
              HddsProtos.LifeCycleState.DELETED);
          retiredCount++;
        } else if (scmState == HddsProtos.LifeCycleState.DELETING
            || scmState == HddsProtos.LifeCycleState.DELETED) {
          retireContainerToDeleted(containerID, container.getState(), scmState);
          retiredCount++;
        }
      }

      LOG.info("Pass 4 (DELETED retirement): offset={}, checked={}, retired={}, "
              + "total_candidates={}.",
          start, checked, retiredCount, total);
      return true;
    } catch (Exception e) {
      LOG.error("Pass 4 (DELETED retirement): unexpected error.", e);
      return false;
    }
  }

  /**
   * Drives a container in Recon from its current {@code reconState} forward
   * to DELETED, applying only the transitions valid from that state.
   *
   * <p>State machine:
   * <pre>
   *   CLOSED      → DELETING (DELETE) → DELETED (CLEANUP)
   *   QUASI_CLOSED → DELETING (DELETE) → DELETED (CLEANUP)
   * </pre>
   *
   * <p>All transitions used here are idempotent beyond their target state,
   * so repeated invocations are safe.
   */
  private void retireContainerToDeleted(ContainerID containerID,
                                        HddsProtos.LifeCycleState reconState,
                                        HddsProtos.LifeCycleState scmState) {
    try {
      // Both CLOSED and QUASI_CLOSED support DELETE → DELETING
      containerManager.updateContainerState(containerID, DELETE);
      // DELETING → DELETED only when SCM has fully completed deletion
      if (scmState == HddsProtos.LifeCycleState.DELETED) {
        containerManager.updateContainerState(containerID, CLEANUP);
        LOG.info("Pass 4 (DELETED retirement): container {} transitioned "
            + "{} → DELETED in Recon.", containerID, reconState);
      } else {
        LOG.info("Pass 4 (DELETED retirement): container {} transitioned "
            + "{} → DELETING in Recon (SCM is still DELETING).", containerID, reconState);
      }
    } catch (InvalidStateTransitionException | IOException e) {
      LOG.warn("Pass 4 (DELETED retirement): failed to retire container {} "
          + "from {} toward DELETED.", containerID, reconState, e);
    }
  }

  // ---------------------------------------------------------------------------
  // Batch size utility
  // ---------------------------------------------------------------------------

  private long getContainerCountPerCall(long totalContainerCount) {
    long hadoopRPCSize = ozoneConfiguration.getInt(
        IPC_MAXIMUM_DATA_LENGTH, IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    long countByRpcLimit = hadoopRPCSize / CONTAINER_ID_PROTO_SIZE_BYTES;
    long countByBatchLimit = ozoneConfiguration.getLong(
        OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE,
        OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE_DEFAULT);

    long batchSize = Math.min(countByRpcLimit, countByBatchLimit);
    return Math.min(totalContainerCount, batchSize);
  }

  /**
   * Uses the state-filtered container-ID list RPC as the source of truth for
   * targeted sync pagination, while the state-aware count RPC is used only to
   * avoid unnecessary list calls when SCM has no containers in the state.
   */
  private List<ContainerID> getContainerIDsByState(
      ContainerID startContainerId,
      HddsProtos.LifeCycleState state) throws IOException {
    long stateTotal = scmServiceProvider.getContainerCount(state);
    if (stateTotal == 0) {
      return Collections.emptyList();
    }
    long batchSize = stateTotal > 0
        ? getContainerCountPerCall(stateTotal)
        : getStatePaginationBatchSize();
    return scmServiceProvider.getListOfContainerIDs(
        startContainerId, (int) batchSize, state);
  }

  private long getStatePaginationBatchSize() {
    return getContainerCountPerCall(Long.MAX_VALUE);
  }
}
