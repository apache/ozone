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
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE_DEFAULT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
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
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that performs targeted incremental sync between SCM and Recon
 * container metadata. Each sync cycle scans the SCM states Recon can safely
 * reconcile (OPEN, QUASI_CLOSED, CLOSED and DELETED), all completing in a
 * single cycle with local pagination. SCM CLOSING and DELETING are skipped
 * deliberately because they are intermediate states.
 *
 * <ol>
 *   <li><b>OPEN:</b> scans only newly created OPEN containers starting from the
 *       last-seen ID ({@code pass2OpenStartContainerId}). Existing containers in
 *       later Recon states are not moved backwards to OPEN.</li>
 *   <li><b>QUASI_CLOSED and CLOSED:</b> paginate SCM state lists; add absent
 *       containers and advance existing Recon containers through valid local
 *       state-machine transitions. If Recon has DELETED but SCM reports one of
 *       these states, Recon rebuilds the container record from SCM metadata.</li>
 *   <li><b>DELETED:</b> paginates SCM's DELETED ID list. For IDs already in
 *       Recon, Recon drives the container to DELETED in a single call. Full
 *       {@code ContainerInfo} is fetched only for IDs missing from Recon. The
 *       DELETING list is intentionally skipped to avoid leaving Recon in an
 *       intermediate DELETING state across cycles.</li>
 * </ol>
 *
 * <h3>Scalability at 100M containers</h3>
 * <ul>
 *   <li>Live-state sync issues <b>one</b>
 *       {@code getExistContainerWithPipelinesInBatch} RPC per sub-batch of
 *       absent containers — not one per absent container.
 *       Sub-batch size is bounded by {@link #safeContainerWithPipelineBatchSize}
 *       to keep the CWP response within the 128 MB IPC limit.</li>
 *   <li>DELETED sync uses ID-only pagination for the common path and fetches
 *       {@code ContainerInfo} only for missing Recon entries.</li>
 * </ul>
 */
class ReconStorageContainerSyncHelper {

  /**
   * Wire size of one {@code ContainerID} proto (varint tag + 8-byte long ≈ 12 bytes).
   * Used to compute the maximum number of IDs that fit in one
   * {@code getListOfContainerIDs} RPC call, where both the request (IDs sent
   * to SCM) and the response (IDs returned by SCM) carry only ContainerID entries.
   * Applies to live-state pagination and DELETED ID lists
   * (DELETED ID list).
   */
  private static final long CONTAINER_ID_PROTO_SIZE_BYTES = 12;
  private static final long DELETED_SYNC_TRANSITION_LOG_SAMPLE_INTERVAL = 1000L;

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
   *   Single-state CWP call (absent-container adds):
   *     128 MB / 1024 bytes = 131,072 containers per call
   *     (actual bytes: 131,072 × 490 ≈ 61 MB — well within limit)
   * </pre>
   *
   * @see #safeContainerWithPipelineBatchSize(int)
   */
  private static final long CONTAINER_WITH_PIPELINE_PROTO_SIZE_BYTES = 1024;

  private static final int LIVE_STATE_SYNC_PROGRESS_LOG_INTERVAL = 50;

  /**
   * Monotonic cursor for OPEN add-only sync. OPEN containers are
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
  private final ReconScmContainerSyncMetrics containerSyncMetrics;

  ReconStorageContainerSyncHelper(StorageContainerServiceProvider scmServiceProvider,
                                  OzoneConfiguration ozoneConfiguration,
                                  ReconContainerManager containerManager,
                                  ReconScmContainerSyncMetrics containerSyncMetrics) {
    this.scmServiceProvider = scmServiceProvider;
    this.ozoneConfiguration = ozoneConfiguration;
    this.containerManager = containerManager;
    this.containerSyncMetrics =
        Objects.requireNonNull(containerSyncMetrics, "containerSyncMetrics");
  }

  /**
   * Runs targeted sync for SCM states Recon can safely reconcile.
   */
  public boolean syncWithSCMContainerInfo() {
    boolean open = syncContainersForState(HddsProtos.LifeCycleState.OPEN, true);
    boolean quasiClosed =
        syncContainersForState(HddsProtos.LifeCycleState.QUASI_CLOSED, false);
    boolean closed =
        syncContainersForState(HddsProtos.LifeCycleState.CLOSED, false);
    boolean deleted = syncDeletedContainers();
    return open && quasiClosed && closed && deleted;
  }

  /**
   * Paginates one SCM lifecycle state and reconciles each returned container ID.
   */
  private boolean syncContainersForState(HddsProtos.LifeCycleState scmState,
                                         boolean incrementalOpen) {
    long startTime = Time.monotonicNow();
    try {
      long total = scmServiceProvider.getContainerCount(scmState);
      updateContainerCountDrift(scmState, total);
      if (total == 0) {
        LOG.debug("{} sync: no containers found in SCM.", scmState);
        return true;
      }

      int batchSize = (int) getContainerCountPerCall(total);
      long initialStart = incrementalOpen ? pass2OpenStartContainerId.get() : 1L;
      ContainerID startContainerId = ContainerID.valueOf(initialStart);
      long retrieved = 0;
      int addedCount = 0;
      int reconciledCount = 0;
      int batchCount = 0;

      LOG.info("{} sync starting: total={}, batchSize={}, startId={}.",
          scmState, total, batchSize, initialStart);
      while (true) {
        List<ContainerID> batch = scmServiceProvider.getListOfContainerIDs(
            startContainerId, batchSize, scmState);
        if (batch == null || batch.isEmpty()) {
          break;
        }

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
              absentIds, scmState, scmState + " sync");
        }

        for (ContainerID containerID : presentIds) {
          reconciledCount += reconcileExistingContainer(containerID, scmState);
        }

        long lastID = batch.get(batch.size() - 1).getId();
        long nextID = lastID + 1;
        if (incrementalOpen) {
          pass2OpenStartContainerId.set(nextID);
        }
        startContainerId = ContainerID.valueOf(nextID);
        retrieved += batch.size();
        batchCount++;

        if (batchCount % LIVE_STATE_SYNC_PROGRESS_LOG_INTERVAL == 0) {
          LOG.info("{} sync progress: batch={}, totalRetrieved={}, added={}, "
                  + "reconciled={}, nextId={}.",
              scmState, batchCount, retrieved, addedCount, reconciledCount,
              nextID);
        }
      }

      LOG.info("{} sync complete from start {}, checked {}, added {}, reconciled {}.",
          scmState, initialStart, retrieved, addedCount, reconciledCount);
      return true;
    } catch (Exception e) {
      LOG.error("{} sync: unexpected error.", scmState, e);
      return false;
    } finally {
      updateContainerSyncDuration(scmState, Time.monotonicNow() - startTime);
    }
  }

  private int reconcileExistingContainer(ContainerID containerID,
                                         HddsProtos.LifeCycleState scmState) {
    try {
      ContainerInfo reconContainer = containerManager.getContainer(containerID);
      HddsProtos.LifeCycleState reconState = reconContainer.getState();
      if (reconState == scmState) {
        return 0;
      }

      switch (scmState) {
      case OPEN:
        LOG.debug("Skipping container {} because SCM reports OPEN while Recon "
            + "already has state {}.", containerID, reconState);
        return 0;
      case QUASI_CLOSED:
        return reconcileToQuasiClosed(containerID, reconContainer, reconState);
      case CLOSED:
        return reconcileToClosed(containerID, reconContainer, reconState);
      default:
        LOG.debug("Skipping container {} for unsupported SCM sync state {}.",
            containerID, scmState);
        return 0;
      }
    } catch (ContainerNotFoundException e) {
      LOG.debug("Container {} vanished from Recon during {} sync.",
          containerID, scmState);
    }
    return 0;
  }

  private int reconcileToQuasiClosed(ContainerID containerID,
                                     ContainerInfo reconContainer,
                                     HddsProtos.LifeCycleState reconState) {
    try {
      if (reconState == HddsProtos.LifeCycleState.DELETED) {
        return rebuildContainerFromScm(containerID,
            HddsProtos.LifeCycleState.QUASI_CLOSED);
      }
      if (reconState == HddsProtos.LifeCycleState.OPEN) {
        containerManager.transitionOpenToClosing(containerID, reconContainer);
        reconState = HddsProtos.LifeCycleState.CLOSING;
      }
      if (reconState == HddsProtos.LifeCycleState.CLOSING) {
        containerManager.updateContainerState(containerID, QUASI_CLOSE);
        LOG.info("Container {} corrected to QUASI_CLOSED based on SCM state.",
            containerID);
        return 1;
      }
      LOG.debug("Skipping container {} because SCM reports QUASI_CLOSED while "
          + "Recon has state {}.", containerID, reconState);
    } catch (InvalidStateTransitionException | IOException e) {
      LOG.warn("Failed to reconcile container {} to QUASI_CLOSED.",
          containerID, e);
    }
    return 0;
  }

  private int reconcileToClosed(ContainerID containerID,
                                ContainerInfo reconContainer,
                                HddsProtos.LifeCycleState reconState) {
    try {
      if (reconState == HddsProtos.LifeCycleState.DELETED) {
        return rebuildContainerFromScm(containerID, HddsProtos.LifeCycleState.CLOSED);
      }
      if (reconState == HddsProtos.LifeCycleState.OPEN) {
        containerManager.transitionOpenToClosing(containerID, reconContainer);
        reconState = HddsProtos.LifeCycleState.CLOSING;
      }
      if (reconState == HddsProtos.LifeCycleState.CLOSING) {
        containerManager.updateContainerState(containerID, CLOSE);
        LOG.info("Container {} corrected to CLOSED based on SCM state.",
            containerID);
        return 1;
      }
      if (reconState == HddsProtos.LifeCycleState.QUASI_CLOSED) {
        containerManager.updateContainerState(containerID, FORCE_CLOSE);
        LOG.info("Container {} corrected from QUASI_CLOSED to CLOSED based "
            + "on SCM state.", containerID);
        return 1;
      }
      LOG.debug("Skipping container {} because SCM reports CLOSED while Recon "
          + "has state {}.", containerID, reconState);
    } catch (InvalidStateTransitionException | IOException e) {
      LOG.warn("Failed to reconcile container {} to CLOSED.", containerID, e);
    }
    return 0;
  }

  private int rebuildContainerFromScm(ContainerID containerID,
                                      HddsProtos.LifeCycleState scmState) {
    try {
      List<ContainerInfo> infos = scmServiceProvider.getListOfContainerInfos(
          containerID, 1, scmState);
      if (infos.isEmpty() || !infos.get(0).containerID().equals(containerID)) {
        LOG.debug("Container {} no longer in SCM state {}; skipping rebuild.",
            containerID, scmState);
        return 0;
      }
      containerManager.deleteContainer(containerID);
      containerManager.addNewContainer(new ContainerWithPipeline(infos.get(0), null));
      LOG.info("Rebuilt container {} in Recon from DELETED to SCM state {}.",
          containerID, scmState);
      return 1;
    } catch (IOException e) {
      LOG.warn("Failed to rebuild container {} from SCM state {}.",
          containerID, scmState, e);
      return 0;
    }
  }

  // ---------------------------------------------------------------------------
  // DELETED sync — SCM-driven, transition only for existing containers.
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
   * <p>Uses ID-only pagination for the common path. Full {@code ContainerInfo}
   * is fetched only for IDs absent from Recon, where adding the missing terminal
   * entry needs SCM's authoritative metadata.
   *
   * @return {@code true} if all RPC calls completed without error
   */
  private boolean syncDeletedContainers() {
    long startTime = Time.monotonicNow();
    try {
      updateDeletedContainerCountDrift();
      int configuredBatch = ozoneConfiguration.getInt(
          OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE,
          OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE_DEFAULT);
      int batchSize = (int) getContainerCountPerCall(configuredBatch);
      int retiredCount = 0;
      long processedCount = 0;

      // Existing Recon containers need only the ID to retire to DELETED. Fetch
      // full ContainerInfo only for IDs absent from Recon, where we must add a
      // missing terminal record with SCM's actual replication metadata.
      //
      // We do NOT scan the DELETING list: processing DELETING would drive Recon
      // to an intermediate DELETING state across cycles (stuck). We wait for SCM
      // to confirm full deletion (DELETED) and then retire atomically.
      ContainerID start = ContainerID.valueOf(1);
      while (true) {
        List<ContainerID> page = scmServiceProvider.getListOfContainerIDs(
            start, batchSize, HddsProtos.LifeCycleState.DELETED);
        if (page == null || page.isEmpty()) {
          break;
        }
        retiredCount += processDeletedPage(page, processedCount);
        processedCount += page.size();
        start = ContainerID.valueOf(
            page.get(page.size() - 1).getId() + 1);
      }

      LOG.info("DELETED sync complete, retired={}.", retiredCount);
      return true;
    } catch (Exception e) {
      LOG.error("DELETED sync: unexpected error.", e);
      return false;
    } finally {
      updateContainerSyncDuration(HddsProtos.LifeCycleState.DELETED,
          Time.monotonicNow() - startTime);
    }
  }

  private void updateDeletedContainerCountDrift() {
    try {
      long total = scmServiceProvider.getContainerCount(
          HddsProtos.LifeCycleState.DELETED);
      updateContainerCountDrift(HddsProtos.LifeCycleState.DELETED, total);
    } catch (Exception e) {
      LOG.warn("DELETED sync: unable to update pre-sync count drift metric.", e);
    }
  }

  private void updateContainerCountDrift(HddsProtos.LifeCycleState state,
                                         long scmCount) {
    long reconCount = containerManager.getContainerStateCount(state);
    containerSyncMetrics.setContainerCountDrift(state,
        scmCount - reconCount);
  }

  private void updateContainerSyncDuration(HddsProtos.LifeCycleState state,
                                           long durationMs) {
    containerSyncMetrics.setContainerSyncDurationMs(state, durationMs);
  }

  /**
   * Processes one page of DELETED container IDs from SCM.
   * For each container:
   * <ul>
   *   <li>If absent from Recon: fetches full {@link ContainerInfo} from SCM and
   *       adds it (preserving the actual replication config — RATIS or EC).</li>
   *   <li>If present in Recon in a non-terminal state: drives it to DELETED.</li>
   *   <li>If already DELETED in Recon: no-op.</li>
   * </ul>
   */
  private int processDeletedPage(List<ContainerID> page,
                                 long processedCountBeforePage) {
    int retiredCount = 0;
    long processedCount = processedCountBeforePage;
    for (ContainerID containerID : page) {
      processedCount++;
      if (!containerManager.containerExist(containerID)) {
        if (addContainerInfoFallback(containerID,
            HddsProtos.LifeCycleState.DELETED, "DELETED sync")) {
          retiredCount++;
        }
        continue;
      }
      try {
        ContainerInfo reconInfo = containerManager.getContainer(containerID);
        if (reconInfo.getState() != HddsProtos.LifeCycleState.DELETED) {
          retireContainerToDeleted(containerID, reconInfo,
              HddsProtos.LifeCycleState.DELETED, processedCount);
          retiredCount++;
        }
        // reconState == DELETED: already terminal, nothing to do.
      } catch (ContainerNotFoundException e) {
        LOG.debug("DELETED sync: container {} vanished from Recon "
            + "between existence check and retirement.", containerID);
      }
    }
    return retiredCount;
  }

  /**
   * Drives a container in Recon from any non-terminal lifecycle state to
   * DELETED by applying the minimum valid state machine transitions.
   *
   * <p>This handles all states that can arrive while processing SCM's DELETED
   * list:
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
   * @param processedCount current number of SCM DELETED IDs scanned in this
   *                       sync cycle
   */
  private void retireContainerToDeleted(ContainerID containerID,
                                        ContainerInfo reconInfo,
                                        HddsProtos.LifeCycleState scmState,
                                        long processedCount) {
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

      if (processedCount % DELETED_SYNC_TRANSITION_LOG_SAMPLE_INTERVAL == 0) {
        LOG.debug("DELETED sync: container {} transitioned "
            + "{} → DELETED in Recon (SCM state: {}).",
            containerID, reconInfo.getState(), scmState);
      }
    } catch (InvalidStateTransitionException | IOException e) {
      LOG.warn("DELETED sync: failed to retire container {} "
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
   *       supported non-OPEN state.</li>
   * </ul>
   *
   * @param absentIds IDs confirmed absent from Recon (may be up to 1M)
   * @param state     lifecycle state of all IDs in {@code absentIds}
   * @param passLabel log prefix
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
   * CLOSED, QUASI_CLOSED, and DELETED containers.
   *
   * @param containerID the container to add
   * @param state       the expected SCM lifecycle state
   * @param passLabel   logging label
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
      LOG.info("{} ({}): added container {} using ContainerInfo fallback.",
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
   * <p>Applies to live-state pagination.
   *
   * @param upperBound cap on the returned batch size; pass the total container
   *                   count in a state when paginating that state, or a
   *                   configured batch size limit when the caller owns the
   *                   upper bound (e.g. DELETED sync uses the configured deleted-check
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
}
