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
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
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
 *   <li><b>DELETED:</b> paginates SCM's DELETED list using
 *       {@code getListOfContainerInfos}. For each container SCM reports as
 *       DELETED, Recon drives the container to DELETED in a single call. The
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
 *   <li>DELETED sync uses {@code getListOfContainerInfos} against SCM's
 *       DELETED list only, bounded by {@link #safeContainerInfoBatchSize(int)}.
 *       </li>
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
   *   Single-state CWP call (absent-container adds):
   *     128 MB / 1024 bytes = 131,072 containers per call
   *     (actual bytes: 131,072 × 490 ≈ 61 MB — well within limit)
   * </pre>
   *
   * @see #safeContainerWithPipelineBatchSize(int)
   */
  private static final long CONTAINER_WITH_PIPELINE_PROTO_SIZE_BYTES = 1024;

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

  ReconStorageContainerSyncHelper(StorageContainerServiceProvider scmServiceProvider,
                                  OzoneConfiguration ozoneConfiguration,
                                  ReconContainerManager containerManager) {
    this.scmServiceProvider = scmServiceProvider;
    this.ozoneConfiguration = ozoneConfiguration;
    this.containerManager = containerManager;
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
    try {
      long total = scmServiceProvider.getContainerCount(scmState);
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
      }

      LOG.info("{} sync complete from start {}, checked {}, added {}, reconciled {}.",
          scmState, initialStart, retrieved, addedCount, reconciledCount);
      return true;
    } catch (Exception e) {
      LOG.error("{} sync: unexpected error.", scmState, e);
      return false;
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
   * <p>Uses {@code getListOfContainerInfos} rather than
   * {@code getExistContainerWithPipelinesInBatch} because DELETED sync needs the
   * DELETED container metadata but does not need pipeline resolution.
   *
   * @return {@code true} if all RPC calls completed without error
   */
  private boolean syncDeletedContainers() {
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

      LOG.info("DELETED sync complete, retired={}.", retiredCount);
      return true;
    } catch (Exception e) {
      LOG.error("DELETED sync: unexpected error.", e);
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
          LOG.info("DELETED sync: added missing DELETED container {} "
              + "(full lifecycle occurred while Recon was down).", containerID);
        } catch (IOException e) {
          LOG.warn("DELETED sync: failed to add missing DELETED "
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

      LOG.info("DELETED sync: container {} transitioned "
          + "{} → DELETED in Recon (SCM state: {}).",
          containerID, reconInfo.getState(), scmState);
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
   * CLOSED and QUASI_CLOSED containers.
   *
   * @param containerID the container to add
   * @param state       the expected SCM lifecycle state (CLOSED or QUASI_CLOSED)
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
}
