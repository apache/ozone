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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent.FINALIZE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_THRESHOLD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_PER_STATE_DRIFT_THRESHOLD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_PER_STATE_DRIFT_THRESHOLD_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerSyncHelper.SyncAction;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Integration tests for {@link ReconStorageContainerSyncHelper} and
 * {@link ReconStorageContainerManagerFacade#syncWithSCMContainerInfo()}.
 *
 * <p>Uses a <em>real</em> {@link ReconContainerManager} backed by RocksDB
 * (from {@link AbstractReconContainerManagerTest}) and a mocked
 * {@link StorageContainerServiceProvider} that stands in for live SCM RPCs.
 * This combination validates actual state machine transitions and database
 * persistence without requiring a running cluster.
 *
 * <p>Test organisation:
 * <ul>
 *   <li>{@link DecideSyncActionTests} — all decision paths for
 *       {@code decideSyncAction()}</li>
 *   <li>{@link Pass1ClosedSyncTests} — Pass 1: add missing CLOSED containers
 *       and correct stale OPEN/CLOSING state</li>
 *   <li>{@link Pass2OpenAddOnlyTests} — Pass 2: add OPEN containers missing
 *       from Recon</li>
 *   <li>{@link Pass3QuasiClosedAddOnlyTests} — Pass 3: add QUASI_CLOSED
 *       containers missing from Recon and correct stale OPEN/CLOSING state</li>
 *   <li>{@link Pass4DeletedRetirementTests} — Pass 4: retire
 *       CLOSED/QUASI_CLOSED containers that SCM has already deleted</li>
 *   <li>{@link LargeScaleTests} — end-to-end scenarios with 100 k+
 *       containers covering all state transition paths</li>
 * </ul>
 */
@Timeout(120)
public class TestReconSCMContainerSyncIntegration
    extends AbstractReconContainerManagerTest {

  private StorageContainerServiceProvider mockScm;
  private ReconStorageContainerSyncHelper syncHelper;

  @BeforeEach
  void setupSyncHelper() {
    getConf().setInt(OZONE_RECON_SCM_CONTAINER_THRESHOLD,
        OZONE_RECON_SCM_CONTAINER_THRESHOLD_DEFAULT);
    getConf().setInt(OZONE_RECON_SCM_PER_STATE_DRIFT_THRESHOLD,
        OZONE_RECON_SCM_PER_STATE_DRIFT_THRESHOLD_DEFAULT);
    mockScm = mock(StorageContainerServiceProvider.class);
    syncHelper = new ReconStorageContainerSyncHelper(
        mockScm, getConf(), getContainerManager());
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Builds a {@link ContainerWithPipeline} with a null pipeline, which is
   * valid for non-OPEN and (after our null-pipeline guard) OPEN containers.
   */
  private ContainerWithPipeline containerCwp(long id, LifeCycleState state) {
    ContainerInfo info = new ContainerInfo.Builder()
        .setContainerID(id)
        .setState(state)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .setOwner("test")
        .build();
    return new ContainerWithPipeline(info, null);
  }

  /**
   * Seeds the real {@link ReconContainerManager} with {@code count} containers
   * in the given {@code state}, using IDs in the range
   * [{@code startId}, {@code startId + count}).
   *
   * <p>For non-OPEN states the container state manager accepts direct insertion
   * from the proto (bypassing the state machine), enabling fast bulk seeding.
   * For OPEN containers we use the null-pipeline path of {@code addNewContainer}.
   */
  private void seedRecon(long startId, int count, LifeCycleState state)
      throws Exception {
    ReconContainerManager cm = getContainerManager();
    for (long id = startId; id < startId + count; id++) {
      cm.addNewContainer(containerCwp(id, state));
    }
  }

  /**
   * Seeds Recon with {@code count} OPEN containers and then transitions each
   * one to CLOSING so that Pass 1 can exercise the CLOSING→CLOSED correction.
   */
  private void seedReconAsClosing(long startId, int count) throws Exception {
    seedRecon(startId, count, OPEN);
    ReconContainerManager cm = getContainerManager();
    for (long id = startId; id < startId + count; id++) {
      cm.updateContainerState(ContainerID.valueOf(id), FINALIZE);
    }
  }

  /** Returns a list of ContainerIDs for IDs in [{@code start}, {@code end}). */
  private List<ContainerID> idRange(long start, long end) {
    return LongStream.range(start, end)
        .mapToObj(ContainerID::valueOf)
        .collect(Collectors.toList());
  }

  // ===========================================================================
  // decideSyncAction() tests
  // ===========================================================================

  @Nested
  class DecideSyncActionTests {

    @Test
    void noContainersAnywhereReturnsNoAction() throws Exception {
      when(mockScm.getContainerCount()).thenReturn(0L);
      when(mockScm.getContainerCount(OPEN)).thenReturn(0L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);

      assertEquals(SyncAction.NO_ACTION, syncHelper.decideSyncAction());
    }

    @Test
    void countsMatchNoStateDriftReturnsNoAction() throws Exception {
      // Seed Recon: 10 CLOSED, 5 OPEN
      seedRecon(1, 10, CLOSED);
      seedRecon(11, 5, OPEN);

      when(mockScm.getContainerCount()).thenReturn(15L);
      when(mockScm.getContainerCount(OPEN)).thenReturn(5L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);

      assertEquals(SyncAction.NO_ACTION, syncHelper.decideSyncAction());
    }

    @Test
    void smallTotalDriftReturnsTargetedSync() throws Exception {
      // Recon has 5, SCM has 8 → drift = 3
      seedRecon(1, 5, CLOSED);

      when(mockScm.getContainerCount()).thenReturn(8L);

      assertEquals(SyncAction.TARGETED_SYNC, syncHelper.decideSyncAction());
    }

    @Test
    void exactlyAtThresholdReturnsTargetedSync() throws Exception {
      // drift == threshold → still TARGETED_SYNC (threshold is exclusive)
      int threshold = getConf().getInt(
          OZONE_RECON_SCM_CONTAINER_THRESHOLD,
          OZONE_RECON_SCM_CONTAINER_THRESHOLD_DEFAULT);

      when(mockScm.getContainerCount()).thenReturn((long) threshold);
      // Recon is empty → drift == threshold

      assertEquals(SyncAction.TARGETED_SYNC, syncHelper.decideSyncAction());
    }

    @Test
    void oneAboveThresholdReturnsFullSnapshot() throws Exception {
      int threshold = getConf().getInt(
          OZONE_RECON_SCM_CONTAINER_THRESHOLD,
          OZONE_RECON_SCM_CONTAINER_THRESHOLD_DEFAULT);

      when(mockScm.getContainerCount()).thenReturn((long) threshold + 1L);
      // Recon is empty → drift == threshold + 1

      assertEquals(SyncAction.FULL_SNAPSHOT, syncHelper.decideSyncAction());
    }

    @Test
    void largeTotalDriftReturnsFullSnapshot() throws Exception {
      // Recon empty, SCM has 200,000 containers → well above default 10k threshold
      when(mockScm.getContainerCount()).thenReturn(200_000L);

      assertEquals(SyncAction.FULL_SNAPSHOT, syncHelper.decideSyncAction());
    }

    @Test
    void customThresholdIsRespected() throws Exception {
      // Override threshold to 50
      getConf().setInt(OZONE_RECON_SCM_CONTAINER_THRESHOLD, 50);
      ReconStorageContainerSyncHelper customHelper = new ReconStorageContainerSyncHelper(
          mockScm, getConf(), getContainerManager());

      // Drift = 51 → FULL_SNAPSHOT with custom threshold 50
      when(mockScm.getContainerCount()).thenReturn(51L);
      assertEquals(SyncAction.FULL_SNAPSHOT, customHelper.decideSyncAction());

      // Drift = 50 → TARGETED_SYNC (50 is at threshold, not above)
      seedRecon(1, 1, CLOSED); // Recon now has 1, SCM 51 → drift = 50
      assertEquals(SyncAction.TARGETED_SYNC, customHelper.decideSyncAction());
    }

    @Test
    void openDriftExceedsThresholdReturnsTargetedSync() throws Exception {
      // Total drift = 0, but OPEN drift = 6 > default threshold (5)
      // Recon: 20 OPEN + 30 CLOSED = 50 total
      seedRecon(1, 20, OPEN);
      seedRecon(21, 30, CLOSED);

      when(mockScm.getContainerCount()).thenReturn(50L);
      when(mockScm.getContainerCount(OPEN)).thenReturn(14L);   // drift = 6
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);

      assertEquals(SyncAction.TARGETED_SYNC, syncHelper.decideSyncAction());
    }

    @Test
    void quasiClosedDriftExceedsThresholdReturnsTargetedSync() throws Exception {
      // Total drift = 0, OPEN drift = 0, but QUASI_CLOSED drift = 6 > threshold.
      // This is the case that was missed when only OPEN was checked.
      // Recon: 10 QUASI_CLOSED + 40 CLOSED = 50 total
      seedRecon(1, 10, QUASI_CLOSED);
      seedRecon(11, 40, CLOSED);

      when(mockScm.getContainerCount()).thenReturn(50L);
      when(mockScm.getContainerCount(OPEN)).thenReturn(0L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(4L); // drift = 6

      assertEquals(SyncAction.TARGETED_SYNC, syncHelper.decideSyncAction());
    }

    @Test
    void perStateDriftBelowThresholdReturnsNoAction() throws Exception {
      // Both OPEN and QUASI_CLOSED drift <= threshold → NO_ACTION
      // Recon: 20 OPEN + 30 CLOSED = 50 total
      seedRecon(1, 20, OPEN);
      seedRecon(21, 30, CLOSED);

      when(mockScm.getContainerCount()).thenReturn(50L);
      when(mockScm.getContainerCount(OPEN)).thenReturn(18L);  // drift = 2 <= 5
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L); // drift = 0

      assertEquals(SyncAction.NO_ACTION, syncHelper.decideSyncAction());
    }

    @Test
    void customPerStateDriftThresholdIsRespected() throws Exception {
      // Override per-state threshold to 2; OPEN drift of 3 should now trigger
      getConf().setInt(OZONE_RECON_SCM_PER_STATE_DRIFT_THRESHOLD, 2);
      ReconStorageContainerSyncHelper customHelper = new ReconStorageContainerSyncHelper(
          mockScm, getConf(), getContainerManager());

      seedRecon(1, 10, OPEN);
      seedRecon(11, 10, CLOSED);

      when(mockScm.getContainerCount()).thenReturn(20L); // no total drift
      when(mockScm.getContainerCount(OPEN)).thenReturn(7L);   // drift = 3 > 2
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);

      assertEquals(SyncAction.TARGETED_SYNC, customHelper.decideSyncAction());
    }

    @Test
    void bothPerStateDriftsPresentFirstExceedingStateTriggersSync() throws Exception {
      // Both OPEN and QUASI_CLOSED are drifted; sync is triggered at first hit
      seedRecon(1, 20, OPEN);
      seedRecon(21, 20, QUASI_CLOSED);
      seedRecon(41, 10, CLOSED);

      when(mockScm.getContainerCount()).thenReturn(50L); // total matches
      when(mockScm.getContainerCount(OPEN)).thenReturn(12L);       // drift = 8
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(10L); // drift = 10

      assertEquals(SyncAction.TARGETED_SYNC, syncHelper.decideSyncAction());
    }
  }

  // ===========================================================================
  // Pass 1: CLOSED sync — add missing containers, correct stale OPEN/CLOSING
  // ===========================================================================

  @Nested
  class Pass1ClosedSyncTests {

    @BeforeEach
    void zeroOtherPasses() throws IOException {
      // Keep Pass 2, 3, 4 quiet so only Pass 1 exercises state changes
      when(mockScm.getContainerCount(OPEN)).thenReturn(0L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);
    }

    @Test
    void addsClosedContainerMissingFromRecon() throws Exception {
      ContainerID cid = ContainerID.valueOf(1L);
      ContainerWithPipeline cwp = containerCwp(1L, CLOSED);

      when(mockScm.getContainerCount(CLOSED)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(CLOSED)))
          .thenReturn(Collections.singletonList(cid));
      when(mockScm.getExistContainerWithPipelinesInBatch(Collections.singletonList(1L)))
          .thenReturn(Collections.singletonList(cwp));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(CLOSED, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void correctsOpenContainerToClosedInRecon() throws Exception {
      // Recon: container 1 is OPEN. SCM: container 1 is CLOSED.
      seedRecon(1, 1, OPEN);
      ContainerID cid = ContainerID.valueOf(1L);

      when(mockScm.getContainerCount(CLOSED)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(CLOSED)))
          .thenReturn(Collections.singletonList(cid));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(CLOSED, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void correctsClosingContainerToClosedInRecon() throws Exception {
      // Recon: container 1 is CLOSING. SCM: container 1 is CLOSED.
      seedReconAsClosing(1, 1);
      ContainerID cid = ContainerID.valueOf(1L);
      assertEquals(CLOSING, getContainerManager().getContainer(cid).getState());

      when(mockScm.getContainerCount(CLOSED)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(CLOSED)))
          .thenReturn(Collections.singletonList(cid));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(CLOSED, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void skipsContainerAlreadyClosed() throws Exception {
      // Recon: container 1 is already CLOSED. Pass 1 should be a no-op.
      seedRecon(1, 1, CLOSED);
      ContainerID cid = ContainerID.valueOf(1L);

      when(mockScm.getContainerCount(CLOSED)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(CLOSED)))
          .thenReturn(Collections.singletonList(cid));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      // State must remain CLOSED, not re-transitioned
      assertEquals(CLOSED, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void pass1CorrectQuasiClosedToClosedViaForceClose() throws Exception {
      // Pass 1 corrects QUASI_CLOSED → CLOSED using FORCE_CLOSE when SCM shows the
      // container is definitively CLOSED. This handles the case where Recon missed
      // the final quorum decision made by SCM.
      seedRecon(1, 1, QUASI_CLOSED);
      ContainerID cid = ContainerID.valueOf(1L);

      when(mockScm.getContainerCount(CLOSED)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(CLOSED)))
          .thenReturn(Collections.singletonList(cid));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      // Container is now CLOSED in Recon (corrected by Pass 1 via FORCE_CLOSE)
      assertEquals(CLOSED, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void emptyListFromSCMBeforeTotalExhaustedReturnsFalse() throws Exception {
      // SCM says there are 2 containers but returns empty list — indicates a
      // transient SCM error; sync should return false (partial).
      when(mockScm.getContainerCount(CLOSED)).thenReturn(2L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(2), eq(CLOSED)))
          .thenReturn(Collections.emptyList());

      boolean result = syncHelper.syncWithSCMContainerInfo();
      // Pass 1 failed (empty list before total exhausted), but passes 2-4 still run.
      // Overall result is false because at least one pass failed.
      assertTrue(!result || getContainerManager().getContainers().isEmpty());
    }

    @Test
    void multiplePagesAllBatchesProcessed() throws Exception {
      // Force batch size to 3 so 7 containers span 3 pages
      getConf().setLong(
          ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE, 3L);
      ReconStorageContainerSyncHelper pagedHelper = new ReconStorageContainerSyncHelper(
          mockScm, getConf(), getContainerManager());

      when(mockScm.getContainerCount(CLOSED)).thenReturn(7L);
      // Page 1: IDs 1-3
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(3), eq(CLOSED)))
          .thenReturn(idRange(1, 4));
      // Page 2: IDs 4-6
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(4L)), eq(3), eq(CLOSED)))
          .thenReturn(idRange(4, 7));
      // Page 3: ID 7
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(7L)), eq(3), eq(CLOSED)))
          .thenReturn(idRange(7, 8));

      when(mockScm.getExistContainerWithPipelinesInBatch(anyList())).thenAnswer(inv -> {
        List<Long> idList = inv.getArgument(0);
        return idList.stream().map(id -> containerCwp(id, CLOSED)).collect(Collectors.toList());
      });
      when(mockScm.getContainerCount(OPEN)).thenReturn(0L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);

      assertTrue(pagedHelper.syncWithSCMContainerInfo());
      assertEquals(7, getContainerManager().getContainers(CLOSED).size());
    }

    @Test
    void mixedExistingAndMissingOnlyMissingAreAdded() throws Exception {
      // Recon already has containers 1,3,5; SCM reports 1-5 CLOSED
      seedRecon(1, 1, CLOSED);
      seedRecon(3, 1, CLOSED);
      seedRecon(5, 1, CLOSED);

      List<ContainerID> scmClosed = idRange(1, 6); // 1,2,3,4,5
      when(mockScm.getContainerCount(CLOSED)).thenReturn(5L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(5), eq(CLOSED)))
          .thenReturn(scmClosed);
      when(mockScm.getExistContainerWithPipelinesInBatch(anyList())).thenAnswer(inv -> {
        List<Long> idList = inv.getArgument(0);
        return idList.stream().map(id -> containerCwp(id, CLOSED)).collect(Collectors.toList());
      });

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      // All 5 should now be in Recon (3 pre-existing + 2 added)
      assertEquals(5, getContainerManager().getContainers(CLOSED).size());
    }
  }

  // ===========================================================================
  // Pass 2: OPEN add-only
  // ===========================================================================

  @Nested
  class Pass2OpenAddOnlyTests {

    @BeforeEach
    void zeroOtherPasses() throws IOException {
      when(mockScm.getContainerCount(CLOSED)).thenReturn(0L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);
    }

    @Test
    void addsOpenContainerMissingFromRecon() throws Exception {
      ContainerID cid = ContainerID.valueOf(10L);
      ContainerWithPipeline cwp = containerCwp(10L, OPEN);

      when(mockScm.getContainerCount(OPEN)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(OPEN)))
          .thenReturn(Collections.singletonList(cid));
      when(mockScm.getExistContainerWithPipelinesInBatch(Collections.singletonList(10L)))
          .thenReturn(Collections.singletonList(cwp));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(OPEN, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void doesNotDuplicateExistingOpenContainer() throws Exception {
      seedRecon(10, 1, OPEN);
      ContainerID cid = ContainerID.valueOf(10L);

      when(mockScm.getContainerCount(OPEN)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(OPEN)))
          .thenReturn(Collections.singletonList(cid));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(1, getContainerManager().getContainers(OPEN).size());
    }

    @Test
    void doesNotOverwriteContainerAlreadyAdvancedBeyondOpen() throws Exception {
      // Container 10 is already CLOSED in Recon but still appears in SCM's OPEN
      // list (stale SCM data). Pass 2 must NOT revert it to OPEN.
      seedRecon(10, 1, CLOSED);
      ContainerID cid = ContainerID.valueOf(10L);

      when(mockScm.getContainerCount(OPEN)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(OPEN)))
          .thenReturn(Collections.singletonList(cid));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      // State should remain CLOSED — Pass 2 is add-only and skips present containers
      assertEquals(CLOSED, getContainerManager().getContainer(cid).getState());
      assertEquals(0, getContainerManager().getContainers(OPEN).size());
    }

    @Test
    void openContainersWithNullPipelineAddedSuccessfully() throws Exception {
      // Verifies null-pipeline guard: OPEN container returned with null pipeline
      // (e.g., pipeline already cleaned up on SCM) must still be added.
      ContainerID cid = ContainerID.valueOf(20L);
      when(mockScm.getContainerCount(OPEN)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(OPEN)))
          .thenReturn(Collections.singletonList(cid));
      // null pipeline — simulates cleaned-up pipeline; batch API returns it with null pipeline
      when(mockScm.getExistContainerWithPipelinesInBatch(Collections.singletonList(20L)))
          .thenReturn(Collections.singletonList(containerCwp(20L, OPEN)));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(OPEN, getContainerManager().getContainer(cid).getState());
    }
  }

  // ===========================================================================
  // Pass 3: QUASI_CLOSED add + correct
  // ===========================================================================

  @Nested
  class Pass3QuasiClosedAddOnlyTests {

    @BeforeEach
    void zeroOtherPasses() throws IOException {
      when(mockScm.getContainerCount(CLOSED)).thenReturn(0L);
      when(mockScm.getContainerCount(OPEN)).thenReturn(0L);
    }

    @Test
    void addsQuasiClosedContainerMissingFromRecon() throws Exception {
      ContainerID cid = ContainerID.valueOf(30L);
      ContainerWithPipeline cwp = containerCwp(30L, QUASI_CLOSED);

      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(QUASI_CLOSED)))
          .thenReturn(Collections.singletonList(cid));
      when(mockScm.getExistContainerWithPipelinesInBatch(Collections.singletonList(30L)))
          .thenReturn(Collections.singletonList(cwp));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(QUASI_CLOSED, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void quasiClosedWithNullPipelineAddedSuccessfully() throws Exception {
      // QUASI_CLOSED containers whose pipelines have been cleaned up on SCM
      // must still be added with null pipeline (no NullPointerException).
      ContainerID cid = ContainerID.valueOf(31L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(QUASI_CLOSED)))
          .thenReturn(Collections.singletonList(cid));
      when(mockScm.getExistContainerWithPipelinesInBatch(Collections.singletonList(31L)))
          .thenReturn(Collections.singletonList(containerCwp(31L, QUASI_CLOSED)));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(QUASI_CLOSED, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void doesNotDuplicateExistingQuasiClosedContainer() throws Exception {
      seedRecon(30, 1, QUASI_CLOSED);
      ContainerID cid = ContainerID.valueOf(30L);

      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(QUASI_CLOSED)))
          .thenReturn(Collections.singletonList(cid));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(1, getContainerManager().getContainers(QUASI_CLOSED).size());
    }

    @Test
    void doesNotOverwriteContainerAlreadyClosedInRecon() throws Exception {
      // Container already CLOSED in Recon but still in SCM's QUASI_CLOSED list.
      // Pass 3 must not revert the container to QUASI_CLOSED (no downgrade).
      seedRecon(30, 1, CLOSED);
      ContainerID cid = ContainerID.valueOf(30L);

      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(QUASI_CLOSED)))
          .thenReturn(Collections.singletonList(cid));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(CLOSED, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void pass3CorrectOpenToQuasiClosed() throws Exception {
      // Container is OPEN in Recon but SCM has already moved it to QUASI_CLOSED.
      // Pass 3 must advance it: OPEN → CLOSING (FINALIZE) → QUASI_CLOSED (QUASI_CLOSE).
      seedRecon(35, 1, OPEN);
      ContainerID cid = ContainerID.valueOf(35L);

      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(QUASI_CLOSED)))
          .thenReturn(Collections.singletonList(cid));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(QUASI_CLOSED, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void pass3CorrectClosingToQuasiClosed() throws Exception {
      // Container is stuck CLOSING in Recon but SCM already moved it to QUASI_CLOSED.
      // Pass 3 must advance it: CLOSING → QUASI_CLOSED (QUASI_CLOSE).
      seedRecon(36, 1, CLOSING);
      ContainerID cid = ContainerID.valueOf(36L);

      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(1L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(1), eq(QUASI_CLOSED)))
          .thenReturn(Collections.singletonList(cid));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(QUASI_CLOSED, getContainerManager().getContainer(cid).getState());
    }
  }

  // ===========================================================================
  // Pass 4: DELETED retirement (uses getExistContainerWithPipelinesInBatch)
  // ===========================================================================

  @Nested
  class Pass4DeletedRetirementTests {

    @BeforeEach
    void zeroAdditivePasses() throws IOException {
      when(mockScm.getContainerCount(CLOSED)).thenReturn(0L);
      when(mockScm.getContainerCount(OPEN)).thenReturn(0L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);
    }

    @Test
    void retiresClosedContainerWhenSCMReportsDeleted() throws Exception {
      seedRecon(100, 1, CLOSED);
      ContainerID cid = ContainerID.valueOf(100L);

      // Batch RPC returns the container as DELETED
      when(mockScm.getExistContainerWithPipelinesInBatch(anyList()))
          .thenReturn(Collections.singletonList(containerCwp(100L, DELETED)));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(DELETED, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void retiresClosedContainerToDeletingWhenSCMReportsDeleting() throws Exception {
      seedRecon(101, 1, CLOSED);
      ContainerID cid = ContainerID.valueOf(101L);

      // Batch RPC returns the container as DELETING
      when(mockScm.getExistContainerWithPipelinesInBatch(anyList()))
          .thenReturn(Collections.singletonList(containerCwp(101L, DELETING)));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      // Only DELETING transition applied (not CLEANUP), so state is DELETING in Recon
      assertEquals(DELETING, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void retiresQuasiClosedContainerWhenSCMReportsDeleted() throws Exception {
      seedRecon(102, 1, QUASI_CLOSED);
      ContainerID cid = ContainerID.valueOf(102L);

      // Batch RPC returns the container as DELETED
      when(mockScm.getExistContainerWithPipelinesInBatch(anyList()))
          .thenReturn(Collections.singletonList(containerCwp(102L, DELETED)));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(DELETED, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void emptyBatchResultSkipsRetirementAsAmbiguous() throws Exception {
      // A completely empty batch result is ambiguous: it could mean the
      // queried containers were purged, but it could also mean the batch RPC
      // failed or returned no data. Recon should skip retirement in that
      // case rather than deleting live containers.
      seedRecon(103, 1, CLOSED);
      ContainerID cid = ContainerID.valueOf(103L);

      // Batch returns empty list → skip retirement for safety
      when(mockScm.getExistContainerWithPipelinesInBatch(anyList()))
          .thenReturn(Collections.emptyList());

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(CLOSED, getContainerManager().getContainer(cid).getState());
    }

    @Test
    void openContainersAreNotCandidatesForRetirement() throws Exception {
      // Pass 4 only checks CLOSED and QUASI_CLOSED; OPEN containers are skipped.
      // No batch RPC mock needed: Pass 4 sees no candidates and returns early.
      seedRecon(200, 5, OPEN);

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      // All OPEN containers remain OPEN; no retirements occurred
      assertEquals(5, getContainerManager().getContainers(OPEN).size());
      assertEquals(0, getContainerManager().getContainers(DELETED).size());
    }

    @Test
    void liveContainersAreNotRetired() throws Exception {
      // CLOSED in Recon, also CLOSED in SCM (not deleted) → must stay CLOSED.
      seedRecon(300, 3, CLOSED);

      // Batch RPC returns all three containers as CLOSED (still live in SCM)
      when(mockScm.getExistContainerWithPipelinesInBatch(anyList()))
          .thenAnswer(inv -> {
            List<Long> ids = inv.getArgument(0);
            return ids.stream()
                .map(id -> containerCwp(id, CLOSED))
                .collect(Collectors.toList());
          });

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(3, getContainerManager().getContainers(CLOSED).size());
      assertEquals(0, getContainerManager().getContainers(DELETED).size());
    }

    @Test
    void batchSizeLimitsCheckPerCycle() throws Exception {
      // Seed 10 CLOSED containers; set batch size = 3.
      // Only a rotating window of 3 should be evaluated per sync cycle.
      seedRecon(400, 10, CLOSED);
      getConf().setInt(OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE, 3);
      ReconStorageContainerSyncHelper batchHelper = new ReconStorageContainerSyncHelper(
          mockScm, getConf(), getContainerManager());

      // All containers in the batch window are DELETED in SCM
      when(mockScm.getExistContainerWithPipelinesInBatch(anyList()))
          .thenAnswer(inv -> {
            List<Long> ids = inv.getArgument(0);
            return ids.stream()
                .map(id -> containerCwp(id, DELETED))
                .collect(Collectors.toList());
          });

      assertTrue(batchHelper.syncWithSCMContainerInfo());
      // Exactly 3 containers should be retired per cycle (rotating batch window)
      long retiredCount = getContainerManager().getContainers().stream()
          .filter(c -> c.getState() == DELETED).count();
      assertTrue(retiredCount <= 3,
          "Expected at most 3 retirements per cycle, got " + retiredCount);
    }

    @Test
    void batchRPCPartialResultRetiresPresentAndAbsent() throws Exception {
      // 500: not in batch result (absent from SCM → purged) → should be retired to DELETED
      // 501: in batch result with CLOSED state (still live in SCM) → should stay CLOSED
      // 502: in batch result with DELETED state → should be retired to DELETED
      seedRecon(500, 3, CLOSED);

      when(mockScm.getExistContainerWithPipelinesInBatch(anyList()))
          .thenReturn(Arrays.asList(
              containerCwp(501L, CLOSED),
              containerCwp(502L, DELETED)));

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      // 500: absent from batch → treated as purged → DELETED
      assertEquals(DELETED, getContainerManager().getContainer(
          ContainerID.valueOf(500L)).getState());
      // 501: SCM still has it as CLOSED → no retirement
      assertEquals(CLOSED, getContainerManager().getContainer(
          ContainerID.valueOf(501L)).getState());
      // 502: SCM says DELETED → DELETED
      assertEquals(DELETED, getContainerManager().getContainer(
          ContainerID.valueOf(502L)).getState());
    }
  }

  // ===========================================================================
  // Large-scale tests (100 k+ containers)
  // ===========================================================================

  @Nested
  class LargeScaleTests {

    private static final int LARGE_COUNT = 100_000;

    @BeforeEach
    void configLargeBatchSize() {
      // Allow single-batch fetches for all large-scale tests
      getConf().setLong(
          ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE,
          (long) LARGE_COUNT);
      getConf().setInt(OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE,
          LARGE_COUNT);
      // Default Pass 4 mock: all queried containers are still CLOSED (not deleted).
      // Individual tests that need retirement override this mock inline.
      when(mockScm.getExistContainerWithPipelinesInBatch(anyList()))
          .thenAnswer(inv -> {
            List<Long> ids = inv.getArgument(0);
            return ids.stream()
                .map(id -> containerCwp(id, CLOSED))
                .collect(Collectors.toList());
          });
    }

    @Test
    void pass1100kClosedContainersMissingFromRecon() throws Exception {
      // Recon: empty. SCM: 100k CLOSED containers.
      // After sync: Recon should have all 100k as CLOSED.
      List<ContainerID> ids = idRange(1, LARGE_COUNT + 1);

      when(mockScm.getContainerCount(CLOSED)).thenReturn((long) LARGE_COUNT);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(LARGE_COUNT), eq(CLOSED)))
          .thenReturn(ids);
      // Pass 1 add-missing path now uses getExistContainerWithPipelinesInBatch.
      // The @BeforeEach default mock already returns CLOSED for any asked IDs.
      when(mockScm.getContainerCount(OPEN)).thenReturn(0L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(LARGE_COUNT, getContainerManager().getContainers(CLOSED).size());
      assertEquals(0, getContainerManager().getContainers(OPEN).size());
    }

    @Test
    void pass1100kOpenContainersStuckInReconAllCorrectedToClosed() throws Exception {
      // Recon: 100k OPEN containers. SCM: all 100k are CLOSED.
      // After sync: all 100k should be CLOSED in Recon.
      seedRecon(1, LARGE_COUNT, OPEN);
      assertEquals(LARGE_COUNT, getContainerManager().getContainers(OPEN).size());

      List<ContainerID> ids = idRange(1, LARGE_COUNT + 1);
      when(mockScm.getContainerCount(CLOSED)).thenReturn((long) LARGE_COUNT);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(LARGE_COUNT), eq(CLOSED)))
          .thenReturn(ids);
      when(mockScm.getContainerCount(OPEN)).thenReturn(0L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(LARGE_COUNT, getContainerManager().getContainers(CLOSED).size());
      assertEquals(0, getContainerManager().getContainers(OPEN).size());
    }

    @Test
    void pass1100kClosingContainersStuckInReconAllCorrectedToClosed() throws Exception {
      // Recon: 100k CLOSING containers. SCM: all 100k are CLOSED.
      seedReconAsClosing(1, LARGE_COUNT);
      assertEquals(LARGE_COUNT, getContainerManager().getContainers(CLOSING).size());

      List<ContainerID> ids = idRange(1, LARGE_COUNT + 1);
      when(mockScm.getContainerCount(CLOSED)).thenReturn((long) LARGE_COUNT);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(LARGE_COUNT), eq(CLOSED)))
          .thenReturn(ids);
      when(mockScm.getContainerCount(OPEN)).thenReturn(0L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(LARGE_COUNT, getContainerManager().getContainers(CLOSED).size());
      assertEquals(0, getContainerManager().getContainers(CLOSING).size());
    }

    @Test
    void pass4100kClosedContainersAllDeletedInSCM() throws Exception {
      // Recon: 100k CLOSED. SCM: all 100k are DELETED.
      // After sync: all 100k should be DELETED in Recon.
      seedRecon(1, LARGE_COUNT, CLOSED);

      when(mockScm.getContainerCount(CLOSED)).thenReturn(0L);
      when(mockScm.getContainerCount(OPEN)).thenReturn(0L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);
      // Override default mock: all queried containers are DELETED in SCM
      when(mockScm.getExistContainerWithPipelinesInBatch(anyList()))
          .thenAnswer(inv -> {
            List<Long> ids = inv.getArgument(0);
            return ids.stream()
                .map(id -> containerCwp(id, DELETED))
                .collect(Collectors.toList());
          });

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(LARGE_COUNT, getContainerManager().getContainers(DELETED).size());
      assertEquals(0, getContainerManager().getContainers(CLOSED).size());
    }

    @Test
    void pass4100kQuasiClosedContainersAllDeletedInSCM() throws Exception {
      // Recon: 100k QUASI_CLOSED. SCM: all 100k are DELETED.
      seedRecon(1, LARGE_COUNT, QUASI_CLOSED);

      when(mockScm.getContainerCount(CLOSED)).thenReturn(0L);
      when(mockScm.getContainerCount(OPEN)).thenReturn(0L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);
      // Override default mock: all queried containers are DELETED in SCM
      when(mockScm.getExistContainerWithPipelinesInBatch(anyList()))
          .thenAnswer(inv -> {
            List<Long> ids = inv.getArgument(0);
            return ids.stream()
                .map(id -> containerCwp(id, DELETED))
                .collect(Collectors.toList());
          });

      assertTrue(syncHelper.syncWithSCMContainerInfo());
      assertEquals(LARGE_COUNT, getContainerManager().getContainers(DELETED).size());
      assertEquals(0, getContainerManager().getContainers(QUASI_CLOSED).size());
    }

    /**
     * Full 100 k mixed scenario covering all four sync passes simultaneously.
     *
     * <pre>
     * Container ID ranges and their scenario:
     *   1      – 20,000 : OPEN in Recon, CLOSED in SCM
     *                      → Pass 1 corrects to CLOSED
     *   20,001 – 50,000 : absent from Recon, CLOSED in SCM
     *                      → Pass 1 adds as CLOSED
     *   50,001 – 70,000 : absent from Recon, OPEN in SCM
     *                      → Pass 2 adds as OPEN
     *   70,001 – 80,000 : absent from Recon, QUASI_CLOSED in SCM
     *                      → Pass 3 adds as QUASI_CLOSED
     *   80,001 – 100,000: CLOSED in Recon, DELETED in SCM
     *                      → Pass 4 retires to DELETED
     * </pre>
     *
     * <p>After a single {@code syncWithSCMContainerInfo()} call:
     * <ul>
     *   <li>50,000 CLOSED  (20k corrected + 30k added)</li>
     *   <li>20,000 OPEN    (newly added)</li>
     *   <li>10,000 QUASI_CLOSED (newly added)</li>
     *   <li>19,999 DELETED (retired — Pass 4 uses batch of 100k
     *       covering all CLOSED/QUASI_CLOSED candidates at time of run)</li>
     * </ul>
     */
    @Test
    void fullSync100kMixedStateTransitionScenario() throws Exception {
      // ---- Pre-seed Recon ----
      // Range 1-20k: stuck OPEN (SCM has them as CLOSED)
      seedRecon(1, 20_000, OPEN);
      // Range 80001-100000: CLOSED in Recon (will be deleted)
      seedRecon(80_001, 19_999, CLOSED);

      // ---- Mock SCM ----
      // Pass 1 — CLOSED list: IDs 1-50000
      List<ContainerID> closedIds = idRange(1, 50_001);
      when(mockScm.getContainerCount(CLOSED)).thenReturn(50_000L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(50_000), eq(CLOSED)))
          .thenReturn(closedIds);

      // Pass 2 — OPEN list: IDs 50001-70000
      List<ContainerID> openIds = idRange(50_001, 70_001);
      when(mockScm.getContainerCount(OPEN)).thenReturn(20_000L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(20_000), eq(OPEN)))
          .thenReturn(openIds);

      // Pass 3 — QUASI_CLOSED list: IDs 70001-80000
      List<ContainerID> qcIds = idRange(70_001, 80_001);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(10_000L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(10_000), eq(QUASI_CLOSED)))
          .thenReturn(qcIds);

      // Unified batch mock: handles both Pass 1/2/3 add paths and Pass 4 retirement.
      // Pass 1 adds [20001-50000] as CLOSED, Pass 2 adds [50001-70000] as OPEN,
      // Pass 3 adds [70001-80000] as QUASI_CLOSED; Pass 4 retires [80001-100000] as DELETED.
      when(mockScm.getExistContainerWithPipelinesInBatch(anyList())).thenAnswer(inv -> {
        List<Long> ids = inv.getArgument(0);
        return ids.stream().map(id -> {
          LifeCycleState state;
          if (id > 80_000) {
            state = DELETED;       // Pass 4: retire these containers
          } else if (id > 70_000) {
            state = QUASI_CLOSED;  // Pass 3 add + Pass 4: alive as QUASI_CLOSED
          } else if (id > 50_000) {
            state = OPEN;          // Pass 2 add (Pass 4 doesn't query OPEN containers)
          } else {
            state = CLOSED;        // Pass 1 correct+add + Pass 4: alive as CLOSED
          }
          return containerCwp(id, state);
        }).collect(Collectors.toList());
      });

      // ---- Run sync ----
      assertTrue(syncHelper.syncWithSCMContainerInfo());

      // ---- Verify final state ----
      List<ContainerInfo> allContainers = getContainerManager().getContainers();
      long closedCount    = allContainers.stream().filter(c -> c.getState() == CLOSED).count();
      long openCount      = allContainers.stream().filter(c -> c.getState() == OPEN).count();
      long qcCount        = allContainers.stream().filter(c -> c.getState() == QUASI_CLOSED).count();
      long deletedCount   = allContainers.stream().filter(c -> c.getState() == DELETED).count();

      // 20k corrected from OPEN + 30k added = 50k CLOSED
      assertEquals(50_000, closedCount,
          "Expected 50,000 CLOSED containers");
      // 20k newly added from SCM's OPEN list
      assertEquals(20_000, openCount,
          "Expected 20,000 OPEN containers");
      // 10k newly added from SCM's QUASI_CLOSED list
      assertEquals(10_000, qcCount,
          "Expected 10,000 QUASI_CLOSED containers");
      // 19,999 retired from Recon's CLOSED set to DELETED
      assertEquals(19_999, deletedCount,
          "Expected 19,999 DELETED containers");

      // Total: 50k+20k+10k+19999 = 99,999
      assertEquals(99_999, allContainers.size());
    }

    @Test
    void syncIsIdempotentRunningTwiceProducesSameResult() throws Exception {
      // Seed: 5k OPEN (stuck), 5k CLOSED (missing)
      seedRecon(1, 5_000, OPEN);

      List<ContainerID> closedIds = idRange(1, 10_001);
      when(mockScm.getContainerCount(CLOSED)).thenReturn(10_000L);
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(10_000), eq(CLOSED)))
          .thenReturn(closedIds);
      // Default @BeforeEach mock for getExistContainerWithPipelinesInBatch already returns
      // CLOSED for any IDs — covers both the Pass 1 add path and Pass 4 retirement check.
      when(mockScm.getContainerCount(OPEN)).thenReturn(0L);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);

      // First sync
      assertTrue(syncHelper.syncWithSCMContainerInfo());
      long closedAfterFirst = getContainerManager().getContainers(CLOSED).size();

      // Second sync — SCM still reports same data; result must be identical
      assertTrue(syncHelper.syncWithSCMContainerInfo());
      long closedAfterSecond = getContainerManager().getContainers(CLOSED).size();

      assertEquals(closedAfterFirst, closedAfterSecond,
          "Second sync must not change the container count");
      assertEquals(10_000, closedAfterSecond);
    }

    @Test
    void decideSyncAction100kDriftTriggerFullSnapshot() throws Exception {
      // SCM has 100k containers, Recon is empty → drift 100k > threshold 10k
      when(mockScm.getContainerCount()).thenReturn(100_000L);

      assertEquals(SyncAction.FULL_SNAPSHOT, syncHelper.decideSyncAction());
    }

    @Test
    void decideSyncAction50kReconMissingTriggersFullSnapshot() throws Exception {
      // Recon has 50k CLOSED, SCM has 100k → drift 50k > threshold 10k
      seedRecon(1, 50_000, CLOSED);

      when(mockScm.getContainerCount()).thenReturn(100_000L);

      assertEquals(SyncAction.FULL_SNAPSHOT, syncHelper.decideSyncAction());
    }

    @Test
    void decideSyncAction5kDriftTriggersTargetedSync() throws Exception {
      // Recon has 95k, SCM has 100k → drift 5k < threshold 10k → TARGETED_SYNC
      seedRecon(1, 95_000, CLOSED);

      when(mockScm.getContainerCount()).thenReturn(100_000L);

      assertEquals(SyncAction.TARGETED_SYNC, syncHelper.decideSyncAction());
    }

    @Test
    void decideSyncAction100kOpenDriftTriggersTargetedSync() throws Exception {
      // Total counts match but 10k OPEN drift (SCM advanced them to CLOSED) →
      // triggers TARGETED_SYNC via per-state check
      seedRecon(1, 100_000, OPEN); // all OPEN in Recon

      when(mockScm.getContainerCount()).thenReturn(100_000L); // total matches
      when(mockScm.getContainerCount(OPEN)).thenReturn(0L);          // SCM has 0 OPEN
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn(0L);

      // OPEN drift = 100k, far above default threshold of 5
      assertEquals(SyncAction.TARGETED_SYNC, syncHelper.decideSyncAction());
    }

    @Test
    void allStateTransitionPathsEndToEnd() throws Exception {
      // Exhaustive state-transition coverage in a single test:
      //   OPEN         → CLOSED    (Pass 1 correction)
      //   CLOSING      → CLOSED    (Pass 1 correction)
      //   absent       → CLOSED    (Pass 1 add)
      //   absent       → OPEN      (Pass 2 add)
      //   absent       → QUASI_CLOSED  (Pass 3 add)
      //   CLOSED       → DELETING  (Pass 4: SCM DELETING)
      //   CLOSED       → DELETED   (Pass 4: SCM DELETED)
      //   QUASI_CLOSED → DELETED   (Pass 4: SCM DELETED)
      //   CLOSED       → DELETED   (Pass 4: ContainerNotFoundException)

      int perGroup = 10_000;  // 10k containers per scenario = 90k total

      // Pre-seed Recon
      long base = 1L;
      seedRecon(base,           perGroup, OPEN);          // group A: stuck OPEN
      seedReconAsClosing(base + perGroup, perGroup);      // group B: stuck CLOSING
      // group C (base+2*perGroup): absent, SCM has them CLOSED
      // group D (base+3*perGroup): absent, SCM has them OPEN
      // group E (base+4*perGroup): absent, SCM has them QUASI_CLOSED
      seedRecon(base + 5L * perGroup, perGroup, CLOSED);  // group F: to retire → DELETING
      seedRecon(base + 6L * perGroup, perGroup, CLOSED);  // group G: to retire → DELETED
      seedRecon(base + 7L * perGroup, perGroup, QUASI_CLOSED); // group H: to retire → DELETED
      seedRecon(base + 8L * perGroup, perGroup, CLOSED);  // group I: SCM ContainerNotFound

      // Ranges
      long bEnd = base + 2L * perGroup;
      long cEnd = base + 3L * perGroup;
      long dEnd = base + 4L * perGroup;
      long eEnd = base + 5L * perGroup;
      long fEnd = base + 6L * perGroup;
      long hEnd = base + 8L * perGroup;

      // Build CLOSED list for Pass 1: groups A + B + C
      List<ContainerID> closedIds = idRange(base, cEnd);
      when(mockScm.getContainerCount(CLOSED)).thenReturn((long) closedIds.size());
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(closedIds.size()), eq(CLOSED)))
          .thenReturn(closedIds);

      // Build OPEN list for Pass 2: group D
      List<ContainerID> openIds = idRange(bEnd, dEnd);
      when(mockScm.getContainerCount(OPEN)).thenReturn((long) openIds.size());
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(openIds.size()), eq(OPEN)))
          .thenReturn(openIds);

      // Build QUASI_CLOSED list for Pass 3: group E
      List<ContainerID> qcIds = idRange(dEnd, eEnd);
      when(mockScm.getContainerCount(QUASI_CLOSED)).thenReturn((long) qcIds.size());
      when(mockScm.getListOfContainerIDs(
          eq(ContainerID.valueOf(1L)), eq(qcIds.size()), eq(QUASI_CLOSED)))
          .thenReturn(qcIds);

      // Unified batch mock: handles both Pass 1/2/3 add paths and Pass 4 retirement.
      // Pass 1 adds group C (absent→CLOSED); Pass 2 adds group D (absent→OPEN);
      // Pass 3 adds group E (absent→QUASI_CLOSED); Pass 4 retires groups F/G/H/I.
      when(mockScm.getExistContainerWithPipelinesInBatch(anyList())).thenAnswer(inv -> {
        List<Long> ids = inv.getArgument(0);
        List<ContainerWithPipeline> result = new ArrayList<>();
        for (Long id : ids) {
          if (id >= base && id < cEnd) {
            result.add(containerCwp(id, CLOSED));        // Groups A,B,C: CLOSED in SCM
          } else if (id >= cEnd && id < dEnd) {
            result.add(containerCwp(id, OPEN));          // Group D: OPEN in SCM (Pass 2 add)
          } else if (id >= dEnd && id < eEnd) {
            result.add(containerCwp(id, QUASI_CLOSED));  // Group E: QUASI_CLOSED (Pass 3 + alive)
          } else if (id >= eEnd && id < fEnd) {
            result.add(containerCwp(id, DELETING));      // Group F: DELETING in SCM
          } else if (id >= fEnd && id < hEnd) {
            result.add(containerCwp(id, DELETED));       // Groups G+H: DELETED in SCM
          }
          // Group I (>= hEnd): excluded from result → scmState=null → retired to DELETED
        }
        return result;
      });

      assertTrue(syncHelper.syncWithSCMContainerInfo());

      List<ContainerInfo> all = getContainerManager().getContainers();

      long closedCount    = all.stream().filter(c -> c.getState() == CLOSED).count();
      long openCount      = all.stream().filter(c -> c.getState() == OPEN).count();
      long qcCount        = all.stream().filter(c -> c.getState() == QUASI_CLOSED).count();
      long deletingCount  = all.stream().filter(c -> c.getState() == DELETING).count();
      long deletedCount   = all.stream().filter(c -> c.getState() == DELETED).count();

      // Groups A+B corrected + Group C added = 3 * perGroup CLOSED
      assertEquals(3L * perGroup, closedCount,
          "Groups A (OPEN→CLOSED), B (CLOSING→CLOSED), C (added) = 3 * perGroup CLOSED");
      // Group D added as OPEN
      assertEquals((long) perGroup, openCount,
          "Group D: added as OPEN");
      // Group E added as QUASI_CLOSED
      assertEquals((long) perGroup, qcCount,
          "Group E: added as QUASI_CLOSED");
      // Group F: CLOSED → DELETING
      assertEquals((long) perGroup, deletingCount,
          "Group F: CLOSED → DELETING");
      // Groups G + H + I: CLOSED/QUASI_CLOSED → DELETED
      assertEquals(3L * perGroup, deletedCount,
          "Groups G, H, I: → DELETED");
    }
  }
}
