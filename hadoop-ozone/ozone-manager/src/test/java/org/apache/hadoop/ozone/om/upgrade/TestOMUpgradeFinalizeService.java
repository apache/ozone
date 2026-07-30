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

package org.apache.hadoop.ozone.om.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OMUpgradeFinalizeService}.
 * Uses {@link org.apache.hadoop.hdds.utils.BackgroundService#runPeriodicalTaskNow()} to execute
 * tasks synchronously on the test thread, avoiding timing-dependent polling.
 */
public class TestOMUpgradeFinalizeService {

  // A long interval so the scheduler never fires automatically during tests;
  // we drive execution manually via runPeriodicalTaskNow().
  private static final long INTERVAL_MS = 60_000;

  private OzoneManager ozoneManager;
  private OMVersionManager versionManager;
  private TypedTable<String, String> metaTable;
  private ScmClient scmClient;
  private StorageContainerLocationProtocol containerClient;
  private OzoneManagerRatisServer omRatisServer;
  private OMUpgradeFinalizeService service;

  @BeforeEach
  void setUp() throws RocksDatabaseException, CodecException {
    ozoneManager = mock(OzoneManager.class);
    OMMetadataManager metadataManager = mock(OMMetadataManager.class);
    metaTable = mock(TypedTable.class);
    when(ozoneManager.getThreadNamePrefix()).thenReturn("");
    when(ozoneManager.getOMNodeId()).thenReturn("clientId");
    when(ozoneManager.getMetadataManager()).thenReturn(metadataManager);
    when(metadataManager.getMetaTable()).thenReturn(metaTable);
    // For most tests, set the finalization command as having been received
    when(metaTable.get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY)).thenReturn("ignored");

    versionManager = mock(OMVersionManager.class);
    // preExecute() calls ozoneManager.getVersionManager().getApparentVersion().serialize()
    when(ozoneManager.getVersionManager()).thenReturn(versionManager);
    when(versionManager.getApparentVersion()).thenReturn(OzoneManagerVersion.DEFAULT_VERSION);

    omRatisServer = mock(OzoneManagerRatisServer.class);
    // OzoneManagerRatisUtils.submitRequest() calls ozoneManager.getOmRatisServer().submitRequest(...)
    when(ozoneManager.getOmRatisServer()).thenReturn(omRatisServer);

    containerClient = mock(StorageContainerLocationProtocol.class);
    scmClient = mock(ScmClient.class);
    when(scmClient.getContainerClient()).thenReturn(containerClient);

    service = new OMUpgradeFinalizeService(ozoneManager, versionManager, scmClient, INTERVAL_MS);
  }

  /**
   * When the OM is not the leader, getTasks() should return an empty queue
   * and no interaction with the SCM client or Ratis server should occur.
   */
  @Test
  void testNoTasksSubmittedWhenNotLeader() throws Exception {
    when(ozoneManager.isLeaderReady()).thenReturn(false);
    when(versionManager.needsFinalization()).thenReturn(true);

    service.runPeriodicalTaskNow();

    verifyNoInteractions(scmClient);
    verifyNoInteractions(omRatisServer);
  }

  /**
   * When finalization is not needed, getTasks() should return an empty queue
   * and no SCM query or Ratis submission should occur.
   */
  @Test
  void testNoTasksSubmittedWhenFinalizationNotNeeded() throws Exception {
    when(ozoneManager.isLeaderReady()).thenReturn(true);
    when(versionManager.needsFinalization()).thenReturn(false);

    service.runPeriodicalTaskNow();

    verifyNoInteractions(scmClient);
    verifyNoInteractions(omRatisServer);
  }

  /**
   * When the OM is the leader, finalization is needed, the finalization command is given and SCM reports
   * hddsFinalized=true, a FinalizeUpgrade request should be submitted via Ratis.
   */
  @Test
  void testFinalizationTriggeredWhenScmIsFinalizedAndFinalizationInProgress() throws Exception {
    when(ozoneManager.isLeaderReady()).thenReturn(true);
    when(versionManager.needsFinalization()).thenReturn(true);

    HddsProtos.UpgradeStatus scmStatus = HddsProtos.UpgradeStatus.newBuilder()
        .setScmFinalized(true)
        .setHddsFinalized(true)
        .setNumDatanodesFinalized(3)
        .setNumDatanodesTotal(3)
        .build();
    when(containerClient.queryUpgradeStatus()).thenReturn(scmStatus);
    // Finalization command not given yet
    when(metaTable.get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY)).thenReturn(null);

    service.runPeriodicalTaskNow();

    verifyNoInteractions(containerClient);
    verifyNoInteractions(omRatisServer);

    when(metaTable.get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY)).thenReturn("ignored");
    service.runPeriodicalTaskNow();

    verify(containerClient).queryUpgradeStatus();
    // Implementation submits a FinalizeUpgrade request through Ratis
    verify(omRatisServer).submitRequest(any(), any(ClientId.class), anyLong());
  }

  /**
   * When SCM reports hddsFinalized=false (SCM is not yet finalized),
   * no Ratis request should be submitted.
   */
  @Test
  void testFinalizationSkippedWhenScmNotYetFinalized() throws Exception {
    when(ozoneManager.isLeaderReady()).thenReturn(true);
    when(versionManager.needsFinalization()).thenReturn(true);

    HddsProtos.UpgradeStatus scmStatus = HddsProtos.UpgradeStatus.newBuilder()
        .setScmFinalized(false)
        .setHddsFinalized(false)
        .setNumDatanodesFinalized(0)
        .setNumDatanodesTotal(3)
        .build();
    when(containerClient.queryUpgradeStatus()).thenReturn(scmStatus);

    service.runPeriodicalTaskNow();

    verify(containerClient).queryUpgradeStatus();
    verifyNoInteractions(omRatisServer);
  }

  /**
   * When the SCM client throws an IOException, the service should absorb it
   * and not submit any Ratis request.
   */
  @Test
  void testExceptionFromScmClientIsHandledGracefully() throws Exception {
    when(ozoneManager.isLeaderReady()).thenReturn(true);
    when(versionManager.needsFinalization()).thenReturn(true);
    when(containerClient.queryUpgradeStatus()).thenThrow(new IOException("SCM unavailable"));

    // The catch block in the task swallows the exception.
    service.runPeriodicalTaskNow();

    verify(containerClient).queryUpgradeStatus();
    verifyNoInteractions(omRatisServer);
  }

  /**
   * When {@code needsFinalization()} returns {@code false}, {@link OMUpgradeFinalizeService#getTasks()}
   * should spawn a stopper thread that calls {@link OMUpgradeFinalizeService#shutdown()} exactly once,
   * even when {@code getTasks()} is driven multiple times (guarded by the internal
   * {@code stopInitiated} AtomicBoolean). No SCM or Ratis interactions should occur.
   * <p>
   * The service is subclassed to intercept {@code shutdown()} via a {@link CountDownLatch},
   * avoiding both actual executor teardown and any need for {@code Thread.sleep}.
   */
  @Test
  void testShutdownTriggeredExactlyOnceWhenFinalizationNoLongerNeeded() throws Exception {
    AtomicInteger shutdownCount = new AtomicInteger(0);
    CountDownLatch firstShutdown = new CountDownLatch(1);

    OMUpgradeFinalizeService testService = new OMUpgradeFinalizeService(
        ozoneManager, versionManager, scmClient, INTERVAL_MS) {
      @Override
      public synchronized void shutdown() {
        shutdownCount.incrementAndGet();
        firstShutdown.countDown();
        // Don't propagate to super — avoids racing on the test executor.
      }
    };

    when(ozoneManager.isLeaderReady()).thenReturn(true);
    when(versionManager.needsFinalization()).thenReturn(false);

    // Drive getTasks() three times; the stopper thread should only fire once.
    testService.runPeriodicalTaskNow();
    testService.runPeriodicalTaskNow();
    testService.runPeriodicalTaskNow();

    // Wait for the single stopper thread to call shutdown().
    assertTrue(firstShutdown.await(5, TimeUnit.SECONDS),
        "shutdown() should have been called by the stopper thread within 5 s");
    // incrementAndGet() happens before countDown(), so the count is already final — no sleep needed.
    assertEquals(1, shutdownCount.get(),
        "shutdown() must be called exactly once despite multiple getTasks() invocations");
    verifyNoInteractions(scmClient);
    verifyNoInteractions(omRatisServer);
  }

  /**
   * When the Ratis submission throws, the service should absorb the error
   * and not propagate the exception.
   */
  @Test
  void testExceptionFromRatisSubmitIsHandledGracefully() throws Exception {
    when(ozoneManager.isLeaderReady()).thenReturn(true);
    when(versionManager.needsFinalization()).thenReturn(true);

    HddsProtos.UpgradeStatus scmStatus = HddsProtos.UpgradeStatus.newBuilder()
        .setScmFinalized(true)
        .setHddsFinalized(true)
        .setNumDatanodesFinalized(3)
        .setNumDatanodesTotal(3)
        .build();
    when(containerClient.queryUpgradeStatus()).thenReturn(scmStatus);
    when(omRatisServer.submitRequest(any(), any(ClientId.class), anyLong()))
        .thenThrow(new ServiceException("Ratis unavailable"));

    // The catch block in the task swallows the exception.
    service.runPeriodicalTaskNow();

    // submitRequest was attempted but threw — service should not propagate the exception.
    verify(omRatisServer).submitRequest(any(), any(ClientId.class), anyLong());
  }
}
