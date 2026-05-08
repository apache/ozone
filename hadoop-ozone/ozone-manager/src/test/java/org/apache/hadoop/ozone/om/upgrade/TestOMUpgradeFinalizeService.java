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

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
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
  private ScmClient scmClient;
  private StorageContainerLocationProtocol containerClient;
  private OMUpgradeFinalizeService service;

  @BeforeEach
  void setUp() {
    ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.getThreadNamePrefix()).thenReturn("");

    versionManager = mock(OMVersionManager.class);

    containerClient = mock(StorageContainerLocationProtocol.class);
    scmClient = mock(ScmClient.class);
    when(scmClient.getContainerClient()).thenReturn(containerClient);

    service = new OMUpgradeFinalizeService(ozoneManager, versionManager, scmClient, INTERVAL_MS);
  }

  /**
   * When the OM is not the leader, getTasks() should return an empty queue
   * and no interaction with the version manager or SCM client should occur.
   */
  @Test
  void testNoTasksSubmittedWhenNotLeader() throws Exception {
    when(ozoneManager.isLeaderReady()).thenReturn(false);
    when(versionManager.needsFinalization()).thenReturn(true);

    service.runPeriodicalTaskNow();

    verifyNoInteractions(scmClient);
    verify(versionManager, never()).finalizeUpgrade();
  }

  /**
   * When finalization is not needed, getTasks() should return an empty queue
   * and finalizeUpgrade() should never be called.
   */
  @Test
  void testNoTasksSubmittedWhenFinalizationNotNeeded() throws Exception {
    when(ozoneManager.isLeaderReady()).thenReturn(true);
    when(versionManager.needsFinalization()).thenReturn(false);

    service.runPeriodicalTaskNow();

    verifyNoInteractions(scmClient);
    verify(versionManager, never()).finalizeUpgrade();
  }

  /**
   * When the OM is the leader, finalization is needed, and SCM reports
   * shouldFinalize=true, finalizeUpgrade() should be called exactly once.
   */
  @Test
  void testFinalizationTriggeredWhenScmIsFinalized() throws Exception {
    when(ozoneManager.isLeaderReady()).thenReturn(true);
    when(versionManager.needsFinalization()).thenReturn(true);

    HddsProtos.UpgradeStatus scmStatus = HddsProtos.UpgradeStatus.newBuilder()
        .setScmFinalized(true)
        .setShouldFinalize(true)
        .setNumDatanodesFinalized(3)
        .setNumDatanodesTotal(3)
        .build();
    when(containerClient.queryUpgradeStatus()).thenReturn(scmStatus);

    service.runPeriodicalTaskNow();

    verify(containerClient).queryUpgradeStatus();
    // TODO - need to validate the ratis call
  }

  /**
   * When SCM reports shouldFinalize=false (SCM is not yet finalized),
   * the OM should not attempt to finalize.
   */
  @Test
  void testFinalizationSkippedWhenScmNotYetFinalized() throws Exception {
    when(ozoneManager.isLeaderReady()).thenReturn(true);
    when(versionManager.needsFinalization()).thenReturn(true);

    HddsProtos.UpgradeStatus scmStatus = HddsProtos.UpgradeStatus.newBuilder()
        .setScmFinalized(false)
        .setShouldFinalize(false)
        .setNumDatanodesFinalized(0)
        .setNumDatanodesTotal(3)
        .build();
    when(containerClient.queryUpgradeStatus()).thenReturn(scmStatus);

    service.runPeriodicalTaskNow();

    verify(containerClient).queryUpgradeStatus();
    verify(versionManager, never()).finalizeUpgrade();
  }

  /**
   * When the SCM client throws an IOException, the service should absorb it
   * and not call finalizeUpgrade().
   */
  @Test
  void testExceptionFromScmClientIsHandledGracefully() throws Exception {
    when(ozoneManager.isLeaderReady()).thenReturn(true);
    when(versionManager.needsFinalization()).thenReturn(true);
    when(containerClient.queryUpgradeStatus()).thenThrow(new IOException("SCM unavailable"));

    // runPeriodicalTaskNow() calls call() directly; the catch block in the task
    // swallows the exception, so this should complete without throwing.
    service.runPeriodicalTaskNow();

    verify(containerClient).queryUpgradeStatus();
    verify(versionManager, never()).finalizeUpgrade();
  }

  /**
   * When finalizeUpgrade() itself throws, the service should absorb the error.
   */
  @Test
  void testExceptionFromFinalizeUpgradeIsHandledGracefully() throws Exception {
    when(ozoneManager.isLeaderReady()).thenReturn(true);
    when(versionManager.needsFinalization()).thenReturn(true);

    HddsProtos.UpgradeStatus scmStatus = HddsProtos.UpgradeStatus.newBuilder()
        .setScmFinalized(true)
        .setShouldFinalize(true)
        .setNumDatanodesFinalized(3)
        .setNumDatanodesTotal(3)
        .build();
    when(containerClient.queryUpgradeStatus()).thenReturn(scmStatus);
    UpgradeException upgradeException = new UpgradeException("finalization failed",
        UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED);
    org.mockito.Mockito.doThrow(upgradeException).when(versionManager).finalizeUpgrade();

    // The catch block in the task swallows the exception.
    service.runPeriodicalTaskNow();
    // TODO - these tests need a bit of rework since moving to ratis.
  }
}
