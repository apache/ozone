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

package org.apache.hadoop.hdds.scm.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationCheckpoint;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizer;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;
import org.mockito.verification.VerificationMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests SCM finalization operations on mocked upgrade state.
 */
public class TestScmFinalization {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestScmFinalization.class);

  // Indicates the current state of the mock pipeline manager's pipeline
  // creation.
  private boolean pipelineCreationFrozen = false;

  /**
   * Order of finalization checkpoints within the enum is used to determine
   * which ones have been passed. If ordering within the enum is changed
   * finalization will not behave correctly.
   */
  @Test
  public void testCheckpointOrder() {
    FinalizationCheckpoint[] checkpoints = FinalizationCheckpoint.values();
    assertEquals(4, checkpoints.length);
    assertEquals(checkpoints[0],
        FinalizationCheckpoint.FINALIZATION_REQUIRED);
    assertEquals(checkpoints[1],
        FinalizationCheckpoint.FINALIZATION_STARTED);
    assertEquals(checkpoints[2],
        FinalizationCheckpoint.MLV_EQUALS_SLV);
    assertEquals(checkpoints[3],
        FinalizationCheckpoint.FINALIZATION_COMPLETE);
  }

  /**
   * Tests that the correct checkpoint is returned based on the value of
   * SCM's layout version and the presence of the finalizing key.
   */
  @Test
  public void testUpgradeStateToCheckpointMapping() throws Exception {
    HDDSLayoutVersionManager versionManager =
        new HDDSLayoutVersionManager(
            HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    PipelineManager pipelineManager =
        getMockPipelineManager(FinalizationCheckpoint.FINALIZATION_REQUIRED);
    // State manager keeps upgrade information in memory as well as writing
    // it to disk, so we can mock the classes that handle disk ops for this
    // test.
    FinalizationStateManager stateManager =
        new FinalizationStateManagerTestImpl.Builder()
            .setFinalizationStore(mock(Table.class))
            .setRatisServer(mock(SCMRatisServer.class))
            .setTransactionBuffer(mock(DBTransactionBuffer.class))
            .setUpgradeFinalizer(new SCMUpgradeFinalizer(versionManager))
            .build();

    // In the actual flow, this would be handled by the FinalizationManager.
    SCMContext scmContext = SCMContext.emptyContext();
    scmContext.setFinalizationCheckpoint(
        stateManager.getFinalizationCheckpoint());
    SCMUpgradeFinalizationContext context =
        new SCMUpgradeFinalizationContext.Builder()
        .setConfiguration(new OzoneConfiguration())
        .setFinalizationStateManager(stateManager)
        .setStorage(mock(SCMStorageConfig.class))
        .setLayoutVersionManager(versionManager)
        .setSCMContext(scmContext)
        .setPipelineManager(pipelineManager)
        .setNodeManager(mock(NodeManager.class))
        .build();
    stateManager.setUpgradeContext(context);

    assertCurrentCheckpoint(scmContext, stateManager,
        FinalizationCheckpoint.FINALIZATION_REQUIRED);
    stateManager.addFinalizingMark();
    assertCurrentCheckpoint(scmContext, stateManager,
        FinalizationCheckpoint.FINALIZATION_STARTED);

    for (HDDSLayoutFeature feature: HDDSLayoutFeature.values()) {
      // Cannot finalize initial version since we are already there.
      if (!feature.equals(HDDSLayoutFeature.INITIAL_VERSION)) {
        stateManager.finalizeLayoutFeature(feature.layoutVersion());
        if (versionManager.needsFinalization()) {
          assertCurrentCheckpoint(scmContext, stateManager,
              FinalizationCheckpoint.FINALIZATION_STARTED);
        } else {
          assertCurrentCheckpoint(scmContext, stateManager,
              FinalizationCheckpoint.MLV_EQUALS_SLV);
        }
      }
    }
    // Make sure we reached this checkpoint if we finished finalizing all
    // layout features.
    assertCurrentCheckpoint(scmContext, stateManager,
        FinalizationCheckpoint.MLV_EQUALS_SLV);

    stateManager.removeFinalizingMark();
    assertCurrentCheckpoint(scmContext, stateManager,
        FinalizationCheckpoint.FINALIZATION_COMPLETE);
  }

  private void assertCurrentCheckpoint(SCMContext context,
      FinalizationStateManager stateManager,
      FinalizationCheckpoint expectedCheckpoint) {

    // SCM context should have been updated with the current checkpoint.
    assertTrue(context.getFinalizationCheckpoint()
        .hasCrossed(expectedCheckpoint));
    for (FinalizationCheckpoint checkpoint: FinalizationCheckpoint.values()) {
      LOG.info("Comparing expected checkpoint {} to {}", expectedCheckpoint,
          checkpoint);
      if (expectedCheckpoint.compareTo(checkpoint) >= 0) {
        // If the expected current checkpoint is >= this checkpoint,
        // then this checkpoint should be crossed according to the state
        // manager.
        assertTrue(stateManager.crossedCheckpoint(checkpoint));
      } else {
        // Else if the expected current checkpoint is < this
        // checkpoint, then this checkpoint should not be crossed according to
        // the state manager.
        assertFalse(stateManager.crossedCheckpoint(checkpoint));
      }
    }
  }

  /**
   * Tests resuming finalization after a failure or leader change, where the
   * disk state will indicate which finalization checkpoint (and therefore
   * set of steps) the SCM must resume from.
   */
  @ParameterizedTest
  @EnumSource(FinalizationCheckpoint.class)
  public void testResumeFinalizationFromCheckpoint(
      FinalizationCheckpoint initialCheckpoint) throws Exception {
    LOG.info("Testing finalization beginning at checkpoint {}",
        initialCheckpoint);

    // Create the table and version manager to appear as if we left off from in
    // progress finalization.
    Table<String, String> finalizationStore =
        getMockTableFromCheckpoint(initialCheckpoint);
    HDDSLayoutVersionManager versionManager =
        getMockVersionManagerFromCheckpoint(initialCheckpoint);
    SCMHAManager haManager = mock(SCMHAManager.class);
    DBTransactionBuffer buffer = mock(DBTransactionBuffer.class);
    when(haManager.getDBTransactionBuffer()).thenReturn(buffer);
    NodeManager nodeManager = mock(NodeManager.class);
    SCMStorageConfig storage = mock(SCMStorageConfig.class);
    SCMContext scmContext = SCMContext.emptyContext();
    scmContext.setFinalizationCheckpoint(initialCheckpoint);
    PipelineManager pipelineManager =
        getMockPipelineManager(initialCheckpoint);

    FinalizationStateManager stateManager =
        new FinalizationStateManagerTestImpl.Builder()
            .setFinalizationStore(finalizationStore)
            .setRatisServer(mock(SCMRatisServer.class))
            .setTransactionBuffer(buffer)
            .setUpgradeFinalizer(new SCMUpgradeFinalizer(versionManager))
            .build();

    FinalizationManager manager = new FinalizationManagerTestImpl.Builder()
        .setFinalizationStateManager(stateManager)
        .setConfiguration(new OzoneConfiguration())
        .setLayoutVersionManager(versionManager)
        .setStorage(storage)
        .setHAManager(SCMHAManagerStub.getInstance(true))
        .setFinalizationStore(finalizationStore)
        .build();

    manager.buildUpgradeContext(nodeManager, pipelineManager, scmContext);

    // Execute upgrade finalization, then check that events happened in the
    // correct order.
    StatusAndMessages status =
        manager.finalizeUpgrade(UUID.randomUUID().toString());
    assertEquals(getStatusFromCheckpoint(initialCheckpoint).status(),
        status.status());

    InOrder inOrder = inOrder(buffer, pipelineManager, nodeManager,
        storage);

    // Once the initial checkpoint's operations are crossed, this count will
    // be increased to 1 to indicate where finalization should have resumed
    // from.
    VerificationMode count = never();
    if (initialCheckpoint == FinalizationCheckpoint.FINALIZATION_REQUIRED) {
      count = times(1);
    }

    // First, SCM should mark that it is beginning finalization.
    inOrder.verify(buffer, count).addToBuffer(
        eq(finalizationStore),
        matches(OzoneConsts.FINALIZING_KEY),
        matches(""));

    // Next, all pipeline creation should be stopped.
    inOrder.verify(pipelineManager, count).freezePipelineCreation();

    if (initialCheckpoint == FinalizationCheckpoint.FINALIZATION_STARTED) {
      count = times(1);
    }

    // Next, each layout feature should be finalized.
    for (HDDSLayoutFeature feature: HDDSLayoutFeature.values()) {
      // Cannot finalize initial version since we are already there.
      if (!feature.equals(HDDSLayoutFeature.INITIAL_VERSION)) {
        inOrder.verify(storage, count)
            .setLayoutVersion(feature.layoutVersion());
        inOrder.verify(storage, count).persistCurrentState();
        // After MLV == SLV, all datanodes should be moved to healthy readonly.
        if (feature.layoutVersion() ==
            HDDSLayoutVersionManager.maxLayoutVersion()) {
          inOrder.verify(nodeManager, count).forceNodesToHealthyReadOnly();
        }
        inOrder.verify(buffer, count).addToBuffer(
            eq(finalizationStore),
            matches(OzoneConsts.LAYOUT_VERSION_KEY),
            eq(String.valueOf(feature.layoutVersion())));
      }
    }
    // If this was not called in the loop, there was an error. To detect this
    // mistake, verify again here.
    verify(nodeManager, count).forceNodesToHealthyReadOnly();

    if (initialCheckpoint == FinalizationCheckpoint.MLV_EQUALS_SLV) {
      count = times(1);
    }

    // Last, the finalizing mark is removed to indicate finalization is
    // complete.
    inOrder.verify(buffer, count).removeFromBuffer(
        eq(finalizationStore),
        matches(OzoneConsts.FINALIZING_KEY));

    // If the initial checkpoint was FINALIZATION_COMPLETE, no mocks should
    // have been invoked.
  }

  /**
   * On startup, the finalization table will be read to determine the
   * checkpoint we are resuming from. After this, the results will be stored
   * in memory and flushed to the table asynchronously by the buffer, so the
   * mock table can continue to return the initial values since the in memory
   * values will be used after the initial table read on start.
   *
   * Layout version stored in the table is only used for ratis snapshot
   * finalization, which is not covered in this test.
   */
  private Table<String, String> getMockTableFromCheckpoint(
      FinalizationCheckpoint initialCheckpoint) throws Exception {
    Table<String, String> finalizationStore = mock(Table.class);
    when(finalizationStore
            .isExist(eq(OzoneConsts.FINALIZING_KEY)))
        .thenReturn(initialCheckpoint.needsFinalizingMark());
    return finalizationStore;
  }

  /**
   * On startup, components will read their version file to get their current
   * layout version and initialize the version manager with that. Simulate
   * that here.
   */
  private HDDSLayoutVersionManager getMockVersionManagerFromCheckpoint(
      FinalizationCheckpoint initialCheckpoint) throws Exception {
    int layoutVersion = HDDSLayoutVersionManager.maxLayoutVersion();
    if (initialCheckpoint.needsMlvBehindSlv()) {
      layoutVersion = HDDSLayoutFeature.INITIAL_VERSION.layoutVersion();
    }
    return new HDDSLayoutVersionManager(layoutVersion);
  }

  /**
   * Returns the expected status when finalization is invoked from the
   * provided checkpoint.
   */
  private StatusAndMessages getStatusFromCheckpoint(
      FinalizationCheckpoint initialCheckpoint) {
    if (initialCheckpoint == FinalizationCheckpoint.FINALIZATION_COMPLETE) {
      return UpgradeFinalization.FINALIZED_MSG;
    } else {
      return UpgradeFinalization.STARTING_MSG;
    }
  }

  private PipelineManager getMockPipelineManager(
      FinalizationCheckpoint inititalCheckpoint) {
    PipelineManager pipelineManager = mock(PipelineManager.class);
    // After finalization, SCM will wait for at least one pipeline to be
    // created. It does not care about the contents of the pipeline list, so
    // just return something with length >= 1.
    when(pipelineManager.getPipelines(any(),
        any())).thenReturn(Arrays.asList(null, null, null));

    // Set the initial value for pipeline creation based on the checkpoint.
    // In a real cluster, this would be set on startup of the
    // PipelineManagerImpl.
    pipelineCreationFrozen =
        !FinalizationManager.shouldCreateNewPipelines(inititalCheckpoint);
    doAnswer(args -> pipelineCreationFrozen = true)
        .when(pipelineManager).freezePipelineCreation();
    doAnswer(args -> pipelineCreationFrozen = false)
        .when(pipelineManager).resumePipelineCreation();

    doAnswer(args -> pipelineCreationFrozen)
        .when(pipelineManager).isPipelineCreationFrozen();

    return pipelineManager;
  }
}
