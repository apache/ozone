package org.apache.hadoop.hdds.scm.upgrade;


import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManagerImpl;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager.FinalizationCheckpoint;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManagerImpl;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.UUID;

public class TestScmHAFinalization {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestScmHAFinalization.class);

  /**
   * Order of finalization checkpoints within the enum is used to determine
   * which ones have been passed. If ordering within the enum is changed
   * finalization will not behave correctly.
   */
  @Test
  public void testCheckpointOrder() {
    FinalizationCheckpoint[] checkpoints = FinalizationCheckpoint.values();
    Assert.assertEquals(4, checkpoints.length);
    Assert.assertEquals(checkpoints[0],
        FinalizationCheckpoint.FINALIZATION_REQUIRED);
    Assert.assertEquals(checkpoints[1],
        FinalizationCheckpoint.FINALIZATION_STARTED);
    Assert.assertEquals(checkpoints[2],
        FinalizationCheckpoint.MLV_EQUALS_SLV);
    Assert.assertEquals(checkpoints[3],
        FinalizationCheckpoint.FINALIZATION_COMPLETE);
  }

  /**
   * Tests that the correct checkpoint is returned based on the value of
   * SCM's layout version and the presence of the finalizing key.
   */
  @Test
  public void testUpgradeStateToCheckpointMapping() throws Exception {
    HDDSLayoutVersionManager versionManager =
        new HDDSLayoutVersionManager(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    // State manager keeps upgrade information in memory as well as writing
    // it to disk, so we can mock the classes that handle disk ops for this
    // test.
    FinalizationStateManager stateManager =
        new FinalizationStateManagerImpl.Builder()
            .setVersionManager(versionManager)
            .setFinalizationStore(Mockito.mock(Table.class))
            .setRatisServer(Mockito.mock(SCMRatisServer.class))
            .setTransactionBuffer(Mockito.mock(DBTransactionBuffer.class))
            .buildForTesting();
    // This is normally handled by the FinalizationManager, which we do not
    // have in this test.
    stateManager.addReplicatedFinalizationStep(lf ->
        versionManager.finalized((HDDSLayoutFeature) lf));

    assertCurrentCheckpoint(stateManager,
        FinalizationCheckpoint.FINALIZATION_REQUIRED);
    stateManager.addFinalizingMark();
    assertCurrentCheckpoint(stateManager,
        FinalizationCheckpoint.FINALIZATION_STARTED);

    for (HDDSLayoutFeature feature: HDDSLayoutFeature.values()) {
      // Cannot finalize initial version since we are already there.
      if (!feature.equals(HDDSLayoutFeature.INITIAL_VERSION)) {
        stateManager.finalizeLayoutFeature(feature.layoutVersion());
        if (versionManager.needsFinalization()) {
          assertCurrentCheckpoint(stateManager,
              FinalizationCheckpoint.FINALIZATION_STARTED);
        } else {
          assertCurrentCheckpoint(stateManager,
              FinalizationCheckpoint.MLV_EQUALS_SLV);
        }
      }
    }
    // Make sure we reached this checkpoint if we finished finalizing all
    // layout features.
    assertCurrentCheckpoint(stateManager,
        FinalizationCheckpoint.MLV_EQUALS_SLV);

    stateManager.removeFinalizingMark();
    assertCurrentCheckpoint(stateManager,
        FinalizationCheckpoint.FINALIZATION_COMPLETE);
  }

  private void assertCurrentCheckpoint(FinalizationStateManager stateManager,
      FinalizationCheckpoint expectedCheckpoint) {
    for (FinalizationCheckpoint checkpoint: FinalizationCheckpoint.values()) {
      LOG.info("Comparing expected checkpoint {} to {}", expectedCheckpoint,
          checkpoint);
      if (expectedCheckpoint.compareTo(checkpoint) >= 0) {
        // If the expected current checkpoint is larger than this checkpoint,
        // then this checkpoint should be passed according to the state manager.
        Assert.assertTrue(stateManager.passedCheckpoint(checkpoint));
      } else {
        // Else if the expected current checkpoint is smaller than this
        // checkpoint, then this checkpoint should not be passed according to
        // the state manager.
        Assert.assertFalse(stateManager.passedCheckpoint(checkpoint));
      }
    }
  }

  @Test
  public void testResumeFinalizationFromCheckpoint() throws Exception {
    PipelineManager pipelineManager = Mockito.mock(PipelineManager.class);
    // After finalization, SCM will wait for at least one pipeline to be
    // created. It does not care about the contents of the pipeline list, so
    // just return something with length >= 1.
    Mockito.when(pipelineManager.getPipelines(Mockito.any(),
        Mockito.any())).thenReturn(Arrays.asList(null, null, null));
    DBTransactionBuffer buffer = Mockito.mock(DBTransactionBuffer.class);
    SCMHAManager haManager = Mockito.mock(SCMHAManager.class);
    Mockito.when(haManager.getDBTransactionBuffer()).thenReturn(buffer);
    NodeManager nodeManager = Mockito.mock(NodeManager.class);
    Table<String, String> finalizationStore = Mockito.mock(Table.class);
    SCMStorageConfig storage = Mockito.mock(SCMStorageConfig.class);
    HDDSLayoutVersionManager versionManager = new HDDSLayoutVersionManager(
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());

    FinalizationStateManager stateManager =
        new FinalizationStateManagerImpl.Builder()
            .setVersionManager(versionManager)
            .setFinalizationStore(finalizationStore)
            .setRatisServer(Mockito.mock(SCMRatisServer.class))
            .setTransactionBuffer(buffer)
            .buildForTesting();

    FinalizationManager manager = new FinalizationManagerImpl.Builder()
        .setConfiguration(new OzoneConfiguration())
        .setLayoutVersionManager(versionManager)
        .setPipelineManager(pipelineManager)
        .setNodeManager(nodeManager)
        .setStorage(storage)
        .setHAManager(SCMHAManagerStub.getInstance(true))
        .setFinalizationStore(finalizationStore)
        .setFinalizationStateManagerForTesting(stateManager)
        .build();

    // Execute upgrade finalization, then check that events happened in the
    // correct order.
    manager.finalizeUpgrade(UUID.randomUUID().toString());

    InOrder inOrder = Mockito.inOrder(buffer, pipelineManager, nodeManager,
        storage);
    // First, SCM should mark that it is beginning finalization.
    inOrder.verify(buffer).addToBuffer(ArgumentMatchers.eq(finalizationStore),
        ArgumentMatchers.matches(OzoneConsts.FINALIZING_KEY),
        ArgumentMatchers.matches(""));
    // Next, all pipeline creation should be stopped.
    inOrder.verify(pipelineManager).freezePipelineCreation();

    // Next, each layout feature should be finalized.
    for (HDDSLayoutFeature feature: HDDSLayoutFeature.values()) {
      // Cannot finalize initial version since we are already there.
      if (!feature.equals(HDDSLayoutFeature.INITIAL_VERSION)) {
        inOrder.verify(storage).setLayoutVersion(feature.layoutVersion());
        inOrder.verify(storage).persistCurrentState();
        // After MLV == SLV, all datanodes should be moved to healthy readonly.
        if (feature.layoutVersion() ==
            HDDSLayoutVersionManager.maxLayoutVersion()) {
          inOrder.verify(nodeManager).forceNodesToHealthyReadOnly();
        }
        // VERSION file is the source of truth, DB is only used for snapshot
        // finalization. Therefore DB update should be last.
        inOrder.verify(buffer).addToBuffer(ArgumentMatchers.eq(finalizationStore),
            ArgumentMatchers.matches(OzoneConsts.LAYOUT_VERSION_KEY),
            ArgumentMatchers.eq(String.valueOf(feature.layoutVersion())));
      }
    }
    Mockito.verify(nodeManager, Mockito.times(1)).forceNodesToHealthyReadOnly();

    // Finally, the finalizing mark is removed to indicate finalization is
    // complete.
    inOrder.verify(buffer).removeFromBuffer(
        ArgumentMatchers.eq(finalizationStore),
        ArgumentMatchers.matches(OzoneConsts.FINALIZING_KEY));

    inOrder.verifyNoMoreInteractions();
  }
}
