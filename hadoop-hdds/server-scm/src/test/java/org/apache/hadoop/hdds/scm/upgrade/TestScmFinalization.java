/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.upgrade;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.SimpleMockNodeManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManagerImpl;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationCheckpoint;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManagerImpl;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizer;
import org.apache.hadoop.hdds.upgrade.HDDSFinalizationRequirements;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.upgrade.DefaultUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;

import static org.apache.hadoop.hdds.upgrade.HDDSFinalizationRequirements.PipelineRequirements.CLOSE_ALL_PIPELINES;
import static org.apache.hadoop.hdds.upgrade.HDDSFinalizationRequirements.PipelineRequirements.NONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Tests SCM finalization operations on mocked upgrade state.
 */
public class TestScmFinalization {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestScmFinalization.class);

  // Indicates the current state of the mock pipeline manager's pipeline
  // creation.
  private boolean pipelineCreationFrozen = false;

  private static final String METHOD_SOURCE =
      "org.apache.hadoop.hdds.scm.upgrade" +
          ".TestScmFinalization#finalizationRequirementsToTest";

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
        getMockPipelineManager(FinalizationCheckpoint.FINALIZATION_REQUIRED,
            versionManager.getFinalizationRequirements());
    // State manager keeps upgrade information in memory as well as writing
    // it to disk, so we can mock the classes that handle disk ops for this
    // test.
    FinalizationStateManager stateManager =
        new FinalizationStateManagerImpl.Builder()
            .setFinalizationStore(Mockito.mock(Table.class))
            .setRatisServer(Mockito.mock(SCMRatisServer.class))
            .setTransactionBuffer(Mockito.mock(DBTransactionBuffer.class))
            .setUpgradeFinalizer(new SCMUpgradeFinalizer(versionManager))
            .buildForTesting();

    // In the actual flow, this would be handled by the FinalizationManager.
    SCMContext scmContext = SCMContext.emptyContext();
    scmContext.setFinalizationCheckpoint(
        stateManager.getFinalizationCheckpoint());
    SCMUpgradeFinalizationContext context =
        new SCMUpgradeFinalizationContext.Builder()
        .setConfiguration(new OzoneConfiguration())
        .setFinalizationStateManager(stateManager)
        .setStorage(Mockito.mock(SCMStorageConfig.class))
        .setLayoutVersionManager(versionManager)
        .setSCMContext(scmContext)
        .setPipelineManager(pipelineManager)
        .setNodeManager(getMockitoNodeManager())
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
    SCMHAManager haManager = Mockito.mock(SCMHAManager.class);
    DBTransactionBuffer buffer = Mockito.mock(DBTransactionBuffer.class);
    Mockito.when(haManager.getDBTransactionBuffer()).thenReturn(buffer);
    NodeManager nodeManager = getMockitoNodeManager();
    SCMStorageConfig storage = Mockito.mock(SCMStorageConfig.class);
    SCMContext scmContext = SCMContext.emptyContext();
    scmContext.setFinalizationCheckpoint(initialCheckpoint);
    PipelineManager pipelineManager =
        getMockPipelineManager(initialCheckpoint,
            versionManager.getFinalizationRequirements());

    FinalizationStateManager stateManager =
        new FinalizationStateManagerImpl.Builder()
            .setFinalizationStore(finalizationStore)
            .setRatisServer(Mockito.mock(SCMRatisServer.class))
            .setTransactionBuffer(buffer)
            .setUpgradeFinalizer(new SCMUpgradeFinalizer(versionManager))
            .buildForTesting();

    FinalizationManager manager = new FinalizationManagerImpl.Builder()
        .setStateManagerForTesting(stateManager)
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

    InOrder inOrder = Mockito.inOrder(buffer, pipelineManager, nodeManager,
        storage);

    // Once the initial checkpoint's operations are crossed, this count will
    // be increased to 1 to indicate where finalization should have resumed
    // from.
    VerificationMode count = Mockito.never();
    if (initialCheckpoint == FinalizationCheckpoint.FINALIZATION_REQUIRED) {
      count = Mockito.times(1);
    }

    // First, SCM should mark that it is beginning finalization.
    inOrder.verify(buffer, count).addToBuffer(
        ArgumentMatchers.eq(finalizationStore),
        ArgumentMatchers.matches(OzoneConsts.FINALIZING_KEY),
        ArgumentMatchers.matches(""));

    // Next, all pipeline creation should be stopped.
    inOrder.verify(pipelineManager, count).freezePipelineCreation();

    if (initialCheckpoint == FinalizationCheckpoint.FINALIZATION_STARTED) {
      count = Mockito.times(1);
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
            ArgumentMatchers.eq(finalizationStore),
            ArgumentMatchers.matches(OzoneConsts.LAYOUT_VERSION_KEY),
            ArgumentMatchers.eq(String.valueOf(feature.layoutVersion())));
      }
    }
    // If this was not called in the loop, there was an error. To detect this
    // mistake, verify again here.
    Mockito.verify(nodeManager, count).forceNodesToHealthyReadOnly();

    if (initialCheckpoint == FinalizationCheckpoint.MLV_EQUALS_SLV) {
      count = Mockito.times(1);
    }

    // Last, the finalizing mark is removed to indicate finalization is
    // complete.
    inOrder.verify(buffer, count).removeFromBuffer(
        ArgumentMatchers.eq(finalizationStore),
        ArgumentMatchers.matches(OzoneConsts.FINALIZING_KEY));

    // If the initial checkpoint was FINALIZATION_COMPLETE, no mocks should
    // have been invoked.
  }

  /**
   * Argument supplier for parameterized tests.
   */
  private static Stream<Arguments> finalizationRequirementsToTest() {
    HDDSFinalizationRequirements noPipelineClose =
        new HDDSFinalizationRequirements.Builder()
            .setPipelineRequirements(NONE)
            .build();
    checkFinalizationRequirements(noPipelineClose, 3, NONE);

    HDDSFinalizationRequirements pipelineClose =
        new HDDSFinalizationRequirements.Builder()
            .setPipelineRequirements(CLOSE_ALL_PIPELINES)
            .build();
    checkFinalizationRequirements(pipelineClose, 3, CLOSE_ALL_PIPELINES);

    HDDSFinalizationRequirements fewerNodesRequired =
        new HDDSFinalizationRequirements.Builder()
            .setMinFinalizedDatanodes(1)
            .build();
    checkFinalizationRequirements(fewerNodesRequired, 1, NONE);

    HDDSFinalizationRequirements moreNodesRequired =
        new HDDSFinalizationRequirements.Builder()
            .setMinFinalizedDatanodes(5)
            .build();
    checkFinalizationRequirements(moreNodesRequired, 5, NONE);

    HDDSFinalizationRequirements aggregateRequirements =
        new HDDSFinalizationRequirements(Arrays.asList(moreNodesRequired,
            fewerNodesRequired, pipelineClose, noPipelineClose));
    checkFinalizationRequirements(aggregateRequirements, 5,
        CLOSE_ALL_PIPELINES);

    return Stream.of(
        Arguments.of(noPipelineClose),
        Arguments.of(pipelineClose),
        Arguments.of(fewerNodesRequired),
        Arguments.of(moreNodesRequired),
        Arguments.of(aggregateRequirements)
    );
  }

  private static void checkFinalizationRequirements(
      HDDSFinalizationRequirements requirements, int minFinalizedDNs,
      HDDSFinalizationRequirements.PipelineRequirements pipelineReqs) {
    assertEquals(minFinalizedDNs, requirements.getMinFinalizedDatanodes());
    assertEquals(pipelineReqs, requirements.getPipelineRequirements());
  }

  @ParameterizedTest
  @MethodSource(METHOD_SOURCE)
  public void testFinalizationRequirements(
      HDDSFinalizationRequirements requirements) throws Exception {

    HDDSLayoutVersionManager versionManager =
        HDDSLayoutVersionManager.newTestInstance(0,
        requirements);
    Table<String, String> finalizationStore = Mockito.mock(Table.class);
    SCMHAManager haManager = Mockito.mock(SCMHAManager.class);
    DBTransactionBuffer buffer = Mockito.mock(DBTransactionBuffer.class);
    Mockito.when(haManager.getDBTransactionBuffer()).thenReturn(buffer);
    MockNodeManager nodeManager =
        new MockNodeManager(requirements.getMinFinalizedDatanodes() - 1);
    SCMStorageConfig storage = Mockito.mock(SCMStorageConfig.class);
    SCMContext scmContext = SCMContext.emptyContext();
    PipelineManager pipelineManager = getMockPipelineManager(
        FinalizationCheckpoint.FINALIZATION_REQUIRED,
        requirements);

    // Use an upgrade finalizer that does not wait between checks of post
    // finalize conditions to speed up testing.
    SCMUpgradeFinalizer finalizer = SCMUpgradeFinalizer.newTestInstance(
        versionManager, new DefaultUpgradeFinalizationExecutor<>(),
        Duration.ofMillis(0));

    FinalizationStateManager stateManager =
        new FinalizationStateManagerImpl.Builder()
            .setFinalizationStore(finalizationStore)
            .setRatisServer(Mockito.mock(SCMRatisServer.class))
            .setTransactionBuffer(buffer)
            .setUpgradeFinalizer(finalizer)
            .buildForTesting();

    FinalizationManager manager = new FinalizationManagerImpl.Builder()
        .setStateManagerForTesting(stateManager)
        .setConfiguration(new OzoneConfiguration())
        .setLayoutVersionManager(versionManager)
        .setStorage(storage)
        .setHAManager(SCMHAManagerStub.getInstance(true))
        .setFinalizationStore(finalizationStore)
        .setUpgradeFinalizerForTesting(finalizer)
        .build();

    manager.buildUpgradeContext(nodeManager, pipelineManager, scmContext);

    // Execute upgrade finalization then check that the correct operations
    // were invoked given the finalization requirements.
    manager.finalizeUpgrade(UUID.randomUUID().toString());

    InOrder inOrder = Mockito.inOrder(pipelineManager);
    VerificationMode pipelineOpCount;
    if (requirements.getPipelineRequirements() == CLOSE_ALL_PIPELINES) {
      pipelineOpCount = Mockito.times(1);
    } else {
      pipelineOpCount = Mockito.times(0);
    }

    inOrder.verify(pipelineManager, pipelineOpCount).freezePipelineCreation();
    inOrder.verify(pipelineManager, pipelineOpCount).resumePipelineCreation();
    // Each query of the invocation count should have increased the healthy
    // node count by one in the mock node manager, so finalization should have
    // had to wait two iterations before it saw its min required finalized
    // datanodes.
    assertEquals(nodeManager.invocationCount, 2);
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
    Table<String, String> finalizationStore = Mockito.mock(Table.class);
    Mockito.when(finalizationStore
            .isExist(ArgumentMatchers.eq(OzoneConsts.FINALIZING_KEY)))
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
      return UpgradeFinalizer.FINALIZED_MSG;
    } else {
      return UpgradeFinalizer.STARTING_MSG;
    }
  }

  private NodeManager getMockitoNodeManager() {
    NodeManager nodeManager = Mockito.mock(NodeManager.class);
    // In this mockito implementation, all datanodes appear to finalize
    // immediately to SCM.
    Mockito.when(nodeManager.getNodeCount(NodeStatus.inServiceHealthy()))
        .thenReturn(Integer.MAX_VALUE);
    return nodeManager;
  }

  private PipelineManager getMockPipelineManager(
      FinalizationCheckpoint initialCheckpoint,
      HDDSFinalizationRequirements requirements) {
    PipelineManager pipelineManager = Mockito.mock(PipelineManager.class);
    // After finalization, SCM will wait for at least one pipeline to be
    // created. It does not care about the contents of the pipeline list, so
    // just return something with length >= 1.
    Mockito.when(pipelineManager.getPipelines(Mockito.any(),
        Mockito.any())).thenReturn(Arrays.asList(null, null, null));

    // Set the initial value for pipeline creation based on the checkpoint.
    // In a real cluster, this would be set on startup of the
    // PipelineManagerImpl.
    pipelineCreationFrozen =
        !FinalizationManager.shouldCreateNewPipelines(initialCheckpoint,
            requirements);
    Mockito.doAnswer(args -> pipelineCreationFrozen = true)
        .when(pipelineManager).freezePipelineCreation();
    Mockito.doAnswer(args -> pipelineCreationFrozen = false)
        .when(pipelineManager).resumePipelineCreation();

    Mockito.doAnswer(args -> pipelineCreationFrozen)
        .when(pipelineManager).isPipelineCreationFrozen();

    return pipelineManager;
  }

  private static class MockNodeManager extends SimpleMockNodeManager {
    private int healthyNodeCount;
    private int invocationCount;

    MockNodeManager(int healthyNodeCount) {
      this.healthyNodeCount = healthyNodeCount;
      this.invocationCount = 0;
    }

    @Override
    public int getNodeCount(NodeStatus nodeStatus) {
      invocationCount++;
      // Each query for healthy nodes causes the number of healthy nodes to
      // increase, simulating all the cluster's datanodes moving towards
      // finalization.
      return healthyNodeCount++;
    }

    @Override
    public List<DatanodeDetails> getAllNodes() {
      // Only the size of this list is checked by the test code to determine
      // the total number of nodes in the cluster.
      return Arrays.asList(null, null, null, null, null);
    }

    public int getInvocationCount() {
      return invocationCount;
    }
  }
}
