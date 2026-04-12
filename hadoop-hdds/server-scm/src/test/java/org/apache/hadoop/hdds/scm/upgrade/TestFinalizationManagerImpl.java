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

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.ALREADY_FINALIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManagerImpl}.
 */
public class TestFinalizationManagerImpl {

  private static final String CLIENT_ID = "test-client";

  /**
   * Finalization must be rejected while SCM is in safe mode and finalization is needed.
   */
  @Test
  public void testFinalizeThrowsWhenInSafeMode() throws Exception {
    // Use INITIAL_VERSION so the cluster genuinely needs finalization.
    FinalizationManager manager = buildManager(new HDDSLayoutVersionManager(INITIAL_VERSION.layoutVersion()));

    SCMContext inSafeMode = createScmContext(SafeModeStatus.INITIAL);
    manager.buildUpgradeContext(mock(NodeManager.class), inSafeMode);

    IOException ex = assertThrows(IOException.class, () -> manager.finalizeUpgrade(CLIENT_ID));
    assertThat(ex.getMessage()).containsIgnoringCase("safemode");
  }

  /**
   * Finalization must be rejected while SCM is in safe mode even if finalization is not needed.
   */
  @Test
  public void testAlreadyFinalizedClusterStillBlockedBySafeMode() throws Exception {
    FinalizationManager manager = buildManager(new HDDSLayoutVersionManager(maxLayoutVersion()));

    SCMContext inSafeMode = createScmContext(SafeModeStatus.INITIAL);
    manager.buildUpgradeContext(mock(NodeManager.class), inSafeMode);

    IOException ex = assertThrows(IOException.class, () -> manager.finalizeUpgrade(CLIENT_ID));
    assertThat(ex.getMessage()).containsIgnoringCase("safemode");
  }

  /**
   * Finalization must not be blocked when SCM is out of safe mode.
   * Using maxLayoutVersion so the cluster is already finalized, which lets
   * the call succeed without wiring up the full replication stack.
   */
  @Test
  public void testFinalizeSucceedsWhenOutOfSafeMode() throws Exception {
    FinalizationManager manager = buildManager(new HDDSLayoutVersionManager(maxLayoutVersion()));

    SCMContext outOfSafeMode = createScmContext(SafeModeStatus.OUT_OF_SAFE_MODE);
    manager.buildUpgradeContext(mock(NodeManager.class), outOfSafeMode);

    StatusAndMessages result = manager.finalizeUpgrade(CLIENT_ID);
    assertEquals(ALREADY_FINALIZED, result.status());
  }

  private static FinalizationManager buildManager(HDDSLayoutVersionManager lvm)
      throws IOException {
    // Use a local variable rather than chaining: the parent builder methods
    // (setLayoutVersionManager, setStorage) return FinalizationManagerImpl.Builder,
    // losing the subtype, so setFinalizationStateManager would not be visible.
    FinalizationManagerTestImpl.Builder builder = new FinalizationManagerTestImpl.Builder();
    builder.setLayoutVersionManager(lvm);
    builder.setStorage(mock(SCMStorageConfig.class));
    builder.setFinalizationStateManager(mock(FinalizationStateManager.class));
    return builder.build();
  }

  private SCMContext createScmContext(SafeModeStatus safeMode) {
    return new SCMContext.Builder()
        .setSafeModeStatus(safeMode)
        .setSCM(mock(StorageContainerManager.class))
        .build();
  }
}
