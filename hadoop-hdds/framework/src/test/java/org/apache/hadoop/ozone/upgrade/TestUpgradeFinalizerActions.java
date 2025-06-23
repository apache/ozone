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

package org.apache.hadoop.ozone.upgrade;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.SCM;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FIRST_UPGRADE_START;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.VALIDATE_IN_PREFINALIZE;
import static org.apache.hadoop.ozone.upgrade.TestUpgradeFinalizerActions.MockLayoutFeature.VERSION_2;
import static org.apache.hadoop.ozone.upgrade.TestUpgradeFinalizerActions.MockLayoutFeature.VERSION_3;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.EnumMap;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.hdds.upgrade.HDDSUpgradeAction;
import org.apache.hadoop.hdds.upgrade.test.MockComponent;
import org.apache.hadoop.hdds.upgrade.test.MockComponent.MockDnUpgradeAction;
import org.apache.hadoop.hdds.upgrade.test.MockComponent.MockScmUpgradeAction;
import org.apache.hadoop.ozone.common.Storage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Class to test upgrade related actions.
 */
public class TestUpgradeFinalizerActions {

  @Test
  public void testRunPrefinalizeStateActions(@TempDir File file)
      throws IOException {

    VERSION_2.addAction(VALIDATE_IN_PREFINALIZE,
        new MockScmUpgradeAction());
    VERSION_3.addAction(ON_FIRST_UPGRADE_START, new MockDnUpgradeAction());
    MockLayoutVersionManager lvm = new MockLayoutVersionManager(1);
    MockUpgradeFinalizer uF = new MockUpgradeFinalizer(lvm);

    MockComponent mockObj = mock(MockComponent.class);

    File scmCurrent = Paths.get(file.toString(), "scm", "current")
        .toFile();
    assertTrue(scmCurrent.mkdirs());
    Storage storage = newStorage(file);
    uF.runPrefinalizeStateActions(storage, mockObj);

    verify(mockObj, times(1)).mockMethodScm();
    verify(mockObj, times(1)).mockMethodDn();

    // Running again does not run the first upgrade start action again.
    uF.runPrefinalizeStateActions(storage, mockObj);
    verify(mockObj, times(2)).mockMethodScm();
    verify(mockObj, times(1)).mockMethodDn();

    // Finalization will make sure these actions don't run again.
    lvm.finalized(VERSION_2);
    lvm.finalized(VERSION_3);
    uF.runPrefinalizeStateActions(storage, mockObj);
    verify(mockObj, times(2)).mockMethodScm();
    verify(mockObj, times(1)).mockMethodDn();
  }

  @Test
  public void testValidationFailureWorks(@TempDir File file) throws Exception {
    VERSION_2.addAction(VALIDATE_IN_PREFINALIZE,
        new MockFailingUpgradeAction());
    MockLayoutVersionManager lvm = new MockLayoutVersionManager(1);
    MockUpgradeFinalizer uF = new MockUpgradeFinalizer(lvm);

    MockComponent mockObj = mock(MockComponent.class);

    File scmCurrent = Paths.get(file.toString(), "scm", "current")
        .toFile();
    assertTrue(scmCurrent.mkdirs());
    Storage storage = newStorage(file);

    UpgradeException upgradeException = assertThrows(UpgradeException.class,
        () -> uF.runPrefinalizeStateActions(storage, mockObj));
    assertThat(upgradeException)
        .hasMessageContaining("Exception while running pre finalize state validation");
  }

  private Storage newStorage(File f) throws IOException {
    return new Storage(SCM, f, "scm", 1) {
      @Override
      protected Properties getNodeProperties() {
        return new Properties();
      }
    };
  }

  /**
   * Mock upgrade finalizer.
   */
  static class MockUpgradeFinalizer extends
      BasicUpgradeFinalizer<MockComponent, MockLayoutVersionManager> {

    MockUpgradeFinalizer(MockLayoutVersionManager versionManager) {
      super(versionManager);
    }

    @Override
    public void postFinalizeUpgrade(MockComponent c) {
      return;
    }

    @Override
    public void finalizeLayoutFeature(LayoutFeature lf, MockComponent c) {
      return;
    }

    @Override
    public void preFinalizeUpgrade(MockComponent c) {
      return;
    }

    @Override
    public void runPrefinalizeStateActions(Storage storage,
                                           MockComponent mockComponent)
        throws IOException {
      super.runPrefinalizeStateActions(
          lf -> ((MockLayoutFeature) lf)::action, storage, mockComponent);
    }
  }

  static class MockLayoutVersionManager extends
      AbstractLayoutVersionManager<MockLayoutFeature> {

    MockLayoutVersionManager(int lV) throws IOException {
      init(lV, MockLayoutFeature.values());
    }
  }

  /**
   * Mock Layout Feature list.
   */
  enum MockLayoutFeature implements LayoutFeature {
    VERSION_1(1),
    VERSION_2(2),
    VERSION_3(3);

    private int layoutVersion;
    private EnumMap<UpgradeActionType, UpgradeAction> actions =
        new EnumMap<>(UpgradeActionType.class);

    MockLayoutFeature(final int layoutVersion) {
      this.layoutVersion = layoutVersion;
    }

    @Override
    public int layoutVersion() {
      return layoutVersion;
    }

    @Override
    public String description() {
      return null;
    }

    public void addAction(UpgradeActionType type, UpgradeAction action) {
      this.actions.put(type, action);
    }

    @Override
    public Optional<? extends UpgradeAction> action(UpgradeActionType phase) {
      return Optional.ofNullable(actions.get(phase));
    }
  }

  /**
   * Mock DN Upgrade Action that fails.
   */
  public static class MockFailingUpgradeAction implements
      HDDSUpgradeAction<MockComponent> {
    @Override
    public void execute(MockComponent arg) throws Exception {
      throw new IllegalStateException("Failed action!!");
    }
  }
}
