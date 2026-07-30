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

package org.apache.hadoop.hdds.scm.server.upgrade;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.DATANODE_SCHEMA_V2;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.WITNESSED_CONTAINER_DB_PROTO_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.ScmUpgradeAction;
import org.apache.hadoop.hdds.upgrade.ScmUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.AbstractComponentVersionManagerTest;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Tests for {@link ScmVersionManager} using on-disk {@link SCMStorageConfig}
 * under a JUnit temp directory.
 */
class TestScmVersionManager extends AbstractComponentVersionManagerTest {

  private OzoneConfiguration conf;

  @TempDir
  private Path tempDir;

  private static final List<ComponentVersion> ALL_VERSIONS;

  static {
    ALL_VERSIONS = new ArrayList<>(Arrays.asList(HDDSLayoutFeature.values()));
    for (HDDSVersion version : HDDSVersion.values()) {
      // Add all defined versions after and including ZDU to get the complete version list.
      if (HDDSVersion.ZDU.isSupportedBy(version) && version != HDDSVersion.UNKNOWN_VERSION) {
        ALL_VERSIONS.add(version);
      }
    }
  }

  @BeforeEach
  public void init() {
    conf = new OzoneConfiguration();
  }

  public static Stream<Arguments> preFinalizedVersionArgs() {
    return ALL_VERSIONS.stream()
        .limit(ALL_VERSIONS.size() - 1)
        .map(org.junit.jupiter.params.provider.Arguments::of);
  }

  @Override
  protected ComponentVersionManager createManager(int serializedApparentVersion) throws IOException {
    return createManager(serializedApparentVersion, HashMap::new);
  }

  private ScmVersionManager createManager(int serializedApparentVersion,
      ComponentUpgradeActionProvider<ScmUpgradeAction> actions) throws IOException {
    SCMStorageConfig storage = newScmStorage(serializedApparentVersion);
    StorageContainerManager context = mock(StorageContainerManager.class);
    return new ScmVersionManager(storage, context, actions);
  }

  private SCMStorageConfig newScmStorage(int apparentVersion) throws IOException {
    Path storageRoot = Files.createTempDirectory(tempDir, "scm-version-manager-");
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, storageRoot.toString());
    SCMStorageConfig storage = new SCMStorageConfig(conf);
    storage.setScmId("test-scm");
    storage.setApparentVersion(apparentVersion);
    storage.initialize();
    return storage;
  }

  @Override
  protected List<ComponentVersion> allVersionsInOrder() {
    return ALL_VERSIONS;
  }

  @Override
  protected ComponentVersion expectedSoftwareVersion() {
    return HDDSVersion.SOFTWARE_VERSION;
  }

  @Override
  @Test
  public void testClasspathScanDiscoversUpgradeActions() throws Exception {
    try (ScmVersionManager versionManager = createManager(INITIAL_VERSION.serialize(),
        new ScmUpgradeActionProvider())) {
      assertTrue(versionManager.needsFinalization());
      ScmUpgradeAction upgradeAction = versionManager.getUpgradeActionsForTesting().get(DATANODE_SCHEMA_V2);
      assertInstanceOf(ScmOnFinalizeActionForDatanodeSchemaV2.class, upgradeAction);
      ScmUpgradeAction zduAction = versionManager.getUpgradeActionsForTesting().get(HDDSVersion.ZDU);
      assertInstanceOf(ZduScmUpgradeActionForTest.class, zduAction);
    }

    try (ScmVersionManager versionManager = createManager(HDDSVersion.SOFTWARE_VERSION.serialize(),
        new ScmUpgradeActionProvider())) {
      assertFalse(versionManager.needsFinalization());
      ScmUpgradeAction upgradeAction = versionManager.getUpgradeActionsForTesting().get(DATANODE_SCHEMA_V2);
      assertInstanceOf(ScmOnFinalizeActionForDatanodeSchemaV2.class, upgradeAction);
      ScmUpgradeAction zduAction = versionManager.getUpgradeActionsForTesting().get(HDDSVersion.ZDU);
      assertInstanceOf(ZduScmUpgradeActionForTest.class, zduAction);
    }
  }

  @Override
  @Test
  public void testFinalizeRunsSuppliedUpgradeAction() throws Exception {
    ScmUpgradeAction mockECAction = mock(ScmUpgradeAction.class);
    ScmUpgradeAction mockZDUAction = mock(ScmUpgradeAction.class);

    ComponentUpgradeActionProvider<ScmUpgradeAction> provider = () -> {
      Map<ComponentVersion, ScmUpgradeAction> m = new HashMap<>();
      m.put(ERASURE_CODED_STORAGE_SUPPORT, mockECAction);
      m.put(HDDSVersion.ZDU, mockZDUAction);
      return m;
    };

    try (ScmVersionManager versionManager = createManager(ERASURE_CODED_STORAGE_SUPPORT.serialize(), provider)) {
      versionManager.finalizeUpgrade();
      assertEquals(HDDSVersion.SOFTWARE_VERSION, versionManager.getApparentVersion());

      // Apparent version is already EC; finalization runs actions for later versions only, not for EC itself.
      verify(mockECAction, never()).execute(any());
      verify(mockZDUAction, atLeastOnce()).execute(any());
      assertScmApparentVersionOnDisk(conf, HDDSVersion.SOFTWARE_VERSION.serialize());
    }
  }

  @Override
  @Test
  public void testUpgradeActionFailureAbortsFinalize() throws Exception {
    ComponentUpgradeActionProvider<ScmUpgradeAction> provider = () -> {
      Map<ComponentVersion, ScmUpgradeAction> m = new HashMap<>();
      m.put(STORAGE_SPACE_DISTRIBUTION, o -> {
        throw new IOException("expected test failure");
      });
      return m;
    };

    try (ScmVersionManager versionManager =
             createManager(WITNESSED_CONTAINER_DB_PROTO_VALUE.serialize(), provider)) {
      UpgradeException thrown = assertThrows(UpgradeException.class, versionManager::finalizeUpgrade);
      assertEquals(UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED, thrown.getResult());
      // WITNESSED_CONTAINER_DB_PROTO_VALUE is the version before STORAGE_SPACE_DISTRIBUTION, which has failed.
      assertEquals(WITNESSED_CONTAINER_DB_PROTO_VALUE, versionManager.getApparentVersion());
      assertScmApparentVersionOnDisk(conf, WITNESSED_CONTAINER_DB_PROTO_VALUE.serialize());
    }
  }

  @Override
  @Test
  public void testPersistFailureRollsBack() throws Exception {
    SCMStorageConfig storage = mock(SCMStorageConfig.class);
    AtomicInteger persistedApparentVersion = new AtomicInteger(INITIAL_VERSION.serialize());
    when(storage.getApparentVersion()).thenAnswer(invocation -> persistedApparentVersion.get());
    doAnswer(invocation -> {
      persistedApparentVersion.set(invocation.getArgument(0));
      return null;
    }).when(storage).setApparentVersion(anyInt());
    doThrow(new IOException("persist failed")).when(storage).persistCurrentState();

    StorageContainerManager context = mock(StorageContainerManager.class);
    try (ScmVersionManager versionManager = new ScmVersionManager(storage, context, HashMap::new)) {
      assertEquals(INITIAL_VERSION, versionManager.getApparentVersion());
      UpgradeException thrown = assertThrows(UpgradeException.class, versionManager::finalizeUpgrade);
      assertEquals(UpgradeException.ResultCodes.APPARENT_VERSION_UPDATE_FAILED, thrown.getResult());
      assertEquals(INITIAL_VERSION, versionManager.getApparentVersion());
      assertEquals(INITIAL_VERSION.serialize(), versionManager.getPersistedApparentVersion());
      assertEquals(INITIAL_VERSION.serialize(), storage.getApparentVersion());
    }
  }

  private static void assertScmApparentVersionOnDisk(OzoneConfiguration conf, int expected)
      throws IOException {
    SCMStorageConfig reloaded = new SCMStorageConfig(conf);
    assertEquals(expected, reloaded.getApparentVersion());
  }
}
