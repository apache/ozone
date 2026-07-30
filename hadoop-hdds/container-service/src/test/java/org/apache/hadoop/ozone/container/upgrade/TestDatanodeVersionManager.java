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

package org.apache.hadoop.ozone.container.upgrade;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.DATANODE_SCHEMA_V3;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.upgrade.DatanodeUpgradeAction;
import org.apache.hadoop.hdds.upgrade.DatanodeUpgradeActionProvider;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.container.common.DatanodeStorage;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.AbstractComponentVersionManagerTest;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.provider.Arguments;
import org.mockito.Mockito;

/**
 * Tests for {@link DatanodeVersionManager} using on-disk {@link DatanodeStorage} under a JUnit temp directory.
 */
class TestDatanodeVersionManager extends AbstractComponentVersionManagerTest {

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
        .map(Arguments::of);
  }

  @Override
  protected ComponentVersionManager createManager(int serializedApparentVersion) throws IOException {
    return createManager(serializedApparentVersion, HashMap::new);
  }

  private DatanodeVersionManager createManager(int serializedApparentVersion,
      ComponentUpgradeActionProvider<DatanodeUpgradeAction> actions) throws IOException {
    DatanodeStorage storage = newDatanodeStorage(serializedApparentVersion);
    return new DatanodeVersionManager(storage, null, actions);
  }

  private DatanodeStorage newDatanodeStorage(int apparentVersion) throws IOException {
    Path storageRoot = Files.createTempDirectory(tempDir, "dn-version-manager-");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageRoot.toString());
    DatanodeStorage storage = new DatanodeStorage(conf, UUID.randomUUID().toString(), apparentVersion);
    storage.setApparentVersion(apparentVersion);
    assertTrue(storage.getCurrentDir().mkdirs());
    storage.persistCurrentState();
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
    // Regardless of whether Datanode is finalized, the same set of upgrade actions should be loaded.
    try (DatanodeVersionManager versionManager = createManager(INITIAL_VERSION.serialize(),
        new DatanodeUpgradeActionProvider())) {
      assertTrue(versionManager.needsFinalization());
      DatanodeUpgradeAction quotaAction = versionManager.getUpgradeActionsForTesting().get(DATANODE_SCHEMA_V3);
      assertInstanceOf(DatanodeSchemaV3FinalizeAction.class, quotaAction);
      DatanodeUpgradeAction zduAction = versionManager.getUpgradeActionsForTesting().get(HDDSVersion.ZDU);
      assertInstanceOf(ZduDatanodeUpgradeActionForTest.class, zduAction);
    }

    try (DatanodeVersionManager versionManager =
        createManager(HDDSVersion.SOFTWARE_VERSION.serialize(), new DatanodeUpgradeActionProvider())) {
      assertFalse(versionManager.needsFinalization());
      DatanodeUpgradeAction quotaAction = versionManager.getUpgradeActionsForTesting().get(DATANODE_SCHEMA_V3);
      assertInstanceOf(DatanodeSchemaV3FinalizeAction.class, quotaAction);
      DatanodeUpgradeAction zduAction = versionManager.getUpgradeActionsForTesting().get(HDDSVersion.ZDU);
      assertInstanceOf(ZduDatanodeUpgradeActionForTest.class, zduAction);
    }
  }

  @Override
  @Test
  public void testFinalizeRunsSuppliedUpgradeAction() throws Exception {
    DatanodeUpgradeAction mockECAction = mock(DatanodeUpgradeAction.class);
    DatanodeUpgradeAction mockZDUAction = mock(DatanodeUpgradeAction.class);

    ComponentUpgradeActionProvider<DatanodeUpgradeAction> provider = () -> {
      Map<ComponentVersion, DatanodeUpgradeAction> m = new HashMap<>();
      m.put(ERASURE_CODED_STORAGE_SUPPORT, mockECAction);
      m.put(HDDSVersion.ZDU, mockZDUAction);
      return m;
    };

    try (DatanodeVersionManager versionManager = createManager(DATANODE_SCHEMA_V3.serialize(), provider)) {
      versionManager.finalizeUpgrade();
      assertEquals(HDDSVersion.SOFTWARE_VERSION, versionManager.getApparentVersion());

      // DATANODE_SCHEMA_V3 was added after EC, so the EC upgrade action should not run when we finalize from this
      // version.
      verify(mockECAction, never()).execute(any());
      verify(mockZDUAction, atLeastOnce()).execute(any());
      assertDatanodeApparentVersionOnDisk(conf, HDDSVersion.SOFTWARE_VERSION.serialize());
    }
  }

  @Override
  @Test
  public void testUpgradeActionFailureAbortsFinalize() throws Exception {
    ComponentUpgradeActionProvider<DatanodeUpgradeAction> provider = () -> {
      Map<ComponentVersion, DatanodeUpgradeAction> m = new HashMap<>();
      m.put(STORAGE_SPACE_DISTRIBUTION, o -> {
        throw new IOException("expected test failure");
      });
      return m;
    };

    try (DatanodeVersionManager versionManager =
             createManager(WITNESSED_CONTAINER_DB_PROTO_VALUE.serialize(), provider)) {
      UpgradeException thrown = assertThrows(UpgradeException.class, versionManager::finalizeUpgrade);
      assertEquals(UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED, thrown.getResult());
      // WITNESSED_CONTAINER_DB_PROTO_VALUE is the version before STORAGE_SPACE_DISTRIBUTION, which has failed.
      assertEquals(WITNESSED_CONTAINER_DB_PROTO_VALUE, versionManager.getApparentVersion());
      assertDatanodeApparentVersionOnDisk(conf, WITNESSED_CONTAINER_DB_PROTO_VALUE.serialize());
    }
  }

  @Override
  @Test
  public void testPersistFailureRollsBack() throws Exception {
    // Create a mock storage instance that throws when persisting version updates.
    DatanodeStorage storage = mock(DatanodeStorage.class);
    AtomicInteger persistedApparentVersion = new AtomicInteger(INITIAL_VERSION.serialize());
    when(storage.getApparentVersion()).thenAnswer(invocation -> persistedApparentVersion.get());
    doAnswer(invocation -> {
      persistedApparentVersion.set(invocation.getArgument(0));
      return null;
    }).when(storage).setApparentVersion(anyInt());
    doThrow(new IOException("persist failed")).when(storage).persistCurrentState();

    DatanodeStateMachine mockDN = Mockito.mock(DatanodeStateMachine.class);
    try (DatanodeVersionManager versionManager = new DatanodeVersionManager(storage, mockDN, HashMap::new)) {
      assertEquals(INITIAL_VERSION, versionManager.getApparentVersion());
      UpgradeException thrown = assertThrows(UpgradeException.class, versionManager::finalizeUpgrade);
      assertEquals(UpgradeException.ResultCodes.APPARENT_VERSION_UPDATE_FAILED, thrown.getResult());
      assertEquals(INITIAL_VERSION, versionManager.getApparentVersion());
      assertEquals(INITIAL_VERSION.serialize(), versionManager.getPersistedApparentVersion());
    }
  }

  private static void assertDatanodeApparentVersionOnDisk(OzoneConfiguration conf, int expected)
      throws IOException {
    DatanodeStorage reloaded = new DatanodeStorage(conf, UUID.randomUUID().toString());
    assertEquals(expected, reloaded.getApparentVersion());
  }
}
