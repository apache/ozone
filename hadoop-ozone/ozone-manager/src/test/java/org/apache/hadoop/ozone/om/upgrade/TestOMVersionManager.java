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

import static org.apache.hadoop.ozone.OzoneManagerVersion.SOFTWARE_VERSION;
import static org.apache.hadoop.ozone.OzoneManagerVersion.ZDU;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.DELEGATION_TOKEN_SYMMETRIC_SIGN;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.HBASE_SUPPORT;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.QUOTA;
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

import java.io.File;
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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.upgrade.AbstractComponentVersionManagerTest;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.provider.Arguments;
import org.mockito.Mockito;

/**
 * Tests for {@link OMVersionManager}. Shared abstract tests use an empty upgrade-action map; action behavior is
 * covered with map-based providers and a required classpath discovery test.
 */
class TestOMVersionManager extends AbstractComponentVersionManagerTest {

  private static final List<ComponentVersion> ALL_VERSIONS;

  private OzoneConfiguration conf;

  @TempDir
  private Path tempFolder;

  static {
    ALL_VERSIONS = new ArrayList<>(Arrays.asList(OMLayoutFeature.values()));
    for (OzoneManagerVersion version : OzoneManagerVersion.values()) {
      // Add all defined versions after and including ZDU to get the complete version list.
      if (ZDU.isSupportedBy(version) && version != OzoneManagerVersion.UNKNOWN_VERSION) {
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
  protected OMVersionManager createManager(int serializedApparentVersion) throws IOException {
    // By default create a version manager which does not have any upgrade actions to run. Production upgrade actions
    // may not be able to run in a test environment during finalization.
    return createManager(serializedApparentVersion, HashMap::new);
  }

  private OMVersionManager createManager(int serializedApparentVersion,
      ComponentUpgradeActionProvider<OmUpgradeAction> actions) throws IOException {
    OMStorage storage = newOmStorage(serializedApparentVersion);
    OzoneManager mockOM = Mockito.mock(OzoneManager.class);
    return new OMVersionManager(storage, mockOM, actions);
  }

  @Override
  protected List<ComponentVersion> allVersionsInOrder() {
    return ALL_VERSIONS;
  }

  @Override
  protected ComponentVersion expectedSoftwareVersion() {
    return OzoneManagerVersion.SOFTWARE_VERSION;
  }

  @Override
  @Test
  public void testClasspathScanDiscoversUpgradeActions() throws Exception {
    // Regardless of whether OM is finalized, the same set of upgrade actions should be loaded.
    try (OMVersionManager versionManager = createManager(INITIAL_VERSION.serialize(), new OMUpgradeActionProvider())) {
      assertTrue(versionManager.needsFinalization());
      OmUpgradeAction quotaAction = versionManager.getUpgradeActionsForTesting().get(QUOTA);
      assertInstanceOf(QuotaRepairUpgradeAction.class, quotaAction);
    }

    try (OMVersionManager versionManager = createManager(SOFTWARE_VERSION.serialize(), new OMUpgradeActionProvider())) {
      assertFalse(versionManager.needsFinalization());
      OmUpgradeAction quotaAction = versionManager.getUpgradeActionsForTesting().get(QUOTA);
      assertInstanceOf(QuotaRepairUpgradeAction.class, quotaAction);
    }
  }

  @Override
  @Test
  public void testFinalizeRunsSuppliedUpgradeAction() throws Exception {
    OmUpgradeAction mockECAction = mock(OmUpgradeAction.class);
    OmUpgradeAction mockZDUAction = mock(OmUpgradeAction.class);

    ComponentUpgradeActionProvider<OmUpgradeAction> provider = () -> {
      Map<ComponentVersion, OmUpgradeAction> m = new HashMap<>();
      m.put(ERASURE_CODED_STORAGE_SUPPORT, mockECAction);
      m.put(ZDU, mockZDUAction);
      return m;
    };

    try (OMVersionManager versionManager = createManager(QUOTA.serialize(), provider)) {
      versionManager.finalizeUpgrade();
      assertEquals(OzoneManagerVersion.SOFTWARE_VERSION, versionManager.getApparentVersion());

      // QUOTA was added after EC, so the EC upgrade action should not run when we finalize from this version.
      verify(mockECAction, never()).execute(any());
      verify(mockZDUAction, atLeastOnce()).execute(any());
      assertOmApparentVersionOnDisk(conf, OzoneManagerVersion.SOFTWARE_VERSION.serialize());
    }
  }

  @Override
  @Test
  public void testUpgradeActionFailureAbortsFinalize() throws Exception {
    ComponentUpgradeActionProvider<OmUpgradeAction> provider = () -> {
      Map<ComponentVersion, OmUpgradeAction> m = new HashMap<>();
      m.put(DELEGATION_TOKEN_SYMMETRIC_SIGN, o -> {
        throw new IOException("expected test failure");
      });
      return m;
    };

    try (OMVersionManager versionManager = createManager(QUOTA.serialize(), provider)) {
      UpgradeException thrown =
          assertThrows(UpgradeException.class, versionManager::finalizeUpgrade);
      assertEquals(UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED, thrown.getResult());
      // HBase is the version before symmetric encrypted delegation tokens, which has failed.
      assertEquals(HBASE_SUPPORT, versionManager.getApparentVersion());
      assertOmApparentVersionOnDisk(conf, HBASE_SUPPORT.serialize());
    }
  }

  @Override
  @Test
  public void testPersistFailureRollsBack() throws Exception {
    // Create a mock storage instance that throws when persisting version updates.
    OMStorage storage = mock(OMStorage.class);
    AtomicInteger persistedApparentVersion = new AtomicInteger(INITIAL_VERSION.serialize());
    when(storage.getApparentVersion()).thenAnswer(invocation -> persistedApparentVersion.get());
    doAnswer(invocation -> {
      persistedApparentVersion.set(invocation.getArgument(0));
      return null;
    }).when(storage).setApparentVersion(anyInt());
    doThrow(new IOException("persist failed")).when(storage).persistCurrentState();

    OzoneManager mockOM = Mockito.mock(OzoneManager.class);
    try (OMVersionManager versionManager = new OMVersionManager(storage, mockOM, HashMap::new)) {
      assertEquals(INITIAL_VERSION, versionManager.getApparentVersion());
      UpgradeException thrown = assertThrows(UpgradeException.class, versionManager::finalizeUpgrade);
      assertEquals(UpgradeException.ResultCodes.APPARENT_VERSION_UPDATE_FAILED, thrown.getResult());
      assertEquals(INITIAL_VERSION, versionManager.getApparentVersion());
      assertEquals(INITIAL_VERSION.serialize(), versionManager.getPersistedApparentVersion());
    }
  }

  private OMStorage newOmStorage(int apparentVersion)
      throws IOException {
    // Reinitialize the configuration to point to a new unique storage location.
    Path dbDir = Files.createDirectory(new File(tempFolder.toFile(), UUID.randomUUID().toString()).toPath());
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbDir.toString());
    OMStorage storage = new OMStorage(conf);
    storage.setClusterId("test-cluster");
    storage.setApparentVersion(apparentVersion);
    storage.setOmId(UUID.randomUUID().toString());
    storage.initialize();
    storage.persistCurrentState();
    return storage;
  }

  private static void assertOmApparentVersionOnDisk(OzoneConfiguration conf, int expected) throws IOException {
    OMStorage reloaded = new OMStorage(conf);
    assertEquals(expected, reloaded.getApparentVersion());
  }
}
