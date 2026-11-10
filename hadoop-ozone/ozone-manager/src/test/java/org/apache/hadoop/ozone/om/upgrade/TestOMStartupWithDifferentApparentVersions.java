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

import static org.apache.hadoop.ozone.OzoneConsts.APPARENT_VERSION_KEY;
import static org.apache.hadoop.ozone.OzoneManagerVersion.SOFTWARE_VERSION;
import static org.apache.hadoop.ozone.OzoneManagerVersion.ZDU;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.UUID;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ensures OM finalizes on startup when the DB records a higher apparent version than the VERSION file, and that it
 * fails startup when the VERSION file records an apparent version that the software version does not recognize.
 */
public class TestOMStartupWithDifferentApparentVersions {

  @TempDir
  private Path folder;

  @Test
  public void testStartupFailsWhenApparentVersionBetweenLastLayoutFeatureAndZdu() throws Exception {
    OMLayoutFeature[] features = OMLayoutFeature.values();
    assertStartupFailsWithInvalidVersionMessage(features[features.length - 1].serialize() + 1);
  }

  @Test
  public void testStartupFailsWhenApparentVersionBeyondLastKnownComponentVersion() throws Exception {
    assertStartupFailsWithInvalidVersionMessage(SOFTWARE_VERSION.serialize() + 1);
  }

  /**
   * An OM that stops after replacing its DB with a finalized checkpoint, but before updating its VERSION file, cannot
   * replay the finalization from the Raft log, so it should finalize up to the version in its DB on restart.
   */
  @Test
  public void testStartupFinalizesToApparentVersionInDB() throws Exception {
    OzoneConfiguration conf = createOMStorage(INITIAL_VERSION.serialize());
    try (OMVersionManager versionManager = createVersionManager(conf);
        OmMetadataManagerImpl metadataManager = new OmMetadataManagerImpl(conf, null)) {
      // Without a version in the DB, startup leaves the OM pre-finalized.
      assertThat(metadataManager.getMetaTable().get(APPARENT_VERSION_KEY)).isNull();
      assertThat(versionManager.getApparentVersion()).isEqualTo(INITIAL_VERSION);

      // The DB is ahead of the VERSION file, as the interrupted install leaves it. The OM finalizes to the DB version.
      // finalizeFromDBIfRequired is called by OM.start(), but calling that here keeps the test fast and self-contained,
      // since starting OM requires SCM and other infrastructure.
      metadataManager.getMetaTable().put(APPARENT_VERSION_KEY, String.valueOf(SOFTWARE_VERSION.serialize()));
      versionManager.finalizeFromDBIfRequired(metadataManager.getMetaTable());

      assertVersions(versionManager, conf, metadataManager, SOFTWARE_VERSION, SOFTWARE_VERSION);
      assertThat(versionManager.needsFinalization()).isFalse();
    }
  }

  /**
   * The VERSION file is ahead of the DB. This can happen if finalization wrote the VERSION file (inside Ratis
   * applyTransaction) but the process crashed before the Ratis transaction buffer was flushed to RocksDB.
   * Ratis log replay is async and may not have finished when this method runs, so this is treated as an expected
   * case and no action is taken — log replay will correct the DB after startup.
   */
  @Test
  public void testFinalizeFromDBNoOpWhenVersionFileAheadOfDB() throws Exception {
    OzoneConfiguration conf = createOMStorage(SOFTWARE_VERSION.serialize());
    try (OMVersionManager versionManager = createVersionManager(conf);
        OmMetadataManagerImpl metadataManager = new OmMetadataManagerImpl(conf, null)) {
      // Force the DB to a lower version to simulate the unflushed transaction buffer.
      metadataManager.getMetaTable().put(APPARENT_VERSION_KEY, String.valueOf(ZDU.serialize()));

      // On restart, finalizeFromDBIfRequired detects DB version < VERSION file version but takes no action.
      versionManager.finalizeFromDBIfRequired(metadataManager.getMetaTable());

      // Apparent version unchanged; DB version also unchanged (left for Ratis log replay to correct).
      assertVersions(versionManager, conf, metadataManager, SOFTWARE_VERSION, ZDU);
    }
  }

  /**
   * When the DB has no apparent version, the component was never finalized. finalizeFromDBIfRequired takes no action.
   */
  @Test
  public void testFinalizeFromDBNoOpWhenDBNull() throws Exception {
    OzoneConfiguration conf = createOMStorage(INITIAL_VERSION.serialize());
    try (OMVersionManager versionManager = createVersionManager(conf);
        OmMetadataManagerImpl metadataManager = new OmMetadataManagerImpl(conf, null)) {
      assertThat(metadataManager.getMetaTable().get(APPARENT_VERSION_KEY)).isNull();
      versionManager.finalizeFromDBIfRequired(metadataManager.getMetaTable());
      assertThat(versionManager.getApparentVersion()).isEqualTo(INITIAL_VERSION);
      assertThat(metadataManager.getMetaTable().get(APPARENT_VERSION_KEY)).isNull();
    }
  }

  /**
   * When the DB matches the apparent version, no finalization or update is needed.
   */
  @Test
  public void testFinalizeFromDBNoOpWhenDBEqualsApparent() throws Exception {
    OzoneConfiguration conf = createOMStorage(ZDU.serialize());
    try (OMVersionManager versionManager = createVersionManager(conf);
        OmMetadataManagerImpl metadataManager = new OmMetadataManagerImpl(conf, null)) {
      metadataManager.getMetaTable().put(APPARENT_VERSION_KEY, String.valueOf(ZDU.serialize()));
      versionManager.finalizeFromDBIfRequired(metadataManager.getMetaTable());
      assertVersions(versionManager, conf, metadataManager, ZDU, ZDU);
      assertTrue(versionManager.needsFinalization());
    }
  }

  /**
   * The DB version is between the apparent version and the software version. This should not happen in practice,
   * but if it does the safest recovery is to partially finalize up to the DB version to avoid mixed apparent versions
   * in the quorum.
   */
  @Test
  public void testFinalizeFromDBFinalizesToIntermediateDBVersion() throws Exception {
    OzoneConfiguration conf = createOMStorage(INITIAL_VERSION.serialize());
    try (OMVersionManager versionManager = createVersionManager(conf);
        OmMetadataManagerImpl metadataManager = new OmMetadataManagerImpl(conf, null)) {
      metadataManager.getMetaTable().put(APPARENT_VERSION_KEY, String.valueOf(ZDU.serialize()));
      versionManager.finalizeFromDBIfRequired(metadataManager.getMetaTable());
      assertVersions(versionManager, conf, metadataManager, ZDU, ZDU);
      // Component is not fully finalized: ZDU < SOFTWARE_VERSION.
      assertTrue(versionManager.needsFinalization());
    }
  }

  /**
   * The DB version exceeds the software version. This cannot be supported and the process must crash.
   */
  @Test
  public void testFinalizeFromDBThrowsWhenDBVersionExceedsSoftwareVersion() throws Exception {
    OzoneConfiguration conf = createOMStorage(INITIAL_VERSION.serialize());
    try (OMVersionManager versionManager = createVersionManager(conf);
        OmMetadataManagerImpl metadataManager = new OmMetadataManagerImpl(conf, null)) {
      metadataManager.getMetaTable().put(APPARENT_VERSION_KEY,
          String.valueOf(SOFTWARE_VERSION.serialize() + 1));
      assertThrows(IOException.class,
          () -> versionManager.finalizeFromDBIfRequired(metadataManager.getMetaTable()));
    }
  }

  private static void assertVersions(OMVersionManager versionManager, OzoneConfiguration conf,
      OmMetadataManagerImpl metadataManager, ComponentVersion expectedApparentVersion,
      ComponentVersion expectedDBVersion) throws IOException {
    assertThat(versionManager.getApparentVersion()).isEqualTo(expectedApparentVersion);
    assertThat(new OMStorage(conf).getApparentVersion()).isEqualTo(expectedApparentVersion.serialize());
    assertThat(metadataManager.getMetaTable().get(APPARENT_VERSION_KEY))
        .isEqualTo(String.valueOf(expectedDBVersion.serialize()));
  }

  private void assertStartupFailsWithInvalidVersionMessage(int serializedApparentVersion) throws Exception {
    OzoneConfiguration conf = createOMStorage(serializedApparentVersion);

    String expectedMessage =
        "Initialization failed. Disk contains unknown apparent version " + serializedApparentVersion
            + " for software version " + OzoneManagerVersion.SOFTWARE_VERSION + ". Make sure OM was not downgraded" +
            " after finalization";

    IOException ioException = assertThrows(IOException.class, () -> createVersionManager(conf));
    assertThat(ioException).hasMessageContaining(expectedMessage);
  }

  private OzoneConfiguration createOMStorage(int serializedApparentVersion) throws IOException {
    Path dbDir = folder.resolve(UUID.randomUUID().toString());
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbDir.toAbsolutePath().toString());

    OMStorage storage = new OMStorage(conf);
    storage.setClusterId(UUID.randomUUID().toString());
    storage.setOmId(UUID.randomUUID().toString());
    storage.setApparentVersion(serializedApparentVersion);
    storage.initialize();
    storage.persistCurrentState();
    return conf;
  }

  private static OMVersionManager createVersionManager(OzoneConfiguration conf) throws IOException {
    OMStorage storage = new OMStorage(conf);
    OzoneManager mockOM = mock(OzoneManager.class);
    return new OMVersionManager(storage, mockOM, HashMap::new);
  }
}
