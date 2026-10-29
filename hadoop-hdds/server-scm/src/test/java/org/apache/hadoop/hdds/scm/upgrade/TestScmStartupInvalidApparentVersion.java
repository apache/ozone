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

import static org.apache.hadoop.hdds.HDDSVersion.SOFTWARE_VERSION;
import static org.apache.hadoop.hdds.HDDSVersion.ZDU;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.OzoneConsts.APPARENT_VERSION_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_HA;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ensures SCM does not start when the VERSION file records an apparent version
 * that is larger than the software version, and that it finalizes on startup
 * when the DB records a higher apparent version than the VERSION file.
 */
public class TestScmStartupInvalidApparentVersion {

  @TempDir
  private Path folder;

  @Test
  public void testStartupFailsWhenApparentVersionBetweenLastLayoutFeatureAndZdu() throws Exception {
    assertStartupFailsWithComponentVersionMessage(
        HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION.serialize() + 1);
  }

  @Test
  public void testStartupFailsWhenApparentVersionBeyondLastKnownComponentVersion() throws Exception {
    assertStartupFailsWithComponentVersionMessage(SOFTWARE_VERSION.serialize() + 1);
  }

  /**
   * An SCM that stops after replacing its DB with a finalized checkpoint, but before updating its VERSION file, cannot
   * replay the finalization from the Raft log, so it should finalize up to the version in its DB on restart.
   */
  @Test
  public void testStartupFinalizesToApparentVersionInDB() throws Exception {
    OzoneConfiguration conf = createScmStorage(INITIAL_VERSION.serialize());
    StorageContainerManager scm = HddsTestUtils.getScm(conf, new SCMConfigurator());
    try {
      // Without a version in the DB, startup leaves the SCM pre-finalized.
      assertThat(scm.getScmMetadataStore().getMetaTable().get(APPARENT_VERSION_KEY)).isNull();
      assertThat(scm.getVersionManager().getApparentVersion()).isEqualTo(INITIAL_VERSION);

      // The DB is ahead of the VERSION file, as the interrupted install leaves it. The SCM finalizes to the
      // software version
      scm = restartWithDBVersion(scm, SOFTWARE_VERSION);
      assertVersions(scm, SOFTWARE_VERSION, SOFTWARE_VERSION);
      assertThat(scm.getVersionManager().needsFinalization()).isFalse();
    } finally {
      scm.stop();
      scm.join();
    }
  }

  /**
   * The VERSION file is ahead of the DB. This can happen if finalization wrote the VERSION file (inside Ratis
   * applyTransaction) but the process crashed before the Ratis transaction buffer was flushed to RocksDB.
   * Ratis log replay should correct this, but finalizeFromSnapshotIfRequired also updates the DB as a safety net.
   */
  @Test
  public void testStartupUpdatesDBWhenVersionFileAheadOfDB() throws Exception {
    // Create an SCM whose VERSION file is already at SOFTWARE_VERSION (simulates the crash scenario where the
    // VERSION file was written during finalization but the DB was not yet flushed).
    OzoneConfiguration conf = createScmStorage(SOFTWARE_VERSION.serialize());
    StorageContainerManager scm = HddsTestUtils.getScm(conf, new SCMConfigurator());
    try {
      // Force the DB to a lower version to simulate the unflushed transaction buffer.
      scm.getScmMetadataStore().getMetaTable().put(APPARENT_VERSION_KEY, String.valueOf(ZDU.serialize()));
      scm.stop();
      scm.join();

      // On restart, finalizeFromSnapshotIfRequired should detect VERSION file > DB and update the DB.
      scm = HddsTestUtils.getScm(scm.getConfiguration(), new SCMConfigurator());
      assertVersions(scm, SOFTWARE_VERSION, SOFTWARE_VERSION);
    } finally {
      scm.stop();
      scm.join();
    }
  }

  /**
   * Writes {@code dbVersion} to the DB of {@code scm}, stops it and creates a new SCM on the same storage.
   */
  private static StorageContainerManager restartWithDBVersion(StorageContainerManager scm, ComponentVersion dbVersion)
      throws Exception {
    scm.getScmMetadataStore().getMetaTable().put(APPARENT_VERSION_KEY, String.valueOf(dbVersion.serialize()));
    scm.stop();
    scm.join();
    return HddsTestUtils.getScm(scm.getConfiguration(), new SCMConfigurator());
  }

  private static void assertVersions(StorageContainerManager scm, ComponentVersion expectedApparentVersion,
      ComponentVersion expectedDBVersion) throws IOException {
    assertThat(scm.getVersionManager().getApparentVersion()).isEqualTo(expectedApparentVersion);
    assertThat(new SCMStorageConfig(scm.getConfiguration()).getApparentVersion())
        .isEqualTo(expectedApparentVersion.serialize());
    assertThat(scm.getScmMetadataStore().getMetaTable().get(APPARENT_VERSION_KEY))
        .isEqualTo(String.valueOf(expectedDBVersion.serialize()));
  }

  private void assertStartupFailsWithComponentVersionMessage(int serializedApparentVersion)
      throws Exception {
    OzoneConfiguration conf = createScmStorage(serializedApparentVersion);

    String expectedMessage =
        "Initialization failed. Disk contains unknown apparent version " + serializedApparentVersion
            + " for software version " + HDDSVersion.SOFTWARE_VERSION + ". Make sure this component was not" +
            " downgraded after finalization";

    IOException ioException =
        assertThrows(IOException.class, () -> new StorageContainerManager(conf));
    assertEquals(expectedMessage, ioException.getMessage());
  }

  private OzoneConfiguration createScmStorage(int serializedApparentVersion) throws IOException {
    File scmSubdir = folder.resolve("scm").resolve("current").toFile();
    assertTrue(scmSubdir.mkdirs());

    File ratisDir = folder.resolve("scm.ratis").toFile();
    File snapshotDir = folder.resolve("scm.ratis.snapshot").toFile();
    assertTrue(ratisDir.mkdirs());
    assertTrue(snapshotDir.mkdirs());

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, folder.toAbsolutePath().toString());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, folder.toAbsolutePath().toString());

    Properties properties = new Properties();
    properties.setProperty(SCM_ID, "scm");
    properties.setProperty(SCM_HA, "true");

    UpgradeTestUtils.createVersionFile(scmSubdir, HddsProtos.NodeType.SCM, serializedApparentVersion, properties);
    return conf;
  }
}
