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

package org.apache.hadoop.hdds.scm.ha;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.nio.file.Path;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test cases for {@link SCMSnapshotProvider}.
 */
class TestSCMSnapshotProvider {

  /**
   * Test constructor succeeds when both Ratis storage and snapshot directories exist.
   */
  @Test
  void testConstructorSucceedsWithExistingDirectories(@TempDir Path tempDir) {
    // Setup: Create both directories (simulating initialized SCM)
    OzoneConfiguration conf = createConfig(tempDir);
    String ratisDir = SCMHAUtils.getSCMRatisDirectory(conf);
    String snapshotDir = SCMHAUtils.getSCMRatisSnapshotDirectory(conf);

    assertTrue(new File(ratisDir).mkdirs());
    assertTrue(new File(snapshotDir).mkdirs());

    CertificateClient certClient = mock(CertificateClient.class);

    // Test
    SCMSnapshotProvider provider = new SCMSnapshotProvider(
        conf, null, certClient);

    // Verify
    assertNotNull(provider);
    assertNotNull(provider.getScmSnapshotDir());
    assertTrue(provider.getScmSnapshotDir().exists());
    assertEquals(snapshotDir, provider.getScmSnapshotDir().getAbsolutePath());
  }

  /**
   * Test constructor fails when snapshot directory does not exist.
   */
  @Test
  void testConstructorFailsWhenSnapshotDirMissing(@TempDir Path tempDir) {
    // Setup: Create only Ratis storage directory, not snapshot directory
    OzoneConfiguration conf = createConfig(tempDir);
    String ratisDir = SCMHAUtils.getSCMRatisDirectory(conf);
    assertTrue(new File(ratisDir).mkdirs());

    CertificateClient certClient = mock(CertificateClient.class);

    // Test & Verify
    IllegalStateException exception = assertThrows(
        IllegalStateException.class,
        () -> new SCMSnapshotProvider(conf, null, certClient));

    assertTrue(exception.getMessage().contains("Ratis snapshot directory does not exist"));
  }

  /**
   * Test constructor fails when Ratis storage directory does not exist.
   */
  @Test
  void testConstructorFailsWhenRatisDirMissing(@TempDir Path tempDir) {
    // Setup: Don't create any directories
    OzoneConfiguration conf = createConfig(tempDir);
    CertificateClient certClient = mock(CertificateClient.class);

    // Test & Verify
    IllegalStateException exception = assertThrows(
        IllegalStateException.class,
        () -> new SCMSnapshotProvider(conf, null, certClient));

    assertTrue(exception.getMessage().contains("Ratis storage directory does not exist"));
  }

  /**
   * Helper method to create OzoneConfiguration with temp directories.
   */
  private OzoneConfiguration createConfig(Path tempDir) {
    OzoneConfiguration conf = new OzoneConfiguration();

    String metadataDir = tempDir.resolve("metadata").toString();
    String ratisStorageDir = tempDir.resolve("ratis").toString();
    String ratisSnapshotDir = tempDir.resolve("ratis-snapshot").toString();

    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metadataDir);
    conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_STORAGE_DIR, ratisStorageDir);
    conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_DIR, ratisSnapshotDir);

    return conf;
  }
}
