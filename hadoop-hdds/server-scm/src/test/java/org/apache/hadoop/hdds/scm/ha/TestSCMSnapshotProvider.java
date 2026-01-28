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
import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test cases for {@link SCMSnapshotProvider} with different startup modes.
 */
class TestSCMSnapshotProvider {

  /**
   * Test 1: Default constructor (no startup option parameter).
   * Should use NORMAL mode by default.
   * Expects both Ratis storage and snapshot directories to exist.
   */
  @Test
  void testDefaultConstructor(@TempDir Path tempDir) throws IOException {
    // Setup: Create both directories (simulating initialized SCM)
    OzoneConfiguration conf = createConfig(tempDir);
    String ratisDir = SCMHAUtils.getSCMRatisDirectory(conf);
    String snapshotDir = SCMHAUtils.getSCMRatisSnapshotDirectory(conf);

    assertTrue(new File(ratisDir).mkdirs());
    assertTrue(new File(snapshotDir).mkdirs());

    CertificateClient certClient = mock(CertificateClient.class);

    // Test: Use default constructor (should use NORMAL mode)
    SCMSnapshotProvider provider = new SCMSnapshotProvider(
        conf, null, certClient);

    // Verify: Provider created successfully
    assertNotNull(provider);
    assertNotNull(provider.getScmSnapshotDir());
    assertTrue(provider.getScmSnapshotDir().exists());
    assertEquals(snapshotDir, provider.getScmSnapshotDir().getAbsolutePath());
  }

  /**
   * Test 2: Default constructor when snapshot directory does not exist.
   * Should fail because NORMAL mode expects directory to exist.
   */
  @Test
  void testDefaultConstructorFailsWhenSnapshotDirMissing(@TempDir Path tempDir) {
    // Setup: Create only Ratis storage directory, not snapshot directory
    OzoneConfiguration conf = createConfig(tempDir);
    String ratisDir = SCMHAUtils.getSCMRatisDirectory(conf);
    assertTrue(new File(ratisDir).mkdirs());

    CertificateClient certClient = mock(CertificateClient.class);

    // Test & Verify: Should throw exception
    IllegalStateException exception = assertThrows(
        IllegalStateException.class,
        () -> new SCMSnapshotProvider(conf, null, certClient));

    assertTrue(exception.getMessage().contains("Ratis snapshot directory does not exist"));
  }

  /**
   * Test 3: Explicit NORMAL mode with existing directories.
   * Should succeed when both directories exist.
   */
  @Test
  void testNormalModeWithExistingDirectories(@TempDir Path tempDir) throws IOException {
    // Setup: Create both directories
    OzoneConfiguration conf = createConfig(tempDir);
    String ratisDir = SCMHAUtils.getSCMRatisDirectory(conf);
    String snapshotDir = SCMHAUtils.getSCMRatisSnapshotDirectory(conf);

    assertTrue(new File(ratisDir).mkdirs());
    assertTrue(new File(snapshotDir).mkdirs());

    CertificateClient certClient = mock(CertificateClient.class);

    // Test: Explicitly use NORMAL mode
    SCMSnapshotProvider provider = new SCMSnapshotProvider(
        conf, null, certClient, SCMSnapshotProvider.StartupOption.NORMAL);

    // Verify: Provider created successfully
    assertNotNull(provider);
    assertNotNull(provider.getScmSnapshotDir());
    assertTrue(provider.getScmSnapshotDir().exists());
  }

  /**
   * Test 4: Explicit NORMAL mode when snapshot directory missing.
   * Should fail with clear error message.
   */
  @Test
  void testNormalModeFailsWhenSnapshotDirMissing(@TempDir Path tempDir) {
    // Setup: Create only Ratis storage directory
    OzoneConfiguration conf = createConfig(tempDir);
    String ratisDir = SCMHAUtils.getSCMRatisDirectory(conf);
    assertTrue(new File(ratisDir).mkdirs());

    CertificateClient certClient = mock(CertificateClient.class);

    // Test & Verify: Should throw exception
    IllegalStateException exception = assertThrows(
        IllegalStateException.class,
        () -> new SCMSnapshotProvider(
            conf, null, certClient, SCMSnapshotProvider.StartupOption.NORMAL));

    assertTrue(exception.getMessage().contains("Ratis snapshot directory does not exist"));
    assertTrue(exception.getMessage().contains("Please run 'ozone scm --init' first"));
  }

  /**
   * Test 5: Explicit NORMAL mode when Ratis storage directory missing.
   * Should fail because SCMRatisServerImpl should have created it.
   */
  @Test
  void testNormalModeFailsWhenRatisDirMissing(@TempDir Path tempDir) {
    // Setup: Don't create any directories
    OzoneConfiguration conf = createConfig(tempDir);
    CertificateClient certClient = mock(CertificateClient.class);

    // Test & Verify: Should throw exception
    IllegalStateException exception = assertThrows(
        IllegalStateException.class,
        () -> new SCMSnapshotProvider(
            conf, null, certClient, SCMSnapshotProvider.StartupOption.NORMAL));

    assertTrue(exception.getMessage().contains("Ratis storage directory does not exist"));
    assertTrue(exception.getMessage().contains("SCMRatisServerImpl.initialize()"));
  }

  /**
   * Test 6: FORMAT mode creates new snapshot directory.
   * Should succeed when snapshot directory does not exist.
   */
  @Test
  void testFormatModeCreatesNewDirectory(@TempDir Path tempDir) throws IOException {
    // Setup: Create only Ratis storage directory (as SCMRatisServerImpl would)
    OzoneConfiguration conf = createConfig(tempDir);
    String ratisDir = SCMHAUtils.getSCMRatisDirectory(conf);
    String snapshotDir = SCMHAUtils.getSCMRatisSnapshotDirectory(conf);

    assertTrue(new File(ratisDir).mkdirs());
    // Snapshot directory should NOT exist for FORMAT mode

    CertificateClient certClient = mock(CertificateClient.class);

    // Test: Use FORMAT mode
    SCMSnapshotProvider provider = new SCMSnapshotProvider(
        conf, null, certClient, SCMSnapshotProvider.StartupOption.FORMAT);

    // Verify: Provider created and snapshot directory was created
    assertNotNull(provider);
    assertNotNull(provider.getScmSnapshotDir());
    assertTrue(provider.getScmSnapshotDir().exists());
    assertEquals(snapshotDir, provider.getScmSnapshotDir().getAbsolutePath());
  }

  /**
   * Test 7: FORMAT mode fails when snapshot directory already exists.
   * Should prevent accidental data loss.
   */
  @Test
  void testFormatModeFailsWhenSnapshotDirExists(@TempDir Path tempDir) {
    // Setup: Create both directories (simulating existing SCM)
    OzoneConfiguration conf = createConfig(tempDir);
    String ratisDir = SCMHAUtils.getSCMRatisDirectory(conf);
    String snapshotDir = SCMHAUtils.getSCMRatisSnapshotDirectory(conf);

    assertTrue(new File(ratisDir).mkdirs());
    assertTrue(new File(snapshotDir).mkdirs());  // This exists!

    CertificateClient certClient = mock(CertificateClient.class);

    // Test & Verify: FORMAT should fail when directory exists
    IllegalStateException exception = assertThrows(
        IllegalStateException.class,
        () -> new SCMSnapshotProvider(
            conf, null, certClient, SCMSnapshotProvider.StartupOption.FORMAT));

    assertTrue(exception.getMessage().contains("Cannot format"));
    assertTrue(exception.getMessage().contains("already exists"));
  }

  /**
   * Test 8: FORMAT mode fails when Ratis storage directory missing.
   * Even in FORMAT mode, Ratis storage dir should exist (created by SCMRatisServerImpl).
   */
  @Test
  void testFormatModeFailsWhenRatisDirMissing(@TempDir Path tempDir) {
    // Setup: Don't create Ratis storage directory
    OzoneConfiguration conf = createConfig(tempDir);
    CertificateClient certClient = mock(CertificateClient.class);

    // Test & Verify: Should throw exception
    IllegalStateException exception = assertThrows(
        IllegalStateException.class,
        () -> new SCMSnapshotProvider(
            conf, null, certClient, SCMSnapshotProvider.StartupOption.FORMAT));

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
