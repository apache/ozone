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

package org.apache.hadoop.hdds.server;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.DATANODE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.OM;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.SCM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link ServerUtils}.
 */
public class TestServerUtils {

  @TempDir
  private Path folder;

  /**
   * Test case for {@link ServerUtils#getPermissions}.
   * Verifies the retrieval of permissions for different configs.
   */
  @Test
  public void testGetPermissions() {
    // Create an OzoneConfiguration object and set the permissions
    // for different keys
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ReconConfigKeys.OZONE_RECON_DB_DIRS_PERMISSIONS, "750");
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS_PERMISSIONS, "775");
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS_PERMISSIONS, "770");
    conf.set(OzoneConfigKeys.OZONE_OM_DB_DIRS_PERMISSIONS, "700");

    // Test getPermissions for different config names and assert the
    // returned permissions
    assertEquals("750",
        ServerUtils.getPermissions(ReconConfigKeys.OZONE_RECON_DB_DIR, conf));
    assertEquals("775",
        ServerUtils.getPermissions(ScmConfigKeys.OZONE_SCM_DB_DIRS, conf));
    assertEquals("770",
        ServerUtils.getPermissions(OzoneConfigKeys.OZONE_METADATA_DIRS, conf));
    assertEquals("700",
        ServerUtils.getPermissions(OzoneConfigKeys.OZONE_OM_DB_DIRS, conf));

  }

  @Test
  public void testGetPermissionsWithDefaults() {
    // Create an OzoneConfiguration without explicitly setting permissions
    // Should fall back to default values from ozone-default.xml (700)
    OzoneConfiguration conf = new OzoneConfiguration();

    // Test getPermissions for different config names and verify they use defaults
    assertEquals("700",
        ServerUtils.getPermissions(ReconConfigKeys.OZONE_RECON_DB_DIR, conf),
        "Should use default 700 for Recon DB dirs");
    assertEquals("700",
        ServerUtils.getPermissions(ScmConfigKeys.OZONE_SCM_DB_DIRS, conf),
        "Should use default 700 for SCM DB dirs");
    assertEquals("700",
        ServerUtils.getPermissions(OzoneConfigKeys.OZONE_METADATA_DIRS, conf),
        "Should use default 700 for metadata dirs");
    assertEquals("700",
        ServerUtils.getPermissions(OzoneConfigKeys.OZONE_OM_DB_DIRS, conf),
        "Should use default 700 for OM DB dirs");
  }

  @Test
  public void testGetDirectoryFromConfigWithOctalPermissions()
      throws IOException {
    // Create a temporary directory
    String filePath = folder.toAbsolutePath().toString();

    // Create an OzoneConfiguration object
    OzoneConfiguration conf = new OzoneConfiguration();
    String key = OzoneConfigKeys.OZONE_METADATA_DIRS;
    String componentName = "Ozone";
    conf.set(key, filePath);

    // Set octal permissions
    String octalPermissionValue = "750";
    String octalExpectedPermissions = "rwxr-x---";
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS_PERMISSIONS,
        octalPermissionValue);

    // Get the directory using ServerUtils method
    File directory =
        ServerUtils.getDirectoryFromConfig(conf, key, componentName);

    // Assert that the directory is not null and exists
    assertNotNull(directory);
    assertTrue(directory.exists());
    assertTrue(directory.isDirectory());

    // Get the path of the directory
    Path directoryPath = directory.toPath();

    // Convert the expected octal permissions string to a
    // set of PosixFilePermission
    Set<PosixFilePermission> octalExpectedPermissionSet =
        PosixFilePermissions.fromString(octalExpectedPermissions);

    // Get the actual permissions of the directory
    Set<PosixFilePermission> actualPermissionSet =
        Files.getPosixFilePermissions(directoryPath);

    // Assert that the expected and actual permissions sets are equal
    assertEquals(octalExpectedPermissionSet, actualPermissionSet);
  }

  @Test
  public void testGetDirectoryFromConfigWithSymbolicPermissions()
      throws IOException {
    // Create a temporary directory
    String filePath = folder.toAbsolutePath().toString();

    OzoneConfiguration conf = new OzoneConfiguration();
    String key = OzoneConfigKeys.OZONE_METADATA_DIRS;
    String componentName = "Ozone";
    conf.set(key, filePath);

    // Set symbolic permissions
    String symbolicPermissionValue = "rwx------";
    String symbolicExpectedPermissions = "rwx------";
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS_PERMISSIONS,
        symbolicPermissionValue);

    File directory =
        ServerUtils.getDirectoryFromConfig(conf, key, componentName);

    assertNotNull(directory);
    assertTrue(directory.exists());
    assertTrue(directory.isDirectory());

    Path directoryPath = directory.toPath();

    Set<PosixFilePermission> symbolicExpectedPermissionSet =
        PosixFilePermissions.fromString(symbolicExpectedPermissions);

    Set<PosixFilePermission> actualPermissionSet =
        Files.getPosixFilePermissions(directoryPath);

    assertEquals(symbolicExpectedPermissionSet, actualPermissionSet);
  }

  @Test
  public void testGetDirectoryFromConfigWithMultipleDirectories()
      throws IOException {
    // Create temporary directories
    File dir1 = new File(folder.toFile(), "dir1");
    File dir2 = new File(folder.toFile(), "dir2");

    // Create an OzoneConfiguration object
    OzoneConfiguration conf = new OzoneConfiguration();
    String key = OzoneConfigKeys.OZONE_METADATA_DIRS;
    String componentName = "Ozone";
    conf.set(key, dir1.getAbsolutePath() + "," + dir2.getAbsolutePath());

    // Assert that an IllegalArgumentException is thrown because Recon does not
    // support multiple metadata dirs currently
    assertThrows(IllegalArgumentException.class, () -> {
      ServerUtils.getDirectoryFromConfig(conf, key, componentName);
    });
  }

  @Test
  public void testGetDirectoryFromConfigWithNoDirectory() {
    // Create an empty OzoneConfiguration object
    OzoneConfiguration conf = new OzoneConfiguration();
    String key = OzoneConfigKeys.OZONE_METADATA_DIRS;
    String componentName = "Ozone";

    // Get the directory using ServerUtils method
    File directory =
        ServerUtils.getDirectoryFromConfig(conf, key, componentName);

    // Assert that the directory is null
    assertNull(directory);
  }

  /**
   * Test {@link ServerUtils#getScmDbDir}.
   */
  @Test
  public void testGetScmDbDir() {
    final File dbDir = new File(folder.toFile(), "scmDbDir");
    final File metaDir = new File(folder.toFile(), "metaDir");
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, dbDir.getPath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      assertFalse(metaDir.exists());
      assertFalse(dbDir.exists());
      assertEquals(dbDir, ServerUtils.getScmDbDir(conf));
      assertTrue(dbDir.exists());
      assertFalse(metaDir.exists());
    } finally {
      FileUtils.deleteQuietly(dbDir);
    }
  }

  /**
   * Test {@link ServerUtils#getScmDbDir} with fallback to OZONE_METADATA_DIRS
   * when OZONE_SCM_DB_DIRS is undefined.
   */
  @Test
  public void testGetScmDbDirWithFallback() {
    final File metaDir = new File(folder.toFile(), "metaDir");
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());
    try {
      assertFalse(metaDir.exists());
      assertEquals(metaDir, ServerUtils.getScmDbDir(conf));
      assertTrue(metaDir.exists());
    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  @Test
  public void testNoScmDbDirConfigured() {
    assertThrows(IllegalArgumentException.class,
        () -> ServerUtils.getScmDbDir(new OzoneConfiguration()));
  }

  @Test
  public void ozoneMetadataDirIsMandatory() {
    assertThrows(IllegalArgumentException.class,
        () -> ServerUtils.getOzoneMetaDirPath(new OzoneConfiguration()));
  }

  @Test
  public void ozoneMetadataDirAcceptsSingleItem() {
    final File metaDir = new File(folder.toFile(), "metaDir");
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      assertFalse(metaDir.exists());
      assertEquals(metaDir, ServerUtils.getOzoneMetaDirPath(conf));
      assertTrue(metaDir.exists());
    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  @Test
  public void ozoneMetadataDirRejectsList() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, "/data/meta1,/data/meta2");
    assertThrows(IllegalArgumentException.class,
        () -> ServerUtils.getOzoneMetaDirPath(conf));
  }

  /**
   * Test that SCM, OM, and Datanode colocated on the same host with only
   * ozone.metadata.dirs configured don't conflict with Ratis directories.
   */
  @Test
  public void testColocatedComponentsWithSharedMetadataDir() {
    final File metaDir = new File(folder.toFile(), "sharedMetaDir");
    final OzoneConfiguration conf = new OzoneConfiguration();

    // Only configure ozone.metadata.dirs (the fallback config)
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      assertFalse(metaDir.exists());

      // Test Ratis directories - each component should get its own with flat naming
      String scmRatisDir = ServerUtils.getDefaultRatisDirectory(conf, SCM);
      String omRatisDir = ServerUtils.getDefaultRatisDirectory(conf, OM);
      String dnRatisDir = ServerUtils.getDefaultRatisDirectory(conf, DATANODE);

      // Verify Ratis directories use flat naming pattern (component.ratis)
      assertEquals(new File(metaDir, "scm.ratis").getPath(), scmRatisDir);
      assertEquals(new File(metaDir, "om.ratis").getPath(), omRatisDir);
      assertEquals(new File(metaDir, "dn.ratis").getPath(), dnRatisDir);

      // Verify all Ratis directories are different
      assertNotEquals(scmRatisDir, omRatisDir);
      assertNotEquals(scmRatisDir, dnRatisDir);
      assertNotEquals(omRatisDir, dnRatisDir);

      // Verify the base metadata dir exists
      assertTrue(metaDir.exists());

    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  @Test
  public void testEmptyOldSharedRatisIgnored() throws IOException {
    final File metaDir = new File(folder.toFile(), "upgradeMetaDir");
    final File oldSharedRatisDir = new File(metaDir, "ratis");
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      // Create old Ratis directory (empty)
      assertTrue(oldSharedRatisDir.mkdirs());

      // SCM should use new SCM path
      String scmRatisDir = ServerUtils.getDefaultRatisDirectory(conf, SCM);
      assertEquals(Paths.get(metaDir.getPath(), "scm.ratis").toString(), scmRatisDir);

      // OM should use new OM path
      String omRatisDir = ServerUtils.getDefaultRatisDirectory(conf, OM);
      assertEquals(Paths.get(metaDir.getPath(), "om.ratis").toString(), omRatisDir);

    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  /**
   * Test backward compatibility: old shared /ratis directory should be used
   * when it exists and is non-empty (simulating upgrade from version 2.0.0).
   */
  @Test
  public void testBackwardCompatibilityWithOldSharedRatisDir() throws IOException {
    final File metaDir = new File(folder.toFile(), "upgradeMetaDir");
    final File oldSharedRatisDir = new File(metaDir, "ratis");
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      // Create old shared ratis directory with some files (simulating existing data)
      assertTrue(oldSharedRatisDir.mkdirs());
      File testFile = new File(oldSharedRatisDir, "test-file");
      assertTrue(testFile.createNewFile());

      // Test that all components use the old shared location
      String scmRatisDir = ServerUtils.getDefaultRatisDirectory(conf, SCM);
      String omRatisDir = ServerUtils.getDefaultRatisDirectory(conf, OM);
      String dnRatisDir = ServerUtils.getDefaultRatisDirectory(conf, DATANODE);

      // All should use the old shared location
      assertEquals(oldSharedRatisDir.getPath(), scmRatisDir);
      assertEquals(oldSharedRatisDir.getPath(), omRatisDir);
      assertEquals(oldSharedRatisDir.getPath(), dnRatisDir);

    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  /**
   * Test backward compatibility: SCM-specific /scm-ha directory should be preferred
   * over shared /ratis directory when both exist and are non-empty.
   * OM and DATANODE should continue using /ratis even when /scm-ha exists.
   */
  @Test
  public void testBackwardCompatibilityWithScmHaDirectory() throws IOException {
    final File metaDir = new File(folder.toFile(), "upgradeMetaDir");
    final File oldScmHaDir = new File(metaDir, "scm-ha");
    final File oldSharedRatisDir = new File(metaDir, "ratis");
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      // Create both scm-ha and ratis directories with files
      assertTrue(oldScmHaDir.mkdirs());
      File scmHaTestFile = new File(oldScmHaDir, "scm-ha-file");
      assertTrue(scmHaTestFile.createNewFile());

      assertTrue(oldSharedRatisDir.mkdirs());
      File ratisTestFile = new File(oldSharedRatisDir, "ratis-file");
      assertTrue(ratisTestFile.createNewFile());

      // SCM should prefer scm-ha over ratis
      String scmRatisDir = ServerUtils.getDefaultRatisDirectory(conf, SCM);
      assertEquals(oldScmHaDir.getPath(), scmRatisDir);
      assertNotEquals(oldSharedRatisDir.getPath(), scmRatisDir);

      // OM and DATANODE should still use ratis even when scm-ha exists
      String omRatisDir = ServerUtils.getDefaultRatisDirectory(conf, OM);
      String dnRatisDir = ServerUtils.getDefaultRatisDirectory(conf, DATANODE);
      assertEquals(oldSharedRatisDir.getPath(), omRatisDir);
      assertEquals(oldSharedRatisDir.getPath(), dnRatisDir);

      // Test that empty scm-ha directory is ignored (SCM should fall back to ratis)
      FileUtils.deleteQuietly(scmHaTestFile);
      String scmRatisDirWithEmptyScmHa = ServerUtils.getDefaultRatisDirectory(conf, SCM);
      assertEquals(oldSharedRatisDir.getPath(), scmRatisDirWithEmptyScmHa);

    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  /**
   * Test that SCM and OM colocated on the same host with only
   * ozone.metadata.dirs configured get separate Ratis snapshot directories.
   */
  @Test
  public void testColocatedComponentsWithSharedMetadataDirForSnapshots() {
    final File metaDir = new File(folder.toFile(), "sharedMetaDir");
    final OzoneConfiguration conf = new OzoneConfiguration();

    // Only configure ozone.metadata.dirs (the fallback config)
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      assertFalse(metaDir.exists());

      // Test Ratis snapshot directories - OM and SCM should get their own
      String scmSnapshotDir = ServerUtils.getDefaultRatisSnapshotDirectory(conf, SCM);
      String omSnapshotDir = ServerUtils.getDefaultRatisSnapshotDirectory(conf, OM);

      // Verify snapshot directories use: <ozone.metadata.dirs>/<component>.ratis.snapshot
      assertEquals(new File(metaDir, "scm.ratis.snapshot").getPath(), scmSnapshotDir);
      assertEquals(new File(metaDir, "om.ratis.snapshot").getPath(), omSnapshotDir);

      // Verify snapshot directories are different
      assertNotEquals(scmSnapshotDir, omSnapshotDir);

      // Verify the base metadata dir exists
      assertTrue(metaDir.exists());

    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  @Test
  public void testSetDataDirectoryPermissionsWithOctal() throws IOException {
    File testDir = new File(folder.toFile(), "testDir");
    assertTrue(testDir.mkdirs());

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DATA_DIR_PERMISSIONS, "700");

    ServerUtils.setDataDirectoryPermissions(testDir, conf,
        ScmConfigKeys.HDDS_DATANODE_DATA_DIR_PERMISSIONS);

    Path dirPath = testDir.toPath();
    Set<PosixFilePermission> expectedPermissions =
        PosixFilePermissions.fromString("rwx------");
    Set<PosixFilePermission> actualPermissions =
        Files.getPosixFilePermissions(dirPath);

    assertEquals(expectedPermissions, actualPermissions);
  }

  @Test
  public void testSetDataDirectoryPermissionsWithSymbolic() throws IOException {
    File testDir = new File(folder.toFile(), "testDir2");
    assertTrue(testDir.mkdirs());

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DATA_DIR_PERMISSIONS, "rwx------");

    ServerUtils.setDataDirectoryPermissions(testDir, conf,
        ScmConfigKeys.HDDS_DATANODE_DATA_DIR_PERMISSIONS);

    Path dirPath = testDir.toPath();
    Set<PosixFilePermission> expectedPermissions =
        PosixFilePermissions.fromString("rwx------");
    Set<PosixFilePermission> actualPermissions =
        Files.getPosixFilePermissions(dirPath);

    assertEquals(expectedPermissions, actualPermissions);
  }

  @Test
  public void testSetDataDirectoryPermissionsWithDefaultValue() throws IOException {
    File testDir = new File(folder.toFile(), "testDir3");
    assertTrue(testDir.mkdirs());

    OzoneConfiguration conf = new OzoneConfiguration();
    // Don't explicitly set the permission config key - should use default value (700)

    // Should use default value from ozone-default.xml (700)
    ServerUtils.setDataDirectoryPermissions(testDir, conf,
        ScmConfigKeys.HDDS_DATANODE_DATA_DIR_PERMISSIONS);

    // Permissions should be set to default value (700 = rwx------)
    Path dirPath = testDir.toPath();
    Set<PosixFilePermission> expectedPermissions =
        PosixFilePermissions.fromString("rwx------");
    Set<PosixFilePermission> actualPermissions =
        Files.getPosixFilePermissions(dirPath);
    assertEquals(expectedPermissions, actualPermissions);
  }

  @Test
  public void testSetDataDirectoryPermissionsWithNonExistentDir() {
    File nonExistentDir = new File(folder.toFile(), "nonExistent");
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DATA_DIR_PERMISSIONS, "700");

    // Should not throw exception for non-existent directory
    ServerUtils.setDataDirectoryPermissions(nonExistentDir, conf,
        ScmConfigKeys.HDDS_DATANODE_DATA_DIR_PERMISSIONS);
  }

  @Test
  public void testSetDataDirectoryPermissionsSkipsReadOnlyDir() throws IOException {
    // Create a directory and set it to read-only
    File readOnlyDir = new File(folder.toFile(), "readOnlyDir");
    assertTrue(readOnlyDir.mkdirs());

    // Set initial permissions and make it read-only
    Path dirPath = readOnlyDir.toPath();
    Set<PosixFilePermission> readOnlyPermissions =
        PosixFilePermissions.fromString("r-xr-xr-x");
    Files.setPosixFilePermissions(dirPath, readOnlyPermissions);

    // Verify directory is read-only
    assertFalse(readOnlyDir.canWrite());

    // Configure system to use 700 permissions
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DATA_DIR_PERMISSIONS, "700");

    // Call setDataDirectoryPermissions on read-only directory
    // Should skip permission setting and not throw exception
    ServerUtils.setDataDirectoryPermissions(readOnlyDir, conf,
        ScmConfigKeys.HDDS_DATANODE_DATA_DIR_PERMISSIONS);

    // Verify permissions were NOT changed (still read-only)
    Set<PosixFilePermission> actualPermissions =
        Files.getPosixFilePermissions(dirPath);
    assertEquals(readOnlyPermissions, actualPermissions);
    assertFalse(readOnlyDir.canWrite());
  }
}
