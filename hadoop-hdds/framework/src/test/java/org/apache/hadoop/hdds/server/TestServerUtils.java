/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.server;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


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

}
