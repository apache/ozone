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
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.test.PathUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * Unit tests for {@link ServerUtils}.
 */
public class TestServerUtils {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  /**
   * Test case for {@link ServerUtils#getPermissions}.
   * Verifies the retrieval of permissions for different configs.
   */
  @Test
  public void testGetPermissions() {
    // Create an OzoneConfiguration object and set the permissions
    // for different keys
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.recon.db.dir.perm", "750");
    conf.set("ozone.scm.db.dirs.permissions", "775");
    conf.set("ozone.metadata.dirs.permissions", "770");
    conf.set("ozone.om.db.dirs.permissions", "700");

    // Test getPermissions for different config names and assert the
    // returned permissions
    assertEquals("750",
        ServerUtils.getPermissions("ozone.recon.db.dir", conf));
    assertEquals("775",
        ServerUtils.getPermissions("ozone.scm.db.dirs", conf));
    assertEquals("770",
        ServerUtils.getPermissions("ozone.metadata.dirs", conf));
    assertEquals("700",
        ServerUtils.getPermissions("ozone.om.db.dirs", conf));

  }

  /**
   * Test case for {@link ServerUtils#getDirectoryFromConfig}.
   * Verifies the creation of a directory with the expected permissions.
   *
   * @throws IOException if an I/O error occurs during the test
   */
  @Test
  public void testGetDirectoryFromConfig() throws IOException {
    // Create a temporary directory
    String filePath = folder.getRoot().getAbsolutePath();

    // Create an OzoneConfiguration object
    OzoneConfiguration conf = new OzoneConfiguration();
    String key = "ozone.metadata.dirs";
    String componentName = "Ozone";
    conf.set(key, filePath);

    // Get the directory using ServerUtils method
    File directory =
        ServerUtils.getDirectoryFromConfig(conf, key, componentName);

    // Assert that the directory is not null and exists
    assertNotNull(directory);
    assertTrue(directory.exists());
    assertTrue(directory.isDirectory());

    // Get the path of the directory
    Path directoryPath = directory.toPath();

    // Define the expected permissions as a string
    String expectedPermissions = "rwxr-x---";

    // Convert the expected permissions string to a set of PosixFilePermission
    Set<PosixFilePermission> expectedPermissionSet =
        PosixFilePermissions.fromString(expectedPermissions);

    // Get the actual permissions of the directory
    Set<PosixFilePermission> actualPermissionSet =
        Files.getPosixFilePermissions(directoryPath);

    // Assert that the expected and actual permissions sets are equal
    assertEquals(expectedPermissionSet, actualPermissionSet);
  }

  @Test
  public void testGetDirectoryFromConfigWithMultipleDirectories()
      throws IOException {
    // Create temporary directories
    File dir1 = folder.newFolder("dir1");
    File dir2 = folder.newFolder("dir2");

    // Create an OzoneConfiguration object
    OzoneConfiguration conf = new OzoneConfiguration();
    String key = "ozone.metadata.dirs";
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
    String key = "ozone.metadata.dirs";
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
    final File testDir = PathUtils.getTestDir(TestServerUtils.class);
    final File dbDir = new File(testDir, "scmDbDir");
    final File metaDir = new File(testDir, "metaDir");
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
    final File testDir = PathUtils.getTestDir(TestServerUtils.class);
    final File metaDir = new File(testDir, "metaDir");
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
    final File testDir = PathUtils.getTestDir(TestServerUtils.class);
    final File metaDir = new File(testDir, "metaDir");
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
