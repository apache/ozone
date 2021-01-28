/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.Assert;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Ozone file system tests that are not covered by contract tests,
 * layout version V1.
 *
 */
@RunWith(Parameterized.class)
public class TestOzoneFileSystemV1 extends TestOzoneFileSystem {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
            new Object[]{true, true},
            new Object[]{true, false},
            new Object[]{false, true},
            new Object[]{false, false});
  }

  @BeforeClass
  public static void init() {
    isBucketFSOptimized = true;
  }

  public TestOzoneFileSystemV1(boolean setDefaultFs, boolean enableOMRatis) {
    super(setDefaultFs, enableOMRatis);
  }

  @After
  @Override
  public void cleanup() {
    super.cleanup();
    try {
      deleteRootDir();
    } catch (IOException e) {
      LOG.info("Failed to cleanup DB tables.", e);
      fail("Failed to cleanup DB tables." + e.getMessage());
    }
  }

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(300000);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneFileSystemV1.class);

  @Test
  public void testListStatusWithoutRecursiveSearch() throws Exception {
    /*
     * Op 1. create file -> /key1
     * Op 2. create dir -> /d1/d2
     * Op 3. create dir -> /d1/d3
     * Op 4. create dir -> /d1/d4
     * Op 5. create file -> /d1/key1
     * Op 6. create file -> /d2/key1
     * Op 7. create file -> /d1/d2/key1
     */
    Path key1 = new Path("/key1");
    try (FSDataOutputStream outputStream = fs.create(key1, false)) {
      assertNotNull("Should be able to create file: key1",
              outputStream);
    }
    Path d1 = new Path("/d1");
    Path dir1Key1 = new Path(d1, "key1");
    try (FSDataOutputStream outputStream = fs.create(dir1Key1, false)) {
      assertNotNull("Should be able to create file: " + dir1Key1,
              outputStream);
    }
    Path d2 = new Path("/d2");
    Path dir2Key1 = new Path(d2, "key1");
    try (FSDataOutputStream outputStream = fs.create(dir2Key1, false)) {
      assertNotNull("Should be able to create file: " + dir2Key1,
              outputStream);
    }
    Path dir1Dir2 = new Path("/d1/d2/");
    Path dir1Dir2Key1 = new Path(dir1Dir2, "key1");
    try (FSDataOutputStream outputStream = fs.create(dir1Dir2Key1, false)) {
      assertNotNull("Should be able to create file: " + dir1Dir2Key1,
              outputStream);
    }
    Path d1Key2 = new Path(d1, "key2");
    try (FSDataOutputStream outputStream = fs.create(d1Key2, false)) {
      assertNotNull("Should be able to create file: " + d1Key2,
              outputStream);
    }

    Path dir1Dir3 = new Path("/d1/d3/");
    Path dir1Dir4 = new Path("/d1/d4/");

    fs.mkdirs(dir1Dir3);
    fs.mkdirs(dir1Dir4);

    // Root Directory
    FileStatus[] fileStatusList = fs.listStatus(new Path("/"));
    assertEquals("FileStatus should return files and directories",
            3, fileStatusList.length);
    ArrayList<String> expectedPaths = new ArrayList<>();
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d2");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/key1");
    for (FileStatus fileStatus : fileStatusList) {
      expectedPaths.remove(fileStatus.getPath().toString());
    }
    assertEquals("Failed to return the filestatus[]" + expectedPaths,
            0, expectedPaths.size());

    // level-1 sub-dirs
    fileStatusList = fs.listStatus(new Path("/d1"));
    assertEquals("FileStatus should return files and directories",
            5, fileStatusList.length);
    expectedPaths = new ArrayList<>();
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d2");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d3");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d4");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/key1");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/key2");
    for (FileStatus fileStatus : fileStatusList) {
      expectedPaths.remove(fileStatus.getPath().toString());
    }
    assertEquals("Failed to return the filestatus[]" + expectedPaths,
            0, expectedPaths.size());

    // level-2 sub-dirs
    fileStatusList = fs.listStatus(new Path("/d1/d2"));
    assertEquals("FileStatus should return files and directories",
            1, fileStatusList.length);
    expectedPaths = new ArrayList<>();
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d2/" +
            "key1");
    for (FileStatus fileStatus : fileStatusList) {
      expectedPaths.remove(fileStatus.getPath().toString());
    }
    assertEquals("Failed to return the filestatus[]" + expectedPaths,
            0, expectedPaths.size());

    // level-2 key2
    fileStatusList = fs.listStatus(new Path("/d1/d2/key1"));
    assertEquals("FileStatus should return files and directories",
            1, fileStatusList.length);
    expectedPaths = new ArrayList<>();
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d2/" +
            "key1");
    for (FileStatus fileStatus : fileStatusList) {
      expectedPaths.remove(fileStatus.getPath().toString());
    }
    assertEquals("Failed to return the filestatus[]" + expectedPaths,
            0, expectedPaths.size());

    // invalid root key
    try {
      fileStatusList = fs.listStatus(new Path("/key2"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // ignore as its expected
    }
    try {
      fileStatusList = fs.listStatus(new Path("/d1/d2/key2"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // ignore as its expected

    }
  }

  @Test
  public void testListFilesRecursive() throws Exception {
    /*
     * Op 1. create file -> /d1/d1/d2/key1
     * Op 2. create dir -> /key1
     * Op 3. create dir -> /key2
     * Op 4. create dir -> /d1/d2/d1/d2/key1
     */
    Path dir1Dir1Dir2Key1 = new Path("/d1/d1/d2/key1");
    try (FSDataOutputStream outputStream = fs.create(dir1Dir1Dir2Key1,
            false)) {
      assertNotNull("Should be able to create file: " + dir1Dir1Dir2Key1,
              outputStream);
    }
    Path key1 = new Path("/key1");
    try (FSDataOutputStream outputStream = fs.create(key1, false)) {
      assertNotNull("Should be able to create file: " + key1,
              outputStream);
    }
    Path key2 = new Path("/key2");
    try (FSDataOutputStream outputStream = fs.create(key2, false)) {
      assertNotNull("Should be able to create file: key2",
              outputStream);
    }
    Path dir1Dir2Dir1Dir2Key1 = new Path("/d1/d2/d1/d2/key1");
    try (FSDataOutputStream outputStream = fs.create(dir1Dir2Dir1Dir2Key1,
            false)) {
      assertNotNull("Should be able to create file: "
              + dir1Dir2Dir1Dir2Key1, outputStream);
    }
    RemoteIterator<LocatedFileStatus> fileStatusItr = fs.listFiles(new Path(
            "/"), true);
    String uriPrefix = "o3fs://" + bucketName + "." + volumeName;
    ArrayList<String> expectedPaths = new ArrayList<>();
    expectedPaths.add(uriPrefix + dir1Dir1Dir2Key1.toString());
    expectedPaths.add(uriPrefix + key1.toString());
    expectedPaths.add(uriPrefix + key2.toString());
    expectedPaths.add(uriPrefix + dir1Dir2Dir1Dir2Key1.toString());
    int expectedFilesCount = expectedPaths.size();
    int actualCount = 0;
    while (fileStatusItr.hasNext()) {
      LocatedFileStatus status = fileStatusItr.next();
      expectedPaths.remove(status.getPath().toString());
      actualCount++;
    }
    assertEquals("Failed to get all the files: " + expectedPaths,
            expectedFilesCount, actualCount);
    assertEquals("Failed to get all the files: " + expectedPaths, 0,
            expectedPaths.size());

    // Recursive=false
    fileStatusItr = fs.listFiles(new Path("/"), false);
    expectedPaths.clear();
    expectedPaths.add(uriPrefix + "/key1");
    expectedPaths.add(uriPrefix + "/key2");
    expectedFilesCount = expectedPaths.size();
    actualCount = 0;
    while (fileStatusItr.hasNext()) {
      LocatedFileStatus status = fileStatusItr.next();
      expectedPaths.remove(status.getPath().toString());
      actualCount++;
    }
    assertEquals("Failed to get all the files: " + expectedPaths, 0,
            expectedPaths.size());
    assertEquals("Failed to get all the files: " + expectedPaths,
            expectedFilesCount, actualCount);
  }

  /**
   * Case-1) fromKeyName should exist, otw throws exception.
   */
  @Test
  public void testRenameWithNonExistentSource() throws Exception {
    // Skip as this will run only in new layout
    if (!enabledFileSystemPaths) {
      return;
    }

    final String root = "/root";
    final String dir1 = root + "/dir1";
    final String dir2 = root + "/dir2";
    final Path source = new Path(fs.getUri().toString() + dir1);
    final Path destin = new Path(fs.getUri().toString() + dir2);

    // creates destin
    fs.mkdirs(destin);
    LOG.info("Created destin dir: {}", destin);

    LOG.info("Rename op-> source:{} to destin:{}}", source, destin);
    try {
      fs.rename(source, destin);
      Assert.fail("Should throw exception : Source doesn't exist!");
    } catch (OMException ome) {
      // expected
      assertEquals(ome.getResult(), OMException.ResultCodes.KEY_NOT_FOUND);
    }
  }

  /**
   * Case-2) Cannot rename a directory to its own subdirectory.
   */
  @Test
  public void testRenameDirToItsOwnSubDir() throws Exception {
    // Skip as this will run only in new layout
    if (!enabledFileSystemPaths) {
      return;
    }

    final String root = "/root";
    final String dir1 = root + "/dir1";
    final Path dir1Path = new Path(fs.getUri().toString() + dir1);
    // Add a sub-dir1 to the directory to be moved.
    final Path subDir1 = new Path(dir1Path, "sub_dir1");
    fs.mkdirs(subDir1);
    LOG.info("Created dir1 {}", subDir1);

    final Path sourceRoot = new Path(fs.getUri().toString() + root);
    LOG.info("Rename op-> source:{} to destin:{}", sourceRoot, subDir1);
    try {
      fs.rename(sourceRoot, subDir1);
      Assert.fail("Should throw exception : Cannot rename a directory to" +
              " its own subdirectory");
    } catch (OMException ome) {
      // expected
      assertEquals(ome.getResult(), OMException.ResultCodes.KEY_RENAME_ERROR);
    }
  }

  /**
   * Cleanup keyTable and directoryTable explicitly as FS delete operation
   * is not yet supported.
   * Fails if the (a) parent of dst does not exist or (b) parent is a file.
   */
  @Test
  public void testRenameDestinationParentDoesntExist() throws Exception {
    // Skip as this will run only in new layout
    if (!enabledFileSystemPaths) {
      return;
    }

    final String root = "/root_dir";
    final String dir1 = root + "/dir1";
    final String dir2 = dir1 + "/dir2";
    final Path dir2SourcePath = new Path(fs.getUri().toString() + dir2);
    fs.mkdirs(dir2SourcePath);

    // (a) parent of dst does not exist.  /root_dir/b/c
    final Path destinPath = new Path(fs.getUri().toString() + root + "/b/c");
    try {
      fs.rename(dir2SourcePath, destinPath);
      Assert.fail("Should fail as parent of dst does not exist!");
    } catch (OMException ome) {
      // expected
      assertEquals(ome.getResult(), OMException.ResultCodes.KEY_RENAME_ERROR);
    }

    // (b) parent of dst is a file. /root_dir/file1/c
    Path filePath = new Path(fs.getUri().toString() + root + "/file1");
    ContractTestUtils.touch(fs, filePath);

    Path newDestinPath = new Path(filePath, "c");
    try {
      fs.rename(dir2SourcePath, newDestinPath);
      Assert.fail("Should fail as parent of dst is a file!");
    } catch (OMException ome) {
      // expected
      assertEquals(ome.getResult(), OMException.ResultCodes.KEY_RENAME_ERROR);
    }
  }

  @Override
  @Test
  @Ignore("TODO:HDDS-2939")
  public void testTrash() throws Exception {
  }

  @Override
  @Test
  @Ignore("TODO:HDDS-2939")
  public void testRenameToTrashEnabled() throws Exception {
  }

  @Override
  @Test
  @Ignore("TODO:HDDS-2939")
  public void testListStatusWithIntermediateDir() throws Exception {
  }
}
