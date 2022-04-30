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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
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
 * - prefix layout.
 *
 */
@RunWith(Parameterized.class)
public class TestOzoneFileSystemWithFSO extends TestOzoneFileSystem {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
            new Object[]{true, true},
            new Object[]{true, false});
  }

  @BeforeClass
  public static void init() {
    setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  public TestOzoneFileSystemWithFSO(boolean setDefaultFs,
      boolean enableOMRatis) {
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

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneFileSystemWithFSO.class);

  @Override
  @Test
  @Ignore("HDDS-2939")
  public void testGetDirectoryModificationTime() {
    // ignore as this is not relevant to PREFIX layout changes
  }

  @Override
  @Test
  @Ignore("HDDS-2939")
  public void testOzoneFsServiceLoader() {
    // ignore as this is not relevant to PREFIX layout changes
  }

  @Override
  @Test
  @Ignore("HDDS-2939")
  public void testCreateWithInvalidPaths() {
    // ignore as this is not relevant to PREFIX layout changes
  }

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
    try (FSDataOutputStream outputStream = getFs().create(key1,
            false)) {
      assertNotNull("Should be able to create file: key1",
              outputStream);
    }
    Path d1 = new Path("/d1");
    Path dir1Key1 = new Path(d1, "key1");
    try (FSDataOutputStream outputStream = getFs().create(dir1Key1, false)) {
      assertNotNull("Should be able to create file: " + dir1Key1,
              outputStream);
    }
    Path d2 = new Path("/d2");
    Path dir2Key1 = new Path(d2, "key1");
    try (FSDataOutputStream outputStream = getFs().create(dir2Key1, false)) {
      assertNotNull("Should be able to create file: " + dir2Key1,
              outputStream);
    }
    Path dir1Dir2 = new Path("/d1/d2/");
    Path dir1Dir2Key1 = new Path(dir1Dir2, "key1");
    try (FSDataOutputStream outputStream = getFs().create(dir1Dir2Key1,
            false)) {
      assertNotNull("Should be able to create file: " + dir1Dir2Key1,
              outputStream);
    }
    Path d1Key2 = new Path(d1, "key2");
    try (FSDataOutputStream outputStream = getFs().create(d1Key2, false)) {
      assertNotNull("Should be able to create file: " + d1Key2,
              outputStream);
    }

    Path dir1Dir3 = new Path("/d1/d3/");
    Path dir1Dir4 = new Path("/d1/d4/");

    getFs().mkdirs(dir1Dir3);
    getFs().mkdirs(dir1Dir4);

    String bucketName = getBucketName();
    String volumeName = getVolumeName();

    // Root Directory
    FileStatus[] fileStatusList = getFs().listStatus(new Path("/"));
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
    fileStatusList = getFs().listStatus(new Path("/d1"));
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
    fileStatusList = getFs().listStatus(new Path("/d1/d2"));
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
    fileStatusList = getFs().listStatus(new Path("/d1/d2/key1"));
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
      fileStatusList = getFs().listStatus(new Path("/key2"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // ignore as its expected
    }
    try {
      fileStatusList = getFs().listStatus(new Path("/d1/d2/key2"));
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
    try (FSDataOutputStream outputStream = getFs().create(dir1Dir1Dir2Key1,
            false)) {
      assertNotNull("Should be able to create file: " + dir1Dir1Dir2Key1,
              outputStream);
    }
    Path key1 = new Path("/key1");
    try (FSDataOutputStream outputStream = getFs().create(key1, false)) {
      assertNotNull("Should be able to create file: " + key1,
              outputStream);
    }
    Path key2 = new Path("/key2");
    try (FSDataOutputStream outputStream = getFs().create(key2, false)) {
      assertNotNull("Should be able to create file: key2",
              outputStream);
    }
    Path dir1Dir2Dir1Dir2Key1 = new Path("/d1/d2/d1/d2/key1");
    try (FSDataOutputStream outputStream = getFs().create(dir1Dir2Dir1Dir2Key1,
            false)) {
      assertNotNull("Should be able to create file: "
              + dir1Dir2Dir1Dir2Key1, outputStream);
    }
    RemoteIterator<LocatedFileStatus> fileStatusItr = getFs().listFiles(
            new Path("/"), true);
    String uriPrefix = "o3fs://" + getBucketName() + "." + getVolumeName();
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
    fileStatusItr = getFs().listFiles(new Path("/"), false);
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
   * Case-2) Cannot rename a directory to its own subdirectory.
   */
  @Test
  public void testRenameDirToItsOwnSubDir() throws Exception {
    final String root = "/root";
    final String dir1 = root + "/dir1";
    final Path dir1Path = new Path(getFs().getUri().toString() + dir1);
    // Add a sub-dir1 to the directory to be moved.
    final Path subDir1 = new Path(dir1Path, "sub_dir1");
    getFs().mkdirs(subDir1);
    LOG.info("Created dir1 {}", subDir1);

    final Path sourceRoot = new Path(getFs().getUri().toString() + root);
    LOG.info("Rename op-> source:{} to destin:{}", sourceRoot, subDir1);
    //  rename should fail and return false
    Assert.assertFalse(getFs().rename(sourceRoot, subDir1));
  }

  /**
   * Cleanup keyTable and directoryTable explicitly as FS delete operation
   * is not yet supported.
   * Fails if the (a) parent of dst does not exist or (b) parent is a file.
   */
  @Test
  public void testRenameDestinationParentDoesntExist() throws Exception {
    final String root = "/root_dir";
    final String dir1 = root + "/dir1";
    final String dir2 = dir1 + "/dir2";
    final Path dir2SourcePath = new Path(getFs().getUri().toString() + dir2);
    getFs().mkdirs(dir2SourcePath);
    // (a) parent of dst does not exist.  /root_dir/b/c
    final Path destinPath = new Path(getFs().getUri().toString()
            + root + "/b/c");

    // rename should fail and return false
    Assert.assertFalse(getFs().rename(dir2SourcePath, destinPath));
    // (b) parent of dst is a file. /root_dir/file1/c
    Path filePath = new Path(getFs().getUri().toString() + root + "/file1");
    ContractTestUtils.touch(getFs(), filePath);
    Path newDestinPath = new Path(filePath, "c");
    // rename should fail and return false
    Assert.assertFalse(getFs().rename(dir2SourcePath, newDestinPath));
  }

  @Override
  @Test
  @Ignore("TODO:HDDS-2939")
  public void testListStatusWithIntermediateDir() throws Exception {
  }

  @Override
  @Test
  @Ignore("TODO:HDDS-5012")
  public void testListStatusOnLargeDirectory() throws Exception {
  }

  @Test
  public void testMultiLevelDirs() throws Exception {
    // reset metrics
    long numKeys = getCluster().getOzoneManager().getMetrics().getNumKeys();
    getCluster().getOzoneManager().getMetrics().decNumKeys(numKeys);
    Assert.assertEquals(0,
        getCluster().getOzoneManager().getMetrics().getNumKeys());

    // Op 1. create dir -> /d1/d2/d3/d4/
    // Op 2. create dir -> /d1/d2/d3/d4/d5
    // Op 3. create dir -> /d1/d2/d3/d4/d6
    Path parent = new Path("/d1/d2/d3/d4/");
    getFs().mkdirs(parent);

    OMMetadataManager omMgr =
        getCluster().getOzoneManager().getMetadataManager();
    OmBucketInfo omBucketInfo = omMgr.getBucketTable()
        .get(omMgr.getBucketKey(getVolumeName(), getBucketName()));
    Assert.assertNotNull("Failed to find bucketInfo", omBucketInfo);

    ArrayList<String> dirKeys = new ArrayList<>();
    long d1ObjectID =
        verifyDirKey(omBucketInfo.getObjectID(), "d1", "/d1", dirKeys, omMgr);
    long d2ObjectID = verifyDirKey(d1ObjectID, "d2", "/d1/d2", dirKeys, omMgr);
    long d3ObjectID =
        verifyDirKey(d2ObjectID, "d3", "/d1/d2/d3", dirKeys, omMgr);
    long d4ObjectID =
        verifyDirKey(d3ObjectID, "d4", "/d1/d2/d3/d4", dirKeys, omMgr);

    Assert.assertEquals("Wrong OM numKeys metrics", 4,
        getCluster().getOzoneManager().getMetrics().getNumKeys());

    // create sub-dirs under same parent
    Path subDir5 = new Path("/d1/d2/d3/d4/d5");
    getFs().mkdirs(subDir5);
    Path subDir6 = new Path("/d1/d2/d3/d4/d6");
    getFs().mkdirs(subDir6);
    long d5ObjectID =
        verifyDirKey(d4ObjectID, "d5", "/d1/d2/d3/d4/d5", dirKeys, omMgr);
    long d6ObjectID =
        verifyDirKey(d4ObjectID, "d6", "/d1/d2/d3/d4/d6", dirKeys, omMgr);
    Assert.assertTrue(
        "Wrong objectIds for sub-dirs[" + d5ObjectID + "/d5, " + d6ObjectID
            + "/d6] of same parent!", d5ObjectID != d6ObjectID);

    Assert.assertEquals("Wrong OM numKeys metrics", 6,
        getCluster().getOzoneManager().getMetrics().getNumKeys());
  }

  @Test
  public void testCreateFile() throws Exception {
    // Op 1. create dir -> /d1/d2/d3/d4/
    Path parent = new Path("/d1/d2/");
    Path file = new Path(parent, "file1");
    FSDataOutputStream outputStream = getFs().create(file);
    String openFileKey = "";

    OMMetadataManager omMgr =
        getCluster().getOzoneManager().getMetadataManager();
    OmBucketInfo omBucketInfo = omMgr.getBucketTable()
        .get(omMgr.getBucketKey(getVolumeName(), getBucketName()));
    Assert.assertNotNull("Failed to find bucketInfo", omBucketInfo);

    ArrayList<String> dirKeys = new ArrayList<>();
    long d1ObjectID =
        verifyDirKey(omBucketInfo.getObjectID(), "d1", "/d1", dirKeys, omMgr);
    long d2ObjectID = verifyDirKey(d1ObjectID, "d2", "/d1/d2", dirKeys, omMgr);
    openFileKey = d2ObjectID + OzoneConsts.OM_KEY_PREFIX + file.getName();

    // trigger CommitKeyRequest
    outputStream.close();

    OmKeyInfo omKeyInfo = omMgr.getKeyTable(getBucketLayout()).get(openFileKey);
    Assert.assertNotNull("Invalid Key!", omKeyInfo);
    verifyOMFileInfoFormat(omKeyInfo, file.getName(), d2ObjectID);

    // wait for DB updates
    GenericTestUtils.waitFor(() -> {
      try {
        return omMgr.getOpenKeyTable(getBucketLayout()).isEmpty();
      } catch (IOException e) {
        LOG.error("DB failure!", e);
        Assert.fail("DB failure!");
        return false;
      }
    }, 1000, 120000);
  }

  private void verifyOMFileInfoFormat(OmKeyInfo omKeyInfo, String fileName,
      long parentID) {
    Assert.assertEquals("Wrong keyName", fileName, omKeyInfo.getKeyName());
    Assert.assertEquals("Wrong parentID", parentID,
        omKeyInfo.getParentObjectID());
    String dbKey = parentID + OzoneConsts.OM_KEY_PREFIX + fileName;
    Assert.assertEquals("Wrong path format", dbKey, omKeyInfo.getPath());
  }

  long verifyDirKey(long parentId, String dirKey, String absolutePath,
      ArrayList<String> dirKeys, OMMetadataManager omMgr)
      throws Exception {
    String dbKey = parentId + "/" + dirKey;
    dirKeys.add(dbKey);
    OmDirectoryInfo dirInfo = omMgr.getDirectoryTable().get(dbKey);
    Assert.assertNotNull("Failed to find " + absolutePath +
        " using dbKey: " + dbKey, dirInfo);
    Assert.assertEquals("Parent Id mismatches", parentId,
        dirInfo.getParentObjectID());
    Assert.assertEquals("Mismatches directory name", dirKey,
        dirInfo.getName());
    Assert.assertTrue("Mismatches directory creation time param",
        dirInfo.getCreationTime() > 0);
    Assert.assertEquals("Mismatches directory modification time param",
        dirInfo.getCreationTime(), dirInfo.getModificationTime());
    Assert.assertEquals("Wrong representation!",
        dbKey + ":" + dirInfo.getObjectID(), dirInfo.toString());
    return dirInfo.getObjectID();
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
