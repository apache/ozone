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

package org.apache.hadoop.fs.ozone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LeaseRecoverable;
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
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone file system tests that are not covered by contract tests,
 * - prefix layout.
 *
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
abstract class AbstractOzoneFileSystemTestWithFSO extends AbstractOzoneFileSystemTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractOzoneFileSystemTestWithFSO.class);

  AbstractOzoneFileSystemTestWithFSO() {
    super(true, BucketLayout.FILE_SYSTEM_OPTIMIZED);
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
      assertNotNull(outputStream, "Should be able to create file: key1");
    }
    Path d1 = new Path("/d1");
    Path dir1Key1 = new Path(d1, "key1");
    try (FSDataOutputStream outputStream = getFs().create(dir1Key1, false)) {
      assertNotNull(outputStream, "Should be able to create file: " + dir1Key1);
    }
    Path d2 = new Path("/d2");
    Path dir2Key1 = new Path(d2, "key1");
    try (FSDataOutputStream outputStream = getFs().create(dir2Key1, false)) {
      assertNotNull(outputStream, "Should be able to create file: " + dir2Key1);
    }
    Path dir1Dir2 = new Path("/d1/d2/");
    Path dir1Dir2Key1 = new Path(dir1Dir2, "key1");
    try (FSDataOutputStream outputStream = getFs().create(dir1Dir2Key1,
            false)) {
      assertNotNull(outputStream, "Should be able to create file: " + dir1Dir2Key1);
    }
    Path d1Key2 = new Path(d1, "key2");
    try (FSDataOutputStream outputStream = getFs().create(d1Key2, false)) {
      assertNotNull(outputStream, "Should be able to create file: " + d1Key2);
    }

    Path dir1Dir3 = new Path("/d1/d3/");
    Path dir1Dir4 = new Path("/d1/d4/");

    getFs().mkdirs(dir1Dir3);
    getFs().mkdirs(dir1Dir4);

    String bucketName = getBucketName();
    String volumeName = getVolumeName();

    // Root Directory
    FileStatus[] fileStatusList = getFs().listStatus(new Path("/"));
    assertEquals(3, fileStatusList.length, "FileStatus should return files and directories");
    ArrayList<String> expectedPaths = new ArrayList<>();
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d2");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/key1");
    for (FileStatus fileStatus : fileStatusList) {
      expectedPaths.remove(fileStatus.getPath().toString());
    }
    assertEquals(0, expectedPaths.size(), "Failed to return the filestatus[]" + expectedPaths);

    // level-1 sub-dirs
    fileStatusList = getFs().listStatus(new Path("/d1"));
    assertEquals(5, fileStatusList.length, "FileStatus should return files and directories");
    expectedPaths = new ArrayList<>();
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d2");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d3");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d4");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/key1");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/key2");
    for (FileStatus fileStatus : fileStatusList) {
      expectedPaths.remove(fileStatus.getPath().toString());
    }
    assertEquals(0, expectedPaths.size(), "Failed to return the filestatus[]" + expectedPaths);

    // level-2 sub-dirs
    fileStatusList = getFs().listStatus(new Path("/d1/d2"));
    assertEquals(1, fileStatusList.length, "FileStatus should return files and directories");
    expectedPaths = new ArrayList<>();
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d2/" +
            "key1");
    for (FileStatus fileStatus : fileStatusList) {
      expectedPaths.remove(fileStatus.getPath().toString());
    }
    assertEquals(0, expectedPaths.size(), "Failed to return the filestatus[]" + expectedPaths);

    // level-2 key2
    fileStatusList = getFs().listStatus(new Path("/d1/d2/key1"));
    assertEquals(1, fileStatusList.length, "FileStatus should return files and directories");
    expectedPaths = new ArrayList<>();
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d2/" +
            "key1");
    for (FileStatus fileStatus : fileStatusList) {
      expectedPaths.remove(fileStatus.getPath().toString());
    }
    assertEquals(0, expectedPaths.size(), "Failed to return the filestatus[]" + expectedPaths);

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
      assertNotNull(outputStream, "Should be able to create file: " + dir1Dir1Dir2Key1);
    }
    Path key1 = new Path("/key1");
    try (FSDataOutputStream outputStream = getFs().create(key1, false)) {
      assertNotNull(outputStream, "Should be able to create file: " + key1);
    }
    Path key2 = new Path("/key2");
    try (FSDataOutputStream outputStream = getFs().create(key2, false)) {
      assertNotNull(outputStream, "Should be able to create file: key2");
    }
    Path dir1Dir2Dir1Dir2Key1 = new Path("/d1/d2/d1/d2/key1");
    try (FSDataOutputStream outputStream = getFs().create(dir1Dir2Dir1Dir2Key1,
            false)) {
      assertNotNull(outputStream, "Should be able to create file: "
              + dir1Dir2Dir1Dir2Key1);
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
    assertEquals(expectedFilesCount, actualCount, "Failed to get all the files: " + expectedPaths);
    assertEquals(0, expectedPaths.size(), "Failed to get all the files: " + expectedPaths);

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
    assertEquals(0, expectedPaths.size(), "Failed to get all the files: " + expectedPaths);
    assertEquals(expectedFilesCount, actualCount, "Failed to get all the files: " + expectedPaths);
  }

  /**
   * Case-2) Cannot rename a directory to its own subdirectory.
   */
  @Override
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
    assertFalse(getFs().rename(sourceRoot, subDir1));
  }

  /**
   * Cleanup keyTable and directoryTable explicitly as FS delete operation
   * is not yet supported.
   * Fails if the (a) parent of dst does not exist or (b) parent is a file.
   */
  @Override
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
    assertFalse(getFs().rename(dir2SourcePath, destinPath));
    // (b) parent of dst is a file. /root_dir/file1/c
    Path filePath = new Path(getFs().getUri().toString() + root + "/file1");
    ContractTestUtils.touch(getFs(), filePath);
    Path newDestinPath = new Path(filePath, "c");
    // rename should fail and return false
    assertFalse(getFs().rename(dir2SourcePath, newDestinPath));
  }

  @Test
  public void testRenameParentDirModificationTime() throws IOException {
    Path dir1 = new Path(getFs().getUri().toString(), "/dir1");
    Path file1 = new Path(dir1, "file1");
    Path dir2 = new Path(getFs().getUri().toString(), "/dir2");
    // mv "/dir1/file1" to "/dir2/file1"
    Path renamedFile1 = new Path(dir2, "file1");
    getFs().mkdirs(dir1);
    getFs().create(file1, false).close();
    getFs().mkdirs(dir2);

    long dir1BeforeMTime = getFs().getFileStatus(dir1).getModificationTime();
    long dir2BeforeMTime = getFs().getFileStatus(dir2).getModificationTime();
    long file1BeforeMTime = getFs().getFileStatus(file1).getModificationTime();
    getFs().rename(file1, renamedFile1);
    long dir1AfterMTime = getFs().getFileStatus(dir1).getModificationTime();
    long dir2AfterMTime = getFs().getFileStatus(dir2).getModificationTime();
    long file1AfterMTime = getFs().getFileStatus(renamedFile1)
        .getModificationTime();
    // rename should change the parent directory of source and object files
    // modification time but not change modification time of the renamed file
    assertThat(dir1BeforeMTime).isLessThan(dir1AfterMTime);
    assertThat(dir2BeforeMTime).isLessThan(dir2AfterMTime);
    assertEquals(file1BeforeMTime, file1AfterMTime);

    // mv "/dir1/subdir1/" to "/dir2/subdir1/"
    Path subdir1 = new Path(dir1, "subdir1");
    Path renamedSubdir1 = new Path(dir2, "subdir1");
    getFs().mkdirs(subdir1);

    dir1BeforeMTime = getFs().getFileStatus(dir1).getModificationTime();
    dir2BeforeMTime = getFs().getFileStatus(dir2).getModificationTime();
    long subdir1BeforeMTime = getFs().getFileStatus(subdir1)
        .getModificationTime();
    getFs().rename(subdir1, renamedSubdir1);
    dir1AfterMTime = getFs().getFileStatus(dir1).getModificationTime();
    dir2AfterMTime = getFs().getFileStatus(dir2).getModificationTime();
    long subdir1AfterMTime = getFs().getFileStatus(renamedSubdir1)
        .getModificationTime();
    assertThat(dir1BeforeMTime).isLessThan(dir1AfterMTime);
    assertThat(dir2BeforeMTime).isLessThan(dir2AfterMTime);
    assertEquals(subdir1BeforeMTime, subdir1AfterMTime);
  }

  @Test
  public void testRenameParentBucketModificationTime() throws IOException {
    OMMetadataManager omMgr =
        getCluster().getOzoneManager().getMetadataManager();

    // mv /file1 -> /renamedFile1, the bucket mtime should be changed
    Path file1 = new Path("/file1");
    Path renamedFile1 = new Path("/renamedFile1");
    getFs().create(file1, false).close();
    renameAndAssert(omMgr, file1, renamedFile1, true);

    // mv /dir1/subFile2 -> /dir2/renamedSubFile2,
    // the bucket mtime should not be changed
    Path dir1 = new Path(getFs().getUri().toString(), "/dir1");
    Path subFile2 = new Path(dir1, "subFile2");
    Path dir2 = new Path(getFs().getUri().toString(), "/dir2");
    Path renamedSubFile2 = new Path(dir2, "renamedSubFile2");
    getFs().mkdirs(dir1);
    getFs().mkdirs(dir2);
    getFs().create(subFile2, false).close();
    renameAndAssert(omMgr, subFile2, renamedSubFile2, false);

    // mv /dir3/subFile3 -> "/renamedFile3"  the bucket mtime should be changed
    Path dir3 = new Path(getFs().getUri().toString(), "/dir3");
    Path subFile3 = new Path(dir3, "subFile3");
    Path renamedFile3 = new Path("/renamedFile3");
    getFs().mkdirs(dir3);
    getFs().create(subFile3, false).close();
    renameAndAssert(omMgr, subFile3, renamedFile3, true);

    // mv /file4 -> "/dir4/renamedFile4"  the bucket mtime should be changed
    Path file4 = new Path("/file4");
    Path dir4 = new Path(getFs().getUri().toString(), "/dir4");
    Path renamedSubFile4 = new Path(dir4, "subFile3");
    getFs().mkdirs(dir4);
    getFs().create(file4, false).close();
    renameAndAssert(omMgr, file4, renamedSubFile4, true);
  }

  private void renameAndAssert(OMMetadataManager omMgr,
      Path from, Path to, boolean exceptChangeMtime) throws IOException {
    OmBucketInfo omBucketInfo = omMgr.getBucketTable()
        .get(omMgr.getBucketKey(getVolumeName(), getBucketName()));
    long bucketBeforeMTime = omBucketInfo.getModificationTime();
    long fileBeforeMTime = getFs().getFileStatus(from).getModificationTime();
    getFs().rename(from, to);
    omBucketInfo = omMgr.getBucketTable()
        .get(omMgr.getBucketKey(getVolumeName(), getBucketName()));
    long bucketAfterMTime = omBucketInfo.getModificationTime();
    long fileAfterMTime = getFs().getFileStatus(to).getModificationTime();
    if (exceptChangeMtime) {
      assertThat(bucketBeforeMTime).isLessThan(bucketAfterMTime);
    } else {
      assertEquals(bucketBeforeMTime, bucketAfterMTime);
    }
    assertEquals(fileBeforeMTime, fileAfterMTime);
  }

  @Test
  public void testMultiLevelDirs() throws Exception {
    // reset metrics
    long numKeys = getCluster().getOzoneManager().getMetrics().getNumKeys();
    getCluster().getOzoneManager().getMetrics().decNumKeys(numKeys);
    assertEquals(0, getCluster().getOzoneManager().getMetrics().getNumKeys());

    // Op 1. create dir -> /d1/d2/d3/d4/
    // Op 2. create dir -> /d1/d2/d3/d4/d5
    // Op 3. create dir -> /d1/d2/d3/d4/d6
    Path parent = new Path("/d1/d2/d3/d4/");
    getFs().mkdirs(parent);

    OMMetadataManager omMgr =
        getCluster().getOzoneManager().getMetadataManager();
    OmBucketInfo omBucketInfo = omMgr.getBucketTable()
        .get(omMgr.getBucketKey(getVolumeName(), getBucketName()));
    assertNotNull(omBucketInfo, "Failed to find bucketInfo");

    final long volumeId = omMgr.getVolumeId(getVolumeName());
    final long bucketId = omMgr.getBucketId(getVolumeName(), getBucketName());

    ArrayList<String> dirKeys = new ArrayList<>();
    long d1ObjectID =
        verifyDirKey(volumeId, bucketId, omBucketInfo.getObjectID(),
                "d1", "/d1", dirKeys, omMgr);
    long d2ObjectID = verifyDirKey(volumeId, bucketId, d1ObjectID,
            "d2", "/d1/d2", dirKeys, omMgr);
    long d3ObjectID =
        verifyDirKey(volumeId, bucketId, d2ObjectID,
                "d3", "/d1/d2/d3", dirKeys, omMgr);
    long d4ObjectID =
        verifyDirKey(volumeId, bucketId, d3ObjectID,
                "d4", "/d1/d2/d3/d4", dirKeys, omMgr);

    assertEquals(4, getCluster().getOzoneManager().getMetrics().getNumKeys(), "Wrong OM numKeys metrics");

    // create sub-dirs under same parent
    Path subDir5 = new Path("/d1/d2/d3/d4/d5");
    getFs().mkdirs(subDir5);
    Path subDir6 = new Path("/d1/d2/d3/d4/d6");
    getFs().mkdirs(subDir6);
    long d5ObjectID =
        verifyDirKey(volumeId, bucketId, d4ObjectID,
                "d5", "/d1/d2/d3/d4/d5", dirKeys, omMgr);
    long d6ObjectID =
        verifyDirKey(volumeId, bucketId, d4ObjectID,
                "d6", "/d1/d2/d3/d4/d6", dirKeys, omMgr);
    assertNotEquals(d5ObjectID, d6ObjectID, "Wrong objectIds for sub-dirs[" + d5ObjectID + "/d5, " + d6ObjectID
        + "/d6] of same parent!");

    assertEquals(6, getCluster().getOzoneManager().getMetrics().getNumKeys(), "Wrong OM numKeys metrics");
  }

  @Test
  @Order(1)
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
    assertNotNull(omBucketInfo, "Failed to find bucketInfo");

    ArrayList<String> dirKeys = new ArrayList<>();

    final long volumeId = omMgr.getVolumeId(getVolumeName());
    final long bucketId = omMgr.getBucketId(getVolumeName(), getBucketName());
    long d1ObjectID =
        verifyDirKey(volumeId, bucketId, omBucketInfo.getObjectID(),
                "d1", "/d1", dirKeys, omMgr);
    long d2ObjectID = verifyDirKey(volumeId, bucketId, d1ObjectID,
            "d2", "/d1/d2", dirKeys, omMgr);
    openFileKey = OzoneConsts.OM_KEY_PREFIX + volumeId +
            OzoneConsts.OM_KEY_PREFIX + bucketId +
            OzoneConsts.OM_KEY_PREFIX + d2ObjectID +
            OzoneConsts.OM_KEY_PREFIX + file.getName();

    // trigger CommitKeyRequest
    outputStream.close();

    OmKeyInfo omKeyInfo = omMgr.getKeyTable(getBucketLayout()).get(openFileKey);
    assertNotNull(omKeyInfo, "Invalid Key!");
    verifyOMFileInfoFormat(omKeyInfo, file.getName(), d2ObjectID);

    // wait for DB updates
    GenericTestUtils.waitFor(() -> {
      try {
        return omMgr.getOpenKeyTable(getBucketLayout()).isEmpty();
      } catch (IOException e) {
        LOG.error("DB failure!", e);
        fail("DB failure!");
        return false;
      }
    }, 1000, 120000);
  }

  /**
   * Verify recoverLease() and isFileClosed() APIs.
   * @throws Exception
   */
  @Test
  public void testLeaseRecoverable() throws Exception {
    // Create a file
    Path parent = new Path("/d1");
    Path source = new Path(parent, "file1");

    LeaseRecoverable fs = (LeaseRecoverable)getFs();
    FSDataOutputStream stream = getFs().create(source);
    try {
      // file not visible yet
      assertThrows(FileNotFoundException.class, () -> fs.isFileClosed(source));
      stream.write(1);
      stream.hsync();
      // file is visible and open
      assertFalse(fs.isFileClosed(source));
      assertTrue(fs.recoverLease(source));
      // file is closed after lease recovery
      assertTrue(fs.isFileClosed(source));
    } finally {
      TestLeaseRecovery.closeIgnoringKeyNotFound(stream);
    }
  }

  @Test
  public void testFSDeleteLogWarnNoExist() throws Exception {
    LogCapturer logCapture = LogCapturer.captureLogs(BasicOzoneClientAdapterImpl.class);
    getFs().delete(new Path("/d1/d3/noexist/"), true);
    assertThat(logCapture.getOutput()).contains(
        "delete key failed Unable to get file status");
    assertThat(logCapture.getOutput()).contains(
        "WARN  ozone.BasicOzoneClientAdapterImpl");
  }

  private void verifyOMFileInfoFormat(OmKeyInfo omKeyInfo, String fileName,
      long parentID) {
    assertEquals(fileName, omKeyInfo.getKeyName(), "Wrong keyName");
    assertEquals(parentID, omKeyInfo.getParentObjectID(), "Wrong parentID");
    String dbKey = parentID + OzoneConsts.OM_KEY_PREFIX + fileName;
    assertEquals(dbKey, omKeyInfo.getPath(), "Wrong path format");
  }

  long verifyDirKey(long volumeId, long bucketId, long parentId,
                    String dirKey, String absolutePath,
                    ArrayList<String> dirKeys, OMMetadataManager omMgr)
      throws Exception {
    String dbKey = "/" + volumeId + "/" + bucketId + "/" +
            parentId + "/" + dirKey;
    dirKeys.add(dbKey);
    OmDirectoryInfo dirInfo = omMgr.getDirectoryTable().get(dbKey);
    assertNotNull(dirInfo, "Failed to find " + absolutePath +
        " using dbKey: " + dbKey);
    assertEquals(parentId, dirInfo.getParentObjectID(), "Parent Id mismatches");
    assertEquals(dirKey, dirInfo.getName(), "Mismatches directory name");
    assertThat(dirInfo.getCreationTime()).isGreaterThan(0);
    assertEquals(dirInfo.getCreationTime(), dirInfo.getModificationTime());
    return dirInfo.getObjectID();
  }

  /**
   * Test to reproduce "Directory Not Empty" bug using public FileSystem API.
   * Tests both checkSubDirectoryExists() and checkSubFileExists() paths.
   * Creates child directory and file, deletes them, then tries to delete parent.
   */
  @Test
  public void testDeleteParentAfterChildDeleted() throws Exception {
    Path parent = new Path("/parent");
    Path childDir = new Path(parent, "childDir");
    Path childFile = new Path(parent, "childFile");

    // Create parent directory
    assertTrue(getFs().mkdirs(parent));
    // Create child directory (tests checkSubDirectoryExists path)
    assertTrue(getFs().mkdirs(childDir));
    // Create child file (tests checkSubFileExists path)
    ContractTestUtils.touch(getFs(), childFile);

    // Pause double buffer to prevent flushing deleted entries to DB
    // This makes the bug reproduce deterministically
    OzoneManagerDoubleBuffer doubleBuffer = getCluster().getOzoneManager()
        .getOmRatisServer().getOmStateMachine().getOzoneManagerDoubleBuffer();
    doubleBuffer.pause();

    try {
      // Delete child directory
      assertTrue(getFs().delete(childDir, false), "Child directory delete should succeed");
      // Delete child file
      assertTrue(getFs().delete(childFile, false), "Child file delete should succeed");

      // Try to delete parent directory (should succeed but may fail with the bug)
      // Without the fix, this fails because deleted children are still in DB
      boolean parentDeleted = getFs().delete(parent, false);
      assertTrue(parentDeleted, "Parent delete should succeed after children deleted");
    } finally {
      // Unpause double buffer to avoid affecting other tests
      doubleBuffer.unpause();
    }
  }

}
