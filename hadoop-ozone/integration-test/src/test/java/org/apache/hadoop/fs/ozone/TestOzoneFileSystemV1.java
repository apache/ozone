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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
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
import java.util.Iterator;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Ozone file system tests that are not covered by contract tests,
 * layout version V1.
 *
 * Note: When adding new test(s), please append it in testFileSystem() to
 * avoid test run time regression.
 */
@RunWith(Parameterized.class)
public class TestOzoneFileSystemV1 extends TestOzoneFileSystem {

  public TestOzoneFileSystemV1(boolean setDefaultFs) {
    super(setDefaultFs);
  }
  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(300000);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneFileSystemV1.class);

  private void testListStatusWithoutRecursiveSearch() throws Exception {
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

  private void testListFilesRecursive() throws Exception {
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

  @Test(timeout = 300_000)
  @Override
  public void testFileSystem() throws Exception {
    setupOzoneFileSystem();

    testOzoneFsServiceLoader();
    o3fs = (OzoneFileSystem) fs;

    testCreateFileShouldCheckExistenceOfDirWithSameName();
    // TODO: Cleanup keyTable and dirTable explicitly as FS delete operation
    //  is not yet implemented. This should be replaced with fs.delete() call.
    tableCleanup();
    testMakeDirsWithAnExistingDirectoryPath();
    tableCleanup();
    testCreateWithInvalidPaths();
    tableCleanup();
    testListStatusWithoutRecursiveSearch();
    tableCleanup();
    testListFilesRecursive();
    tableCleanup();

    testGetDirectoryModificationTime();
    tableCleanup();

    testListStatusOnRoot();
    tableCleanup();
    testListStatus();
    tableCleanup();
    testListStatusOnSubDirs();
    tableCleanup();
    testListStatusOnLargeDirectory();
    tableCleanup();
  }

  /**
   * Cleanup keyTable and directoryTable explicitly as FS delete operation
   * is not yet supported.
   *
   * @throws IOException DB failure
   */
  protected void tableCleanup() throws IOException {
    OMMetadataManager metadataMgr = cluster.getOzoneManager()
            .getMetadataManager();
    TableIterator<String, ? extends
            Table.KeyValue<String, OmDirectoryInfo>> dirTableIterator =
            metadataMgr.getDirectoryTable().iterator();
    dirTableIterator.seekToFirst();
    ArrayList <String> dirList = new ArrayList<>();
    while (dirTableIterator.hasNext()) {
      String key = dirTableIterator.key();
      if (StringUtils.isNotBlank(key)) {
        dirList.add(key);
      }
      dirTableIterator.next();
    }

    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmDirectoryInfo>>>
            cacheIterator = metadataMgr.getDirectoryTable().cacheIterator();
    while(cacheIterator.hasNext()){
      cacheIterator.next();
      cacheIterator.remove();
    }

    for (String dirKey : dirList) {
      metadataMgr.getDirectoryTable().delete(dirKey);
      Assert.assertNull("Unexpected entry!",
              metadataMgr.getDirectoryTable().get(dirKey));
    }

    Assert.assertTrue("DirTable is not empty",
            metadataMgr.getDirectoryTable().isEmpty());

    Assert.assertFalse(metadataMgr.getDirectoryTable().cacheIterator()
            .hasNext());

    TableIterator<String, ? extends
            Table.KeyValue<String, OmKeyInfo>> keyTableIterator =
            metadataMgr.getKeyTable().iterator();
    keyTableIterator.seekToFirst();
    ArrayList <String> fileList = new ArrayList<>();
    while (keyTableIterator.hasNext()) {
      String key = keyTableIterator.key();
      if (StringUtils.isNotBlank(key)) {
        fileList.add(key);
      }
      keyTableIterator.next();
    }

    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmDirectoryInfo>>>
            keyCacheIterator = metadataMgr.getDirectoryTable().cacheIterator();
    while(keyCacheIterator.hasNext()){
      keyCacheIterator.next();
      keyCacheIterator.remove();
    }

    for (String fileKey : fileList) {
      metadataMgr.getKeyTable().delete(fileKey);
      Assert.assertNull("Unexpected entry!",
              metadataMgr.getKeyTable().get(fileKey));
    }

    Assert.assertTrue("KeyTable is not empty",
            metadataMgr.getKeyTable().isEmpty());

    rootItemCount = 0;
  }

  @NotNull
  @Override
  protected OzoneConfiguration getOzoneConfig() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(FS_TRASH_INTERVAL_KEY, 1);
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
        enabledFileSystemPaths);
    conf.set(OMConfigKeys.OZONE_OM_LAYOUT_VERSION, "V1");
    return conf;
  }
}
