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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.DirectoryDeletingService;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.junit.Assert.fail;

/**
 * Directory deletion service test cases.
 */
public class TestDirectoryDeletingServiceV1 {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDirectoryDeletingServiceV1.class);

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private static boolean isBucketFSOptimized = true;
  private static boolean enabledFileSystemPaths = true;
  private static boolean omRatisEnabled = true;

  private static MiniOzoneCluster cluster;
  private static FileSystem fs;
  private static String volumeName;
  private static String bucketName;

  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OzoneConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 5);
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, omRatisEnabled);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    if (isBucketFSOptimized) {
      TestOMRequestUtils.configureFSOptimizedPaths(conf,
          enabledFileSystemPaths, OMConfigKeys.OZONE_OM_LAYOUT_VERSION_V1);
    } else {
      conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
          enabledFileSystemPaths);
    }
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();

    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster);
    volumeName = bucket.getVolumeName();
    bucketName = bucket.getName();

    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);

    fs = FileSystem.get(conf);
  }

  @AfterClass
  public static void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @After
  public void cleanup() {
    try {
      Path root = new Path("/");
      FileStatus[] fileStatuses = fs.listStatus(root);
      for (FileStatus fileStatus : fileStatuses) {
        fs.delete(fileStatus.getPath(), true);
      }
    } catch (IOException ex) {
      fail("Failed to cleanup files.");
    }
  }

  @Test
  public void testDeleteEmptyDirectory() throws Exception {
    Path root = new Path("/rootDir");
    Path appRoot = new Path(root, "appRoot");
    fs.mkdirs(appRoot);

    Table<String, OmKeyInfo> deletedDirTable =
        cluster.getOzoneManager().getMetadataManager().getDeletedDirTable();
    Table<String, OmDirectoryInfo> dirTable =
        cluster.getOzoneManager().getMetadataManager().getDirectoryTable();

    // Before delete
    GenericTestUtils.waitFor(() ->
            assertTableRowCount(0, deletedDirTable),
        1000, 120000); // 2 minutes

    GenericTestUtils.waitFor(() ->
            assertTableRowCount(2, dirTable),
        1000, 120000); // 2 minutes

    // Delete the appRoot, empty dir
    fs.delete(appRoot, true);


    // After Delete
    checkPath(appRoot);

    GenericTestUtils.waitFor(() ->
            assertTableRowCount(0, deletedDirTable),
        1000, 120000); // 2 minutes

    GenericTestUtils.waitFor(() ->
            assertTableRowCount(1, dirTable),
        1000, 120000); // 2 minutes

    Assert.assertTrue(dirTable.iterator().hasNext());
    Assert.assertEquals(root.getName(),
        dirTable.iterator().next().getValue().getName());

    DirectoryDeletingService dirDeletingService =
        (DirectoryDeletingService) cluster.getOzoneManager().getKeyManager()
            .getDirDeletingService();

    GenericTestUtils.waitFor(
        () -> dirDeletingService.getDeletedDirsCount() >= 1,
        1000, 10000);
    GenericTestUtils.waitFor(
        () -> dirDeletingService.getMovedFilesCount() >= 0,
        1000, 10000);

    Assert.assertTrue(dirDeletingService.getRunCount() > 1);
  }

  @Test
  public void testDeleteWithMultiLevels() throws Exception {
    Path root = new Path("/rootDir");
    Path appRoot = new Path(root, "appRoot");

    for (int i = 1; i <= 3; i++) {
      Path parent = new Path(appRoot, "parentDir" + i);
      Path child = new Path(parent, "childFile");
      ContractTestUtils.touch(fs, child);
    }

    Table<String, OmKeyInfo> deletedDirTable =
        cluster.getOzoneManager().getMetadataManager().getDeletedDirTable();
    Table<String, OmKeyInfo> keyTable =
        cluster.getOzoneManager().getMetadataManager().getKeyTable();
    Table<String, OmDirectoryInfo> dirTable =
        cluster.getOzoneManager().getMetadataManager().getDirectoryTable();

    // Before delete
    GenericTestUtils.waitFor(() ->
            assertTableRowCount(0, deletedDirTable),
        1000, 120000); // 2 minutes

    GenericTestUtils.waitFor(() ->
            assertTableRowCount(3, keyTable),
        1000, 120000); // 2 minutes

    GenericTestUtils.waitFor(() ->
            assertTableRowCount(5, dirTable),
        1000, 120000); // 2 minutes

    // Delete the rootDir, which should delete all keys.
    fs.delete(root, true);

    // After Delete
    checkPath(root);

    GenericTestUtils.waitFor(() ->
            assertTableRowCount(0, deletedDirTable),
        1000, 120000); // 2 minutes

    GenericTestUtils.waitFor(() ->
            assertTableRowCount(0, keyTable),
        1000, 120000); // 2 minutes

    GenericTestUtils.waitFor(() ->
            assertTableRowCount(0, dirTable),
        1000, 120000); // 2 minutes

    DirectoryDeletingService dirDeletingService =
        (DirectoryDeletingService) cluster.getOzoneManager().getKeyManager()
            .getDirDeletingService();

    GenericTestUtils.waitFor(
        () -> dirDeletingService.getDeletedDirsCount() >= 5,
        1000, 10000);
    GenericTestUtils.waitFor(
        () -> dirDeletingService.getMovedFilesCount() >= 3,
        1000, 10000);

    Assert.assertTrue(dirDeletingService.getRunCount() > 1);
  }

  private boolean assertTableRowCount(int expectedCount,
                                      Table<String, ?> table) {
    long count = 0L;
    try {
      count = cluster.getOzoneManager().getMetadataManager()
          .countRowsInTable(table);
      LOG.info("{} row count={}", table.getName(), count);
    } catch (IOException ex) {
      fail("testDoubleBuffer failed with: " + ex);
    }
    return count == expectedCount;
  }

  private void checkPath(Path path) {
    try {
      fs.getFileStatus(path);
      fail("testRecursiveDelete failed");
    } catch (IOException ex) {
      Assert.assertTrue(ex instanceof FileNotFoundException);
      Assert.assertTrue(ex.getMessage().contains("No such file or directory"));
    }
  }

}
