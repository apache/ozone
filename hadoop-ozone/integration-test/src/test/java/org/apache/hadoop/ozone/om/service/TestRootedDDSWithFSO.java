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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.service.TestDirectoryDeletingServiceWithFSO.assertSubPathsCount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directory deletion service test cases using rooted ozone filesystem
 * ofs://volume/bucket/path.
 */
public class TestRootedDDSWithFSO {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRootedDDSWithFSO.class);

  private static MiniOzoneCluster cluster;
  private static FileSystem fs;
  private static Path volumePath;
  private static Path bucketPath;
  private static OzoneClient client;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setStrings(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, "5s");
    conf.setInt(OMConfigKeys.OZONE_THREAD_NUMBER_DIR_DELETION, 1);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.FILE_SYSTEM_OPTIMIZED.name());
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(client, getFSOBucketLayout());
    String volumeName = bucket.getVolumeName();
    volumePath = new Path(OZONE_URI_DELIMITER, volumeName);
    String bucketName = bucket.getName();
    bucketPath = new Path(volumePath, bucketName);

    String rootPath = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);

    fs = FileSystem.get(conf);
  }

  @AfterAll
  public static void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @AfterEach
  public void cleanup() {
    assertDoesNotThrow(() -> {
      Path root = new Path("/");
      FileStatus[] fileStatuses = fs.listStatus(root);
      for (FileStatus fileStatus : fileStatuses) {
        fs.delete(fileStatus.getPath(), false);
      }
    });
  }

  @Test
  public void testDeleteVolumeAndBucket() throws Exception {
    int totalDirCount = 10;
    int totalFilesCount = 6;

    for (int i = 1; i <= 5; i++) {
      String dirStr1 = "dir1" + i;
      String dirStr2 = "dir2" + i;
      String fileStr3 = "file3" + i;
      Path dirLevel1 = new Path(bucketPath, dirStr1);
      Path dirLevel2 = new Path(dirLevel1, dirStr2);
      fs.mkdirs(dirLevel2);
      Path filePath3 = new Path(dirLevel2, fileStr3);

      try (FSDataOutputStream out1 = fs.create(filePath3)) {
        out1.write(2);
      }
    }
    // create another top level file
    Path file16 = new Path(bucketPath, "file16");
    try (FSDataOutputStream out1 = fs.create(file16)) {
      out1.write(2);
    }

    /*         bucket
        _________|_________________________
        |       |      |     |      |     |
       dir11   dir12  dir13 dir14 dir15   file16
        |       |      |      |     |
       dir21   dir22  dir23 dir24  dir25
        |       |      |      |     |
        file31  file32 file33 file34 file35

        Total dirs =10 , files = 6 , keys = 16
     */

    Table<String, OmKeyInfo> deletedDirTable =
        cluster.getOzoneManager().getMetadataManager().getDeletedDirTable();
    Table<String, OmKeyInfo> keyTable =
        cluster.getOzoneManager().getMetadataManager()
            .getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> dirTable =
        cluster.getOzoneManager().getMetadataManager().getDirectoryTable();

    DirectoryDeletingService dirDeletingService =
        (DirectoryDeletingService) cluster.getOzoneManager().getKeyManager()
            .getDirDeletingService();

    // Before delete
    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(dirTable, totalDirCount);
    assertTableRowCount(keyTable, totalFilesCount);

    OMMetrics omMetrics = cluster.getOzoneManager().getMetrics();
    long prevDeletes = omMetrics.getNumKeyDeletes();
    assertTrue(fs.delete(bucketPath, true));
    assertTrue(fs.delete(volumePath, false));
    GenericTestUtils.waitFor(() -> {
      long keyCount = omMetrics.getNumKeys();
      return keyCount == 0;
    }, 1000, 30000);
    long deletes = omMetrics.getNumKeyDeletes();
    assertEquals(prevDeletes + totalDirCount + totalFilesCount, deletes);

    // After Delete
    checkPath(volumePath);

    assertTableRowCount(dirTable, 0);
    assertTableRowCount(keyTable, 0);
    // There is an immediate file under bucket,
    // which was moved along with bucket delete.
    int movedFilesCount = totalFilesCount - 1;
    assertSubPathsCount(dirDeletingService::getMovedFilesCount,
        movedFilesCount);
    assertSubPathsCount(dirDeletingService::getDeletedDirsCount,
        totalDirCount);
  }

  private void checkPath(Path path) {
    FileNotFoundException ex = assertThrows(FileNotFoundException.class, () ->
        fs.getFileStatus(path), "testRecursiveDelete failed");
    assertThat(ex.getMessage()).contains("File not found");
  }

  private void assertTableRowCount(Table<String, ?> table, int count)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> assertTableRowCount(count, table), 1000,
        120000); // 2 minutes
  }

  private boolean assertTableRowCount(int expectedCount,
      Table<String, ?> table) {
    AtomicLong count = new AtomicLong(0L);
    assertDoesNotThrow(() -> {
      count.set(cluster.getOzoneManager().getMetadataManager().countRowsInTable(table));
      LOG.info("{} actual row count={}, expectedCount={}", table.getName(),
          count.get(), expectedCount);
    });
    return count.get() == expectedCount;
  }

  private static BucketLayout getFSOBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
