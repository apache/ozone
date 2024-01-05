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
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.service.SnapshotDirectoryCleaningService;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test Snapshot Directory Service.
 */
@Timeout(300)
public class TestSnapshotDirectoryCleaningService {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSnapshotDirectoryCleaningService.class);

  private static MiniOzoneCluster cluster;
  private static FileSystem fs;
  private static String volumeName;
  private static String bucketName;
  private static OzoneClient client;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OMConfigKeys.OZONE_SNAPSHOT_DIRECTORY_SERVICE_INTERVAL, 2500);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 2500,
        TimeUnit.MILLISECONDS);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
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

  @SuppressWarnings("checkstyle:LineLength")
  @Test
  public void testExclusiveSizeWithDirectoryDeepClean() throws Exception {

    Table<String, OmKeyInfo> deletedDirTable =
        cluster.getOzoneManager().getMetadataManager().getDeletedDirTable();
    Table<String, OmKeyInfo> keyTable =
        cluster.getOzoneManager().getMetadataManager()
            .getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    Table<String, OmDirectoryInfo> dirTable =
        cluster.getOzoneManager().getMetadataManager().getDirectoryTable();
    Table<String, RepeatedOmKeyInfo> deletedKeyTable =
        cluster.getOzoneManager().getMetadataManager().getDeletedTable();
    Table<String, SnapshotInfo> snapshotInfoTable =
        cluster.getOzoneManager().getMetadataManager().getSnapshotInfoTable();
    SnapshotDirectoryCleaningService snapshotDirectoryCleaningService =
        cluster.getOzoneManager().getKeyManager().getSnapshotDirectoryService();

    /*    DirTable
    /v/b/snapDir
    /v/b/snapDir/appRoot0-2/
    /v/b/snapDir/appRoot0-2/parentDir0-2/
          FileTable
    /v/b/snapDir/testKey0 - testKey4  = 5 keys
    /v/b/snapDir/appRoot0-2/parentDir0-2/childFile = 9 keys
    /v/b/snapDir/appRoot0/parentDir0-2/childFile0-4 = 15 keys
     */

    Path root = new Path("/snapDir");
    // Create parent dir from root.
    fs.mkdirs(root);

    // Add 5 files inside root dir
    // Creates /v/b/snapDir/testKey0 - testKey4
    for (int i = 0; i < 5; i++) {
      Path path = new Path(root, "testKey" + i);
      try (FSDataOutputStream stream = fs.create(path)) {
        stream.write(1);
      }
    }

    // Creates /v/b/snapDir/appRoot0-2/parentDir0-2/childFile
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        Path appRoot = new Path(root, "appRoot" + j);
        Path parent = new Path(appRoot, "parentDir" + i);
        Path child = new Path(parent, "childFile");
        try (FSDataOutputStream stream = fs.create(child)) {
          stream.write(1);
        }
      }
    }

    assertTableRowCount(keyTable, 14);
    assertTableRowCount(dirTable, 13);
    // Create snapshot
    client.getObjectStore().createSnapshot(volumeName, bucketName, "snap1");

    // Creates /v/b/snapDir/appRoot0/parentDir0-2/childFile0-4
    for (int i = 0; i < 3; i++) {
      Path appRoot = new Path(root, "appRoot0");
      Path parent = new Path(appRoot, "parentDir" + i);
      for (int j = 0; j < 5; j++) {
        Path child = new Path(parent, "childFile" + j);
        try (FSDataOutputStream stream = fs.create(child)) {
          stream.write(1);
        }
      }
    }

    for (int i = 5; i < 10; i++) {
      Path path = new Path(root, "testKey" + i);
      try (FSDataOutputStream stream = fs.create(path)) {
        stream.write(1);
      }
    }

    assertTableRowCount(deletedDirTable, 0);
    assertTableRowCount(keyTable, 34);
    assertTableRowCount(dirTable, 13);
    Path appRoot0 = new Path(root, "appRoot0");
    // Only parentDir0-2/childFile under appRoot0 is exclusive for snap1
    fs.delete(appRoot0, true);
    assertTableRowCount(deletedDirTable, 1);
    client.getObjectStore().createSnapshot(volumeName, bucketName, "snap2");

    // Delete testKey0-9
    for (int i = 0; i < 10; i++) {
      Path testKey = new Path(root, "testKey" + i);
      fs.delete(testKey, false);
    }

    fs.delete(root, true);
    assertTableRowCount(deletedKeyTable, 10);
    client.getObjectStore().createSnapshot(volumeName, bucketName, "snap3");
    long prevRunCount = snapshotDirectoryCleaningService.getRunCount().get();
    GenericTestUtils.waitFor(() -> snapshotDirectoryCleaningService.getRunCount().get()
        > prevRunCount + 1, 100, 10000);

    Thread.sleep(2000);
    Map<String, Long> expectedSize = new HashMap<String, Long>() {{
      // /v/b/snapDir/appRoot0/parentDir0-2/childFile contribute
      // exclusive size, /v/b/snapDir/appRoot0/parentDir0-2/childFile0-4
      // are deep cleaned and hence don't contribute to size.
        put("snap1", 3L);
      // Only testKey5-9 contribute to the exclusive size
        put("snap2", 5L);
        put("snap3", 0L);
      }};
    Thread.sleep(500);
    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
        iterator = snapshotInfoTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, SnapshotInfo> snapshotEntry = iterator.next();
        String snapshotName = snapshotEntry.getValue().getName();
        assertEquals(expectedSize.get(snapshotName), snapshotEntry.getValue().
            getExclusiveSize());
        // Since for the test we are using RATIS/THREE
        assertEquals(expectedSize.get(snapshotName) * 3,
            snapshotEntry.getValue().getExclusiveReplicatedSize());

      }
    }
  }

  private void assertTableRowCount(Table<String, ?> table, int count)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> assertTableRowCount(count, table), 1000,
        120000); // 2 minutes
  }

  private boolean assertTableRowCount(int expectedCount,
                                      Table<String, ?> table) {
    long count = 0L;
    try {
      count = cluster.getOzoneManager().getMetadataManager()
          .countRowsInTable(table);
      LOG.info("{} actual row count={}, expectedCount={}", table.getName(),
          count, expectedCount);
    } catch (IOException ex) {
      fail("testDoubleBuffer failed with: " + ex);
    }
    return count == expectedCount;
  }
}
