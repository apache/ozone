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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_DEEP_CLEANING_ENABLED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.service.DirectoryDeletingService;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Snapshot Directory Service.
 */
public class TestSnapshotDirectoryCleaningService {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSnapshotDirectoryCleaningService.class);

  private static MiniOzoneCluster cluster;
  private static FileSystem fs;
  private static String volumeName;
  private static String bucketName;
  private static OzoneClient client;
  private static AtomicLong counter = new AtomicLong(0L);

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 2500);
    conf.setBoolean(OZONE_SNAPSHOT_DEEP_CLEANING_ENABLED, true);
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
    assertDoesNotThrow(() -> {
      Path root = new Path("/");
      FileStatus[] fileStatuses = fs.listStatus(root);
      for (FileStatus fileStatus : fileStatuses) {
        fs.delete(fileStatus.getPath(), true);
      }
    });
  }

  @SuppressWarnings("checkstyle:LineLength")
  @Test
  @Flaky("HDDS-11129")
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
    DirectoryDeletingService directoryDeletingService =
        cluster.getOzoneManager().getKeyManager().getDirDeletingService();
    SnapshotChainManager snapshotChainManager = ((OmMetadataManagerImpl)cluster.getOzoneManager().getMetadataManager())
        .getSnapshotChainManager();

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
    long prevRunCount = directoryDeletingService.getRunCount().get();
    GenericTestUtils.waitFor(() -> directoryDeletingService.getRunCount().get()
        > prevRunCount + 1, 100, 10000);
    Map<String, Long> expectedSize = new HashMap<String, Long>() {{
      // /v/b/snapDir/appRoot0/parentDir0-2/childFile contribute
      // exclusive size, /v/b/snapDir/appRoot0/parentDir0-2/childFile0-4
      // are deep cleaned and hence don't contribute to size.
        put("snap1", 3L);
      // Only testKey5-9 contribute to the exclusive size
        put("snap2", 5L);
        put("snap3", 0L);
      }};

    try (Table.KeyValueIterator<String, SnapshotInfo>
        iterator = snapshotInfoTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, SnapshotInfo> snapshotEntry = iterator.next();
        String snapshotName = snapshotEntry.getValue().getName();

        GenericTestUtils.waitFor(() -> {
          try {
            SnapshotInfo nextSnapshot = SnapshotUtils.getNextSnapshot(cluster.getOzoneManager(), snapshotChainManager,
                snapshotEntry.getValue());
            return nextSnapshot == null || (nextSnapshot.isDeepCleanedDeletedDir() && nextSnapshot.isDeepCleaned());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }, 1000, 10000);
        SnapshotInfo snapshotInfo = snapshotInfoTable.get(snapshotEntry.getKey());
        assertEquals(expectedSize.get(snapshotName),
            snapshotInfo.getExclusiveSize() + snapshotInfo.getExclusiveSizeDeltaFromDirDeepCleaning());
        // Since for the test we are using RATIS/THREE
        assertEquals(expectedSize.get(snapshotName) * 3,
            snapshotInfo.getExclusiveReplicatedSize() + snapshotInfo.getExclusiveReplicatedSizeDeltaFromDirDeepCleaning());
      }
    }
  }

  private SnapshotDiffReportOzone getSnapDiffReport(String volume,
      String bucket,
      String fromSnapshot,
      String toSnapshot)
      throws InterruptedException, IOException {
    SnapshotDiffResponse response;
    do {
      response = client.getObjectStore().snapshotDiff(volume, bucket, fromSnapshot,
          toSnapshot, null, 0, true, true);
      Thread.sleep(response.getWaitTimeInMs());
    } while (response.getJobStatus() != DONE);
    assertEquals(DONE, response.getJobStatus());
    return response.getSnapshotDiffReport();
  }

  /**
   * Testing Scenario:
   * 1) Create dir1/dir2/dir3/dir4
   * 2) Suspend KeyDeletingService & DirectoryDeletingService
   * 3) Delete dir1/dir2
   * 4) Create snapshot snap1
   * 5) Create dir1/dir3/dir6
   * 6) Create snapshot snap2
   * 7) Resume KeyDeletingService & DirectoryDeletingService
   * 8) Wait for snap1 to get Deep cleaned completely.
   * 9) Create dir1/dir4/dir5
   * 9) Create snapshot snap3
   * 10) Perform SnapshotDiff b/w snap2 & snap3
   * @throws Exception
   */
  @Test
  public void testSnapshotDiffBeforeAndAfterDeepCleaning() throws Exception {
    String volume = "vol-" + counter.incrementAndGet();
    String bucket = "buc-" + counter.incrementAndGet();
    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket volBucket = TestDataUtil.createVolumeAndBucket(client, volume, bucket,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    volBucket.createDirectory("dir1/dir2/dir3/dir4");
    cluster.getOzoneManager().getKeyManager().getDirDeletingService().suspend();
    cluster.getOzoneManager().getKeyManager().getDeletingService().suspend();
    volBucket.deleteDirectory("dir1/dir2", true);
    client.getObjectStore().createSnapshot(volume, bucket, "snap1");
    volBucket.createDirectory("dir1/dir3/dir6");
    client.getObjectStore().createSnapshot(volume, bucket, "snap2");
    volBucket.createDirectory("dir1/dir4/dir5");
    cluster.getOzoneManager().getKeyManager().getDirDeletingService().resume();
    cluster.getOzoneManager().getKeyManager().getDeletingService().resume();
    GenericTestUtils.waitFor(() -> {
      try {
        SnapshotInfo snapshotInfo = cluster.getOzoneManager().getSnapshotInfo(volume, bucket, "snap1");
        return snapshotInfo.isDeepCleaned() && snapshotInfo.isDeepCleanedDeletedDir();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 1000, 10000);
    client.getObjectStore().createSnapshot(volume, bucket, "snap3");
    SnapshotDiffReportOzone diff = getSnapDiffReport(volume, bucket, "snap2", "snap3");
    assertEquals(2, diff.getDiffList().size());
    assertEquals(Arrays.asList(
        new DiffReportEntry(SnapshotDiffReport.DiffType.CREATE, StringUtils.string2Bytes("dir1/dir4")),
        new DiffReportEntry(SnapshotDiffReport.DiffType.CREATE, StringUtils.string2Bytes("dir1/dir4/dir5"))),
        diff.getDiffList());
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
}
