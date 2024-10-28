/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.fs.ozone;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.repair.om.FSORepairTool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

/**
 * FSORepairTool test cases.
 */
public class TestFSORepairTool {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestFSORepairTool.class);

  private static MiniOzoneHAClusterImpl cluster;
  private static FileSystem fs;
  private static OzoneClient client;


  @BeforeAll
  public static void init() throws Exception {
    // Set configs.
    OzoneConfiguration conf = new OzoneConfiguration();
    // deletion services will be triggered manually.
    conf.setTimeDuration(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL,
        1_000_000, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 1_000_000,
        TimeUnit.SECONDS);
    conf.setInt(OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK, 10);
    conf.setInt(OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK, 10);
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
    // Since delete services use RocksDB iterators, make sure the double
    // buffer is flushed between runs.
    conf.setInt(OMConfigKeys.OZONE_OM_UNFLUSHED_TRANSACTION_MAX_COUNT, 1);

    // Build cluster.
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
            .setNumOfOzoneManagers(1)
            .setOMServiceId("omservice")
            .setNumDatanodes(3)
            .build();
    cluster.waitForClusterToBeReady();

    // Init ofs.
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    fs = FileSystem.get(conf);
    client = OzoneClientFactory.getRpcClient("omservice", conf);
  }

  @AfterEach
  public void cleanNamespace() throws Exception {
    if (fs.exists(new Path("/vol1"))) {
      fs.delete(new Path("/vol1"), true);
    }
    if (fs.exists(new Path("/vol2"))) {
      fs.delete(new Path("/vol2"), true);
    }
    runDeletes();
    assertFileAndDirTablesEmpty();
  }

  @AfterAll
  public static void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @Test
  public void testFSORepairToolWithVolumeAndBucketFilter() throws Exception {
    FSORepairTool.Report reportVol1Buck1 = buildDisconnectedTree("vol1", "bucket1", 10);
    FSORepairTool.Report reportVol2Buck2 = buildDisconnectedTree("vol2", "bucket2", 10);

    FSORepairTool repairToolFiltered = new FSORepairTool(
        getOmDB(), getOmDBLocation(), false, "vol1", "bucket1");
    FSORepairTool.Report filteredReport = repairToolFiltered.run();

    Assertions.assertEquals(reportVol1Buck1, filteredReport,
        "Filtered report should match the unreachable points in vol1/bucket1.");
    Assertions.assertNotEquals(reportVol2Buck2, filteredReport,
        "Filtered report should not include vol2/bucket2.");

    assertDisconnectedObjectsMarkedForDelete(1);
  }

  @Test
  public void testConnectedTreeOneBucket() throws Exception {
    org.apache.hadoop.ozone.repair.om.FSORepairTool.Report expectedReport = buildConnectedTree("vol1", "bucket1");

    // Test the connected tree in debug mode.
    FSORepairTool fsoTool = new FSORepairTool(getOmDB(),
        getOmDBLocation(), true, null, null);
    FSORepairTool.Report debugReport = fsoTool.run();

    Assertions.assertEquals(expectedReport, debugReport);
    assertConnectedTreeReadable("vol1", "bucket1");
    assertDeleteTablesEmpty();

    // Running again in repair mode should give same results since the tree
    // is connected.
    fsoTool = new org.apache.hadoop.ozone.repair.om.FSORepairTool(getOmDB(),
        getOmDBLocation(), false, null, null);
    org.apache.hadoop.ozone.repair.om.FSORepairTool.Report repairReport = fsoTool.run();

    Assertions.assertEquals(expectedReport, repairReport);
    assertConnectedTreeReadable("vol1", "bucket1");
    assertDeleteTablesEmpty();
  }

  @Test
  public void testReportedDataSize() throws Exception {
    FSORepairTool.Report report1 = buildDisconnectedTree("vol1", "bucket1", 10);
    FSORepairTool.Report report2 = buildConnectedTree("vol1", "bucket2", 10);
    FSORepairTool.Report expectedReport = new FSORepairTool.Report(report1, report2);

    FSORepairTool
        repair = new FSORepairTool(getOmDB(),
        getOmDBLocation(), false, null, null);
    FSORepairTool.Report debugReport = repair.run();
    Assertions.assertEquals(expectedReport, debugReport);
  }

  @Test
  public void testMultipleBucketsAndVolumes() throws Exception {
    FSORepairTool.Report report1 = buildConnectedTree("vol1", "bucket1");
    FSORepairTool.Report report2 = buildDisconnectedTree("vol2", "bucket2");
    FSORepairTool.Report expectedAggregateReport = new org.apache.hadoop.ozone.repair.om.FSORepairTool.Report(
        report1, report2);

    org.apache.hadoop.ozone.repair.om.FSORepairTool
        repair = new org.apache.hadoop.ozone.repair.om.FSORepairTool(getOmDB(),
        getOmDBLocation(), false, null, null);
    org.apache.hadoop.ozone.repair.om.FSORepairTool.Report generatedReport = repair.run();

    Assertions.assertEquals(generatedReport, expectedAggregateReport);
    assertConnectedTreeReadable("vol1", "bucket1");
    assertDisconnectedTreePartiallyReadable("vol2", "bucket2");
    assertDisconnectedObjectsMarkedForDelete(1);
  }

  /**
   * Tests having multiple entries in the deleted file and directory tables
   * for the same objects.
   */
  @Test
  public void testDeleteOverwrite() throws Exception {
    // Create files and dirs under dir1. To make sure they are added to the
    // delete table, the keys must have data.
    buildConnectedTree("vol1", "bucket1", 10);
    // Move soon to be disconnected objects to the deleted table.
    fs.delete(new Path("/vol1/bucket1/dir1/dir2/file3"), true);
    fs.delete(new Path("/vol1/bucket1/dir1/dir2"), true);
    fs.delete(new Path("/vol1/bucket1/dir1/file1"), true);
    fs.delete(new Path("/vol1/bucket1/dir1/file2"), true);

    // Recreate deleted objects, then disconnect dir1.
    // This means after the repair runs, these objects will be
    // the deleted tables multiple times. Some will have the same dir1 parent ID
    // in their key name too.
    ContractTestUtils.touch(fs, new Path("/vol1/bucket1/dir1/dir2/file3"));
    ContractTestUtils.touch(fs, new Path("/vol1/bucket1/dir1/file1"));
    ContractTestUtils.touch(fs, new Path("/vol1/bucket1/dir1/file2"));
    disconnectDirectory("dir1");

    org.apache.hadoop.ozone.repair.om.FSORepairTool
        repair = new org.apache.hadoop.ozone.repair.om.FSORepairTool(getOmDB(),
        getOmDBLocation(), false, null, null);
    org.apache.hadoop.ozone.repair.om.FSORepairTool.Report generatedReport = repair.run();

    Assertions.assertEquals(1, generatedReport.getUnreachableDirs());
    Assertions.assertEquals(3, generatedReport.getUnreachableFiles());

    assertDisconnectedObjectsMarkedForDelete(2);
  }

  @Test
  public void testEmptyFileTrees() throws Exception {
    // Run when there are no file trees.
    org.apache.hadoop.ozone.repair.om.FSORepairTool
        repair = new org.apache.hadoop.ozone.repair.om.FSORepairTool(getOmDB(),
        getOmDBLocation(), false, null, null);
    org.apache.hadoop.ozone.repair.om.FSORepairTool.Report generatedReport = repair.run();
    Assertions.assertEquals(generatedReport, new org.apache.hadoop.ozone.repair.om.FSORepairTool.Report());
    assertDeleteTablesEmpty();

    // Create an empty volume and bucket.
    fs.mkdirs(new Path("/vol1"));
    fs.mkdirs(new Path("/vol2/bucket1"));

    // Run on an empty volume and bucket.
    repair = new org.apache.hadoop.ozone.repair.om.FSORepairTool(getOmDB(),
        getOmDBLocation(), false, null, null);
    generatedReport = repair.run();
    Assertions.assertEquals(generatedReport, new org.apache.hadoop.ozone.repair.om.FSORepairTool.Report());
    assertDeleteTablesEmpty();
  }

  @Test
  public void testNonFSOBucketsSkipped() throws Exception {
    ObjectStore store = client.getObjectStore();
    try {
      // Create legacy and OBS buckets.
      store.createVolume("vol1");
      store.getVolume("vol1").createBucket("obs-bucket",
          BucketArgs.newBuilder().setBucketLayout(BucketLayout.OBJECT_STORE)
              .build());
      store.getVolume("vol1").createBucket("legacy-bucket",
          BucketArgs.newBuilder().setBucketLayout(BucketLayout.LEGACY)
              .build());

      // Put a key in the legacy and OBS buckets.
      OzoneOutputStream obsStream = store.getVolume("vol1")
          .getBucket("obs-bucket")
          .createKey("prefix/test-key", 3);
      obsStream.write(new byte[]{1, 1, 1});
      obsStream.close();

      OzoneOutputStream legacyStream = store.getVolume("vol1")
          .getBucket("legacy-bucket")
          .createKey("prefix/test-key", 3);
      legacyStream.write(new byte[]{1, 1, 1});
      legacyStream.close();

      // Add an FSO bucket with data.
      org.apache.hadoop.ozone.repair.om.FSORepairTool.Report connectReport = buildConnectedTree("vol1", "fso" +
          "-bucket");

      // Even in repair mode there should be no action. legacy and obs buckets
      // will be skipped and FSO tree is connected.
      org.apache.hadoop.ozone.repair.om.FSORepairTool
          repair = new org.apache.hadoop.ozone.repair.om.FSORepairTool(getOmDB(),
          getOmDBLocation(), false, null, null);
      org.apache.hadoop.ozone.repair.om.FSORepairTool.Report generatedReport = repair.run();

      Assertions.assertEquals(connectReport, generatedReport);
      assertConnectedTreeReadable("vol1", "fso-bucket");
      assertDeleteTablesEmpty();
    } finally {
      // Need to manually delete obs bucket. It cannot be deleted with ofs as
      // part of the normal test cleanup.
      store.getVolume("vol1").getBucket("obs-bucket")
          .deleteKey("prefix/test-key");
      store.getVolume("vol1").deleteBucket("obs-bucket");
    }
  }


  private org.apache.hadoop.ozone.repair.om.FSORepairTool.Report buildConnectedTree(String volume, String bucket)
      throws Exception {
    return buildConnectedTree(volume, bucket, 0);
  }

  /**
   * Creates a tree with 3 reachable directories and 4 reachable files.
   */
  private org.apache.hadoop.ozone.repair.om.FSORepairTool.Report buildConnectedTree(String volume, String bucket,
                                                                                    int fileSize)
      throws Exception {
    Path bucketPath = new Path("/" + volume + "/" + bucket);
    Path dir1 = new Path(bucketPath, "dir1");
    Path file1 = new Path(dir1, "file1");
    Path file2 = new Path(dir1, "file2");

    Path dir2 = new Path(bucketPath, "dir1/dir2");
    Path file3 = new Path(dir2, "file3");

    Path dir3 = new Path(bucketPath, "dir3");
    Path file4 = new Path(bucketPath, "file4");

    fs.mkdirs(dir1);
    fs.mkdirs(dir2);
    fs.mkdirs(dir3);

    // Content to put in every file.
    String data = new String(new char[fileSize]);

    FSDataOutputStream stream = fs.create(file1);
    stream.write(data.getBytes(StandardCharsets.UTF_8));
    stream.close();
    stream = fs.create(file2);
    stream.write(data.getBytes(StandardCharsets.UTF_8));
    stream.close();
    stream = fs.create(file3);
    stream.write(data.getBytes(StandardCharsets.UTF_8));
    stream.close();
    stream = fs.create(file4);
    stream.write(data.getBytes(StandardCharsets.UTF_8));
    stream.close();

    assertConnectedTreeReadable(volume, bucket);

    return new org.apache.hadoop.ozone.repair.om.FSORepairTool.Report.Builder()
        .setReachableDirs(3)
        .setReachableFiles(4)
        .setReachableBytes(fileSize * 4L)
        .build();
  }

  private void assertConnectedTreeReadable(String volume, String bucket)
      throws IOException {
    Path bucketPath = new Path("/" + volume + "/" + bucket);
    Path dir1 = new Path(bucketPath, "dir1");
    Path file1 = new Path(dir1, "file1");
    Path file2 = new Path(dir1, "file2");

    Path dir2 = new Path(bucketPath, "dir1/dir2");
    Path file3 = new Path(dir2, "file3");

    Path dir3 = new Path(bucketPath, "dir3");
    Path file4 = new Path(bucketPath, "file4");

    Assertions.assertTrue(fs.exists(dir1));
    Assertions.assertTrue(fs.exists(dir2));
    Assertions.assertTrue(fs.exists(dir3));
    Assertions.assertTrue(fs.exists(file1));
    Assertions.assertTrue(fs.exists(file2));
    Assertions.assertTrue(fs.exists(file3));
    Assertions.assertTrue(fs.exists(file4));
  }

  private org.apache.hadoop.ozone.repair.om.FSORepairTool.Report buildDisconnectedTree(String volume, String bucket)
      throws Exception {
    return buildDisconnectedTree(volume, bucket, 0);
  }

  /**
   * Creates a tree with 2 reachable directories, 1 reachable file, 1
   * unreachable directory, and 3 unreachable files.
   */
  private org.apache.hadoop.ozone.repair.om.FSORepairTool.Report buildDisconnectedTree(String volume, String bucket,
                                                                                       int fileSize) throws Exception {
    buildConnectedTree(volume, bucket, fileSize);

    // Manually remove dir1. This should disconnect 3 of the files and 1 of
    // the directories.
    disconnectDirectory("dir1");

    assertDisconnectedTreePartiallyReadable(volume, bucket);

    return new org.apache.hadoop.ozone.repair.om.FSORepairTool.Report.Builder()
        .setReachableDirs(1)
        .setReachableFiles(1)
        .setReachableBytes(fileSize)
        // dir1 does not count towards the unreachable directories the tool
        // will see. It was deleted completely so the tool will never see it.
        .setUnreachableDirs(1)
        .setUnreachableFiles(3)
        .setUnreachableBytes(fileSize * 3L)
        .build();
  }

  private void disconnectDirectory(String dirName) throws Exception {
    OzoneManager leader = cluster.getOMLeader();
    Table<String, OmDirectoryInfo> dirTable =
        leader.getMetadataManager().getDirectoryTable();
    try (TableIterator<String, ?
        extends Table.KeyValue<String, OmDirectoryInfo>> iterator =
            dirTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> entry = iterator.next();
        String key = entry.getKey();
        if (key.contains(dirName)) {
          dirTable.delete(key);
          break;
        }
      }
    }
  }

  private void assertDisconnectedTreePartiallyReadable(
      String volume, String bucket) throws Exception {
    Path bucketPath = new Path("/" + volume + "/" + bucket);
    Path dir1 = new Path(bucketPath, "dir1");
    Path file1 = new Path(dir1, "file1");
    Path file2 = new Path(dir1, "file2");

    Path dir2 = new Path(bucketPath, "dir1/dir2");
    Path file3 = new Path(dir2, "file3");

    Path dir3 = new Path(bucketPath, "dir3");
    Path file4 = new Path(bucketPath, "file4");

    Assertions.assertFalse(fs.exists(dir1));
    Assertions.assertFalse(fs.exists(dir2));
    Assertions.assertTrue(fs.exists(dir3));
    Assertions.assertFalse(fs.exists(file1));
    Assertions.assertFalse(fs.exists(file2));
    Assertions.assertFalse(fs.exists(file3));
    Assertions.assertTrue(fs.exists(file4));
  }

  /**
   * Checks that the disconnected tree's unreachable objects are correctly
   * moved to the delete table. If the tree was written and deleted multiple
   * times, it makes sure the delete entries with the same name are preserved.
   */
  private void assertDisconnectedObjectsMarkedForDelete(int numWrites)
      throws Exception {

    Map<String, Integer> pendingDeleteDirCounts = new HashMap<>();

    // Check deleted directory table.
    OzoneManager leader = cluster.getOMLeader();
    Table<String, OmKeyInfo> deletedDirTable =
        leader.getMetadataManager().getDeletedDirTable();
    try (TableIterator<String, ?
        extends Table.KeyValue<String, OmKeyInfo>> iterator =
            deletedDirTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> entry = iterator.next();
        String key = entry.getKey();
        OmKeyInfo value = entry.getValue();

        String dirName = key.split("/")[4];
        LOG.info("In deletedDirTable, extracting directory name {}  from DB " +
            "key {}", dirName, key);

        // Check that the correct dir info was added.
        // FSO delete path will fill in the whole path to the key in the
        // proto when it is deleted. Once the tree is disconnected that can't
        // be done, so just make sure the dirName contained in the key name
        // somewhere.
        Assertions.assertTrue(value.getKeyName().contains(dirName));

        int count = pendingDeleteDirCounts.getOrDefault(dirName, 0);
        pendingDeleteDirCounts.put(dirName, count + 1);
      }
    }

    // 1 directory is disconnected in the tree. dir1 was totally deleted so
    // the repair tool will not see it.
    Assertions.assertEquals(1, pendingDeleteDirCounts.size());
    Assertions.assertEquals(numWrites, pendingDeleteDirCounts.get("dir2"));

    // Check that disconnected files were put in deleting tables.
    Map<String, Integer> pendingDeleteFileCounts = new HashMap<>();

    Table<String, RepeatedOmKeyInfo> deletedFileTable =
        leader.getMetadataManager().getDeletedTable();
    try (TableIterator<String, ?
        extends Table.KeyValue<String, RepeatedOmKeyInfo>> iterator =
            deletedFileTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, RepeatedOmKeyInfo> entry = iterator.next();
        String key = entry.getKey();
        RepeatedOmKeyInfo value = entry.getValue();

        String[] keyParts = key.split("/");
        String fileName = keyParts[keyParts.length - 1];

        LOG.info("In deletedTable, extracting file name {}  from DB " +
            "key {}", fileName, key);

        for (OmKeyInfo fileInfo: value.getOmKeyInfoList()) {
          // Check that the correct file info was added.
          Assertions.assertTrue(fileInfo.getKeyName().contains(fileName));

          int count = pendingDeleteFileCounts.getOrDefault(fileName, 0);
          pendingDeleteFileCounts.put(fileName, count + 1);
        }
      }
    }

    // 3 files are disconnected in the tree.
    // TODO: dir2 ended up in here with count = 1. file3 also had count=1
    //  Likely that the dir2/file3 entry got split in two.
    Assertions.assertEquals(3, pendingDeleteFileCounts.size());
    Assertions.assertEquals(numWrites, pendingDeleteFileCounts.get("file1"));
    Assertions.assertEquals(numWrites, pendingDeleteFileCounts.get("file2"));
    Assertions.assertEquals(numWrites, pendingDeleteFileCounts.get("file3"));
  }

  private void assertDeleteTablesEmpty() throws IOException {
    OzoneManager leader = cluster.getOMLeader();
    Assertions.assertTrue(leader.getMetadataManager().getDeletedDirTable().isEmpty());
    Assertions.assertTrue(leader.getMetadataManager().getDeletedTable().isEmpty());
  }

  private void runDeletes() throws Exception {
    OzoneManager leader = cluster.getOMLeader();

    int i = 0;
    while (!leader.getMetadataManager().getDeletedDirTable().isEmpty()) {
      LOG.info("Running iteration {} of DirectoryDeletingService.", i++);
      leader.getKeyManager().getDirDeletingService().runPeriodicalTaskNow();
      // Wait for work from this run to flush through the double buffer.
      Thread.sleep(500);
    }

    i = 0;
    while (!leader.getMetadataManager().getDeletedTable().isEmpty()) {
      LOG.info("Running iteration {} of KeyDeletingService.", i++);
      leader.getKeyManager().getDeletingService().runPeriodicalTaskNow();
      // Wait for work from this run to flush through the double buffer.
      Thread.sleep(500);
    }
  }

  private void assertFileAndDirTablesEmpty() throws Exception {
    OzoneManager leader = cluster.getOMLeader();
    Assertions.assertTrue(leader.getMetadataManager().getDirectoryTable().isEmpty());
    Assertions.assertTrue(leader.getMetadataManager().getFileTable().isEmpty());
  }

  private DBStore getOmDB() {
    return cluster.getOMLeader().getMetadataManager().getStore();
  }

  private String getOmDBLocation() {
    return cluster.getOMLeader().getMetadataManager().getStore().getDbLocation().toString();
  }
}
