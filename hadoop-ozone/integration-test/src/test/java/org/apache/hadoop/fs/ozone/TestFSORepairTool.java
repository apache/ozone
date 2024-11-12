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
import org.apache.hadoop.hdds.cli.GenericCli;
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
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.repair.om.FSORepairTool;
import org.apache.hadoop.ozone.shell.OzoneShell;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static java.lang.System.err;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * FSORepairTool test cases.
 */
public class TestFSORepairTool {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestFSORepairTool.class);

  private static final String DEFAULT_ENCODING = UTF_8.name();
  private static MiniOzoneHAClusterImpl cluster;
  private static FileSystem fs;
  private static OzoneClient client;
  private static OzoneConfiguration conf = null;
  private static FSORepairTool tool;

  @BeforeAll
  public static void init() throws Exception {
    // Set configs.
    conf = new OzoneConfiguration();
    // deletion services will be triggered manually.
    conf.setInt(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 2000);
    conf.setInt(OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK, 5);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
            TimeUnit.MILLISECONDS);
    conf.setInt(OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK, 20);
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
    // Since delete services use RocksDB iterators, make sure the double
    // buffer is flushed between runs.
    conf.setInt(OMConfigKeys.OZONE_OM_UNFLUSHED_TRANSACTION_MAX_COUNT, 2);

    // Build cluster.
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
            .setNumOfOzoneManagers(1)
            .setOMServiceId("omservice")
            .setNumDatanodes(3)
            .build();
    cluster.waitForClusterToBeReady();

    // Init ofs.
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, cluster.getOzoneManager().getOMServiceId());
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    fs = FileSystem.get(conf);
    client = OzoneClientFactory.getRpcClient("omservice", conf);
  }

  @AfterEach
  public void cleanNamespace() throws Exception {
    OzoneShell shell = new OzoneShell();

    if (fs.exists(new Path("/vol1"))) {
      String[] args1 = new String[]{"volume", "delete", "-r", "-y", "vol1"};
      int exitC = execute(shell, args1);
      assertEquals(0, exitC);
    }

    if (fs.exists(new Path("/vol2"))) {
      String[] args1 = new String[]{"volume", "delete", "-r", "-y", "vol2"};
      int exitC = execute(shell, args1);
      assertEquals(0, exitC);
    }

    cluster.getOzoneManager().prepareOzoneManager(120L, 5L);
    runDeletes();
    assertFileAndDirTablesEmpty();
  }

  private int execute(GenericCli shell, String[] args) {
    LOG.info("Executing shell command with args {}", Arrays.asList(args));
    CommandLine cmd = shell.getCmd();

    CommandLine.IExecutionExceptionHandler exceptionHandler =
            (ex, commandLine, parseResult) -> {
              new PrintStream(err, true, DEFAULT_ENCODING).println(ex.getMessage());
              return commandLine.getCommandSpec().exitCodeOnExecutionException();
            };

    // Since there is no elegant way to pass Ozone config to the shell,
    // the idea is to use 'set' to place those OM HA configs.
    String[] argsWithHAConf = getHASetConfStrings(args);

    cmd.setExecutionExceptionHandler(exceptionHandler);
    return cmd.execute(argsWithHAConf);
  }

  private String getSetConfStringFromConf(String key) {
    return String.format("--set=%s=%s", key, conf.get(key));
  }

  private String generateSetConfString(String key, String value) {
    return String.format("--set=%s=%s", key, value);
  }

  private String[] getHASetConfStrings(int numOfArgs) {
    assert (numOfArgs >= 0);
    String[] res = new String[1 + 1 + 1 + numOfArgs];
    final int indexOmServiceIds = 0;
    final int indexOmNodes = 1;
    final int indexOmAddressStart = 2;

    res[indexOmServiceIds] = getSetConfStringFromConf(
            OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY);

    String omNodesKey = ConfUtils.addKeySuffixes(
            OMConfigKeys.OZONE_OM_NODES_KEY, "omservice");
    String omNodesVal = conf.get(omNodesKey);
    res[indexOmNodes] = generateSetConfString(omNodesKey, omNodesVal);

    String[] omNodesArr = omNodesVal.split(",");
    // Sanity check
    assert (omNodesArr.length == 1);
    for (int i = 0; i < 1; i++) {
      res[indexOmAddressStart + i] =
              getSetConfStringFromConf(ConfUtils.addKeySuffixes(
                      OMConfigKeys.OZONE_OM_ADDRESS_KEY, "omservice", omNodesArr[i]));
    }

    return res;
  }

  /**
   * Helper function to create a new set of arguments that contains HA configs.
   * @param existingArgs Existing arguments to be fed into OzoneShell command.
   * @return String array.
   */
  private String[] getHASetConfStrings(String[] existingArgs) {
    // Get a String array populated with HA configs first
    String[] res = getHASetConfStrings(existingArgs.length);

    int indexCopyStart = res.length - existingArgs.length;
    // Then copy the existing args to the returned String array
    System.arraycopy(existingArgs, 0, res, indexCopyStart,
            existingArgs.length);
    return res;
  }

  @AfterAll
  public static void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @Test
  public void testConnectedTreeOneBucket() throws Exception {
    FSORepairTool.Report expectedReport = buildConnectedTree("vol1", "bucket1");

    // Test the connected tree in debug mode.
    tool = new FSORepairTool(getOmDB(),
        getOmDBLocation(), true, false, null, null);
    FSORepairTool.Report debugReport = tool.run();

    Assertions.assertEquals(expectedReport, debugReport);
    assertConnectedTreeReadable("vol1", "bucket1");
    assertDeleteTablesEmpty();

    // Running again in repair mode should give same results since the tree
    // is connected.
    tool = new FSORepairTool(getOmDB(),
        getOmDBLocation(), false, true, null, null);
    FSORepairTool.Report repairReport = tool.run();

    Assertions.assertEquals(expectedReport, repairReport);
    assertConnectedTreeReadable("vol1", "bucket1");
    assertDeleteTablesEmpty();
  }

  @Test
  public void testReportedDataSize() throws Exception {
    FSORepairTool.Report report1 = buildDisconnectedTree("vol1", "bucket1", 10);
    FSORepairTool.Report report2 = buildConnectedTree("vol1", "bucket2", 10);
    FSORepairTool.Report expectedReport = new FSORepairTool.Report(report1, report2);

    tool = new FSORepairTool(getOmDB(),
        getOmDBLocation(), false, true, null, null);
    FSORepairTool.Report debugReport = tool.run();
    Assertions.assertEquals(expectedReport, debugReport);
  }

  /**
   * Test to verify how the tool processes the volume and bucket
   * filters.
   */
  @Test
  public void testVolumeAndBucketFilter() throws Exception {
    FSORepairTool.Report report1 = buildDisconnectedTree("vol1", "bucket1", 10);
    FSORepairTool.Report report2 = buildConnectedTree("vol2", "bucket2", 10);
    FSORepairTool.Report expectedReport1 = new FSORepairTool.Report(report1);
    FSORepairTool.Report expectedReport2 = new FSORepairTool.Report(report2);

    // When volume filter is passed
    tool = new FSORepairTool(getOmDB(),
        getOmDBLocation(), false, true, "/vol1", null);
    FSORepairTool.Report result1 = tool.run();
    Assertions.assertEquals(expectedReport1, result1);

    // When both volume and bucket filters are passed
    tool = new FSORepairTool(getOmDB(),
        getOmDBLocation(), false, true, "/vol2", "bucket2");
    FSORepairTool.Report result2 = tool.run();
    Assertions.assertEquals(expectedReport2, result2);

    PrintStream originalOut = System.out;

    // When a non-existent bucket filter is passed
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
         PrintStream ps = new PrintStream(outputStream)) {
      System.setOut(ps);
      tool = new FSORepairTool(getOmDB(),
              getOmDBLocation(), false, true, "/vol1", "bucket2");
      tool.run();
      String output = outputStream.toString();
      Assertions.assertTrue(output.contains("Bucket 'bucket2' does not exist in volume '/vol1'."));
    } finally {
      System.setOut(originalOut);
    }

    // When a non-existent volume filter is passed
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
         PrintStream ps = new PrintStream(outputStream)) {
      System.setOut(ps);
      tool = new FSORepairTool(getOmDB(),
              getOmDBLocation(), false, true, "/vol3", "bucket2");
      tool.run();
      String output = outputStream.toString();
      Assertions.assertTrue(output.contains("Volume '/vol3' does not exist."));
    } finally {
      System.setOut(originalOut);
    }

    // When bucket filter is passed without the volume filter.
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
         PrintStream ps = new PrintStream(outputStream)) {
      System.setOut(ps);
      tool = new FSORepairTool(getOmDB(),
              getOmDBLocation(), false, true, null, "bucket2");
      tool.run();
      String output = outputStream.toString();
      Assertions.assertTrue(output.contains("--bucket flag cannot be used without specifying --volume."));
    } finally {
      System.setOut(originalOut);
    }

  }

  @Test
  public void testMultipleBucketsAndVolumes() throws Exception {
    Table<String, OmDirectoryInfo> dirTable =
            cluster.getOzoneManager().getMetadataManager().getDirectoryTable();
    Table<String, OmKeyInfo> keyTable =
            cluster.getOzoneManager().getMetadataManager().getKeyTable(getFSOBucketLayout());
    FSORepairTool.Report report1 = buildConnectedTree("vol1", "bucket1");
    FSORepairTool.Report report2 = buildDisconnectedTree("vol2", "bucket2");
    FSORepairTool.Report expectedAggregateReport = new FSORepairTool.Report(
        report1, report2);

    tool = new FSORepairTool(getOmDB(),
        getOmDBLocation(), false, true, null, null);
    FSORepairTool.Report generatedReport = tool.run();

    Assertions.assertEquals(generatedReport, expectedAggregateReport);
    assertConnectedTreeReadable("vol1", "bucket1");
    assertDisconnectedTreePartiallyReadable("vol2", "bucket2");

    // This assertion ensures that only specific directories and keys remain in the active tables,
    // as the remaining entries are expected to be moved to the deleted tables by the background service.
    // However, since the timing of the background deletion service is not predictable,
    // assertions on the deleted tables themselves may lead to flaky tests.
    assertEquals(4, countTableEntries(dirTable));
    assertEquals(5, countTableEntries(keyTable));
  }

  /**
   * Tests having multiple entries in the deleted file and directory tables
   * for the same objects.
   */
  @Test
  public void testDeleteOverwrite() throws Exception {
    Table<String, OmKeyInfo> keyTable =
            cluster.getOzoneManager().getMetadataManager().getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> dirTable =
            cluster.getOzoneManager().getMetadataManager().getDirectoryTable();

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

    tool = new FSORepairTool(getOmDB(),
        getOmDBLocation(), false, true, null, null);
    FSORepairTool.Report generatedReport = tool.run();

    Assertions.assertEquals(1, generatedReport.getUnreachableDirs());
    Assertions.assertEquals(3, generatedReport.getUnreachableFiles());

    // This assertion ensures that only specific directories and keys remain in the active tables,
    // as the remaining entries are expected to be moved to the deleted tables by the background service.
    // However, since the timing of the background deletion service is not predictable,
    // assertions on the deleted tables themselves may lead to flaky tests.
    assertEquals(1, countTableEntries(keyTable));
    assertEquals(1, countTableEntries(dirTable));
  }

  @Test
  public void testEmptyFileTrees() throws Exception {
    // Run when there are no file trees.
    tool = new FSORepairTool(getOmDB(),
        getOmDBLocation(), false, true, null, null);
    FSORepairTool.Report generatedReport = tool.run();
    Assertions.assertEquals(generatedReport, new FSORepairTool.Report());
    assertDeleteTablesEmpty();

    // Create an empty volume and bucket.
    fs.mkdirs(new Path("/vol1"));
    fs.mkdirs(new Path("/vol2/bucket1"));

    // Run on an empty volume and bucket.
    tool = new FSORepairTool(getOmDB(),
        getOmDBLocation(), false, true, null, null);
    generatedReport = tool.run();
    Assertions.assertEquals(generatedReport, new FSORepairTool.Report());
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
      FSORepairTool.Report connectReport =
          buildConnectedTree("vol1", "fso-bucket");

      // Even in repair mode there should be no action. legacy and obs buckets
      // will be skipped and FSO tree is connected.
      tool = new FSORepairTool(getOmDB(),
          getOmDBLocation(), false, true, null, null);
      FSORepairTool.Report generatedReport = tool.run();

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


  private FSORepairTool.Report buildConnectedTree(String volume, String bucket)
      throws Exception {
    return buildConnectedTree(volume, bucket, 0);
  }

  /**
   * Creates a tree with 3 reachable directories and 4 reachable files.
   */
  private FSORepairTool.Report buildConnectedTree(String volume, String bucket, int fileSize)
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

  private FSORepairTool.Report buildDisconnectedTree(String volume, String bucket)
      throws Exception {
    return buildDisconnectedTree(volume, bucket, 0);
  }

  /**
   * Creates a tree with 1 reachable directory, 1 reachable file, 1
   * unreachable directory, and 3 unreachable files.
   */
  private FSORepairTool.Report buildDisconnectedTree(String volume, String bucket,
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

  private void assertDeleteTablesEmpty() throws Exception {
    OzoneManager leader = cluster.getOMLeader();

    GenericTestUtils.waitFor(() -> {
      try {
        return leader.getMetadataManager().getDeletedDirTable().isEmpty();
      } catch (Exception e) {
        LOG.error("DB failure!", e);
        fail("DB failure!");
        return false;
      }
    }, 1000, 120000);
    GenericTestUtils.waitFor(() -> {
      try {
        return leader.getMetadataManager().getDeletedTable().isEmpty();
      } catch (Exception e) {
        LOG.error("DB failure!", e);
        fail("DB failure!");
        return false;
      }
    }, 1000, 120000);
  }

  private void assertFileAndDirTablesEmpty() throws Exception {
    OzoneManager leader = cluster.getOMLeader();
    GenericTestUtils.waitFor(() -> {
      try {
        return leader.getMetadataManager().getDirectoryTable().isEmpty();
      } catch (Exception e) {
        LOG.error("DB failure!", e);
        fail("DB failure!");
        return false;
      }
    }, 1000, 120000);
    GenericTestUtils.waitFor(() -> {
      try {
        return leader.getMetadataManager().getFileTable().isEmpty();
      } catch (Exception e) {
        LOG.error("DB failure!", e);
        fail("DB failure!");
        return false;
      }
    }, 1000, 120000);
  }

  private DBStore getOmDB() {
    return cluster.getOMLeader().getMetadataManager().getStore();
  }

  private String getOmDBLocation() {
    return cluster.getOMLeader().getMetadataManager().getStore().getDbLocation().toString();
  }

  private static BucketLayout getFSOBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
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

  private <K, V> int countTableEntries(Table<K, V> table) throws Exception {
    int count = 0;
    try (TableIterator<K, ? extends Table.KeyValue<K, V>> iterator = table.iterator()) {
      while (iterator.hasNext()) {
        iterator.next();
        count++;
      }
    }
    System.out.println("Total number of entries: " + count);
    return count;
  }
}
