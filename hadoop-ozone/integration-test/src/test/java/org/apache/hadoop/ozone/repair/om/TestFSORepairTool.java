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

package org.apache.hadoop.ozone.repair.om;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.ozone.test.IntLambda.withTextFromSystemIn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * FSORepairTool test cases.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestFSORepairTool {

  private static final Logger LOG = LoggerFactory.getLogger(TestFSORepairTool.class);
  private static final int ORDER_DRY_RUN = 1;
  //private static final int ORDER_REPAIR_SOME = 2; // TODO add test case
  private static final int ORDER_REPAIR_ALL = 3;
  private static final int ORDER_REPAIR_ALL_AGAIN = 4;
  private static final int ORDER_RESTART_OM = 5;

  private static MiniOzoneCluster cluster;
  private static FileSystem fs;
  private static OzoneClient client;
  private static CommandLine cmd;
  private static String dbPath;
  private static FSORepairTool.Report vol1Report;
  private static FSORepairTool.Report vol2Report;
  private static FSORepairTool.Report fullReport;
  private static FSORepairTool.Report emptyReport;
  private static FSORepairTool.Report unreachableReport;

  private static GenericTestUtils.PrintStreamCapturer out;
  private static GenericTestUtils.PrintStreamCapturer err;

  @BeforeAll
  public static void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();

    // Init ofs.
    final String rootPath = String.format("%s://%s/", OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    fs = FileSystem.get(conf);

    out = GenericTestUtils.captureOut();
    err = GenericTestUtils.captureErr();
    cmd = new OzoneRepair().getCmd();
    dbPath = new File(OMStorage.getOmDbDir(conf) + "/" + OM_DB_NAME).getPath();

    // Build multiple connected and disconnected trees
    FSORepairTool.Report report1 = buildConnectedTree("vol1", "bucket1", 10);
    FSORepairTool.Report report2 = buildDisconnectedTree("vol2", "bucket1", 10);
    FSORepairTool.Report report3 = buildConnectedTree("vol2", "bucket2", 10);
    FSORepairTool.Report report4 = buildEmptyTree();
    FSORepairTool.Report report5 = buildTreeWithUnreachableObjects("vol-unreachable", "bucket-unreachable", 5);

    vol1Report = new FSORepairTool.Report(report1);
    vol2Report = new FSORepairTool.Report(report2, report3);
    fullReport = new FSORepairTool.Report(report1, report2, report3, report4, report5);
    emptyReport = new FSORepairTool.Report(report4);
    unreachableReport = new FSORepairTool.Report(report5);

    client = OzoneClientFactory.getRpcClient(conf);
    ObjectStore store = client.getObjectStore();

    // Create legacy and OBS buckets.
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

    // Stop the OM before running the tool
    cluster.getOzoneManager().stop();
  }

  @BeforeEach
  public void init() throws Exception {
    out.reset();
    err.reset();
  }

  @AfterAll
  public static void reset() throws IOException {
    IOUtils.closeQuietly(fs, client, cluster, out, err);
  }

  /**
   * Test to verify that if parent is in deletedDirectoryTable then its
   * children should be marked unreachable, not unreferenced.
   */
  @Order(ORDER_DRY_RUN)
  @Test
  public void testUnreachableObjectsWithParentInDeletedTable() {
    String expectedOutput = serializeReport(unreachableReport);

    int exitCode = dryRun("-v", "/vol-unreachable", "-b", "bucket-unreachable");
    assertEquals(0, exitCode);

    String cliOutput = out.getOutput();
    String reportOutput = extractRelevantSection(cliOutput);

    assertEquals(expectedOutput, reportOutput);
  }

  /**
   * Test to check a connected tree with one bucket.
   * The output remains the same in debug and repair mode as the tree is connected.
   */
  @Order(ORDER_DRY_RUN)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testConnectedTreeOneBucket(boolean dryRun) {
    String expectedOutput = serializeReport(vol1Report);

    int exitCode = execute(dbPath, dryRun, "-v", "/vol1", "-b", "bucket1");
    assertEquals(0, exitCode, err.getOutput());

    String cliOutput = out.getOutput();
    String reportOutput = extractRelevantSection(cliOutput);
    assertEquals(expectedOutput, reportOutput);
  }

  /**
   * Test to verify the file size of the tree.
   */
  @Order(ORDER_DRY_RUN)
  @Test
  public void testReportedDataSize() {
    String expectedOutput = serializeReport(vol2Report);

    int exitCode = dryRun("-v", "/vol2");
    assertEquals(0, exitCode);

    String cliOutput = out.getOutput();
    String reportOutput = extractRelevantSection(cliOutput);

    assertEquals(expectedOutput, reportOutput);
  }

  /**
   * Test to verify how the tool processes the volume and bucket filters.
   * - Volume filter only.
   * - Both volume and bucket filters.
   * - Non-existent bucket.
   * - Non-existent volume.
   * - Using a bucket filter without specifying a volume.
   */
  @Order(ORDER_DRY_RUN)
  @Test
  public void testVolumeFilter() {
    String expectedOutput1 = serializeReport(vol1Report);

    // When volume filter is passed
    int exitCode1 = dryRun("--volume", "/vol1");
    assertEquals(0, exitCode1);

    String cliOutput1 = out.getOutput();
    String reportOutput1 = extractRelevantSection(cliOutput1);
    assertEquals(expectedOutput1, reportOutput1);
  }

  @Order(ORDER_DRY_RUN)
  @Test
  public void testVolumeAndBucketFilter() {
    String expectedOutput2 = serializeReport(vol1Report);

    // When both volume and bucket filters are passed
    int exitCode2 = dryRun("--volume", "/vol1", "--bucket", "bucket1");
    assertEquals(0, exitCode2);

    String cliOutput2 = out.getOutput();
    String reportOutput2 = extractRelevantSection(cliOutput2);
    assertEquals(expectedOutput2, reportOutput2);
  }

  @Order(ORDER_DRY_RUN)
  @Test
  public void testNonExistentBucket() {
    // When a non-existent bucket filter is passed
    int exitCode = dryRun("--volume", "/vol1", "--bucket", "bucket3");
    assertEquals(0, exitCode);
    String cliOutput = err.getOutput();
    assertThat(cliOutput).contains("Bucket 'bucket3' does not exist in volume '/vol1'.");
  }

  @Order(ORDER_DRY_RUN)
  @Test
  public void testNonExistentVolume() {
    // When a non-existent volume filter is passed
    int exitCode = dryRun("--volume", "/vol5");
    assertEquals(0, exitCode);
    String cliOutput = err.getOutput();
    assertThat(cliOutput).contains("Volume '/vol5' does not exist.");
  }

  @Order(ORDER_DRY_RUN)
  @Test
  public void testBucketFilterWithoutVolume() {
    // When bucket filter is passed without the volume filter.
    int exitCode = dryRun("--bucket", "bucket1");
    assertEquals(0, exitCode);
    String cliOutput = err.getOutput();
    assertThat(cliOutput).contains("--bucket flag cannot be used without specifying --volume.");
  }

  /**
   * Test to verify that non-fso buckets, such as legacy and obs, are skipped during the process.
   */
  @Order(ORDER_DRY_RUN)
  @Test
  public void testNonFSOBucketsSkipped() {
    int exitCode = dryRun();
    assertEquals(0, exitCode);

    String cliOutput = out.getOutput();
    assertThat(cliOutput).contains("Skipping non-FSO bucket /vol1/obs-bucket");
    assertThat(cliOutput).contains("Skipping non-FSO bucket /vol1/legacy-bucket");
  }

  /**
   * If no file is present inside a vol/bucket, the report statistics should be zero.
   */
  @Order(ORDER_DRY_RUN)
  @Test
  public void testEmptyFileTrees() {
    String expectedOutput = serializeReport(emptyReport);

    // Run on an empty volume and bucket
    int exitCode = dryRun("-v", "/vol-empty", "-b", "bucket-empty");
    assertEquals(0, exitCode);

    String cliOutput = out.getOutput();
    String reportOutput = extractRelevantSection(cliOutput);
    assertEquals(expectedOutput, reportOutput);
  }

  @Order(ORDER_DRY_RUN)
  @Test
  public void testAlternateOmDbDirName(@TempDir java.nio.file.Path tempDir) throws Exception {
    File original = new File(dbPath);
    File backup = tempDir.resolve("om-db-backup").toFile();

    FileUtils.copyDirectory(original, backup);

    out.reset();
    String expectedOutput = serializeReport(fullReport);
    int exitCode = execute(backup.getPath(), true);

    assertEquals(0, exitCode, err.getOutput());

    String cliOutput = out.getOutput();
    String reportOutput = extractRelevantSection(cliOutput);
    assertEquals(expectedOutput, reportOutput);
  }

  /**
   * Test in repair mode. This test ensures that:
   * - The initial repair correctly resolves unreferenced objects.
   * - Subsequent repair runs do not find any unreferenced objects to process.
   */
  @Order(ORDER_REPAIR_ALL)
  @Test
  public void testMultipleBucketsAndVolumes() {
    String expectedOutput = serializeReport(fullReport);

    int exitCode = repair();
    assertEquals(0, exitCode);

    String cliOutput = out.getOutput();
    String reportOutput = extractRelevantSection(cliOutput);
    assertEquals(expectedOutput, reportOutput);
    assertThat(cliOutput).contains("Unreferenced (Orphaned):\n\tDirectories: 1\n\tFiles: 3\n\tBytes: 30");
  }

  @Order(ORDER_REPAIR_ALL_AGAIN)
  @Test
  public void repairAllAgain() {
    int exitCode = repair();
    assertEquals(0, exitCode);
    String cliOutput = out.getOutput();
    assertThat(cliOutput).contains("Unreferenced (Orphaned):\n\tDirectories: 0\n\tFiles: 0\n\tBytes: 0");
  }

  /**
   * Validate cluster state after OM restart by checking the tables.
   */
  @Order(ORDER_RESTART_OM)
  @Test
  public void validateClusterAfterRestart() throws Exception {
    cluster.getOzoneManager().restart();

    // 5 volumes (/s3v, /vol1, /vol2, /vol-empty, /vol-unreachable)
    assertEquals(5, countTableEntries(cluster.getOzoneManager().getMetadataManager().getVolumeTable()));
    // 7 buckets (vol1/bucket1, vol2/bucket1, vol2/bucket2, vol-empty/bucket-empty, vol/legacy-bucket, vol1/obs-bucket,
    // /vol-unreachable/bucket-unreachable)
    assertEquals(7, countTableEntries(cluster.getOzoneManager().getMetadataManager().getBucketTable()));
    // 1 directory is unreferenced and moved to the deletedDirTable during repair mode
    // 1 is moved to deletedDirTable for testing
    assertEquals(2, countTableEntries(cluster.getOzoneManager().getMetadataManager().getDeletedDirTable()));
    // 3 files are unreferenced and moved to the deletedTable during repair mode.
    assertEquals(3, countTableEntries(cluster.getOzoneManager().getMetadataManager().getDeletedTable()));
  }

  private int repair(String... args) {
    return execute(dbPath, false, args);
  }

  private int dryRun(String... args) {
    return execute(dbPath, true, args);
  }

  private int execute(String effectiveDbPath, boolean dryRun, String... args) {
    List<String> argList = new ArrayList<>(Arrays.asList("om", "fso-tree", "--db", effectiveDbPath));
    if (dryRun) {
      argList.add("--dry-run");
    }
    argList.addAll(Arrays.asList(args));

    return withTextFromSystemIn("y")
        .execute(() -> cmd.execute(argList.toArray(new String[0])));
  }

  private <K, V> int countTableEntries(Table<K, V> table) throws Exception {
    int count = 0;
    try (Table.KeyValueIterator<K, V> iterator = table.iterator()) {
      while (iterator.hasNext()) {
        iterator.next();
        count++;
      }
    }
    return count;
  }

  private String extractRelevantSection(String cliOutput) {
    int startIndex = cliOutput.indexOf("Reachable:");
    if (startIndex == -1) {
      throw new AssertionError("Output does not contain 'Reachable' section.");
    }
    return cliOutput.substring(startIndex).trim();
  }

  private String serializeReport(FSORepairTool.Report report) {
    return String.format(
        "Reachable:%n\tDirectories: %d%n\tFiles: %d%n\tBytes: %d%n" +
        "Unreachable (Pending to delete):%n\tDirectories: %d%n\tFiles: %d%n\tBytes: %d%n" +
        "Unreferenced (Orphaned):%n\tDirectories: %d%n\tFiles: %d%n\tBytes: %d",
        report.getReachable().getDirs(),
        report.getReachable().getFiles(),
        report.getReachable().getBytes(),
        report.getPendingToDelete().getDirs(),
        report.getPendingToDelete().getFiles(),
        report.getPendingToDelete().getBytes(),
        report.getOrphaned().getDirs(),
        report.getOrphaned().getFiles(),
        report.getOrphaned().getBytes()
    );
  }

  /**
   * Creates a tree with 3 reachable directories and 4 reachable files.
   */
  private static FSORepairTool.Report buildConnectedTree(String volume, String bucket, int fileSize) throws Exception {
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

    FSORepairTool.ReportStatistics reachableCount =
            new FSORepairTool.ReportStatistics(3, 4, fileSize * 4L);
    return new FSORepairTool.Report.Builder()
        .setReachable(reachableCount)
        .build();
  }

  private static FSORepairTool.Report buildEmptyTree() throws IOException {
    fs.mkdirs(new Path("/vol-empty/bucket-empty"));
    FSORepairTool.ReportStatistics reachableCount =
            new FSORepairTool.ReportStatistics(0, 0, 0);
    FSORepairTool.ReportStatistics pendingToDeleteCount =
            new FSORepairTool.ReportStatistics(0, 0, 0);
    FSORepairTool.ReportStatistics orphanedCount =
            new FSORepairTool.ReportStatistics(0, 0, 0);
    return new FSORepairTool.Report.Builder()
        .setReachable(reachableCount)
        .setPendingToDelete(pendingToDeleteCount)
        .setOrphaned(orphanedCount)
        .build();
  }

  private static FSORepairTool.Report buildTreeWithUnreachableObjects(String volume, String bucket, int fileSize)
      throws Exception {
    Path bucketPath = new Path("/" + volume + "/" + bucket);
    // Create a parent directory that will be moved to deleted directory table
    Path parentToDelete = new Path(bucketPath, "parentToDelete");
    Path childDir = new Path(parentToDelete, "childDir");
    Path file1 = new Path(parentToDelete, "file1.txt");
    Path file2 = new Path(childDir, "file2.txt");
    
    Path reachableDir = new Path(bucketPath, "reachableDir");
    Path reachableFile = new Path(reachableDir, "reachableFile.txt");
    
    fs.mkdirs(childDir);
    fs.mkdirs(reachableDir);

    // Content to put in every file.
    String data = new String(new char[fileSize]);

    FSDataOutputStream stream = fs.create(file1);
    stream.write(data.getBytes(StandardCharsets.UTF_8));
    stream.close();
    stream = fs.create(file2);
    stream.write(data.getBytes(StandardCharsets.UTF_8));
    stream.close();
    stream = fs.create(reachableFile);
    stream.write(data.getBytes(StandardCharsets.UTF_8));
    stream.close();
    
    // Simulate parent deletion by moving parentToDelete to deleted directory table
    // This makes childDir, file1 and file2 unreachable
    moveDirectoryToDeletedTable(volume, bucket, "parentToDelete");

    FSORepairTool.ReportStatistics reachableCount = 
        new FSORepairTool.ReportStatistics(1, 1, fileSize);
    FSORepairTool.ReportStatistics pendingToDeleteCount =
        new FSORepairTool.ReportStatistics(1, 2, fileSize * 2L);
    FSORepairTool.ReportStatistics orphanedCount =
        new FSORepairTool.ReportStatistics(0, 0, 0);
    return new FSORepairTool.Report.Builder()
        .setReachable(reachableCount)
        .setPendingToDelete(pendingToDeleteCount)
        .setOrphaned(orphanedCount)
        .build();
  }

  /**
   * Move a directory from directory table to deleted directory table.
   * This is used to verify unreachable objects.
   */
  private static void moveDirectoryToDeletedTable(String volumeName, String bucketName, String dirName) 
      throws Exception {
    Table<String, OmDirectoryInfo> dirTable = cluster.getOzoneManager().getMetadataManager().getDirectoryTable();
    Table<String, OmKeyInfo> deletedDirTable = cluster.getOzoneManager().getMetadataManager().getDeletedDirTable();
    
    try (Table.KeyValueIterator<String, OmDirectoryInfo> iterator = dirTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> entry = iterator.next();
        String key = entry.getKey();
        OmDirectoryInfo dirInfo = entry.getValue();
        
        // Find the directory by name, remove from directory table and add to deleted directory table
        if (key.contains(dirName) && dirInfo.getName().equals(dirName)) {
          dirTable.delete(key);
          String deleteDirKeyName = key + OM_KEY_PREFIX + dirInfo.getObjectID();
          // Convert directory to OmKeyInfo for the deleted table
          OmKeyInfo dirAsKeyInfo = OMFileRequest.getOmKeyInfo(volumeName, bucketName, dirInfo, dirInfo.getName());
          deletedDirTable.put(deleteDirKeyName, dirAsKeyInfo);
          break;
        }
      }
    }
  }

  private static void assertConnectedTreeReadable(String volume, String bucket) throws IOException {
    Path bucketPath = new Path("/" + volume + "/" + bucket);
    Path dir1 = new Path(bucketPath, "dir1");
    Path file1 = new Path(dir1, "file1");
    Path file2 = new Path(dir1, "file2");

    Path dir2 = new Path(bucketPath, "dir1/dir2");
    Path file3 = new Path(dir2, "file3");

    Path dir3 = new Path(bucketPath, "dir3");
    Path file4 = new Path(bucketPath, "file4");

    assertTrue(fs.exists(dir1));
    assertTrue(fs.exists(dir2));
    assertTrue(fs.exists(dir3));
    assertTrue(fs.exists(file1));
    assertTrue(fs.exists(file2));
    assertTrue(fs.exists(file3));
    assertTrue(fs.exists(file4));
  }

  /**
   * Creates a tree with 1 reachable directory, 1 reachable file, 1
   * unreferenced directory, and 3 unreferenced files.
   */
  private static FSORepairTool.Report buildDisconnectedTree(String volume, String bucket, int fileSize)
        throws Exception {
    buildConnectedTree(volume, bucket, fileSize);

    // Manually remove dir1. This should disconnect 3 of the files and 1 of
    // the directories.
    disconnectDirectory("dir1");

    assertDisconnectedTreePartiallyReadable(volume, bucket);

    // dir1 does not count towards the orphaned directories the tool
    // will see. It was deleted completely so the tool will never see it.
    FSORepairTool.ReportStatistics reachableCount =
            new FSORepairTool.ReportStatistics(1, 1, fileSize);
    FSORepairTool.ReportStatistics orphanedCount =
            new FSORepairTool.ReportStatistics(1, 3, fileSize * 3L);
    return new FSORepairTool.Report.Builder()
        .setReachable(reachableCount)
        .setOrphaned(orphanedCount)
        .build();
  }

  private static void disconnectDirectory(String dirName) throws Exception {
    Table<String, OmDirectoryInfo> dirTable = cluster.getOzoneManager().getMetadataManager().getDirectoryTable();
    try (Table.KeyValueIterator<String, OmDirectoryInfo> iterator = dirTable.iterator()) {
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

  private static void assertDisconnectedTreePartiallyReadable(String volume, String bucket) throws Exception {
    Path bucketPath = new Path("/" + volume + "/" + bucket);
    Path dir1 = new Path(bucketPath, "dir1");
    Path file1 = new Path(dir1, "file1");
    Path file2 = new Path(dir1, "file2");

    Path dir2 = new Path(bucketPath, "dir1/dir2");
    Path file3 = new Path(dir2, "file3");

    Path dir3 = new Path(bucketPath, "dir3");
    Path file4 = new Path(bucketPath, "file4");

    assertFalse(fs.exists(dir1));
    assertFalse(fs.exists(dir2));
    assertTrue(fs.exists(dir3));
    assertFalse(fs.exists(file1));
    assertFalse(fs.exists(file2));
    assertFalse(fs.exists(file3));
    assertTrue(fs.exists(file4));
  }
}
