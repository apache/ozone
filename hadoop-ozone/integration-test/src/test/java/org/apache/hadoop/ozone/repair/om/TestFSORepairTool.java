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
package org.apache.hadoop.ozone.repair.om;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * FSORepairTool test cases.
 */
public class TestFSORepairTool {
  public static final Logger LOG = LoggerFactory.getLogger(TestFSORepairTool.class);
  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;
  private static final String DEFAULT_ENCODING = UTF_8.name();
  private MiniOzoneCluster cluster;
  private FileSystem fs;
  private OzoneClient client;
  private OzoneConfiguration conf = null;

  @BeforeEach
  public void init() throws Exception {
    // Set configs.
    conf = new OzoneConfiguration();

    // Build cluster.
    cluster =  MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();

    // Init ofs.
    final String rootPath = String.format("%s://%s/", OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    fs = FileSystem.get(conf);
    client = OzoneClientFactory.getRpcClient(conf);

    System.setOut(new PrintStream(out, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(err, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void reset() throws IOException {
    // reset stream after each unit test
    out.reset();
    err.reset();

    // restore system streams
    System.setOut(OLD_OUT);
    System.setErr(OLD_ERR);

    if (cluster != null) {
      cluster.shutdown();
    }
    if (client != null) {
      client.close();
    }
    IOUtils.closeQuietly(fs);
  }

  @Test
  public void testConnectedTreeOneBucket() throws Exception {
    CommandLine cmd = new CommandLine(new OzoneRepair()).addSubcommand(new CommandLine(new OMRepair())
            .addSubcommand(new FSORepairCLI()));
    String dbPath = new File(OMStorage.getOmDbDir(conf) + "/" + OM_DB_NAME).getPath();

    FSORepairTool.Report expectedReport = buildConnectedTree("vol1", "bucket1");
    String expectedOutput = serializeReport(expectedReport);

    // Test the connected tree in debug mode.
    cluster.getOzoneManager().stop();

    String[] args = new String[] {"om", "fso-tree", "--db", dbPath};
    int exitCode = cmd.execute(args);
    assertEquals(0, exitCode);

    String cliOutput = out.toString(DEFAULT_ENCODING);
    String reportOutput = extractRelevantSection(cliOutput);
    Assertions.assertEquals(expectedOutput, reportOutput);

    out.reset();
    err.reset();

    // Running again in repair mode should give same results since the tree is connected.
    String[] args1 = new String[] {"om", "fso-tree", "--db", dbPath, "--repair"};
    int exitCode1 = cmd.execute(args1);
    assertEquals(0, exitCode1);

    String cliOutput1 = out.toString(DEFAULT_ENCODING);
    String reportOutput1 = extractRelevantSection(cliOutput1);
    Assertions.assertEquals(expectedOutput, reportOutput1);

    cluster.getOzoneManager().restart();
  }

  @Test
  public void testReportedDataSize() throws Exception {
    CommandLine cmd = new CommandLine(new OzoneRepair()).addSubcommand(new CommandLine(new OMRepair())
            .addSubcommand(new FSORepairCLI()));
    String dbPath = new File(OMStorage.getOmDbDir(conf) + "/" + OM_DB_NAME).getPath();

    FSORepairTool.Report report1 = buildDisconnectedTree("vol1", "bucket1", 10);
    FSORepairTool.Report report2 = buildConnectedTree("vol1", "bucket2", 10);
    FSORepairTool.Report expectedReport = new FSORepairTool.Report(report1, report2);
    String expectedOutput = serializeReport(expectedReport);

    cluster.getOzoneManager().stop();

    String[] args = new String[] {"om", "fso-tree", "--db", dbPath, "--repair"};
    int exitCode = cmd.execute(args);
    assertEquals(0, exitCode);

    String cliOutput = out.toString(DEFAULT_ENCODING);
    String reportOutput = extractRelevantSection(cliOutput);

    Assertions.assertEquals(expectedOutput, reportOutput);
    cluster.getOzoneManager().restart();
  }

  /**
   * Test to verify how the tool processes the volume and bucket
   * filters.
   */
  @Test
  public void testVolumeAndBucketFilter() throws Exception {
    CommandLine cmd = new CommandLine(new OzoneRepair()).addSubcommand(new CommandLine(new OMRepair())
            .addSubcommand(new FSORepairCLI()));
    String dbPath = new File(OMStorage.getOmDbDir(conf) + "/" + OM_DB_NAME).getPath();

    FSORepairTool.Report report1 = buildDisconnectedTree("vol1", "bucket1", 10);
    FSORepairTool.Report report2 = buildConnectedTree("vol2", "bucket2", 10);
    FSORepairTool.Report expectedReport1 = new FSORepairTool.Report(report1);
    FSORepairTool.Report expectedReport2 = new FSORepairTool.Report(report2);

    cluster.getOzoneManager().stop();

    // When volume filter is passed
    String[] args1 = new String[]{"om", "fso-tree", "--db", dbPath, "--volume", "/vol1"};
    int exitCode1 = cmd.execute(args1);
    assertEquals(0, exitCode1);

    String cliOutput1 = out.toString(DEFAULT_ENCODING);
    String reportOutput1 = extractRelevantSection(cliOutput1);
    String expectedOutput1 = serializeReport(expectedReport1);
    Assertions.assertEquals(expectedOutput1, reportOutput1);

    out.reset();
    err.reset();

    // When both volume and bucket filters are passed
    String[] args2 = new String[]{"om", "fso-tree", "--db", dbPath, "--volume", "/vol2",
        "--bucket", "bucket2"};
    int exitCode2 = cmd.execute(args2);
    assertEquals(0, exitCode2);

    String cliOutput2 = out.toString(DEFAULT_ENCODING);
    String reportOutput2 = extractRelevantSection(cliOutput2);
    String expectedOutput2 = serializeReport(expectedReport2);
    Assertions.assertEquals(expectedOutput2, reportOutput2);

    out.reset();
    err.reset();

    // When a non-existent bucket filter is passed
    String[] args3 = new String[]{"om", "fso-tree", "--db", dbPath, "--volume", "/vol1",
        "--bucket", "bucket2"};
    int exitCode3 = cmd.execute(args3);
    assertEquals(0, exitCode3);
    String cliOutput3 = out.toString(DEFAULT_ENCODING);
    Assertions.assertTrue(cliOutput3.contains("Bucket 'bucket2' does not exist in volume '/vol1'."));

    out.reset();
    err.reset();

    // When a non-existent volume filter is passed
    String[] args4 = new String[]{"om", "fso-tree", "--db", dbPath, "--volume", "/vol3"};
    int exitCode4 = cmd.execute(args4);
    assertEquals(0, exitCode4);
    String cliOutput4 = out.toString(DEFAULT_ENCODING);
    Assertions.assertTrue(cliOutput4.contains("Volume '/vol3' does not exist."));

    out.reset();
    err.reset();

    // When bucket filter is passed without the volume filter.
    String[] args5 = new String[]{"om", "fso-tree", "--db", dbPath, "--bucket", "bucket1"};
    int exitCode5 = cmd.execute(args5);
    assertEquals(0, exitCode5);
    String cliOutput5 = out.toString(DEFAULT_ENCODING);
    Assertions.assertTrue(cliOutput5.contains("--bucket flag cannot be used without specifying --volume."));

    cluster.getOzoneManager().restart();
  }

  @Test
  public void testMultipleBucketsAndVolumes() throws Exception {
    CommandLine cmd = new CommandLine(new OzoneRepair()).addSubcommand(new CommandLine(new OMRepair())
            .addSubcommand(new FSORepairCLI()));
    String dbPath = new File(OMStorage.getOmDbDir(conf) + "/" + OM_DB_NAME).getPath();

    FSORepairTool.Report report1 = buildConnectedTree("vol1", "bucket1");
    FSORepairTool.Report report2 = buildDisconnectedTree("vol2", "bucket2");
    FSORepairTool.Report expectedAggregateReport = new FSORepairTool.Report(report1, report2);
    String expectedOutput = serializeReport(expectedAggregateReport);

    cluster.getOzoneManager().stop();

    String[] args = new String[] {"om", "fso-tree", "--db", dbPath, "--repair"};
    int exitCode = cmd.execute(args);
    assertEquals(0, exitCode);

    String cliOutput = out.toString(DEFAULT_ENCODING);
    String reportOutput = extractRelevantSection(cliOutput);
    Assertions.assertEquals(expectedOutput, reportOutput);

    cluster.getOzoneManager().restart();
  }

  /**
   * Tests having multiple entries in the deleted file and directory tables
   * for the same objects.
   */
  @Test
  public void testDeleteOverwrite() throws Exception {
    CommandLine cmd = new CommandLine(new OzoneRepair()).addSubcommand(new CommandLine(new OMRepair())
            .addSubcommand(new FSORepairCLI()));
    String dbPath = new File(OMStorage.getOmDbDir(conf) + "/" + OM_DB_NAME).getPath();

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

    cluster.getOzoneManager().stop();

    String[] args = new String[]{"om", "fso-tree", "--db", dbPath, "--repair"};
    int exitCode = cmd.execute(args);
    assertEquals(0, exitCode);

    String cliOutput = out.toString(DEFAULT_ENCODING);
    Assertions.assertTrue(cliOutput.contains("Unreferenced:\n\tDirectories: 1\n\tFiles: 3"));

    cluster.getOzoneManager().restart();
  }

  @Test
  public void testEmptyFileTrees() throws Exception {
    CommandLine cmd = new CommandLine(new OzoneRepair()).addSubcommand(new CommandLine(new OMRepair())
            .addSubcommand(new FSORepairCLI()));
    String dbPath = new File(OMStorage.getOmDbDir(conf) + "/" + OM_DB_NAME).getPath();

    FSORepairTool.Report emptyReport = buildEmptyTree();
    String expectedOutput = serializeReport(emptyReport);

    cluster.getOzoneManager().stop();

    // Run when there are no file trees.
    String[] args = new String[] {"om", "fso-tree", "--db", dbPath, "--repair"};
    int exitCode = cmd.execute(args);
    assertEquals(0, exitCode);

    String cliOutput = out.toString(DEFAULT_ENCODING);
    String reportOutput = extractRelevantSection(cliOutput);
    Assertions.assertEquals(expectedOutput, reportOutput);

    out.reset();
    err.reset();
    cluster.getOzoneManager().restart();

    // Create an empty volume and bucket.
    fs.mkdirs(new Path("/vol1"));
    fs.mkdirs(new Path("/vol2/bucket1"));

    cluster.getOzoneManager().stop();

    // Run on an empty volume and bucket.
    String[] args1 = new String[] {"om", "fso-tree", "--db", dbPath, "--repair"};
    int exitCode1 = cmd.execute(args1);
    assertEquals(0, exitCode1);

    String cliOutput2 = out.toString(DEFAULT_ENCODING);
    String reportOutput2 = extractRelevantSection(cliOutput2);
    Assertions.assertEquals(expectedOutput, reportOutput2);

    cluster.getOzoneManager().restart();
  }

  @Test
  public void testNonFSOBucketsSkipped() throws Exception {
    ObjectStore store = client.getObjectStore();

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

    CommandLine cmd = new CommandLine(new OzoneRepair()).addSubcommand(new CommandLine(new OMRepair())
            .addSubcommand(new FSORepairCLI()));
    String dbPath = new File(OMStorage.getOmDbDir(conf) + "/" + OM_DB_NAME).getPath();

    // Add an FSO bucket with data.
    FSORepairTool.Report connectReport = buildConnectedTree("vol1", "fso-bucket");

    cluster.getOzoneManager().stop();

    // Even in repair mode there should be no action. legacy and obs buckets
    // will be skipped and FSO tree is connected.
    String[] args = new String[] {"om", "fso-tree", "--db", dbPath, "--repair"};
    int exitCode = cmd.execute(args);
    assertEquals(0, exitCode);

    String cliOutput = out.toString(DEFAULT_ENCODING);
    String reportOutput = extractRelevantSection(cliOutput);
    String expectedOutput = serializeReport(connectReport);

    Assertions.assertEquals(expectedOutput, reportOutput);
    Assertions.assertTrue(cliOutput.contains("Skipping non-FSO bucket /vol1/obs-bucket"));
    Assertions.assertTrue(cliOutput.contains("Skipping non-FSO bucket /vol1/legacy-bucket"));

    cluster.getOzoneManager().restart();
  }

  private FSORepairTool.Report buildConnectedTree(String volume, String bucket) throws Exception {
    return buildConnectedTree(volume, bucket, 0);
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
        "Unreachable:%n\tDirectories: %d%n\tFiles: %d%n\tBytes: %d%n" +
        "Unreferenced:%n\tDirectories: %d%n\tFiles: %d%n\tBytes: %d",
        report.getReachable().getDirs(),
        report.getReachable().getFiles(),
        report.getReachable().getBytes(),
        report.getUnreachable().getDirs(),
        report.getUnreachable().getFiles(),
        report.getUnreachable().getBytes(),
        report.getUnreferenced().getDirs(),
        report.getUnreferenced().getFiles(),
        report.getUnreferenced().getBytes()
    );
  }

  /**
   * Creates a tree with 3 reachable directories and 4 reachable files.
   */
  private FSORepairTool.Report buildConnectedTree(String volume, String bucket, int fileSize) throws Exception {
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

  private FSORepairTool.Report buildEmptyTree() {
    FSORepairTool.ReportStatistics reachableCount =
            new FSORepairTool.ReportStatistics(0, 0, 0);
    FSORepairTool.ReportStatistics unreachableCount =
            new FSORepairTool.ReportStatistics(0, 0, 0);
    FSORepairTool.ReportStatistics unreferencedCount =
            new FSORepairTool.ReportStatistics(0, 0, 0);
    return new FSORepairTool.Report.Builder()
        .setReachable(reachableCount)
        .setUnreachable(unreachableCount)
        .setUnreferenced(unreferencedCount)
        .build();
  }

  private void assertConnectedTreeReadable(String volume, String bucket) throws IOException {
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

  private FSORepairTool.Report buildDisconnectedTree(String volume, String bucket) throws Exception {
    return buildDisconnectedTree(volume, bucket, 0);
  }

  /**
   * Creates a tree with 1 reachable directory, 1 reachable file, 1
   * unreachable directory, and 3 unreachable files.
   */
  private FSORepairTool.Report buildDisconnectedTree(String volume, String bucket, int fileSize) throws Exception {
    buildConnectedTree(volume, bucket, fileSize);

    // Manually remove dir1. This should disconnect 3 of the files and 1 of
    // the directories.
    disconnectDirectory("dir1");

    assertDisconnectedTreePartiallyReadable(volume, bucket);

    // dir1 does not count towards the unreachable directories the tool
    // will see. It was deleted completely so the tool will never see it.
    FSORepairTool.ReportStatistics reachableCount =
            new FSORepairTool.ReportStatistics(1, 1, fileSize);
    FSORepairTool.ReportStatistics unreferencedCount =
            new FSORepairTool.ReportStatistics(1, 3, fileSize * 3L);
    return new FSORepairTool.Report.Builder()
        .setReachable(reachableCount)
        .setUnreferenced(unreferencedCount)
        .build();
  }

  private void disconnectDirectory(String dirName) throws Exception {
    Table<String, OmDirectoryInfo> dirTable = cluster.getOzoneManager().getMetadataManager().getDirectoryTable();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>> iterator = dirTable.iterator()) {
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

  private void assertDisconnectedTreePartiallyReadable(String volume, String bucket) throws Exception {
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
}
