/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.fs.ozone;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.stream.Stream;

import com.google.common.base.Strings;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;

/**
 * Test client-side CRUD snapshot operations with Ozone Manager.
 * Setting a timeout for every test method to 300 seconds.
 */
@Timeout(value = 300)
public class TestOzoneFsSnapshot {

  private static MiniOzoneCluster cluster;
  private static final String OM_SERVICE_ID = "om-service-test1";
  private static OzoneManager ozoneManager;
  private static OzoneFsShell shell;
  private static final String VOLUME =
      "vol-" + RandomStringUtils.randomNumeric(5);;
  private static final String BUCKET =
      "buck-" + RandomStringUtils.randomNumeric(5);
  private static final String KEY =
      "key-" + RandomStringUtils.randomNumeric(5);
  private static final String BUCKET_PATH =
      OM_KEY_PREFIX + VOLUME + OM_KEY_PREFIX + BUCKET;
  private static final String BUCKET_WITH_SNAPSHOT_INDICATOR_PATH =
      BUCKET_PATH + OM_KEY_PREFIX + OM_SNAPSHOT_INDICATOR;
  private static final String KEY_PATH =
      BUCKET_PATH + OM_KEY_PREFIX + KEY;

  @BeforeAll
  public static void initClass() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    // Start the cluster
    cluster = MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(1)
        .build();
    cluster.waitForClusterToBeReady();
    ozoneManager = cluster.getOzoneManager();

    String hostPrefix = OZONE_OFS_URI_SCHEME + "://" + OM_SERVICE_ID;
    OzoneConfiguration clientConf =
        new OzoneConfiguration(cluster.getConf());
    clientConf.set(FS_DEFAULT_NAME_KEY, hostPrefix);

    shell = new OzoneFsShell(clientConf);

    createVolBuckKey();
  }

  @AfterAll
  public static void shutdown() throws IOException {
    shell.close();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static void createVolBuckKey()
      throws Exception {
    // Create volume and bucket
    int res = ToolRunner.run(shell,
        new String[]{"-mkdir", "-p", BUCKET_PATH});
    Assertions.assertEquals(0, res);
    // Create key
    res = ToolRunner.run(shell, new String[]{"-touch", KEY_PATH});
    Assertions.assertEquals(0, res);
    // List the bucket to make sure that bucket exists.
    res = ToolRunner.run(shell, new String[]{"-ls", BUCKET_PATH});
    Assertions.assertEquals(0, res);

  }

  @Test
  public void testCreateSnapshotDuplicateName() throws Exception {
    String snapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    int res = ToolRunner.run(shell,
        new String[]{"-createSnapshot", BUCKET_PATH, snapshotName});
    // Asserts that create request succeeded
    Assertions.assertEquals(0, res);

    res = ToolRunner.run(shell,
        new String[]{"-createSnapshot", BUCKET_PATH, snapshotName});
    // Asserts that create request fails since snapshot name provided twice
    Assertions.assertEquals(1, res);
  }

  /**
   * Create snapshot should succeed.
   * 1st case: valid snapshot name
   * 2nd case: snapshot name length is less than 64 chars
   */
  @ParameterizedTest
  @ValueSource(strings = {"snap-1",
      "snap75795657617173401188448010125899089001363595171500499231286"})
  public void testCreateSnapshotSuccess(String snapshotName)
      throws Exception {
    int res = ToolRunner.run(shell,
        new String[]{"-createSnapshot", BUCKET_PATH, snapshotName});
    // Asserts that create request succeeded
    Assertions.assertEquals(0, res);

    SnapshotInfo snapshotInfo = ozoneManager
        .getMetadataManager()
        .getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(VOLUME, BUCKET, snapshotName));

    // Assert that snapshot exists in RocksDB.
    // We can't use list or valid if snapshot directory exists because DB
    // transaction might not be flushed by the time.
    Assertions.assertNotNull(snapshotInfo);
  }

  private static Stream<Arguments> createSnapshotFailureScenarios() {
    String invalidBucketPath = "/invalid/uri";
    return Stream.of(
        Arguments.of("1st case: snapshot name contains invalid char",
            BUCKET_PATH,
            "snapa?b",
            "Invalid snapshot name",
            1),
        Arguments.of("2nd case: snapshot name consists only of numbers",
            BUCKET_PATH,
            "1234",
            "Invalid snapshot name",
            1),
        Arguments.of("3rd case: bucket path is invalid",
            invalidBucketPath,
            "validSnapshotName12",
            "No such file or directory",
            1),
        Arguments.of("4th case: snapshot name length is more than 64 chars",
            BUCKET_PATH,
            "snap156808943643007724443266605711479126926050896107709081166294",
            "Invalid snapshot name",
            1),
        Arguments.of("5th case: all parameters are missing",
            "",
            "",
            "Can not create a Path from an empty string",
            -1)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("createSnapshotFailureScenarios")
  public void testCreateSnapshotFailure(String description,
                                        String paramBucketPath,
                                        String snapshotName,
                                        String expectedMessage,
                                        int expectedResponse)
      throws Exception {
    String errorMessage = execShellCommandAndGetOutput(expectedResponse,
        new String[]{"-createSnapshot", paramBucketPath, snapshotName});

    Assertions.assertTrue(errorMessage
        .contains(expectedMessage));
  }

  /**
   * Test list snapshot and snapshot keys with "ozone fs -ls".
   */
  @Test
  public void testFsLsSnapshot() throws Exception {
    String newKey = "key-" + RandomStringUtils.randomNumeric(5);
    String newKeyPath = BUCKET_PATH + OM_KEY_PREFIX + newKey;

    // Create new key, while the old one
    // might be deleted from a previous test.
    execShellCommandAndGetOutput(0,
        new String[]{"-touch", newKeyPath});

    // Create snapshot
    String snapshotName = createSnapshot();
    // Setup snapshot paths
    String snapshotPath = BUCKET_WITH_SNAPSHOT_INDICATOR_PATH +
        OM_KEY_PREFIX + snapshotName;
    String snapshotKeyPath = snapshotPath + OM_KEY_PREFIX + newKey;

    // Check for snapshot with "ozone fs -ls"
    String listSnapOut = execShellCommandAndGetOutput(0,
        new String[]{"-ls", BUCKET_WITH_SNAPSHOT_INDICATOR_PATH});

    // Assert that output contains above snapshotName
    Assertions.assertTrue(listSnapOut
        .contains(snapshotPath));

    // Check for snapshot keys with "ozone fs -ls"
    String listSnapKeyOut = execShellCommandAndGetOutput(0,
        new String[]{"-ls", snapshotPath});

    // Assert that output contains the snapshot key
    Assertions.assertTrue(listSnapKeyOut
        .contains(snapshotKeyPath));
  }

  @Test
  public void testDeleteBucketWithSnapshot() throws Exception {
    String snapshotName = createSnapshot();

    String snapshotPath = BUCKET_WITH_SNAPSHOT_INDICATOR_PATH
        + OM_KEY_PREFIX + snapshotName;
    String snapshotKeyPath = snapshotPath + OM_KEY_PREFIX + KEY;

    // Delete bucket key should succeed
    String deleteKeyOut = execShellCommandAndGetOutput(0,
        new String[]{"-rm", "-r", "-skipTrash", KEY_PATH});

    Assertions.assertTrue(deleteKeyOut
        .contains("Deleted " + BUCKET_PATH));

    // Delete bucket should fail due to existing snapshot
    String deleteBucketOut = execShellCommandAndGetOutput(1,
        new String[]{"-rm", "-r", "-skipTrash", BUCKET_PATH});
    Assertions.assertTrue(deleteBucketOut
          .contains(BUCKET + " can't be deleted when it has snapshots"));

    // Key shouldn't exist under bucket
    String listKeyOut = execShellCommandAndGetOutput(0,
        new String[]{"-ls", BUCKET_PATH});
    Assertions.assertTrue(Strings.isNullOrEmpty(listKeyOut));

    // Key should still exist under snapshot
    String listSnapKeyOut = execShellCommandAndGetOutput(0,
        new String[]{"-ls", snapshotPath});
    Assertions.assertTrue(listSnapKeyOut.contains(snapshotKeyPath));
  }

  /**
   * Execute a shell command with provided arguments
   * and return a string of the output.
   */
  private String execShellCommandAndGetOutput(
      int response, String[] args) throws Exception {
    ByteArrayOutputStream successBytes = new ByteArrayOutputStream();
    ByteArrayOutputStream errorBytes = new ByteArrayOutputStream();

    // Setup output streams
    System.setOut(new PrintStream(
        successBytes, false, StandardCharsets.UTF_8.name()));
    System.setErr(new PrintStream(
        errorBytes, false, StandardCharsets.UTF_8.name()));

    // Execute command
    int res = ToolRunner.run(shell, args);
    Assertions.assertEquals(response, res);

    // Store command output to a string,
    // if command should succeed then
    // get successBytes else get errorBytes
    String output = response == 0 ?
        successBytes.toString(StandardCharsets.UTF_8.name()) :
        errorBytes.toString(StandardCharsets.UTF_8.name());

    // Flush byte array streams
    successBytes.flush();
    errorBytes.flush();

    // Restore output streams
    System.setOut(new PrintStream(
        successBytes, false, StandardCharsets.UTF_8.name()));
    System.setErr(new PrintStream(
        errorBytes, false, StandardCharsets.UTF_8.name()));

    return output;
  }

  private String createSnapshot() throws Exception {
    String snapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    // Create snapshot
    int res = ToolRunner.run(shell,
        new String[]{"-createSnapshot", BUCKET_PATH, snapshotName});
    // Asserts that create request succeeded
    Assertions.assertEquals(0, res);

    File metaDir = OMStorage
        .getOmDbDir(ozoneManager.getConfiguration());

    // wait till the snapshot directory exists
    SnapshotInfo snapshotInfo = ozoneManager.getMetadataManager()
        .getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(VOLUME, BUCKET, snapshotName));
    String snapshotDirName = metaDir + OM_KEY_PREFIX +
        OM_SNAPSHOT_DIR + OM_KEY_PREFIX + OM_DB_NAME +
        snapshotInfo.getCheckpointDirName() + OM_KEY_PREFIX + "CURRENT";
    GenericTestUtils.waitFor(() -> new File(snapshotDirName).exists(),
        1000, 100000);

    return snapshotName;
  }
}
