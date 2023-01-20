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

import java.io.IOException;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
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
  private OzoneFsShell shell;
  private String volume;
  private String bucket;
  private String key;
  private String bucketPath;
  private String bucketWithSnapshotIndicatorPath;

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
  }

  @BeforeEach
  public void init() throws Exception {
    String hostPrefix = OZONE_OFS_URI_SCHEME + "://" + OM_SERVICE_ID;
    OzoneConfiguration clientConf =
        new OzoneConfiguration(cluster.getConf());
    clientConf.set(FS_DEFAULT_NAME_KEY, hostPrefix);

    shell = new OzoneFsShell(clientConf);

    this.volume = "vol-" + RandomStringUtils.randomNumeric(5);
    this.bucket = "buck-" + RandomStringUtils.randomNumeric(5);
    this.key = "key-" + RandomStringUtils.randomNumeric(5);

    this.bucketPath = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket;
    this.bucketWithSnapshotIndicatorPath = bucketPath +
        OM_KEY_PREFIX + OM_SNAPSHOT_INDICATOR;

    String keyPath = bucketPath + OM_KEY_PREFIX + key;
    createVolBuckKey(bucketPath, keyPath);
  }

  @AfterEach
  public void cleanup() throws IOException {
    shell.close();
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void createVolBuckKey(String testVolBucket, String testKey)
      throws Exception {

    // Create volume and bucket
    int res = ToolRunner.run(shell,
        new String[]{"-mkdir", "-p", testVolBucket});
    Assertions.assertEquals(0, res);
    // Create key
    res = ToolRunner.run(shell, new String[]{"-touch", testKey});
    Assertions.assertEquals(0, res);
    // List the bucket to make sure that bucket exists.
    res = ToolRunner.run(shell, new String[]{"-ls", testVolBucket});
    Assertions.assertEquals(0, res);

  }

  @Test
  public void testCreateSnapshot() throws Exception {
    String snapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    int res = ToolRunner.run(shell,
        new String[]{"-createSnapshot", bucketPath, snapshotName});
    // Asserts that create request succeeded
    Assertions.assertEquals(0, res);

    SnapshotInfo snapshotInfo = ozoneManager
        .getMetadataManager()
        .getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volume, bucket, snapshotName));

    // Assert that snapshot exists in RocksDB.
    // We can't use list or valid if snapshot directory exists because DB
    // transaction might not be flushed by the time.
    Assertions.assertNotNull(snapshotInfo);
  }

  @Test
  public void testCreateSnapshotDuplicateName() throws Exception {
    String snapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    int res = ToolRunner.run(shell,
        new String[]{"-createSnapshot", bucketPath, snapshotName});
    // Asserts that create request succeeded
    Assertions.assertEquals(0, res);

    res = ToolRunner.run(shell,
        new String[]{"-createSnapshot", bucketPath, snapshotName});
    // Asserts that create request fails since snapshot name provided twice
    Assertions.assertEquals(1, res);
  }

  @Test
  public void testCreateSnapshotInvalidName() throws Exception {
    String snapshotName = "snapa?b";

    int res = ToolRunner.run(shell,
        new String[]{"-createSnapshot", bucketPath, snapshotName});
    // Asserts that create request failed since invalid name passed
    Assertions.assertEquals(1, res);
  }

  @Test
  public void testCreateSnapshotOnlyNumericName() throws Exception {
    String snapshotName = "1234";

    int res = ToolRunner.run(shell,
        new String[]{"-createSnapshot", bucketPath, snapshotName});
    // Asserts that create request failed since only numeric name passed
    Assertions.assertEquals(1, res);
  }

  @Test
  public void testCreateSnapshotInvalidURI() throws Exception {

    int res = ToolRunner.run(shell,
        new String[]{"-createSnapshot", "invalidURI"});
    // Asserts that create request failed since
    // invalid volume-bucket URI passed
    Assertions.assertEquals(1, res);
  }

  @Test
  public void testCreateSnapshotNameLength() throws Exception {
    String name63 =
        "snap75795657617173401188448010125899089001363595171500499231286";
    String name64 =
        "snap156808943643007724443266605711479126926050896107709081166294";

    int res = ToolRunner.run(shell,
        new String[]{"-createSnapshot", bucketPath, name63});
    // Asserts that create request succeeded since name length
    // less than 64 char
    Assertions.assertEquals(0, res);

    res = ToolRunner.run(shell,
        new String[]{"-createSnapshot", bucketPath, name64});
    // Asserts that create request fails since name length
    // more than 64 char
    Assertions.assertEquals(1, res);

    SnapshotInfo snapshotInfo = ozoneManager
        .getMetadataManager()
        .getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volume, bucket, name63));

    Assertions.assertNotNull(snapshotInfo);
  }

  @Test
  public void testCreateSnapshotParameterMissing() throws Exception {

    int res = ToolRunner.run(shell,
        new String[]{"-createSnapshot"});
    // Asserts that create request failed since mandatory params not passed
    Assertions.assertEquals(-1, res);
  }

  /**
   * Test list snapshots and keys with "ozone fs -ls".
   */
  @Test
  public void testFsLsSnapshot() throws Exception {
    String snapshotName = "snap-" + RandomStringUtils.randomNumeric(5);
    String snapshotPath = bucketWithSnapshotIndicatorPath +
        OM_KEY_PREFIX + snapshotName;
    String snapshotKeyPath = snapshotPath + OM_KEY_PREFIX + key;

    // Create snapshot
    int res = ToolRunner.run(shell,
        new String[]{"-createSnapshot", bucketPath, snapshotName});
    // Asserts that create request succeeded
    Assertions.assertEquals(0, res);

    try (GenericTestUtils.SystemOutCapturer capture =
             new GenericTestUtils.SystemOutCapturer()) {

      // Check for snapshot with "ozone fs -ls"
      res = ToolRunner.run(shell,
          new String[]{"-ls", bucketWithSnapshotIndicatorPath});
      // Asserts that -ls request succeeded
      Assertions.assertEquals(0, res);

      // Assert that output contains above snapshotName
      Assertions.assertTrue(capture.getOutput()
          .contains(bucketWithSnapshotIndicatorPath
              + OM_KEY_PREFIX + snapshotName));

      // Check for snapshot keys with "ozone fs -ls"
      res = ToolRunner.run(shell,
          new String[]{"-ls", snapshotPath});
      // Asserts that -ls request succeeded
      Assertions.assertEquals(0, res);

      // Assert that output contains the snapshot key
      Assertions.assertTrue(capture.getOutput()
          .contains(snapshotKeyPath));
    }
  }

  @Test
  public void testDeleteBucketWithSnapshot() throws Exception {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buc-" + RandomStringUtils.randomNumeric(5);
    String key = "key-" + RandomStringUtils.randomNumeric(5);
    String snapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    String testVolBucket = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket;
    String testKey = testVolBucket + OM_KEY_PREFIX + key;

    createVolBuckKey(testVolBucket, testKey);

    OzoneFsShell shell = new OzoneFsShell(clientConf);
    try {
      int res = ToolRunner.run(shell,
              new String[]{"-createSnapshot", testVolBucket, snapshotName});
      // Asserts that create request succeeded
      assertEquals(0, res);

      res = ToolRunner.run(shell,
              new String[]{"-rm", "-r", "-skipTrash", testKey});
      assertEquals(0, res);

      res = ToolRunner.run(shell,
              new String[]{"-rm", "-r", "-skipTrash", testVolBucket});
      assertEquals(1, res);

      res = ToolRunner.run(shell,
              new String[]{"-ls", testVolBucket});
      assertEquals(0, res);

      String snapshotPath = testVolBucket + OM_KEY_PREFIX + ".snapshot"
              + OM_KEY_PREFIX + snapshotName + OM_KEY_PREFIX;
      res = ToolRunner.run(shell,
              new String[]{"-ls", snapshotPath});
      assertEquals(0, res);

    } finally {
      shell.close();
    }
  }
}
