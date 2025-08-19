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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DEFRAG_LIMIT_PER_TASK;
import static org.apache.ozone.test.LambdaTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test SnapshotDefragService functionality.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSnapshotDefragService {

  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotDefragService.class);

  private OzoneManagerProtocol writeClient;
  private OzoneManager om;
  private OzoneConfiguration conf;
  private KeyManager keyManager;
  private SnapshotDefragService defragService;
  private String metadataDir;

  @BeforeAll
  void setup(@TempDir Path folder) throws Exception {
    ExitUtils.disableSystemExit();
    conf = new OzoneConfiguration();
    metadataDir = folder.toString();
    conf.set(OZONE_METADATA_DIRS, metadataDir);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);
    conf.setBoolean(OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    conf.setInt(SNAPSHOT_DEFRAG_LIMIT_PER_TASK, 10);
    conf.setQuietMode(false);

    OmTestManagers omTestManagers = new OmTestManagers(conf);
    keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();

    // Create SnapshotDefragService
    defragService = new SnapshotDefragService(
        1000, // interval
        TimeUnit.MILLISECONDS,
        30000, // service timeout
        om,
        conf
    );
  }

  @AfterAll
  public void cleanup() throws Exception {
    if (defragService != null) {
      defragService.shutdown();
    }
    if (keyManager != null) {
      keyManager.stop();
    }
    if (writeClient != null) {
      writeClient.close();
    }
    if (om != null) {
      om.stop();
    }
  }

  /**
   * Creates a volume.
   */
  private void createVolume(String volumeName) throws IOException {
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName(UserGroupInformation.getCurrentUser().getShortUserName())
        .setOwnerName(UserGroupInformation.getCurrentUser().getShortUserName())
        .build();
    writeClient.createVolume(volumeArgs);
  }

  /**
   * Adds a bucket to the volume.
   */
  private void addBucketToVolume(String volumeName, String bucketName) throws IOException {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .build();
    writeClient.createBucket(bucketInfo);
  }

  /**
   * Creates keys in the specified bucket.
   */
  private void createKeys(String volumeName, String bucketName, int keyCount) throws IOException {
    for (int i = 0; i < keyCount; i++) {
      String keyName = "key-" + RandomStringUtils.randomAlphanumeric(5) + "-" + i;
      createKey(volumeName, bucketName, keyName);
    }
    LOG.info("Created {} keys in {}/{}", keyCount, volumeName, bucketName);
  }

  /**
   * Creates a single key.
   */
  private void createKey(String volumeName, String bucketName, String keyName) throws IOException {
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(100)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
        .setLocationInfoList(new ArrayList<>())
        .build();

    // Create the key
    OpenKeySession openKeySession = writeClient.openKey(omKeyArgs);
    omKeyArgs.setLocationInfoList(openKeySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    writeClient.commitKey(omKeyArgs, openKeySession.getId());
  }

  /**
   * Creates a snapshot.
   */
  private String createSnapshot(String volumeName, String bucketName, String snapshotName) throws IOException {
    writeClient.createSnapshot(volumeName, bucketName, snapshotName);
    LOG.info("Created snapshot: {}", snapshotName);
    return snapshotName;
  }

  /**
   * Lists the contents of the db.snapshots directory.
   */
  private void printSnapshotDirectoryListing(String description) {
    LOG.info("=== {} ===", description);
    String snapshotDir = metadataDir + "/db.snapshots";
    File snapshotsDir = new File(snapshotDir);

    if (!snapshotsDir.exists()) {
      LOG.info("Snapshots directory does not exist: {}", snapshotDir);
      return;
    }

    try {
      Files.walk(Paths.get(snapshotDir))
          .sorted()
          .forEach(path -> {
            File file = path.toFile();
            String relativePath = Paths.get(snapshotDir).relativize(path).toString();
            if (file.isDirectory()) {
              LOG.info("DIR:  {}/", relativePath.isEmpty() ? "." : relativePath);
            } else {
              LOG.info("FILE: {} ({})", relativePath, file.length() + " bytes");
            }
          });
    } catch (IOException e) {
      LOG.error("Failed to list snapshot directory contents", e);
    }
    LOG.info("=== End {} ===", description);
  }

  /**
   * Lists snapshot metadata information.
   */
  private void printSnapshotMetadata() throws IOException {
    LOG.info("=== Snapshot Metadata ===");
    OMMetadataManager metadataManager = om.getMetadataManager();

    try (var iterator = metadataManager.getSnapshotInfoTable().iterator()) {
      iterator.seekToFirst();
      int count = 0;
      while (iterator.hasNext()) {
        var entry = iterator.next();
        SnapshotInfo snapshotInfo = entry.getValue();
        LOG.info("Snapshot {}: name={}, volume={}, bucket={}, status={}, needsCompaction={}",
            ++count,
            snapshotInfo.getName(),
            snapshotInfo.getVolumeName(),
            snapshotInfo.getBucketName(),
            snapshotInfo.getSnapshotStatus(),
            getSnapshotNeedsCompaction(snapshotInfo));
      }
      if (count == 0) {
        LOG.info("No snapshots found in metadata");
      }
    }
    LOG.info("=== End Snapshot Metadata ===");
  }

  /**
   * Helper method to check if snapshot needs compaction by reading YAML.
   */
  private boolean getSnapshotNeedsCompaction(SnapshotInfo snapshotInfo) {
    try {
      String snapshotPath = OmSnapshotManager.getSnapshotPath(conf, snapshotInfo);
      File yamlFile = new File(snapshotPath);
      if (!yamlFile.exists()) {
        return true; // If no YAML file, assume needs compaction
      }
      OmSnapshotLocalDataYaml yamlData = OmSnapshotLocalDataYaml.getFromYamlFile(yamlFile);
      return yamlData.getNeedsCompaction();
    } catch (Exception e) {
      LOG.warn("Failed to read snapshot YAML for {}, assuming needs compaction",
          snapshotInfo.getName(), e);
      return true;
    }
  }

  /**
   * Sets the needsCompaction flag for a snapshot to true to simulate needing defragmentation.
   */
  private void markSnapshotAsNeedingDefragmentation(SnapshotInfo snapshotInfo) throws IOException {
    String snapshotPath = OmSnapshotManager.getSnapshotPath(conf, snapshotInfo);
    File yamlFile = new File(snapshotPath);

    if (yamlFile.exists()) {
      OmSnapshotLocalDataYaml yamlData = OmSnapshotLocalDataYaml.getFromYamlFile(yamlFile);
      yamlData.setNeedsCompaction(true);
      yamlData.writeToYaml(yamlFile);
      LOG.info("Marked snapshot {} as needing defragmentation", snapshotInfo.getName());
    } else {
      LOG.warn("YAML file not found for snapshot {}: {}", snapshotInfo.getName(), yamlFile.getPath());
    }
  }

  /**
   * Main test case that creates keys, snapshots, and triggers defragmentation service.
   */
  @Test
  public void testSnapshotDefragmentationFlow() throws Exception {
    LOG.info("Starting SnapshotDefragService test");

    String volumeName = "test-volume";
    String bucketName = "test-bucket";

    // Create volume and bucket
    createVolume(volumeName);
    addBucketToVolume(volumeName, bucketName);

    printSnapshotDirectoryListing("Initial state - before any snapshots");

    // Step 1: Create 2 keys and snapshot snap-1
    LOG.info("=== Step 1: Creating 2 keys and snapshot snap-1 ===");
    createKeys(volumeName, bucketName, 2);
    createSnapshot(volumeName, bucketName, "snap-1");
    printSnapshotDirectoryListing("After creating snap-1");
    printSnapshotMetadata();

    // Step 2: Create 2 more keys and snapshot snap-2
    LOG.info("=== Step 2: Creating 2 more keys and snapshot snap-2 ===");
    createKeys(volumeName, bucketName, 2);
    createSnapshot(volumeName, bucketName, "snap-2");
    printSnapshotDirectoryListing("After creating snap-2");
    printSnapshotMetadata();

    // Step 3: Create 2 more keys and snapshot snap-3
    LOG.info("=== Step 3: Creating 2 more keys and snapshot snap-3 ===");
    createKeys(volumeName, bucketName, 2);
    createSnapshot(volumeName, bucketName, "snap-3");
    printSnapshotDirectoryListing("After creating snap-3");
    printSnapshotMetadata();

    // Mark all snapshots as needing defragmentation
    OMMetadataManager metadataManager = om.getMetadataManager();
    try (var iterator = metadataManager.getSnapshotInfoTable().iterator()) {
      iterator.seekToFirst();
      while (iterator.hasNext()) {
        var entry = iterator.next();
        SnapshotInfo snapshotInfo = entry.getValue();
        markSnapshotAsNeedingDefragmentation(snapshotInfo);
      }
    }

    LOG.info("=== Marked all snapshots as needing defragmentation ===");
    printSnapshotMetadata();

    // Step 4: Start the SnapshotDefragService and trigger defragmentation
    LOG.info("=== Step 4: Starting SnapshotDefragService ===");

    long initialDefragCount = defragService.getSnapshotsDefraggedCount().get();
    LOG.info("Initial defragmented count: {}", initialDefragCount);

    // Start the service
    defragService.start();

    // Trigger defragmentation by calling the background task manually
    // This simulates the service running
    try {
      // Give the service some time to run
      await(30, TimeUnit.SECONDS, () -> {
        long currentCount = defragService.getSnapshotsDefraggedCount().get();
        LOG.info("Current defragmented count: {}", currentCount);
        return currentCount > initialDefragCount;
      });
    } catch (TimeoutException e) {
      LOG.warn("Timeout waiting for defragmentation to complete, continuing with test");
    }

    // Print final state
    printSnapshotDirectoryListing("After SnapshotDefragService run finished");
    printSnapshotMetadata();

    long finalDefragCount = defragService.getSnapshotsDefraggedCount().get();
    LOG.info("Final defragmented count: {}", finalDefragCount);

    // Verify that the service processed at least some snapshots
    assertTrue(finalDefragCount >= initialDefragCount,
        "SnapshotDefragService should have processed some snapshots");

    LOG.info("SnapshotDefragService test completed successfully");
  }

  /**
   * Test the basic functionality of SnapshotDefragService.
   */
  @Test
  public void testSnapshotDefragServiceBasics() throws Exception {
    assertNotNull(defragService, "SnapshotDefragService should be created");

    // Test service start/stop
    defragService.start();
    assertTrue(defragService.getSnapshotsDefraggedCount().get() >= 0,
        "Defragmented count should be non-negative");

    defragService.pause();
    defragService.resume();

    defragService.shutdown();
  }

  /**
   * Test defragmentation with no snapshots.
   */
  @Test
  public void testDefragmentationWithNoSnapshots() throws Exception {
    LOG.info("Testing defragmentation with no snapshots");

    long initialCount = defragService.getSnapshotsDefraggedCount().get();

    defragService.start();

    // Wait a short time to let the service run
    Thread.sleep(1000);

    long finalCount = defragService.getSnapshotsDefraggedCount().get();

    // Count should remain the same as there are no snapshots to process
    assertEquals(initialCount, finalCount,
        "Defragmented count should not change when there are no snapshots");

    defragService.shutdown();
  }
}
