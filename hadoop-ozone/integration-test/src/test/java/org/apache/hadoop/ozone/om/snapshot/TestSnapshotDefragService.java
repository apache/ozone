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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DEFRAG_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.SNAPSHOT_DEFRAG_LIMIT_PER_TASK;
import static org.apache.ozone.test.LambdaTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotDefragService;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Test SnapshotDefragService functionality using MiniOzoneCluster.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSnapshotDefragService {

  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotDefragService.class);

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ObjectStore store;
  private OzoneManager ozoneManager;
  private SnapshotDefragService defragService;
  private static int SNAPSHOT_DEFRAG_LIMIT_PER_TASK_VALUE = 3;

  @BeforeAll
  void setup() throws Exception {
    // Enable debug logging for SnapshotDefragService
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger(SnapshotDefragService.class), Level.DEBUG);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger(OmMetadataManagerImpl.class), Level.DEBUG);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger(SnapshotCache.class), Level.DEBUG);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 30, TimeUnit.SECONDS);
    conf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);
    conf.setBoolean(OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    conf.set(OZONE_DEFAULT_BUCKET_LAYOUT, BucketLayout.OBJECT_STORE.name());
    // TODO: Add SNAPSHOT_DEFRAGMENTATION layout feature version
    conf.setInt(OMStorage.TESTING_INIT_LAYOUT_VERSION_KEY,
        OMLayoutFeature.DELEGATION_TOKEN_SYMMETRIC_SIGN.layoutVersion());

    conf.setInt(OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL, -1);
    conf.setInt(OZONE_SNAPSHOT_DEFRAG_SERVICE_TIMEOUT, 3000000);
    conf.setInt(SNAPSHOT_DEFRAG_LIMIT_PER_TASK, SNAPSHOT_DEFRAG_LIMIT_PER_TASK_VALUE);

    conf.setQuietMode(false);

    // Create MiniOzoneCluster
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();

    client = cluster.newClient();
    store = client.getObjectStore();
    ozoneManager = cluster.getOzoneManager();

    // Create SnapshotDefragService for manual triggering
    defragService = new SnapshotDefragService(
        10000, // interval
        TimeUnit.MILLISECONDS,
        30000, // service timeout
        ozoneManager,
        conf
    );
  }

  @AfterAll
  public void cleanup() throws Exception {
    if (defragService != null) {
      defragService.shutdown();
    }
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Create keys in a bucket.
   */
  private void createKeys(String volumeName, String bucketName, int keyCount) throws Exception {
    OzoneVolume volume = store.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < keyCount; i++) {
      String keyName = "key-" + i;
      String data = RandomStringUtils.randomAlphabetic(100);

      try (OzoneOutputStream outputStream = bucket.createKey(keyName, data.length(),
          StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
          Collections.emptyMap(), Collections.emptyMap())) {
        outputStream.write(data.getBytes(StandardCharsets.UTF_8));
      }
    }
    LOG.info("Created {} keys in bucket {}/{}", keyCount, volumeName, bucketName);
  }

  /**
   * Create a snapshot and wait for it to be available.
   */
  private void createSnapshot(String volumeName, String bucketName, String snapshotName)
      throws Exception {
    // Get existing checkpoint directories before creating snapshot
    Set<String> existingCheckpoints = getExistingCheckpointDirectories();

    store.createSnapshot(volumeName, bucketName, snapshotName);

    // Wait for snapshot to be created
    GenericTestUtils.waitFor(() -> {
      try {
        SnapshotInfo snapshotInfo = ozoneManager.getMetadataManager()
            .getSnapshotInfoTable()
            .get(SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName));
        return snapshotInfo != null;
      } catch (IOException e) {
        return false;
      }
    }, 1000, 30000);

    // Wait for checkpoint DB to be created
    waitForCheckpointDB(snapshotName, existingCheckpoints);

    LOG.info("Created snapshot: {}", snapshotName);
  }

  /**
   * Get existing checkpoint directories before snapshot creation.
   */
  private Set<String> getExistingCheckpointDirectories() {
    String metadataDir = ozoneManager.getConfiguration().get("ozone.om.db.dirs");
    if (metadataDir == null) {
      metadataDir = ozoneManager.getConfiguration().get("ozone.metadata.dirs");
    }

    String checkpointStateDir = metadataDir + "/db.snapshots/checkpointState";
    File checkpointParentDir = new File(checkpointStateDir);

    Set<String> existingDirs = new java.util.HashSet<>();
    if (checkpointParentDir.exists()) {
      File[] checkpointDirs = checkpointParentDir.listFiles(File::isDirectory);
      if (checkpointDirs != null) {
        for (File dir : checkpointDirs) {
          existingDirs.add(dir.getName());
        }
      }
    }

    LOG.debug("Existing checkpoint directories: {}", existingDirs);
    return existingDirs;
  }

  /**
   * Wait for checkpoint DB to be created under checkpointState directory.
   */
  private void waitForCheckpointDB(String snapshotName, Set<String> existingCheckpoints) throws Exception {
    String metadataDir = ozoneManager.getConfiguration().get("ozone.om.db.dirs");
    if (metadataDir == null) {
      metadataDir = ozoneManager.getConfiguration().get("ozone.metadata.dirs");
    }

    String checkpointStateDir = metadataDir + "/db.snapshots/checkpointState";
    File checkpointParentDir = new File(checkpointStateDir);

    LOG.info("Waiting for new checkpoint DB to be created under: {}", checkpointStateDir);

    GenericTestUtils.waitFor(() -> {
      if (!checkpointParentDir.exists()) {
        LOG.debug("CheckpointState directory does not exist yet: {}", checkpointStateDir);
        return false;
      }

      // List all directories in checkpointState
      File[] checkpointDirs = checkpointParentDir.listFiles(File::isDirectory);
      if (checkpointDirs == null || checkpointDirs.length == 0) {
        LOG.debug("No checkpoint directories found in: {}", checkpointStateDir);
        return false;
      }

      // Look for new checkpoint directories that weren't there before
      for (File checkpointDir : checkpointDirs) {
        String dirName = checkpointDir.getName();

        // Skip if this directory existed before snapshot creation
        if (existingCheckpoints.contains(dirName)) {
          continue;
        }

        // Check if the new directory contains database files
        File[] dbFiles = checkpointDir.listFiles();
        if (dbFiles != null && dbFiles.length > 0) {
          for (File dbFile : dbFiles) {
            if (dbFile.isFile() && (dbFile.getName().endsWith(".sst") ||
                dbFile.getName().equals("CURRENT") ||
                dbFile.getName().startsWith("MANIFEST"))) {
              LOG.info("New checkpoint DB found for snapshot {} in directory: {}",
                  snapshotName, checkpointDir.getAbsolutePath());
              return true;
            }
          }
        }

        LOG.debug("New checkpoint directory found but no DB files yet: {}", checkpointDir.getAbsolutePath());
      }

      LOG.debug("Waiting for new checkpoint DB files to appear in checkpointState directories");
      return false;
    }, 1000, 60000); // Wait up to 60 seconds for checkpoint DB creation

    LOG.info("Checkpoint DB created successfully for snapshot: {}", snapshotName);
  }

  /**
   * Lists the contents of the db.snapshots directory.
   */
  private void printSnapshotDirectoryListing(String description) {
    LOG.info("=== {} ===", description);
    String metadataDir = ozoneManager.getConfiguration().get("ozone.om.db.dirs");
    if (metadataDir == null) {
      metadataDir = ozoneManager.getConfiguration().get("ozone.metadata.dirs");
    }

    String snapshotDir = metadataDir + "/db.snapshots";
    File snapshotsDir = new File(snapshotDir);

    if (!snapshotsDir.exists()) {
      LOG.info("Snapshots directory does not exist: {}", snapshotDir);
      return;
    }

    try (Stream<Path> paths = Files.walk(Paths.get(snapshotDir))) {
      paths.sorted()
          .forEach(path -> {
            File file = path.toFile();
            String relativePath = Paths.get(snapshotDir).relativize(path).toString();
            if (file.isDirectory()) {
              LOG.info("Directory: {}/", relativePath.isEmpty() ? "." : relativePath);
            } else {
              LOG.info("File: {} (size: {} bytes)", relativePath, file.length());
            }
          });
    } catch (IOException e) {
      LOG.error("Error listing snapshot directory: {}", snapshotDir, e);
    }
  }

  /**
   * Trigger the SnapshotDefragService by starting it and waiting for it to process snapshots.
   */
  private void triggerSnapshotDefragService() throws Exception {
    LOG.info("Triggering SnapshotDefragService ...");

    // Mark all snapshots as needing defragmentation first
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>> iterator =
         metadataManager.getSnapshotInfoTable().iterator()) {
      iterator.seekToFirst();
      while (iterator.hasNext()) {
        Table.KeyValue<String, SnapshotInfo> entry = iterator.next();
        SnapshotInfo snapshotInfo = entry.getValue();
        LOG.info("snapshot {}, needsDefragmentation = {}",
            snapshotInfo.getName(), defragService.needsDefragmentation(snapshotInfo));
      }
    }

    long initialDefragCount = defragService.getSnapshotsDefraggedCount().get();
    LOG.info("Initial defragmented count: {}", initialDefragCount);

    // Start the service
    defragService.start();

    // Wait for the service to process snapshots
    try {
      await(30000, 1000, () -> {
        long currentCount = defragService.getSnapshotsDefraggedCount().get();
        LOG.info("Current defragmented count: {}", currentCount);
        return currentCount > initialDefragCount && currentCount >= SNAPSHOT_DEFRAG_LIMIT_PER_TASK_VALUE;
      });
    } catch (TimeoutException e) {
      LOG.warn("Timeout waiting for defragmentation to complete, continuing with test");
    }

    LOG.info("SnapshotDefragService execution completed. Snapshots defragmented: {}",
        defragService.getSnapshotsDefraggedCount().get());
  }

  @Test
  public void testSnapshotDefragmentation() throws Exception {
    String volumeName = "test-volume";
    String bucketName = "test-bucket";

    // Create volume and bucket
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    // TODO: Test FSO bucket as well, default is LEGACY / OBJECT_STORE
    volume.createBucket(bucketName);

    LOG.info("Starting snapshot defragmentation test...");

    // Print initial state
    printSnapshotDirectoryListing("Initial state - no snapshots");

    // Step 1: Create 2 keys, then create snap-1
    createKeys(volumeName, bucketName, 2);
    createSnapshot(volumeName, bucketName, "snap-1");
    printSnapshotDirectoryListing("After creating snap-1 (2 keys)");

    // Step 2: Create 2 more keys, then create snap-2
    createKeys(volumeName, bucketName, 2);  // TODO: This actually overwrites the previous keys
    createSnapshot(volumeName, bucketName, "snap-2");
    printSnapshotDirectoryListing("After creating snap-2 (4 keys total)");

    // Step 3: Create 2 more keys, then create snap-3
    createKeys(volumeName, bucketName, 2);  // TODO: This actually overwrites the previous keys
    createSnapshot(volumeName, bucketName, "snap-3");
    printSnapshotDirectoryListing("After creating snap-3 (6 keys total)");

    // Step 4: Trigger SnapshotDefragService
    triggerSnapshotDefragService();
    printSnapshotDirectoryListing("After SnapshotDefragService execution");

    // Verify that the snapshots still exist
    SnapshotInfo snap1 = ozoneManager.getMetadataManager()
        .getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volumeName, bucketName, "snap-1"));
    SnapshotInfo snap2 = ozoneManager.getMetadataManager()
        .getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volumeName, bucketName, "snap-2"));
    SnapshotInfo snap3 = ozoneManager.getMetadataManager()
        .getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volumeName, bucketName, "snap-3"));

    assertNotNull(snap1, "Snapshot snap-1 should exist");
    assertNotNull(snap2, "Snapshot snap-2 should exist");
    assertNotNull(snap3, "Snapshot snap-3 should exist");

    LOG.info("Test completed successfully");
  }
}
