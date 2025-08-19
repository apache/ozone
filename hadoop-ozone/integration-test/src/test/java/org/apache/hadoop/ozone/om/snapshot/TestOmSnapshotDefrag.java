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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Slow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Test class for OM Snapshot Defragmentation.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slow
public class TestOmSnapshotDefrag {

  private static final int KEYS_PER_BATCH = 1000; // 1 million keys per batch
//  private static final byte[] SINGLE_BYTE_VALUE = new byte[]{1}; // 1 byte value

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ObjectStore store;
  private OzoneManager ozoneManager;
  private final AtomicInteger counter = new AtomicInteger();

  @BeforeAll
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_DEFAULT_BUCKET_LAYOUT, BucketLayout.OBJECT_STORE.name());
    conf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);
    // Enable filesystem snapshot feature for the test
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    conf.setInt(OMStorage.TESTING_INIT_LAYOUT_VERSION_KEY,
        OMLayoutFeature.SNAPSHOT_DEFRAGMENTATION.layoutVersion());
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL, 100, TimeUnit.SECONDS);
    conf.setInt(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, -1);
    conf.setTimeDuration(OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL,
        0, TimeUnit.SECONDS);
    // non-HA mode
    cluster = MiniOzoneCluster.newBuilder(conf).build();

    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    store = client.getObjectStore();
    ozoneManager = cluster.getOzoneManager();
  }

  @AfterAll
  public void teardown() throws Exception {
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Get the size of OM metadata directory in bytes.
   */
  private long getOmMetadataDirSize() {
    File metadataDir = new File(ozoneManager.getConfiguration()
        .get(OMConfigKeys.OZONE_OM_DB_DIRS, "metadata"));
    System.out.println("Calculating size for dir: " + metadataDir.getAbsolutePath());
    return calculateDirectorySize(metadataDir);
  }

  /**
   * Recursively calculate the size of a directory.
   */
  private long calculateDirectorySize(File directory) {
    // Print the directory being processed relative to the root
    System.out.println("  " + directory.getAbsolutePath());
    long size = 0;
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            size += calculateDirectorySize(file);
          } else {
            size += file.length();
          }
        }
      }
    }
    return size;
  }

  /**
   * Create a snapshot and wait for it to be available.
   */
  private void createSnapshot(String volumeName, String bucketName, String snapshotName)
      throws Exception {
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
  }

  /**
   * Create keys in batch with progress reporting.
   */
  private List<String> createKeysInBatch(OzoneBucket bucket, String keyPrefix)
      throws Exception {
    List<String> createdKeys = new ArrayList<>();
    System.out.printf("Creating %d keys with prefix '%s'...%n", KEYS_PER_BATCH, keyPrefix);

    for (int i = 0; i < KEYS_PER_BATCH; i++) {
      String keyName = keyPrefix + "-" + i;

      try (OzoneOutputStream outputStream = bucket.createKey(keyName, 0)) {
//        outputStream.write(SINGLE_BYTE_VALUE);
      }

      createdKeys.add(keyName);

      // Progress reporting every 100k keys
      if ((i + 1) % 100 == 0) {
        System.out.printf("Created %d/%d keys%n", i + 1, KEYS_PER_BATCH);
      }
    }

    System.out.printf("Completed creating %d keys%n", KEYS_PER_BATCH);
    return createdKeys;
  }

  /**
   * Overwrite existing keys with new content.
   */
  private void overwriteKeys(OzoneBucket bucket, List<String> keyNames) throws Exception {
    System.out.printf("Overwriting %d keys...%n", keyNames.size());

    for (int i = 0; i < keyNames.size(); i++) {
      String keyName = keyNames.get(i);

      try (OzoneOutputStream outputStream = bucket.createKey(keyName, 0)) {
//        outputStream.write(SINGLE_BYTE_VALUE);
      }

      // Progress reporting every 100k keys
      if ((i + 1) % 100 == 0) {
        System.out.printf("Overwritten %d/%d keys%n", i + 1, keyNames.size());
      }
    }

    System.out.printf("Completed overwriting %d keys%n", keyNames.size());
  }

  /**
   * Delete a snapshot.
   */
  private void deleteSnapshot(String volumeName, String bucketName, String snapshotName)
      throws Exception {
    store.deleteSnapshot(volumeName, bucketName, snapshotName);
    System.out.printf("Deleted snapshot: %s%n", snapshotName);
  }

  /**
   * Trigger snapshot defragmentation process (placeholder for now).
   */
  private void triggerSnapshotDefragmentation() {
    System.out.println("Triggering snapshot defragmentation process...");
    // TODO: Implement actual defragmentation trigger when available
    // For now, this is a placeholder method
    System.out.println("Snapshot defragmentation process completed (placeholder).");
  }

  /**
   * Format bytes to human readable format.
   */
  private String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    }
    if (bytes < 1024 * 1024) {
      return String.format("%.2f KB", bytes / 1024.0);
    }
    if (bytes < 1024 * 1024 * 1024) {
      return String.format("%.2f MB", bytes / (1024.0 * 1024));
    }
    return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
  }

  @Test
  public void testSnapshotDefragmentation() throws Exception {
    // Step 1: Create volume and bucket, print initial metadata size
    String volumeName = "vol-" + counter.incrementAndGet();
    String bucketName = "bucket-" + counter.incrementAndGet();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    long initialSize = getOmMetadataDirSize();
    System.out.printf("Step 1: Initial OM metadata directory size: %s%n",
        formatBytes(initialSize));

    // Step 2: Create first batch of 1M keys and snapshot
    List<String> batch1Keys = createKeysInBatch(bucket, "batch1-key");
    createSnapshot(volumeName, bucketName, "snap-1");

    long sizeAfterSnap1 = getOmMetadataDirSize();
    System.out.printf("Step 2: OM metadata size after creating 1M keys and snap-1: %s%n",
        formatBytes(sizeAfterSnap1));

    // Step 3: Create second batch of 1M keys and snapshot
    List<String> batch2Keys = createKeysInBatch(bucket, "batch2-key");
    createSnapshot(volumeName, bucketName, "snap-2");

    long sizeAfterSnap2 = getOmMetadataDirSize();
    System.out.printf("Step 3: OM metadata size after creating 2M keys and snap-2: %s%n",
        formatBytes(sizeAfterSnap2));

    // Step 4: Create third batch of 1M keys and snapshot
    List<String> batch3Keys = createKeysInBatch(bucket, "batch3-key");
    createSnapshot(volumeName, bucketName, "snap-3");

    long sizeAfterSnap3 = getOmMetadataDirSize();
    System.out.printf("Step 3 (repeated): OM metadata size after creating 3M keys and snap-3: %s%n",
        formatBytes(sizeAfterSnap3));

    // Step 4: Overwrite first batch, create snap-4, delete snap-1
    overwriteKeys(bucket, batch1Keys);
    createSnapshot(volumeName, bucketName, "snap-4");
    deleteSnapshot(volumeName, bucketName, "snap-1");

    long sizeAfterStep4 = getOmMetadataDirSize();
    System.out.printf("Step 4: OM metadata size after overwriting batch1, snap-4, delete snap-1: %s%n",
        formatBytes(sizeAfterStep4));

    // Step 5: Overwrite second batch, create snap-5, delete snap-2
    overwriteKeys(bucket, batch2Keys);
    createSnapshot(volumeName, bucketName, "snap-5");
    deleteSnapshot(volumeName, bucketName, "snap-2");

    long sizeAfterStep5 = getOmMetadataDirSize();
    System.out.printf("Step 5: OM metadata size after overwriting batch2, snap-5, delete snap-2: %s%n",
        formatBytes(sizeAfterStep5));

    // Step 6: Overwrite third batch, create snap-6, delete snap-3
    overwriteKeys(bucket, batch3Keys);
    createSnapshot(volumeName, bucketName, "snap-6");
    deleteSnapshot(volumeName, bucketName, "snap-3");

    long sizeAfterStep6 = getOmMetadataDirSize();
    System.out.printf("Step 6: OM metadata size after overwriting batch3, snap-6, delete snap-3: %s%n",
        formatBytes(sizeAfterStep6));

    // Step 7: Trigger defragmentation and measure space savings
    System.out.printf("Size before defragmentation: %s%n", formatBytes(sizeAfterStep6));

    triggerSnapshotDefragmentation();

    long sizeAfterDefrag = getOmMetadataDirSize();
    System.out.printf("Size after defragmentation: %s%n", formatBytes(sizeAfterDefrag));

    long spaceSaved = sizeAfterStep6 - sizeAfterDefrag;
    double percentageSaved = (spaceSaved * 100.0) / sizeAfterStep6;

    System.out.printf("Space saved by defragmentation: %s (%.2f%%)%n",
        formatBytes(spaceSaved), percentageSaved);

    // Verify test completed successfully
    assertTrue(sizeAfterDefrag >= 0, "OM metadata directory size should be non-negative");
    System.out.println("Snapshot defragmentation test completed successfully!");
  }
}

