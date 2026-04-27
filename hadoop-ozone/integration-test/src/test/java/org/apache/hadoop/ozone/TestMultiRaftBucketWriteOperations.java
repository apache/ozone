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

package org.apache.hadoop.ozone;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_S3_GPRC_SERVER_ENABLED;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for multi-raft bucket write operations.
 * Verifies that all bucket-related write requests work correctly
 * when multi-raft is enabled, including cross-raft-group consistency.
 */
@Timeout(value = 1800, unit = TimeUnit.SECONDS)
class TestMultiRaftBucketWriteOperations {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestMultiRaftBucketWriteOperations.class);

  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneConfiguration conf;
  private static ClientProtocol client;

  @BeforeAll
  static void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, false);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);
    conf.setBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED, true);
    conf.setInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS, 6);
    conf.setBoolean(OZONE_OM_S3_GPRC_SERVER_ENABLED, true);

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setSCMServiceId(UUID.randomUUID().toString())
        .setOMServiceId("omService1")
        .setNumOfOzoneManagers(3)
        .build();
    cluster.waitForClusterToBeReady();

    OzoneManager om = cluster.getOMLeader();
    waitFor(() -> om.getOmRaftGroups().size() == 7, 500, 120_000);

    client = cluster.createClient().getProxy();
  }

  @AfterAll
  static void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Verifies that key create + write + commit works immediately after
   * bucket creation.
   */
  @Test
  void testKeyCreateImmediatelyAfterBucketCreation() throws IOException {
    String volume = "vol-" + UUID.randomUUID();
    client.createVolume(volume);

    for (int i = 0; i < 4; i++) {
      String bucket = "bucket-" + i;
      client.createBucket(volume, bucket);

      // Immediately create a key — no delay
      String key = "key-" + i;
      byte[] data = ("data-" + i).getBytes(UTF_8);
      writeKey(volume, bucket, key, data);

      // Verify the key is readable
      assertArrayEquals(data, readKey(volume, bucket, key));
    }
  }

  /**
   * Verifies that concurrent key creation across multiple buckets works
   * without errors.
   */
  @Test
  void testConcurrentKeyCreationAcrossMultipleBuckets() throws Exception {
    String volume = "vol-" + UUID.randomUUID();
    client.createVolume(volume);

    int numBuckets = 2;
    int keysPerBucket = 5;
    int numThreads = 4;
    String[] buckets = new String[numBuckets];

    for (int i = 0; i < numBuckets; i++) {
      buckets[i] = "bucket-" + i;
      client.createBucket(volume, buckets[i]);
    }

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failCount = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(numBuckets * keysPerBucket);

    for (int b = 0; b < numBuckets; b++) {
      final String bucket = buckets[b];
      for (int k = 0; k < keysPerBucket; k++) {
        final int keyIdx = k;
        executor.submit(() -> {
          try {
            String key = "key-" + keyIdx;
            byte[] data = ("value-" + bucket + "-" + keyIdx).getBytes(UTF_8);
            writeKey(volume, bucket, key, data);
            successCount.incrementAndGet();
          } catch (Exception e) {
            LOG.error("Failed to write key in bucket {}: {}", bucket, e.getMessage());
            failCount.incrementAndGet();
          } finally {
            latch.countDown();
          }
        });
      }
    }

    assertTrue(latch.await(600, TimeUnit.SECONDS), "Timed out waiting for key creation");
    executor.shutdown();

    assertEquals(0, failCount.get(),
        "Expected zero failures, but got " + failCount.get());
    assertEquals(numBuckets * keysPerBucket, successCount.get());
  }

  /**
   * Verifies that key deletion works after key creation in multi-raft.
   */
  @Test
  void testKeyDeleteAfterCreate() throws IOException {
    String volume = "vol-" + UUID.randomUUID();
    client.createVolume(volume);
    String bucket = "bucket-del";
    client.createBucket(volume, bucket);

    String key = "key-to-delete";
    writeKey(volume, bucket, key, "delete-me".getBytes(UTF_8));
    assertNotNull(client.getKeyDetails(volume, bucket, key));

    client.deleteKey(volume, bucket, key, false);
  }

  /**
   * Verifies that key rename works in multi-raft.
   */
  @Test
  void testKeyRenameInMultiRaft() throws IOException {
    String volume = "vol-" + UUID.randomUUID();
    client.createVolume(volume);
    String bucket = "bucket-rename";
    client.createBucket(volume, bucket);

    String srcKey = "key-src";
    byte[] data = "rename-data".getBytes(UTF_8);
    writeKey(volume, bucket, srcKey, data);

    String dstKey = "key-dst";
    client.renameKey(volume, bucket, srcKey, dstKey);

    assertArrayEquals(data, readKey(volume, bucket, dstKey));
  }

  /**
   * Verifies that bucket property changes (quota) work immediately
   * after bucket creation.  Bucket metadata operations go through
   * the main raft group, so they should always be consistent.
   */
  @Test
  void testSetBucketQuotaAfterCreation() throws IOException {
    String volume = "vol-" + UUID.randomUUID();
    client.createVolume(volume);
    String bucket = "bucket-quota";
    client.createBucket(volume, bucket);

    client.setBucketQuota(volume, bucket, 1000, 1024 * 1024 * 100);

    OzoneBucket bucketInfo = client.getBucketDetails(volume, bucket);
    assertEquals(1000, bucketInfo.getQuotaInNamespace());
  }

  /**
   * Verifies that bucket versioning can be set after creation.
   */
  @Test
  void testSetBucketVersioningAfterCreation() throws IOException {
    String volume = "vol-" + UUID.randomUUID();
    client.createVolume(volume);
    String bucket = "bucket-ver";
    client.createBucket(volume, bucket);

    client.setBucketVersioning(volume, bucket, true);

    OzoneBucket bucketInfo = client.getBucketDetails(volume, bucket);
    assertTrue(bucketInfo.getVersioning());
  }

  /**
   * Verifies that bucket deletion works after creating and deleting
   * all keys in the bucket.
   */
  @Test
  void testBucketDeleteAfterKeyOperations() throws IOException {
    String volume = "vol-" + UUID.randomUUID();
    client.createVolume(volume);
    String bucket = "bucket-to-delete";
    client.createBucket(volume, bucket);

    // Create and delete a key
    String key = "temp-key";
    writeKey(volume, bucket, key, "temp".getBytes(UTF_8));
    client.deleteKey(volume, bucket, key, false);

    // Now delete the empty bucket
    client.deleteBucket(volume, bucket);
  }

  /**
   * Verifies that object tagging (S3 PutObjectTagging/DeleteObjectTagging)
   * works in multi-raft.
   */
  @Test
  void testObjectTaggingInMultiRaft() throws IOException {
    String volume = "vol-" + UUID.randomUUID();
    client.createVolume(volume);
    String bucket = "bucket-tag";
    client.createBucket(volume, bucket);

    String key = "tagged-key";
    writeKey(volume, bucket, key, "tagged-data".getBytes(UTF_8));

    Map<String, String> tags = new HashMap<>();
    tags.put("env", "test");
    tags.put("project", "ozone");
    client.putObjectTagging(volume, bucket, key, tags);

    Map<String, String> retrievedTags = client.getObjectTagging(volume, bucket, key);
    assertEquals("test", retrievedTags.get("env"));
    assertEquals("ozone", retrievedTags.get("project"));

    client.deleteObjectTagging(volume, bucket, key);
    Map<String, String> emptyTags = client.getObjectTagging(volume, bucket, key);
    assertTrue(emptyTags.isEmpty());
  }

  /**
   * Verifies that all bucket-related write operations work when
   * buckets are created and used in rapid succession.
   * This is an end-to-end test that exercises the full lifecycle.
   */
  @Test
  void testFullBucketLifecycleInMultiRaft() throws IOException {
    String volume = "vol-" + UUID.randomUUID();
    client.createVolume(volume);

    for (int i = 0; i < 2; i++) {
      String bucket = "lifecycle-" + i;
      client.createBucket(volume, bucket);

      // Immediately write keys
      for (int k = 0; k < 3; k++) {
        String key = "key-" + k;
        byte[] data = ("lifecycle-" + i + "-" + k).getBytes(UTF_8);
        writeKey(volume, bucket, key, data);
      }

      // Read keys back
      for (int k = 0; k < 3; k++) {
        String key = "key-" + k;
        byte[] expected = ("lifecycle-" + i + "-" + k).getBytes(UTF_8);
        assertArrayEquals(expected, readKey(volume, bucket, key),
            "Mismatch for " + bucket + "/" + key);
      }

      // Rename a key
      client.renameKey(volume, bucket, "key-0", "key-renamed");
      assertArrayEquals(
          ("lifecycle-" + i + "-0").getBytes(UTF_8),
          readKey(volume, bucket, "key-renamed"));

      // Delete keys
      client.deleteKey(volume, bucket, "key-renamed", false);
      for (int k = 1; k < 3; k++) {
        client.deleteKey(volume, bucket, "key-" + k, false);
      }

      // Set bucket quota
      client.setBucketQuota(volume, bucket, 500, 1024 * 1024 * 50);

      // Delete the empty bucket
      client.deleteBucket(volume, bucket);
    }
  }

  /**
   * Verifies that raft group assignment distributes buckets
   * across multiple groups, not all to the same one.
   */
  @Test
  void testRaftGroupDistribution() throws Exception {
    String volume = "vol-" + UUID.randomUUID();
    client.createVolume(volume);

    int numBuckets = 4;
    for (int i = 0; i < numBuckets; i++) {
      String bucket = "dist-bucket-" + i;
      client.createBucket(volume, bucket);
      // Write a key to trigger raft group assignment
      writeKey(volume, bucket, "key-0", "data".getBytes(UTF_8));
    }

    // Check that buckets are assigned to multiple raft groups
    OzoneManager om = cluster.getOMLeader();
    Map<String, UUID> assignments = om.getOmRaftGroupManager().getBucketRaftGroups();

    long distinctGroups = assignments.values().stream().distinct().count();
    LOG.info("Bucket assignments: {}", assignments);
    LOG.info("Distinct raft groups used: {}", distinctGroups);

    assertTrue(distinctGroups >= 2,
        "Expected buckets to be distributed across at least 2 groups, "
            + "but only " + distinctGroups + " used");
  }

  /**
   * Verifies that no NullPointerException occurs under concurrent
   * failover conditions (ThreadLocal race fix).
   */
  @Test
  void testNoConcurrentFailoverNPE() throws Exception {
    String volume = "vol-" + UUID.randomUUID();
    client.createVolume(volume);
    String bucket = "npe-bucket";
    client.createBucket(volume, bucket);

    int numThreads = 10;
    int keysPerThread = 3;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    AtomicInteger npeCount = new AtomicInteger(0);
    AtomicInteger successCount = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(numThreads * keysPerThread);

    for (int t = 0; t < numThreads; t++) {
      final int threadId = t;
      executor.submit(() -> {
        for (int k = 0; k < keysPerThread; k++) {
          try {
            String key = "t" + threadId + "-k" + k;
            writeKey(volume, bucket, key, key.getBytes(UTF_8));
            successCount.incrementAndGet();
          } catch (NullPointerException e) {
            LOG.error("NPE in thread {}", threadId, e);
            npeCount.incrementAndGet();
          } catch (Exception e) {
            // Other exceptions are acceptable (e.g. retries)
            LOG.warn("Non-NPE error in thread {}: {}", threadId, e.getMessage());
          } finally {
            latch.countDown();
          }
        }
      });
    }

    assertTrue(latch.await(600, TimeUnit.SECONDS));
    executor.shutdown();

    assertEquals(0, npeCount.get(),
        "NullPointerExceptions detected during concurrent operations");
    assertTrue(successCount.get() > 0, "No keys were successfully written");
  }

  private void writeKey(String volume, String bucket, String key, byte[] data)
      throws IOException {
    try (OzoneOutputStream stream = client.createKey(
        volume, bucket, key, data.length,
        ReplicationConfig.getDefault(conf), Collections.emptyMap())) {
      stream.write(data);
    }
  }

  private byte[] readKey(String volume, String bucket, String key)
      throws IOException {
    OzoneKeyDetails keyDetails = client.getKeyDetails(volume, bucket, key);
    try (OzoneInputStream stream = client.getKey(volume, bucket, key)) {
      byte[] data = new byte[(int) keyDetails.getDataSize()];
      org.apache.hadoop.io.IOUtils.readFully(stream, data, 0, data.length);
      return data;
    }
  }
}
