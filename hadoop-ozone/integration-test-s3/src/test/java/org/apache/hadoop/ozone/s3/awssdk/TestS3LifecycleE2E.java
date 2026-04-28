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

package org.apache.hadoop.ozone.s3.awssdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmLCAbortIncompleteMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.service.KeyLifecycleService;
import org.apache.hadoop.ozone.s3.OzoneConfigurationHolder;
import org.apache.hadoop.ozone.s3.S3ClientFactory;
import org.apache.hadoop.ozone.s3.S3GatewayService;
import org.apache.ozone.test.ClusterForTests;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;

/**
 * End-to-end tests for KeyLifecycleService with AbortIncompleteMultipartUpload.
 * This test class creates a separate cluster with lifecycle service enabled.
 */
class TestS3LifecycleE2E extends ClusterForTests<MiniOzoneCluster> {

  private S3Client s3Client;

  @Override
  protected OzoneConfiguration createOzoneConfig() {
    OzoneConfiguration conf = createBaseConfiguration();
    // Enable lifecycle service
    conf.setBoolean(OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_ENABLED, true);
    // Set a short interval for faster testing
    conf.setTimeDuration(OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL,
        1, TimeUnit.SECONDS);
    // Use OBJECT_STORE layout for simpler MPU key format (name-based)
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE.name());
    return conf;
  }

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    return newClusterBuilder()
        .addService(new S3GatewayService())
        .build();
  }

  @Override
  protected void onClusterReady() throws Exception {
    // Use OzoneConfigurationHolder to get the S3 Gateway configuration
    // which has the actual HTTP address (random port assigned at start)
    S3ClientFactory s3Factory = new S3ClientFactory(OzoneConfigurationHolder.configuration());
    s3Client = s3Factory.createS3ClientV2();
  }

  /**
   * End-to-end test verifying that KeyLifecycleService correctly aborts
   * incomplete multipart uploads based on lifecycle configuration.
   *
   * This test:
   * 1. Creates multipart uploads using S3 API
   * 2. Modifies creation times directly in OM metadata to simulate old uploads
   * 3. Sets lifecycle rule with prefix "temp/" and daysAfterInitiation=1
   * 4. Verifies only old MPUs with matching prefix are aborted
   */
  @Test
  void testAbortIncompleteMultipartUploadE2E() throws Exception {
    final String bucketName = "test-lifecycle-" + UUID.randomUUID().toString().substring(0, 8);
    s3Client.createBucket(b -> b.bucket(bucketName));

    // Get S3 volume name and metadata manager
    String s3VolumeName;
    try (OzoneClient ozoneClient = getCluster().newClient()) {
      s3VolumeName = ozoneClient.getObjectStore().getS3Volume().getName();
    }

    OzoneManager ozoneManager = getCluster().getOzoneManager();
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();

    // Initiate multipart uploads using S3 API
    // MPU 1: matches prefix "temp/" with old creation time - should be aborted
    String matchingOldKey = "temp/old-file.txt";
    CreateMultipartUploadResponse mpu1 = s3Client.createMultipartUpload(
        b -> b.bucket(bucketName).key(matchingOldKey));

    // MPU 2: does NOT match prefix with old creation time - should remain
    String nonMatchingOldKey = "permanent/old-file.txt";
    CreateMultipartUploadResponse mpu2 = s3Client.createMultipartUpload(
        b -> b.bucket(bucketName).key(nonMatchingOldKey));

    // MPU 3: matches prefix "temp/" with recent creation time - should remain
    String matchingRecentKey = "temp/recent-file.txt";
    s3Client.createMultipartUpload(b -> b.bucket(bucketName).key(matchingRecentKey));

    // Verify all three MPUs exist
    ListMultipartUploadsResponse listBefore =
        s3Client.listMultipartUploads(b -> b.bucket(bucketName));
    assertEquals(3, listBefore.uploads().size());

    // Modify creation times to simulate old uploads (2 days ago)
    long oldCreationTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2);
    updateMpuCreationTime(metadataManager, s3VolumeName, bucketName,
        matchingOldKey, mpu1.uploadId(), oldCreationTime);
    updateMpuCreationTime(metadataManager, s3VolumeName, bucketName,
        nonMatchingOldKey, mpu2.uploadId(), oldCreationTime);
    // Keep MPU 3 with recent creation time (no modification needed)

    // Set the lifecycle configuration
    // Rule: abort MPUs with prefix "temp/" after 1 day
    try (OzoneClient ozoneClient = getCluster().newClient()) {
      OzoneVolume s3Volume = ozoneClient.getObjectStore().getS3Volume();
      OzoneBucket bucket = s3Volume.getBucket(bucketName);

      OmLCRule lcRule = new OmLCRule.Builder()
          .setId("abort-temp-uploads")
          .setPrefix("temp/")
          .setEnabled(true)
          .addAction(new OmLCAbortIncompleteMultipartUpload.Builder()
              .setDaysAfterInitiation(1)
              .build())
          .build();

      OmLifecycleConfiguration lifecycleConfig = new OmLifecycleConfiguration.Builder()
          .setVolume(s3VolumeName)
          .setBucket(bucketName)
          .setBucketLayout(bucket.getBucketLayout())
          .addRule(lcRule)
          .build();

      bucket.setLifecycleConfiguration(lifecycleConfig);
    }

    // Trigger KeyLifecycleService to run
    KeyLifecycleService lifecycleService = ozoneManager.getKeyManager().getKeyLifecycleService();
    lifecycleService.runPeriodicalTaskNow();

    // Wait for the abort operation to complete
    GenericTestUtils.waitFor(() -> {
      ListMultipartUploadsResponse response =
          s3Client.listMultipartUploads(b -> b.bucket(bucketName));
      // Should have 2 remaining: non-matching prefix (old) and matching prefix (recent)
      return response.uploads().size() == 2;
    }, 500, 30000);

    // Verify the correct MPUs remain
    ListMultipartUploadsResponse listAfter =
        s3Client.listMultipartUploads(b -> b.bucket(bucketName));
    assertEquals(2, listAfter.uploads().size());

    List<String> remainingKeys = listAfter.uploads().stream()
        .map(u -> u.key())
        .collect(Collectors.toList());

    // The matching old MPU should be aborted
    assertFalse(remainingKeys.contains(matchingOldKey),
        "MPU with matching prefix and old creation time should be aborted");
    // The non-matching prefix MPU should remain
    assertTrue(remainingKeys.contains(nonMatchingOldKey),
        "MPU with non-matching prefix should remain");
    // The recent MPU should remain (even though it matches prefix)
    assertTrue(remainingKeys.contains(matchingRecentKey),
        "MPU with recent creation time should remain");
  }

  /**
   * Helper method to update the creation time of a multipart upload entry.
   */
  private void updateMpuCreationTime(OMMetadataManager metadataManager,
      String volumeName, String bucketName, String keyName,
      String uploadId, long newCreationTime) throws Exception {
    // Get the multipart key
    String multipartKey = metadataManager.getMultipartKey(
        volumeName, bucketName, keyName, uploadId);

    // Get the existing entry
    OmMultipartKeyInfo existingInfo = metadataManager.getMultipartInfoTable().get(multipartKey);
    if (existingInfo == null) {
      throw new RuntimeException("Multipart key info not found for " + multipartKey);
    }

    // Create updated entry with new creation time
    OmMultipartKeyInfo updatedInfo = new OmMultipartKeyInfo.Builder()
        .setUploadID(existingInfo.getUploadID())
        .setCreationTime(newCreationTime)
        .setReplicationConfig(existingInfo.getReplicationConfig())
        .setObjectID(existingInfo.getObjectID())
        .setUpdateID(existingInfo.getUpdateID())
        .setParentID(existingInfo.getParentID())
        .build();

    // Update the entry
    metadataManager.getMultipartInfoTable().put(multipartKey, updatedInfo);
  }
}
