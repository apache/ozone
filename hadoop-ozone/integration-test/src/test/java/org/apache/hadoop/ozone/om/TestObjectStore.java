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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests to verify Object store without prefix enabled.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestObjectStore implements NonHATests.TestCase {
  private OzoneConfiguration conf;
  private OzoneClient client;

  @BeforeAll
  void init() throws Exception {
    conf = cluster().getConf();
    client = cluster().newClient();
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(client);
  }

  @Test
  public void testCreateBucketWithBucketLayout() throws Exception {
    String sampleVolumeName = UUID.randomUUID().toString();
    String sampleBucketName = UUID.randomUUID().toString();
    ObjectStore store = client.getObjectStore();
    store.createVolume(sampleVolumeName);
    OzoneVolume volume = store.getVolume(sampleVolumeName);

    // Case 1: Bucket layout: Empty and OM default bucket layout: FSO
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    volume.createBucket(sampleBucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(sampleBucketName);
    assertEquals(sampleBucketName, bucket.getName());
    assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        bucket.getBucketLayout());

    // Case 2: Bucket layout: OBJECT_STORE
    sampleBucketName = UUID.randomUUID().toString();
    builder.setBucketLayout(BucketLayout.OBJECT_STORE);
    volume.createBucket(sampleBucketName, builder.build());
    bucket = volume.getBucket(sampleBucketName);
    assertEquals(sampleBucketName, bucket.getName());
    assertEquals(BucketLayout.OBJECT_STORE,
        bucket.getBucketLayout());

    // Case 3: Bucket layout: LEGACY
    sampleBucketName = UUID.randomUUID().toString();
    builder.setBucketLayout(BucketLayout.LEGACY);
    volume.createBucket(sampleBucketName, builder.build());
    bucket = volume.getBucket(sampleBucketName);
    assertEquals(sampleBucketName, bucket.getName());
    assertEquals(BucketLayout.LEGACY, bucket.getBucketLayout());

    // Case 3: Bucket layout: FILE_SYSTEM_OPTIMIZED
    sampleBucketName = UUID.randomUUID().toString();
    builder.setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    volume.createBucket(sampleBucketName, builder.build());
    bucket = volume.getBucket(sampleBucketName);
    assertEquals(sampleBucketName, bucket.getName());
    assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        bucket.getBucketLayout());
  }

  /**
   * Ensure Link Buckets have same BucketLayout as source buckets.
   * @throws Exception
   */
  @Test
  public void testCreateLinkBucketWithBucketLayout() throws Exception {
    String volumeName = UUID.randomUUID().toString();

    String sourceBucket1Name = UUID.randomUUID().toString();
    BucketLayout sourceBucket1Layout = BucketLayout.FILE_SYSTEM_OPTIMIZED;

    String sourceBucket2Name = UUID.randomUUID().toString();
    BucketLayout sourceBucket2Layout = BucketLayout.OBJECT_STORE;

    String linkBucket1Name = UUID.randomUUID().toString();
    String linkBucket2Name = UUID.randomUUID().toString();
    // Chained link bucket
    String linkBucket3Name = UUID.randomUUID().toString();

    ObjectStore store = client.getObjectStore();

    // Create volume
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);

    // Create source buckets
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setBucketLayout(sourceBucket1Layout);
    volume.createBucket(sourceBucket1Name, builder.build());
    builder.setBucketLayout(sourceBucket2Layout);
    volume.createBucket(sourceBucket2Name, builder.build());

    // Create link buckets
    createLinkBucket(volume, sourceBucket1Name, linkBucket1Name);
    createLinkBucket(volume, sourceBucket2Name, linkBucket2Name);
    // linkBucket3 is chained onto linkBucket1
    createLinkBucket(volume, linkBucket1Name, linkBucket3Name);

    // Check that Link Buckets' layouts match source bucket layouts
    OzoneBucket bucket = volume.getBucket(linkBucket1Name);
    assertEquals(sourceBucket1Layout, bucket.getBucketLayout());

    bucket = volume.getBucket(linkBucket2Name);
    assertEquals(sourceBucket2Layout, bucket.getBucketLayout());

    // linkBucket3 is chained onto linkBucket1, hence its bucket layout matches
    // linkBucket1's source bucket.
    bucket = volume.getBucket(linkBucket3Name);
    assertEquals(sourceBucket1Layout, bucket.getBucketLayout());
    assertEquals(linkBucket1Name, bucket.getSourceBucket());
  }

  @Test
  public void testCreateDanglingLinkBucket() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    // Does not exist
    String sourceBucketName = UUID.randomUUID().toString();

    ObjectStore store = client.getObjectStore();

    // Create volume
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);

    // Dangling link bucket
    String danglingLinkBucketName = UUID.randomUUID().toString();

    // danglingLinkBucket is a dangling link over a source bucket that doesn't
    // exist.
    createLinkBucket(volume, sourceBucketName, danglingLinkBucketName);

    // since sourceBucket does not exist, layout depends on
    // OZONE_DEFAULT_BUCKET_LAYOUT config.
    OzoneBucket bucket = volume.getBucket(danglingLinkBucketName);
    assertEquals(BucketLayout.fromString(
            conf.get(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT)),
        bucket.getBucketLayout());
    assertEquals(sourceBucketName, bucket.getSourceBucket());
  }

  @Test
  public void testLoopInLinkBuckets() throws Exception {
    String volumeName = UUID.randomUUID().toString();

    ObjectStore store = client.getObjectStore();

    // Create volume
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);

    String linkBucket1Name = UUID.randomUUID().toString();
    String linkBucket2Name = UUID.randomUUID().toString();
    String linkBucket3Name = UUID.randomUUID().toString();

    // Create a loop in the link buckets
    createLinkBucket(volume, linkBucket1Name, linkBucket2Name);
    createLinkBucket(volume, linkBucket2Name, linkBucket3Name);
    createLinkBucket(volume, linkBucket3Name, linkBucket1Name);

    OMException oe =
        assertThrows(OMException.class, () -> volume.getBucket(linkBucket1Name),
            "Should throw Exception due to loop in Link Buckets");
    // Expected exception
    assertEquals(OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS,
        oe.getResult());
  }

  /**
   * Helper method to create Link Buckets.
   *
   * @param sourceVolume Name of source volume for Link Bucket.
   * @param sourceBucket Name of source bucket for Link Bucket.
   * @param linkBucket   Name of Link Bucket
   * @throws IOException
   */
  private void createLinkBucket(OzoneVolume sourceVolume, String sourceBucket,
                                String linkBucket) throws IOException {
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setBucketLayout(BucketLayout.DEFAULT)
        .setSourceVolume(sourceVolume.getName())
        .setSourceBucket(sourceBucket);
    sourceVolume.createBucket(linkBucket, builder.build());
  }
}
