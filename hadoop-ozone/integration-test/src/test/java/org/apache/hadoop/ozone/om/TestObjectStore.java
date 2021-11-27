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
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.*;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.UUID;

/**
 * Tests to verify Object store without prefix enabled.
 */
public class TestObjectStore {
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String omId;

  @Rule
  public Timeout timeout = new Timeout(1200000);

  /**
   * Create a MiniOzoneCluster for testing.
   * <p>
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Shutdown MiniOzoneCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testCreateBucketWithBucketLayout() throws Exception {
    String sampleVolumeName = UUID.randomUUID().toString();
    String sampleBucketName = UUID.randomUUID().toString();
    OzoneClient client = cluster.getClient();
    ObjectStore store = client.getObjectStore();
    store.createVolume(sampleVolumeName);
    OzoneVolume volume = store.getVolume(sampleVolumeName);

    // Case 1: Bucket layout: Empty and OM default bucket layout: OBJECT_STORE
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    volume.createBucket(sampleBucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(sampleBucketName);
    Assert.assertEquals(sampleBucketName, bucket.getName());
    Assert.assertEquals(BucketLayout.OBJECT_STORE,
        bucket.getBucketLayout());

    // Case 2: Bucket layout: DEFAULT
    sampleBucketName = UUID.randomUUID().toString();
    builder.setBucketLayout(BucketLayout.DEFAULT);
    volume.createBucket(sampleBucketName, builder.build());
    bucket = volume.getBucket(sampleBucketName);
    Assert.assertEquals(sampleBucketName, bucket.getName());
    Assert.assertEquals(BucketLayout.OBJECT_STORE,
        bucket.getBucketLayout());

    // Case 3: Bucket layout: LEGACY
    sampleBucketName = UUID.randomUUID().toString();
    builder.setBucketLayout(BucketLayout.LEGACY);
    volume.createBucket(sampleBucketName, builder.build());
    bucket = volume.getBucket(sampleBucketName);
    Assert.assertEquals(sampleBucketName, bucket.getName());
    Assert.assertNotEquals(BucketLayout.LEGACY, bucket.getBucketLayout());
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

    OzoneClient client = cluster.getClient();
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
    Assert.assertEquals(sourceBucket1Layout, bucket.getBucketLayout());

    bucket = volume.getBucket(linkBucket2Name);
    Assert.assertEquals(sourceBucket2Layout, bucket.getBucketLayout());

    // linkBucket3 is chained onto linkBucket1, hence its bucket layout matches
    // linkBucket1's source bucket.
    bucket = volume.getBucket(linkBucket3Name);
    Assert.assertEquals(sourceBucket1Layout, bucket.getBucketLayout());
    Assert.assertEquals(linkBucket1Name, bucket.getSourceBucket());
  }

  @Test
  public void testCreateDanglingLinkBucket() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    // Does not exist
    String sourceBucketName = UUID.randomUUID().toString();

    OzoneClient client = cluster.getClient();
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
    Assert.assertEquals(BucketLayout.fromString(
            conf.get(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT)),
        bucket.getBucketLayout());
    Assert.assertEquals(sourceBucketName, bucket.getSourceBucket());
  }

  @Test
  public void testLoopInLinkBuckets() throws Exception {
    String volumeName = UUID.randomUUID().toString();

    OzoneClient client = cluster.getClient();
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

    try {
      volume.getBucket(linkBucket1Name);
      Assert.fail("Should throw Exception due to loop in Link Buckets");
    } catch (OMException oe) {
      // Expected exception
    }
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
