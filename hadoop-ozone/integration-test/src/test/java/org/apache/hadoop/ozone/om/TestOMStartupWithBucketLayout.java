/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.UUID;

/**
 * Verifies OM startup with different layout.
 */
public class TestOMStartupWithBucketLayout {

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private static MiniOzoneCluster cluster;

  public static void startClusterWithSimpleLayout() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.OBJECT_STORE.name());
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();
  }

  public static void startClusterWithPrefixLayout() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.FILE_SYSTEM_OPTIMIZED.name());
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();
  }

  public static void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testRestartWithPrefixLayout() throws Exception {
    startClusterWithPrefixLayout();
    try {
      OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster,
          BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket, BucketLayout.FILE_SYSTEM_OPTIMIZED, true);

      cluster.stop();
      cluster.getOzoneManager().restart();

      bucket = TestDataUtil.createVolumeAndBucket(cluster,
          BucketLayout.LEGACY);
      verifyBucketLayout(bucket, BucketLayout.FILE_SYSTEM_OPTIMIZED, true);

      cluster.stop();
      cluster.getOzoneManager().restart();

      bucket = TestDataUtil.createVolumeAndBucket(cluster,
          BucketLayout.OBJECT_STORE);
      verifyBucketLayout(bucket, BucketLayout.OBJECT_STORE, false);

      cluster.stop();
      cluster.getOzoneManager().restart();

      bucket = TestDataUtil.createVolumeAndBucket(cluster,
          BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket, BucketLayout.FILE_SYSTEM_OPTIMIZED, true);

    } finally {
      teardown();
    }
  }

  @Test
  public void testRestartWithSimpleLayout() throws Exception {
    startClusterWithSimpleLayout();
    try {
      OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster,
          BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket, BucketLayout.FILE_SYSTEM_OPTIMIZED, true);

      cluster.stop();
      cluster.getOzoneManager().restart();

      bucket = TestDataUtil.createVolumeAndBucket(cluster,
          BucketLayout.LEGACY);
      verifyBucketLayout(bucket, BucketLayout.OBJECT_STORE, false);

      cluster.stop();
      cluster.getOzoneManager().restart();

      bucket = TestDataUtil.createVolumeAndBucket(cluster,
          BucketLayout.OBJECT_STORE);
      verifyBucketLayout(bucket, BucketLayout.OBJECT_STORE, false);

      cluster.stop();
      cluster.getOzoneManager().restart();

      bucket = TestDataUtil.createVolumeAndBucket(cluster,
          BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket, BucketLayout.FILE_SYSTEM_OPTIMIZED, true);

    } finally {
      teardown();
    }
  }

  private void verifyBucketLayout(OzoneBucket bucket,
      BucketLayout metadataLayout, boolean isFSOBucket) {
    Assert.assertNotNull(bucket);
    Assert.assertEquals(isFSOBucket,
        bucket.getBucketLayout().isFileSystemOptimized());
    Assert.assertEquals(metadataLayout, bucket.getBucketLayout());
  }

}