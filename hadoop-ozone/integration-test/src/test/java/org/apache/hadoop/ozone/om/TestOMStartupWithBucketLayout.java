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

  public static void startCluster(OzoneConfiguration conf)
      throws Exception {
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOmId(omId).withoutDatanodes().build();
    cluster.waitForClusterToBeReady();
  }

  public static void restartCluster()
      throws Exception {
    // restart om
    cluster.stop();
    cluster.getOzoneManager().restart();
  }

  public static void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testRestartWithFSOLayout() throws Exception {
    try {
      // 1. build cluster with default bucket layout FSO
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
          BucketLayout.FILE_SYSTEM_OPTIMIZED.name());
      startCluster(conf);

      // 2. create bucket with FSO bucket layout and verify
      OzoneBucket bucket1 = TestDataUtil.createVolumeAndBucket(cluster,
          BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket1, BucketLayout.FILE_SYSTEM_OPTIMIZED);

      // 3. verify OM default behavior with empty
      restartCluster();
      OzoneBucket bucket2 = TestDataUtil.createVolumeAndBucket(cluster,
          null);
      verifyBucketLayout(bucket2, BucketLayout.FILE_SYSTEM_OPTIMIZED);

      // 4. create bucket with OBS bucket layout and verify
      restartCluster();
      OzoneBucket bucket3 = TestDataUtil.createVolumeAndBucket(cluster,
          BucketLayout.OBJECT_STORE);
      verifyBucketLayout(bucket3, BucketLayout.OBJECT_STORE);

      // 5. verify all bucket
      restartCluster();
      verifyBucketLayout(bucket1, BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket2, BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket3, BucketLayout.OBJECT_STORE);

      // 6. verify after update default bucket layout
      conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
          BucketLayout.OBJECT_STORE.name());
      restartCluster();
      verifyBucketLayout(bucket1, BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket2, BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket3, BucketLayout.OBJECT_STORE);
    } finally {
      teardown();
    }
  }

  @Test
  public void testRestartWithOBSLayout() throws Exception {
    try {
      // 1. build cluster with default bucket layout OBS
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
          BucketLayout.OBJECT_STORE.name());
      startCluster(conf);

      // 2. create bucket with FSO bucket layout and verify
      OzoneBucket bucket1 = TestDataUtil.createVolumeAndBucket(cluster,
          BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket1, BucketLayout.FILE_SYSTEM_OPTIMIZED);

      // 3. verify OM default behavior with empty
      restartCluster();
      OzoneBucket bucket2 = TestDataUtil.createVolumeAndBucket(cluster,
          null);
      verifyBucketLayout(bucket2, BucketLayout.OBJECT_STORE);

      // 4. create bucket with OBS bucket layout and verify
      restartCluster();
      OzoneBucket bucket3 = TestDataUtil.createVolumeAndBucket(cluster,
          BucketLayout.OBJECT_STORE);
      verifyBucketLayout(bucket3, BucketLayout.OBJECT_STORE);

      // 5. verify all bucket
      restartCluster();
      verifyBucketLayout(bucket1, BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket2, BucketLayout.OBJECT_STORE);
      verifyBucketLayout(bucket3, BucketLayout.OBJECT_STORE);

      // 6. verify after update default bucket layout
      conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
          BucketLayout.FILE_SYSTEM_OPTIMIZED.name());
      restartCluster();
      verifyBucketLayout(bucket1, BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket2, BucketLayout.OBJECT_STORE);
      verifyBucketLayout(bucket3, BucketLayout.OBJECT_STORE);
    } finally {
      teardown();
    }
  }

  private void verifyBucketLayout(OzoneBucket bucket,
      BucketLayout metadataLayout) {
    Assert.assertNotNull(bucket);
    Assert.assertEquals(metadataLayout, bucket.getBucketLayout());
  }

}
