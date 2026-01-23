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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.Test;

/**
 * Verifies OM startup with different layout.
 */
public class TestOMStartupWithBucketLayout {

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;

  public static void startCluster(OzoneConfiguration conf)
      throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf)
        .withoutDatanodes().build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }

  public static void restartCluster()
      throws Exception {
    // restart om
    cluster.stop();
    cluster.getOzoneManager().restart();
  }

  public static void teardown() {
    IOUtils.closeQuietly(client);
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
      OzoneBucket bucket1 = TestDataUtil.createVolumeAndBucket(client,
          BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket1, BucketLayout.FILE_SYSTEM_OPTIMIZED);

      // 3. verify OM default behavior with empty
      restartCluster();
      OzoneBucket bucket2 = TestDataUtil.createVolumeAndBucket(client,
          null);
      verifyBucketLayout(bucket2, BucketLayout.FILE_SYSTEM_OPTIMIZED);

      // 4. create bucket with OBS bucket layout and verify
      restartCluster();
      OzoneBucket bucket3 = TestDataUtil.createVolumeAndBucket(client,
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
      OzoneBucket bucket1 = TestDataUtil.createVolumeAndBucket(client,
          BucketLayout.FILE_SYSTEM_OPTIMIZED);
      verifyBucketLayout(bucket1, BucketLayout.FILE_SYSTEM_OPTIMIZED);

      // 3. verify OM default behavior with empty
      restartCluster();
      OzoneBucket bucket2 = TestDataUtil.createVolumeAndBucket(client,
          null);
      verifyBucketLayout(bucket2, BucketLayout.OBJECT_STORE);

      // 4. create bucket with OBS bucket layout and verify
      restartCluster();
      OzoneBucket bucket3 = TestDataUtil.createVolumeAndBucket(client,
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
    assertNotNull(bucket);
    assertEquals(metadataLayout, bucket.getBucketLayout());
  }

}
