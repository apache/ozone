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
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METADATA_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METADATA_LAYOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_METADATA_LAYOUT_PREFIX;

/**
 * Verifies OM startup with different layout.
 */
public class TestOMStartupWithLayout {

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private static MiniOzoneCluster cluster;

  @BeforeClass
  public static void startClusterWithSimpleLayout() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();
    TestOMRequestUtils.configureFSOptimizedPaths(conf, true,
        OZONE_OM_METADATA_LAYOUT_DEFAULT);
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();
  }

  @AfterClass
  public static void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testWithDifferentClusterLayout() throws Exception {
    OzoneConfiguration conf = cluster.getOzoneManager().getConfiguration();

    // create a volume and a bucket with default(SIMPLE) metadata format.
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster);
    verifyBucketLayout(bucket, OZONE_OM_METADATA_LAYOUT_DEFAULT, false);

    cluster.getOzoneManager().stop();

    // case-1) Configured cluster layout as PREFIX. Bucket exists with SIMPLE
    // layout format. OM startup should fail.
    conf.set(OZONE_OM_METADATA_LAYOUT, OZONE_OM_METADATA_LAYOUT_PREFIX);
    verifyOMStartupFailure(OZONE_OM_METADATA_LAYOUT_PREFIX);
    verifyOMRestartFailure(OZONE_OM_METADATA_LAYOUT_PREFIX);

    // case-2) Configured cluster layout as SIMPLE. Bucket exists with SIMPLE
    // layout format. OM startup should be successful.
    conf.set(OZONE_OM_METADATA_LAYOUT, OZONE_OM_METADATA_LAYOUT_DEFAULT);
    // ensure everything works again with SIMPLE layout format
    cluster.getOzoneManager().restart();
    OzoneBucket bucket2 = TestDataUtil.createVolumeAndBucket(cluster);
    verifyBucketLayout(bucket2, OZONE_OM_METADATA_LAYOUT_DEFAULT, false);

    // Cleanup buckets so that the cluster can be started with PREFIX
    OzoneClient client = cluster.getClient();
    OzoneVolume volume =
        client.getObjectStore().getVolume(bucket.getVolumeName());
    OzoneVolume volume2 =
        client.getObjectStore().getVolume(bucket2.getVolumeName());
    volume.deleteBucket(bucket.getName());
    volume2.deleteBucket(bucket2.getName());

    // case-3) Configured cluster layout as PREFIX and ENABLE_FSPATH=false.
    // OM startup should fail as this is INVALID config.
    cluster.getOzoneManager().stop();
    conf.set(OZONE_OM_METADATA_LAYOUT, OZONE_OM_METADATA_LAYOUT_PREFIX);
    conf.setBoolean(OZONE_OM_ENABLE_FILESYSTEM_PATHS, false);
    verifyOmStartWithInvalidConfig(OZONE_OM_METADATA_LAYOUT_PREFIX);
    verifyOmRestartWithInvalidConfig(OZONE_OM_METADATA_LAYOUT_PREFIX);

    // case-4) Configured cluster layout as INVALID.
    // OM startup should fail as this is INVALID config.
    conf.set(OZONE_OM_METADATA_LAYOUT, "INVALID");
    verifyOmStartWithInvalidConfig("INVALID");
    verifyOmRestartWithInvalidConfig("INVALID");

    // case-5) Configured cluster layout as PREFIX and ENABLE_FSPATH=true.
    // No buckets. OM startup should be successful.
    conf.set(OZONE_OM_METADATA_LAYOUT, OZONE_OM_METADATA_LAYOUT_PREFIX);
    conf.setBoolean(OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    cluster.getOzoneManager().restart();
    OzoneBucket bucket3 = TestDataUtil.createVolumeAndBucket(cluster);
    verifyBucketLayout(bucket3, OZONE_OM_METADATA_LAYOUT_PREFIX, true);

    // case-6) Configured cluster layout as SIMPLE. Bucket exists with PREFIX
    // layout format. OM startup should fail.
    conf.set(OZONE_OM_METADATA_LAYOUT, OZONE_OM_METADATA_LAYOUT_DEFAULT);
    cluster.getOzoneManager().stop();
    verifyOMStartupFailure(OZONE_OM_METADATA_LAYOUT_DEFAULT);
    verifyOMRestartFailure(OZONE_OM_METADATA_LAYOUT_DEFAULT);
  }

  private void verifyBucketLayout(OzoneBucket bucket, String metadataLayout,
      boolean isFSOBucket) {
    Assert.assertNotNull(bucket);
    Assert.assertEquals(2, bucket.getMetadata().size());
    Assert.assertEquals(isFSOBucket,
        OzoneFSUtils.isFSOptimizedBucket(bucket.getMetadata()));
    Assert.assertEquals(metadataLayout,
        bucket.getMetadata().get(OZONE_OM_METADATA_LAYOUT));
  }

  private void verifyOMStartupFailure(String clusterLayout) {
    try {
      cluster.getOzoneManager().start();
      Assert.fail("Should fail OM startup in " + clusterLayout + " layout");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Failed to start OM in " + clusterLayout + " layout format",
          ioe);
    }
    cluster.getOzoneManager().stop();
  }

  private void verifyOMRestartFailure(String clusterLayout) {
    try {
      cluster.getOzoneManager().restart();
      Assert.fail("Should fail OM startup in " + clusterLayout + " layout");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Failed to start OM in " + clusterLayout + " layout format",
          ioe);
    }
    cluster.getOzoneManager().stop();
  }

  private void verifyOmStartWithInvalidConfig(String clusterLayout)
      throws IOException {
    try {
      cluster.getOzoneManager().start();
      Assert.fail("Should fail OM startup in " + clusterLayout + " layout");
    } catch (IllegalArgumentException iae) {
      GenericTestUtils.assertExceptionContains(
          "Failed to start OM in " + clusterLayout + " layout format", iae);
    }
    cluster.getOzoneManager().stop();
  }

  private void verifyOmRestartWithInvalidConfig(String clusterLayout)
      throws IOException {
    try {
      cluster.getOzoneManager().restart();
      Assert.fail("Should fail OM startup in " + clusterLayout + " layout");
    } catch (IllegalArgumentException iae) {
      GenericTestUtils.assertExceptionContains(
          "Failed to start OM in " + clusterLayout + " layout format", iae);
    }
    cluster.getOzoneManager().stop();
  }
}
