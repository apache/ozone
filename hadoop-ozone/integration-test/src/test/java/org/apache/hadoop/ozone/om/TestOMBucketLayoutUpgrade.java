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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;
import static org.apache.hadoop.ozone.om.OMUpgradeTestUtils.waitForFinalization;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager.maxLayoutVersion;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

/**
 * Upgrade testing for Bucket Layout Feature.
 * <p>
 * Expected behavior:
 * 1. Pre-Finalize: OM should not allow creation of buckets with new bucket
 * layouts. Only LEGACY buckets are allowed.
 * <p>
 * 2. Post-Finalize: OM should allow creation of buckets with new bucket
 * layouts.
 */
@RunWith(Parameterized.class)
public class TestOMBucketLayoutUpgrade {
  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(300000);
  private MiniOzoneHAClusterImpl cluster;
  private OzoneManager ozoneManager;
  private ClientProtocol clientProtocol;
  private static final String VOLUME_NAME = "vol-" + UUID.randomUUID();
  private int fromLayoutVersion;
  private OzoneManagerProtocol omClient;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOMBucketLayoutUpgrade.class);

  /**
   * Defines a "from" layout version to finalize from.
   *
   * @return
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {INITIAL_VERSION}
    });
  }


  public TestOMBucketLayoutUpgrade(OMLayoutFeature fromVersion) {
    this.fromLayoutVersion = fromVersion.layoutVersion();
  }

  /**
   * Create a MiniDFSCluster for testing.
   */
  @Before
  public void setup() throws Exception {
    org.junit.Assume.assumeTrue("Check if there is need to finalize.",
        maxLayoutVersion() > fromLayoutVersion);

    OzoneConfiguration conf = new OzoneConfiguration();
    String omServiceId = UUID.randomUUID().toString();
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(3)
        .setNumDatanodes(1)
        .setOmLayoutVersion(fromLayoutVersion)
        .build();

    cluster.waitForClusterToBeReady();
    ozoneManager = cluster.getOzoneManager();
    ObjectStore objectStore = OzoneClientFactory.getRpcClient(omServiceId, conf)
        .getObjectStore();
    clientProtocol = objectStore.getClientProxy();
    omClient = clientProtocol.getOzoneManagerClient();

    // create sample volume.
    omClient.createVolume(
        new OmVolumeArgs.Builder()
            .setVolume(VOLUME_NAME)
            .setOwnerName("user1")
            .setAdminName("user1")
            .build());
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Tests that OM blocks all requests to create any buckets with a new bucket
   * layout.
   *
   * @throws Exception
   */
  @Test
  public void testCreateBucketWithBucketLayoutsDuringUpgrade()
      throws Exception {
    // Assert OM layout version is 'fromLayoutVersion' on deploy.
    assertEquals(fromLayoutVersion,
        ozoneManager.getVersionManager().getMetadataLayoutVersion());
    assertNull(ozoneManager.getMetadataManager().getMetaTable()
        .get(LAYOUT_VERSION_KEY));

    // Test bucket creation with new bucket layouts.
    // FSO and OBS bucket creation should fail.
    verifyBucketCreationBlockedWithNewLayouts();

    // Bucket creation with LEGACY layout should succeed in Pre-Finalized state.
    LOG.info("Creating legacy bucket during Pre-Finalize");
    verifyBucketCreationWithLayout(new BucketLayout[]{BucketLayout.LEGACY});

    // Finalize the cluster upgrade.
    finalizeUpgrade();

    // Cluster upgrade is now complete,
    // Bucket creation should now succeed with all layouts.
    verifyBucketCreationWithLayout(new BucketLayout[]{
        BucketLayout.LEGACY,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        BucketLayout.OBJECT_STORE
    });
  }


  /**
   * Tests that OM allows bucket creation with given bucket layouts.
   *
   * @param bucketLayouts bucket layouts to test
   * @throws Exception if any
   */
  private void verifyBucketCreationWithLayout(BucketLayout[] bucketLayouts)
      throws Exception {
    String bucketName;
    for (BucketLayout layout : bucketLayouts) {
      LOG.info("Creating bucket with layout {} after OM finalization", layout);

      bucketName = createBucketWithLayout(layout);

      // Make sure the bucket exists in the bucket table with the
      // expected layout.
      assertEquals(
          omClient.getBucketInfo(VOLUME_NAME, bucketName).getBucketName(),
          bucketName);
      assertEquals(
          omClient.getBucketInfo(VOLUME_NAME, bucketName).getBucketLayout(),
          layout);
    }
  }

  /**
   * Tests that OM blocks all requests to create any buckets with a new bucket
   * layout.
   *
   * @throws Exception if any
   */
  private void verifyBucketCreationBlockedWithNewLayouts() throws Exception {
    BucketLayout[] bucketLayouts = new BucketLayout[]{
        BucketLayout.OBJECT_STORE,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
    };

    for (BucketLayout layout : bucketLayouts) {
      try {
        LOG.info("Creating bucket with layout {} during Pre-Finalize", layout);
        createBucketWithLayout(layout);
        fail("Expected to fail creating bucket with layout " + layout);
      } catch (OMException e) {
        // Expected exception.
        assertEquals(
            OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION,
            e.getResult());
      }
    }
  }

  /**
   * Helper method to create a bucket with the given layout.
   *
   * @param bucketLayout the layout to use for the bucket.
   * @return the name of the bucket created.
   * @throws Exception if there is an error creating the bucket.
   */
  private String createBucketWithLayout(BucketLayout bucketLayout)
      throws Exception {
    String bucketName = RandomStringUtils.randomAlphabetic(10).toLowerCase();
    omClient.createBucket(
        new OmBucketInfo.Builder()
            .setVolumeName(VOLUME_NAME)
            .setBucketName(bucketName)
            .setBucketLayout(bucketLayout)
            .build());

    return bucketName;
  }

  /**
   * Complete the cluster upgrade.
   *
   * @throws Exception if upgrade fails.
   */
  private void finalizeUpgrade() throws Exception {
    UpgradeFinalizer.StatusAndMessages response =
        omClient.finalizeUpgrade("finalize-test");
    System.out.println("Finalization Messages : " + response.msgs());

    waitForFinalization(omClient);

    LambdaTestUtils.await(30000, 3000, () -> {
      String lvString = ozoneManager.getMetadataManager().getMetaTable()
          .get(LAYOUT_VERSION_KEY);
      return maxLayoutVersion() == Integer.parseInt(lvString);
    });
  }
}
