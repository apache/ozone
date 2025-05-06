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

import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;
import static org.apache.hadoop.ozone.om.OMUpgradeTestUtils.waitForFinalization;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager.maxLayoutVersion;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Upgrade testing for Bucket Layout Feature.
 * <p>
 * Expected behavior:
 * 1. Pre-Finalize: OM should not allow creation of buckets with new bucket
 * layouts. Only LEGACY buckets are allowed.
 * <p>
 * 2. Post-Finalize: OM should allow creation of buckets with new bucket
 * layouts.
 *
 * Test cases are run on the same single cluster.  Order is important,
 * because "upgrade" changes its state.  Test cases are therefore assigned to
 * 3 groups: before, during and after upgrade.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestOMBucketLayoutUpgrade {

  private static final int PRE_UPGRADE = 100;
  private static final int DURING_UPGRADE = 200;
  private static final int POST_UPGRADE = 300;

  private MiniOzoneHAClusterImpl cluster;
  private OzoneManager ozoneManager;
  private static final String VOLUME_NAME = "vol-" + UUID.randomUUID();
  private final int fromLayoutVersion = INITIAL_VERSION.layoutVersion();
  private OzoneManagerProtocol omClient;

  private OzoneClient client;

  @BeforeAll
  void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OMStorage.TESTING_INIT_LAYOUT_VERSION_KEY, fromLayoutVersion);
    String omServiceId = UUID.randomUUID().toString();
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    builder.setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(3)
        .setNumDatanodes(1);
    cluster = builder.build();

    cluster.waitForClusterToBeReady();
    ozoneManager = cluster.getOzoneManager();
    client = cluster.newClient();
    omClient = client.getObjectStore().getClientProxy().getOzoneManagerClient();

    // create sample volume.
    omClient.createVolume(
        new OmVolumeArgs.Builder()
            .setVolume(VOLUME_NAME)
            .setOwnerName("user1")
            .setAdminName("user1")
            .build());
  }

  @AfterAll
  void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  @Order(PRE_UPGRADE)
  void omLayoutBeforeUpgrade() throws IOException {
    assertEquals(fromLayoutVersion,
        ozoneManager.getVersionManager().getMetadataLayoutVersion());
    assertNull(ozoneManager.getMetadataManager().getMetaTable()
        .get(LAYOUT_VERSION_KEY));
  }

  /**
   * Tests that OM blocks all requests to create any buckets with a new bucket
   * layout.
   */
  @Order(PRE_UPGRADE)
  @ParameterizedTest
  @MethodSource("layoutsNotAllowedBeforeUpgrade")
  void blocksNewLayoutBeforeUpgrade(BucketLayout layout) {
    OMException e = assertThrows(OMException.class,
        () -> createBucketWithLayout(layout));
    assertEquals(NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION, e.getResult());
  }

  @Order(PRE_UPGRADE)
  @Test
  void allowsLegacyBucketBeforeUpgrade() throws Exception {
    assertCreateBucketWithLayout(BucketLayout.LEGACY);
  }

  /**
   * Complete the cluster upgrade.
   */
  @Test
  @Order(DURING_UPGRADE)
  void finalizeUpgrade() throws Exception {
    UpgradeFinalization.StatusAndMessages response =
        omClient.finalizeUpgrade("finalize-test");
    System.out.println("Finalization Messages : " + response.msgs());

    waitForFinalization(omClient);

    final String expectedVersion = String.valueOf(maxLayoutVersion());
    LambdaTestUtils.await(30000, 3000,
        () -> expectedVersion.equals(
            ozoneManager.getMetadataManager().getMetaTable()
                .get(LAYOUT_VERSION_KEY)));
  }

  @Order(POST_UPGRADE)
  @ParameterizedTest
  @EnumSource
  void allowsBucketCreationWithAnyLayoutAfterUpgrade(BucketLayout layout)
      throws Exception {
    assertCreateBucketWithLayout(layout);
  }

  private void assertCreateBucketWithLayout(BucketLayout layout)
      throws Exception {
    String bucketName = createBucketWithLayout(layout);

    // Make sure the bucket exists in the bucket table with the
    // expected layout.
    OmBucketInfo bucketInfo = omClient.getBucketInfo(VOLUME_NAME, bucketName);
    assertEquals(bucketName, bucketInfo.getBucketName());
    assertEquals(layout, bucketInfo.getBucketLayout());
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
    String bucketName = RandomStringUtils.secure().nextAlphabetic(10).toLowerCase();
    omClient.createBucket(
        new OmBucketInfo.Builder()
            .setVolumeName(VOLUME_NAME)
            .setBucketName(bucketName)
            .setBucketLayout(bucketLayout)
            .build());

    return bucketName;
  }

  private static Set<BucketLayout> layoutsNotAllowedBeforeUpgrade() {
    return EnumSet.complementOf(EnumSet.of(BucketLayout.LEGACY));
  }
}
