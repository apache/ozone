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

import static org.apache.hadoop.ozone.om.OMUpgradeTestUtils.waitForFinalization;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Upgrade testing for the bucket storage policy feature.
 * <p>
 * Expected behavior:
 * 1. Pre-Finalize:
 *    - CreateBucket requests that carry a storagePolicy and/or
 *      allowFallbackStoragePolicy field succeed, but those fields are
 *      silently stripped from the request before it is applied, so the
 *      created bucket ends up with no storage policy, as if the fields had
 *      never been sent.
 *    - SetBucketProperty requests that carry storagePolicy,
 *      allowFallbackStoragePolicy or unsetStoragePolicy are rejected.
 *    - Requests that carry none of these fields, such as those sent by an
 *      older client, are unaffected either way.
 * <p>
 * 2. Post-Finalize: CreateBucket honors the requested storage policy fields
 *    and SetBucketProperty requests carrying them succeed; in both cases the
 *    policy is persisted.
 * <p>
 * The cluster starts at the layout version immediately below
 * {@link OMLayoutFeature#BUCKET_STORAGE_POLICY_SUPPORT} so that only this
 * feature is un-finalized and the assertions cannot be satisfied by an
 * unrelated validator.
 * <p>
 * Note this covers the pre-finalization gate, not a mixed-version OM ring:
 * every OM here runs the same binary at the same metadata layout version.
 * Real mixed-binary coverage lives in the non-rolling-upgrade acceptance suite
 * under {@code hadoop-ozone/dist/src/main/compose/upgrade}.
 */
public class TestOMStoragePolicyUpgrade {

  private static final String VOLUME_NAME = "vol-" + UUID.randomUUID();

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private OzoneManagerProtocol omClient;
  private final OzoneConfiguration conf = new OzoneConfiguration();

  /** Bucket created before finalization, used by the update assertions. */
  private String bucketName;

  @BeforeEach
  public void init() throws Exception {
    conf.setInt(OMStorage.TESTING_INIT_LAYOUT_VERSION_KEY,
        OMLayoutFeature.MPU_PARTS_TABLE_SPLIT.layoutVersion());

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    omClient = client.getObjectStore().getClientProxy().getOzoneManagerClient();

    omClient.createVolume(new OmVolumeArgs.Builder()
        .setVolume(VOLUME_NAME)
        .setOwnerName("user1")
        .setAdminName("user1")
        .build());

    // A bucket without any storage policy field, as an older client would
    // create it. This must be allowed while pre-finalized.
    bucketName = createBucket(null, null);
  }

  @Test
  public void upgrade() throws Exception {
    preFinalizationChecks();
    finalizeOMUpgrade();
    postFinalizationChecks();
  }

  private void preFinalizationChecks() throws Exception {
    assertThat(cluster.getOzoneManager().getVersionManager()
        .getMetadataLayoutVersion())
        .isEqualTo(OMLayoutFeature.MPU_PARTS_TABLE_SPLIT.layoutVersion());

    // CreateBucket carrying BucketInfo storage policy field: the request
    // succeeds, but the storage policy field is silently stripped.
    String strippedPolicyBucket = createBucket(OzoneStoragePolicy.COLD, null);
    assertThat(omClient.getBucketInfo(VOLUME_NAME, strippedPolicyBucket)
        .getStoragePolicy()).isNull();

    // SetBucketProperty carrying any of the three BucketArgs fields.
    assertRejected(() -> setStoragePolicy(OzoneStoragePolicy.HOT, true, null));
    assertRejected(() -> setStoragePolicy(null, null, true));

    // Controls: the gate must not reject requests that carry no storage policy field
    createBucket(null, null);
    omClient.setBucketProperty(OmBucketArgs.newBuilder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(bucketName)
        .setIsVersionEnabled(true)
        .build());
    assertThat(omClient.getBucketInfo(VOLUME_NAME, bucketName)
        .getStoragePolicy()).isNull();
  }

  /**
   * Trigger OM upgrade finalization and block until it completes. The client id
   * must match the one {@link OMUpgradeTestUtils#waitForFinalization} polls
   * with.
   */
  private void finalizeOMUpgrade() throws Exception {
    omClient.finalizeUpgrade("finalize-test");
    waitForFinalization(omClient);
  }

  private void postFinalizationChecks() throws Exception {
    String created = createBucket(OzoneStoragePolicy.HOT, true);
    OmBucketInfo createdInfo = omClient.getBucketInfo(VOLUME_NAME, created);
    assertThat(createdInfo.getStoragePolicy()).isEqualTo(OzoneStoragePolicy.HOT);
    assertThat(createdInfo.getAllowFallbackStoragePolicy()).isTrue();

    setStoragePolicy(OzoneStoragePolicy.HOT, null, null);
    assertThat(omClient.getBucketInfo(VOLUME_NAME, bucketName)
        .getStoragePolicy()).isEqualTo(OzoneStoragePolicy.HOT);

    setStoragePolicy(null, null, true);
    assertThat(omClient.getBucketInfo(VOLUME_NAME, bucketName)
        .getStoragePolicy()).isNull();
  }

  private void assertRejected(Executable executable) {
    OMException ex = assertThrows(OMException.class, executable);
    assertThat(ex.getResult())
        .isEqualTo(NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
    assertThat(ex.getMessage())
        .contains("Cluster does not have the bucket storage policy support"
            + " feature finalized yet");
  }

  /**
   * Creates a bucket through {@link OzoneManagerProtocol} rather than the
   * {@code OzoneBucket} API, because {@code RpcClient#createBucket} substitutes
   * the default storage policy for null and so cannot express a request without
   * the field.
   *
   * @return the name of the bucket created.
   */
  private String createBucket(StoragePolicy storagePolicy,
      Boolean allowFallbackStoragePolicy) throws Exception {
    String name = "buck-" + UUID.randomUUID();
    OmBucketInfo.Builder builder = OmBucketInfo.newBuilder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(name);
    if (storagePolicy != null) {
      builder.setStoragePolicy(storagePolicy);
    }
    if (allowFallbackStoragePolicy != null) {
      builder.setAllowFallbackStoragePolicy(allowFallbackStoragePolicy);
    }
    omClient.createBucket(builder.build());
    return name;
  }

  private void setStoragePolicy(StoragePolicy storagePolicy,
      Boolean allowFallbackStoragePolicy, Boolean unsetStoragePolicy)
      throws Exception {
    OmBucketArgs.Builder builder = OmBucketArgs.newBuilder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(bucketName);
    if (storagePolicy != null) {
      builder.setStoragePolicy(storagePolicy);
    }
    if (allowFallbackStoragePolicy != null) {
      builder.setAllowFallbackStoragePolicy(allowFallbackStoragePolicy);
    }
    if (unsetStoragePolicy != null) {
      builder.setUnsetStoragePolicy(unsetStoragePolicy);
    }
    omClient.setBucketProperty(builder.build());
  }

  @AfterEach
  public void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
