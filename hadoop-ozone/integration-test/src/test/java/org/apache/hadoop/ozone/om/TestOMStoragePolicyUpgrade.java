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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_DEFAULT_STORAGE_POLICY_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Upgrade testing for the bucket storage policy feature.
 * <p>
 * Expected behavior:
 * 1. Pre-Finalize:
 *    - CreateBucket requests that carry a storagePolicy and/or
 *      allowFallbackStoragePolicy field are rejected, and no bucket is created.
 *    - SetBucketProperty requests that carry storagePolicy,
 *      allowFallbackStoragePolicy or unsetStoragePolicy are rejected.
 *    - Requests that carry none of these fields, such as those sent by an
 *      older client, are unaffected either way.
 * <p>
 * 2. Post-Finalize: CreateBucket honors the requested storage policy fields
 *    and SetBucketProperty requests carrying them succeed; in both cases the
 *    policy is persisted. A CreateBucket without the fields stores the OM's
 *    configured default policy, with fallback allowed.
 * <p>
 * See {@link AbstractOMStoragePolicyUpgradeTest} for the cluster setup, which
 * leaves only {@link  OzoneManagerVersion#STORAGE_POLICY_SUPPORT} un-finalized.
 */
public class TestOMStoragePolicyUpgrade extends AbstractOMStoragePolicyUpgradeTest {

  /** Bucket created before finalization, used by the update assertions. */
  private String bucketName;

  @Override
  protected void configure(OzoneConfiguration config) {
    // Not WARM, so the test fails if the OM applies WARM regardless of its configured default.
    config.set(OZONE_OM_DEFAULT_STORAGE_POLICY_KEY, OzoneStoragePolicy.COLD.name());
  }

  @BeforeEach
  public void createInitialBucket() throws Exception {
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
    assertStoragePolicyNotFinalized();

    // CreateBucket carrying either of the BucketInfo storage policy fields.
    assertRejected(() -> createBucket(OzoneStoragePolicy.COLD, null));
    assertRejected(() -> createBucket(null, true));
    assertRejected(() -> createBucket(OzoneStoragePolicy.HOT, true));

    // SetBucketProperty carrying any of the three BucketArgs fields.
    assertRejected(() -> setStoragePolicy(OzoneStoragePolicy.HOT, true, null));
    assertRejected(() -> setStoragePolicy(null, null, true));

    // Through the client API, RpcClient refuses to send storage policy fields to an OM below
    // STORAGE_POLICY_SUPPORT. It fails on the client, so the OM never sees the request.
    OzoneVolume volume = getClient().getObjectStore().getVolume(VOLUME_NAME);
    String clientRejected = "buck-" + UUID.randomUUID();
    assertClientRejected(() -> volume.createBucket(clientRejected,
        BucketArgs.newBuilder().setStoragePolicy(OzoneStoragePolicy.HOT).build()));
    assertClientRejected(() -> volume.createBucket(clientRejected,
        BucketArgs.newBuilder().setAllowFallbackStoragePolicy(true).build()));
    assertThatThrownBy(() -> getOmClient().getBucketInfo(VOLUME_NAME, clientRejected))
        .isInstanceOfSatisfying(OMException.class, ex -> assertThat(ex.getResult())
            .isEqualTo(OMException.ResultCodes.BUCKET_NOT_FOUND));
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertClientRejected(() -> bucket.setStoragePolicy(OzoneStoragePolicy.HOT));
    assertClientRejected(() -> bucket.setStoragePolicyProperty(null, null, true));
    assertThat(getOmClient().getBucketInfo(VOLUME_NAME, bucketName).getStoragePolicy()).isNull();

    // Controls: the gate must not reject requests that carry no storage policy field
    createBucket(null, null);
    // The client API must not add a policy field on its own, or the gate rejects this request.
    String clientCreated = "buck-" + UUID.randomUUID();
    getClient().getObjectStore().getVolume(VOLUME_NAME).createBucket(clientCreated);
    assertThat(getOmClient().getBucketInfo(VOLUME_NAME, clientCreated).getStoragePolicy()).isNull();

    getOmClient().setBucketProperty(OmBucketArgs.newBuilder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(bucketName)
        .setIsVersionEnabled(true)
        .build());
    assertThat(getOmClient().getBucketInfo(VOLUME_NAME, bucketName)
        .getStoragePolicy()).isNull();
  }

  private void postFinalizationChecks() throws Exception {
    assertStoragePolicyFinalized();

    // CreateBucket without storage policy options: the OM applies its configured default.
    String defaultPolicyBucket = "buck-" + UUID.randomUUID();
    getClient().getObjectStore().getVolume(VOLUME_NAME).createBucket(defaultPolicyBucket);
    OmBucketInfo defaultedPolicyBucketInfo = getOmClient().getBucketInfo(VOLUME_NAME, defaultPolicyBucket);
    assertThat(defaultedPolicyBucketInfo.getStoragePolicy()).isEqualTo(OzoneStoragePolicy.COLD);
    assertThat(defaultedPolicyBucketInfo.getAllowFallbackStoragePolicy()).isTrue();

    String created = createBucket(OzoneStoragePolicy.HOT, true);
    OmBucketInfo createdInfo = getOmClient().getBucketInfo(VOLUME_NAME, created);
    assertThat(createdInfo.getStoragePolicy()).isEqualTo(OzoneStoragePolicy.HOT);
    assertThat(createdInfo.getAllowFallbackStoragePolicy()).isTrue();

    setStoragePolicy(OzoneStoragePolicy.HOT, null, null);
    assertThat(getOmClient().getBucketInfo(VOLUME_NAME, bucketName)
        .getStoragePolicy()).isEqualTo(OzoneStoragePolicy.HOT);

    setStoragePolicy(null, null, true);
    assertThat(getOmClient().getBucketInfo(VOLUME_NAME, bucketName)
        .getStoragePolicy()).isNull();

    // A client connected after finalization sees the new OM version and may send storage policy fields.
    // The client used before finalization cached the old OM version, so it cannot be reused here.
    OzoneClient postFinalizeClient = getCluster().newClient();
    try {
      OzoneVolume volume = postFinalizeClient.getObjectStore().getVolume(VOLUME_NAME);
      String clientCreated = "buck-" + UUID.randomUUID();
      volume.createBucket(clientCreated, BucketArgs.newBuilder()
          .setStoragePolicy(OzoneStoragePolicy.HOT)
          .setAllowFallbackStoragePolicy(false)
          .build());
      OmBucketInfo clientCreatedInfo = getOmClient().getBucketInfo(VOLUME_NAME, clientCreated);
      assertThat(clientCreatedInfo.getStoragePolicy()).isEqualTo(OzoneStoragePolicy.HOT);
      assertThat(clientCreatedInfo.getAllowFallbackStoragePolicy()).isFalse();

      volume.getBucket(bucketName).setStoragePolicy(OzoneStoragePolicy.COLD);
      assertThat(getOmClient().getBucketInfo(VOLUME_NAME, bucketName)
          .getStoragePolicy()).isEqualTo(OzoneStoragePolicy.COLD);
    } finally {
      IOUtils.closeQuietly(postFinalizeClient);
    }
  }

  private void assertClientRejected(ThrowingCallable call) {
    assertThatThrownBy(call)
        .isInstanceOf(IOException.class)
        .isNotInstanceOf(OMException.class)
        .hasMessage("OzoneManager does not support storage policy");
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
    getOmClient().setBucketProperty(builder.build());
  }
}
