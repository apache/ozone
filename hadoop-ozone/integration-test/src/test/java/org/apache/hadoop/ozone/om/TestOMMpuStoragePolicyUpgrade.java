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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.junit.jupiter.api.Test;

/**
 * Upgrade testing for the storage policy of a multipart key.
 * <p>
 * Expected behavior:
 * 1. Pre-Finalize: InitiateMultiPartUpload requests succeed, but the storage
 *    policy is not stamped on the request, so the multipart info and the open
 *    key end up with no storage policy. This holds whether or not the client
 *    sent one. An OM without storage policy support ignores the field, so
 *    stamping it would make OMs apply the same request differently.
 * <p>
 * 2. Post-Finalize: the storage policy sent by the client is persisted. If the
 *    client sent none, the bucket's storage policy, then the cluster default,
 *    is persisted.
 * <p>
 * See {@link AbstractOMStoragePolicyUpgradeTest} for the cluster setup, which
 * leaves only {@link OMLayoutFeature#STORAGE_POLICY_SUPPORT} un-finalized.
 */
public class TestOMMpuStoragePolicyUpgrade extends AbstractOMStoragePolicyUpgradeTest {

  @Test
  public void upgrade() throws Exception {
    preFinalizationChecks();
    finalizeOMUpgrade();
    postFinalizationChecks();
  }

  private void preFinalizationChecks() throws Exception {
    assertStoragePolicyNotFinalized();

    // The bucket cannot carry a policy yet: CreateBucket strips it while pre-finalized.
    String bucket = createBucket(null, null, BucketLayout.OBJECT_STORE);

    // The policy sent by the client is dropped.
    assertMultipartKeyStoragePolicy(bucket, OzoneStoragePolicy.COLD, null);
    // Control: a request that carries no policy, such as one sent by an older client, behaves the same.
    assertMultipartKeyStoragePolicy(bucket, null, null);
  }

  private void postFinalizationChecks() throws Exception {
    String bucketWithoutPolicy = createBucket(null, null, BucketLayout.OBJECT_STORE);
    assertMultipartKeyStoragePolicy(bucketWithoutPolicy, OzoneStoragePolicy.COLD, OzoneStoragePolicy.COLD);
    assertMultipartKeyStoragePolicy(bucketWithoutPolicy, null, OzoneStoragePolicy.getDefaultPolicy());

    String bucketWithPolicy = createBucket(OzoneStoragePolicy.WARM, null, BucketLayout.OBJECT_STORE);
    assertMultipartKeyStoragePolicy(bucketWithPolicy, OzoneStoragePolicy.COLD, OzoneStoragePolicy.COLD);
    assertMultipartKeyStoragePolicy(bucketWithPolicy, null, OzoneStoragePolicy.WARM);
  }

  /**
   * Initiates an upload asking for {@code requestedPolicy} (none if null) and checks that
   * {@code expectedPolicy} is stored on both the multipart info and the open key.
   */
  private void assertMultipartKeyStoragePolicy(String bucketName, StoragePolicy requestedPolicy,
      StoragePolicy expectedPolicy) throws Exception {
    String keyName = "key-" + UUID.randomUUID();
    OmKeyArgs.Builder keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOwnerName("user1")
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.ONE));
    if (requestedPolicy != null) {
      keyArgs.setStoragePolicy(requestedPolicy);
    }
    String uploadID = getOmClient().initiateMultipartUpload(keyArgs.build()).getUploadID();

    OMMetadataManager metadataManager = getCluster().getOzoneManager().getMetadataManager();
    String multipartKey = metadataManager.getMultipartKey(VOLUME_NAME, bucketName, keyName, uploadID);
    assertThat(metadataManager.getMultipartInfoTable().get(multipartKey).getStoragePolicy())
        .isEqualTo(expectedPolicy);
    assertThat(metadataManager.getOpenKeyTable(BucketLayout.OBJECT_STORE).get(multipartKey).getStoragePolicy())
        .isEqualTo(expectedPolicy);
  }
}
