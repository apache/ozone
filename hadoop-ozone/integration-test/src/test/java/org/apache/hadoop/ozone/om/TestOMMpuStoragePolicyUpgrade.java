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

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.junit.jupiter.api.Test;

/**
 * Upgrade testing for the storage policy of a multipart key.
 * <p>
 * Expected behavior:
 * 1. Pre-Finalize:
 *    - InitiateMultiPartUpload requests that carry a storage policy are
 *      rejected, and no upload is created. An OM without storage policy
 *      support ignores the field, so accepting it would make OMs apply the
 *      same request differently.
 *    - Requests that carry none, such as those sent by an older client,
 *      succeed. No policy is stamped on the request, so the multipart info
 *      and the open key end up with no storage policy.
 * <p>
 * 2. Post-Finalize: the storage policy sent by the client is persisted. If the
 *    client sent none, the bucket's storage policy, then the cluster default,
 *    is persisted.
 * <p>
 * See {@link AbstractOMStoragePolicyUpgradeTest} for the cluster setup, which
 * leaves only {@link OzoneManagerVersion#STORAGE_POLICY_SUPPORT} un-finalized.
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

    // A request that carries a policy is rejected and creates no upload.
    assertRejected(() -> initiateMultipartUpload(bucket, "key-" + UUID.randomUUID(), OzoneStoragePolicy.COLD));
    OMMetadataManager metadataManager = getCluster().getOzoneManager().getMetadataManager();
    assertThat(metadataManager.getMultipartInfoTable().isEmpty()).isTrue();
    assertThat(metadataManager.getOpenKeyTable(BucketLayout.OBJECT_STORE).isEmpty()).isTrue();

    // Control: a request that carries no policy succeeds.
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
   * Sent straight to the OM, bypassing RpcClient, so that the client side version check cannot reject the request
   * before the OM's validator sees it.
   *
   * @return the upload ID.
   */
  private String initiateMultipartUpload(String bucketName, String keyName, StoragePolicy requestedPolicy)
      throws IOException {
    OmKeyArgs.Builder keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOwnerName("user1")
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.ONE));
    if (requestedPolicy != null) {
      keyArgs.setStoragePolicy(requestedPolicy);
    }
    return getOmClient().initiateMultipartUpload(keyArgs.build()).getUploadID();
  }

  /**
   * Initiates an upload asking for {@code requestedPolicy} (none if null) and checks that
   * {@code expectedPolicy} is stored on both the multipart info and the open key.
   */
  private void assertMultipartKeyStoragePolicy(String bucketName, StoragePolicy requestedPolicy,
      StoragePolicy expectedPolicy) throws Exception {
    String keyName = "key-" + UUID.randomUUID();
    String uploadID = initiateMultipartUpload(bucketName, keyName, requestedPolicy);

    OMMetadataManager metadataManager = getCluster().getOzoneManager().getMetadataManager();
    String multipartKey = metadataManager.getMultipartKey(VOLUME_NAME, bucketName, keyName, uploadID);
    assertThat(metadataManager.getMultipartInfoTable().get(multipartKey).getStoragePolicy())
        .isEqualTo(expectedPolicy);
    assertThat(metadataManager.getOpenKeyTable(BucketLayout.OBJECT_STORE).get(multipartKey).getStoragePolicy())
        .isEqualTo(expectedPolicy);
  }
}
