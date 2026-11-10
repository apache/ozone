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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.junit.jupiter.api.Test;

/**
 * Upgrade testing for the storage policy of a file.
 * <p>
 * Expected behavior:
 * 1. Pre-Finalize:
 *    - CreateFile requests that carry a storage policy are rejected by the OM,
 *      and no open file is created. An OM without storage policy support
 *      ignores the field, so accepting it would make OMs apply the same
 *      request differently.
 *    - The client does not send a storage policy to an OM that does not
 *      support it.
 *    - Requests that carry none, such as those sent by an older client,
 *      succeed. No policy is stamped on the request, so the open file ends up
 *      with no storage policy.
 * <p>
 * 2. Post-Finalize: the storage policy sent by the client is persisted. If the
 *    client sent none, the bucket's storage policy, then the cluster default,
 *    is persisted.
 * <p>
 * See {@link AbstractOMStoragePolicyUpgradeTest} for the cluster setup, which
 * leaves only {@link OzoneManagerVersion#STORAGE_POLICY_SUPPORT} un-finalized.
 * Its single datanode only has DISK volumes, so the tests use the policies
 * that can be placed there: the default (WARM) and HOT, which falls back to DISK.
 */
public class TestOMFileStoragePolicyUpgrade extends AbstractOMStoragePolicyUpgradeTest {

  private static final List<BucketLayout> FILE_BUCKET_LAYOUTS =
      Arrays.asList(BucketLayout.FILE_SYSTEM_OPTIMIZED, BucketLayout.LEGACY);

  private static final ReplicationConfig REPLICATION = RatisReplicationConfig.getInstance(ReplicationFactor.ONE);

  @Test
  public void upgrade() throws Exception {
    preFinalizationChecks();
    finalizeOMUpgrade();
    postFinalizationChecks();
  }

  private void preFinalizationChecks() throws Exception {
    assertStoragePolicyNotFinalized();
    OMMetadataManager metadataManager = getCluster().getOzoneManager().getMetadataManager();

    for (BucketLayout bucketLayout : FILE_BUCKET_LAYOUTS) {
      // The bucket cannot carry a policy yet: CreateBucket rejects one while pre-finalized.
      String bucket = createBucket(null, null, bucketLayout);

      // The client does not send the policy to the OM in the first place.
      IOException clientError = assertThrows(IOException.class,
          () -> createFileThroughClient(getClient(), bucket, OzoneStoragePolicy.HOT));
      assertThat(clientError).hasMessage("OzoneManager does not support storage policy");
      assertThat(metadataManager.getOpenKeyTable(bucketLayout).isEmpty()).isTrue();

      // A request that carries a policy is rejected by the OM and creates no open file.
      assertRejected(() -> createFile(bucket, OzoneStoragePolicy.HOT));
      assertThat(metadataManager.getOpenKeyTable(bucketLayout).isEmpty()).isTrue();

      // Control: a request that carries no policy succeeds and stores none.
      assertThat(createFile(bucket, null).getKeyInfo().getStoragePolicy()).isNull();
    }
  }

  private void postFinalizationChecks() throws Exception {
    assertStoragePolicyFinalized();

    // A client reads the OM version when it is created, so the one from before finalization cannot be reused.
    try (OzoneClient finalizedClient = getCluster().newClient()) {
      for (BucketLayout bucketLayout : FILE_BUCKET_LAYOUTS) {
        String bucketWithoutPolicy = createBucket(null, null, bucketLayout);

        // The client no longer rejects the policy, and the policy it sent is stored.
        assertThat(createFileThroughClient(finalizedClient, bucketWithoutPolicy, OzoneStoragePolicy.HOT)
            .getStoragePolicy()).isEqualTo(OzoneStoragePolicy.HOT);

        assertFileStoragePolicy(bucketWithoutPolicy, OzoneStoragePolicy.HOT, OzoneStoragePolicy.HOT);
        assertFileStoragePolicy(bucketWithoutPolicy, null, OzoneStoragePolicy.getDefaultPolicy());

        String bucketWithPolicy = createBucket(OzoneStoragePolicy.HOT, null, bucketLayout);
        assertFileStoragePolicy(bucketWithPolicy, OzoneStoragePolicy.WARM, OzoneStoragePolicy.WARM);
        assertFileStoragePolicy(bucketWithPolicy, null, OzoneStoragePolicy.HOT);
      }
    }
  }

  /**
   * Goes through the public client API, so the client side version check applies.
   * The file is closed empty and read back through the same client.
   */
  private OzoneKeyDetails createFileThroughClient(OzoneClient client, String bucketName, StoragePolicy requestedPolicy)
      throws IOException {
    OzoneBucket bucket = client.getObjectStore().getVolume(VOLUME_NAME).getBucket(bucketName);
    String keyName = "file-" + UUID.randomUUID();
    bucket.createFile(keyName, 0, REPLICATION, false, false, requestedPolicy).close();
    return bucket.getKey(keyName);
  }

  /**
   * Sent straight to the OM, bypassing RpcClient, so that the client side version check cannot reject the request
   * before the OM's validator sees it.
   */
  private OpenKeySession createFile(String bucketName, StoragePolicy requestedPolicy) throws IOException {
    OmKeyArgs.Builder keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(bucketName)
        .setKeyName("file-" + UUID.randomUUID())
        .setOwnerName("user1")
        .setReplicationConfig(REPLICATION);
    if (requestedPolicy != null) {
      keyArgs.setStoragePolicy(requestedPolicy);
    }
    return getOmClient().createFile(keyArgs.build(), false, false);
  }

  /**
   * Creates a file asking for {@code requestedPolicy} (none if null) and checks that {@code expectedPolicy} is stored.
   */
  private void assertFileStoragePolicy(String bucketName, StoragePolicy requestedPolicy, StoragePolicy expectedPolicy)
      throws IOException {
    assertThat(createFile(bucketName, requestedPolicy).getKeyInfo().getStoragePolicy()).isEqualTo(expectedPolicy);
  }
}
