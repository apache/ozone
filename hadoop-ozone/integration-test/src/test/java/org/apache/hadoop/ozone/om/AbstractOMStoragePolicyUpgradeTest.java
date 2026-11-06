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
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.function.Executable;

/**
 * Base class for the upgrade tests of the storage policy feature.
 * <p>
 * The cluster starts at the layout version immediately below
 * {@link OzoneManagerVersion#STORAGE_POLICY_SUPPORT} so that only this feature is
 * un-finalized and the assertions cannot be satisfied by an unrelated
 * validator.
 */
public abstract class AbstractOMStoragePolicyUpgradeTest {

  protected static final String VOLUME_NAME = "vol-" + UUID.randomUUID();

  private MiniOzoneCluster cluster;
  private OzoneManagerProtocol omClient;
  private OzoneClient client;
  private final OzoneConfiguration conf = new OzoneConfiguration();

  @BeforeEach
  public void init() throws Exception {
    conf.setInt(OMStorage.TESTING_INIT_APPARENT_VERSION_KEY,
        OzoneManagerVersion.S3_IF_NONE_MATCH_ETAG.serialize());
    configure(conf);

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
  }

  /**
   * Lets a subclass add configuration before the cluster is started.
   */
  protected void configure(OzoneConfiguration config) {
  }

  protected MiniOzoneCluster getCluster() {
    return cluster;
  }

  protected OzoneManagerProtocol getOmClient() {
    return omClient;
  }

  @AfterEach
  public void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  protected void assertStoragePolicyNotFinalized() {
    assertThat(cluster.getOzoneManager().getVersionManager().getApparentVersion())
        .isEqualTo(OzoneManagerVersion.S3_IF_NONE_MATCH_ETAG);
  }

  protected void assertStoragePolicyFinalized() {
    assertThat(cluster.getOzoneManager().getVersionManager().getApparentVersion())
        .isEqualTo(OzoneManagerVersion.SOFTWARE_VERSION);
  }

  /**
   * Asserts the request is rejected because the storage policy feature is not finalized.
   */
  protected void assertRejected(Executable executable) {
    OMException ex = assertThrows(OMException.class, executable);
    assertThat(ex.getResult())
        .isEqualTo(NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
    assertThat(ex.getMessage())
        .contains("Cluster does not have the storage policy"
            + " feature finalized yet");
  }

  /**
   * Trigger OM upgrade finalization and block until it completes. The client id
   * must match the one {@link OMUpgradeTestUtils#waitForFinalization} polls
   * with.
   */
  protected void finalizeOMUpgrade() throws Exception {
    omClient.finalizeUpgrade();
    waitForFinalization(omClient);
  }

  /**
   * Creates a bucket through {@link OzoneManagerProtocol} rather than the
   * {@code OzoneBucket} API, because {@code RpcClient#createBucket} substitutes
   * the default storage policy for null and so cannot express a request without
   * the field.
   *
   * @return the name of the bucket created.
   */
  protected String createBucket(StoragePolicy storagePolicy,
      Boolean allowFallbackStoragePolicy) throws Exception {
    return createBucket(storagePolicy, allowFallbackStoragePolicy, null);
  }

  /**
   * Same as {@link #createBucket(StoragePolicy, Boolean)}, with an explicit
   * bucket layout (the server default if null).
   */
  protected String createBucket(StoragePolicy storagePolicy,
      Boolean allowFallbackStoragePolicy, BucketLayout bucketLayout) throws Exception {
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
    if (bucketLayout != null) {
      builder.setBucketLayout(bucketLayout);
    }
    omClient.createBucket(builder.build());
    return name;
  }
}
