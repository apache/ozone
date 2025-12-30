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

package org.apache.hadoop.ozone.om.multitenant;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_ENABLED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.isDone;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.isStarting;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils.VoidCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that S3 requests for a tenant are directed to that tenant's volume,
 * and that users not belonging to a tenant are directed to the default S3
 * volume.
 */
public class TestMultiTenantVolume {
  private static MiniOzoneCluster cluster;
  private static String s3VolumeName;

  private static final String TENANT_ID = "tenant";
  private static final String USER_PRINCIPAL = "username";
  private static final String BUCKET_NAME = "bucket";
  private static final String ACCESS_ID = "tenant$username";
  private static OzoneClient client;

  @BeforeAll
  public static void initClusterProvider() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(
        OMMultiTenantManagerImpl.OZONE_OM_TENANT_DEV_SKIP_RANGER, true);
    conf.setBoolean(OZONE_OM_MULTITENANCY_ENABLED, true);
    conf.setInt(OMStorage.TESTING_INIT_LAYOUT_VERSION_KEY, OMLayoutFeature.INITIAL_VERSION.layoutVersion());
    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newBuilder(conf)
        .withoutDatanodes();
    cluster = builder.build();
    client = cluster.newClient();
    s3VolumeName = HddsClientUtils.getDefaultS3VolumeName(conf);

    preFinalizationChecks(getStoreForAccessID(ACCESS_ID));
    finalizeOMUpgrade();
  }

  @AfterAll
  public static void shutdownClusterProvider() {
    IOUtils.closeQuietly(client);
    cluster.shutdown();
  }

  private static void expectFailurePreFinalization(VoidCallable eval) {
    OMException omException = assertThrows(OMException.class, eval::call);
    assertThat(omException.getMessage())
        .contains("cannot be invoked before finalization");
  }

  /**
   * Perform sanity checks before triggering upgrade finalization.
   */
  private static void preFinalizationChecks(ObjectStore store)
      throws Exception {

    // None of the tenant APIs is usable before the upgrade finalization step
    expectFailurePreFinalization(
        store::listTenant);
    expectFailurePreFinalization(() ->
        store.listUsersInTenant(TENANT_ID, ""));
    expectFailurePreFinalization(() ->
        store.tenantGetUserInfo(USER_PRINCIPAL));
    expectFailurePreFinalization(() ->
        store.createTenant(TENANT_ID));
    expectFailurePreFinalization(() ->
        store.tenantAssignUserAccessId(USER_PRINCIPAL, TENANT_ID, ACCESS_ID));
    expectFailurePreFinalization(() ->
        store.tenantAssignAdmin(USER_PRINCIPAL, TENANT_ID, true));
    expectFailurePreFinalization(() ->
        store.tenantRevokeAdmin(ACCESS_ID, TENANT_ID));
    expectFailurePreFinalization(() ->
        store.tenantRevokeUserAccessId(ACCESS_ID));
    expectFailurePreFinalization(() ->
        store.deleteTenant(TENANT_ID));

    // S3 get/set/revoke secret APIs still work before finalization
    final String accessId = "testUser1accessId1";
    S3SecretValue s3SecretValue = store.getS3Secret(accessId);
    assertEquals(accessId, s3SecretValue.getAwsAccessKey());
    final String setSecret = "testsecret";
    s3SecretValue = store.setS3Secret(accessId, setSecret);
    assertEquals(accessId, s3SecretValue.getAwsAccessKey());
    assertEquals(setSecret, s3SecretValue.getAwsSecret());
    store.revokeS3Secret(accessId);
  }

  /**
   * Trigger OM upgrade finalization from the client and block until completion
   * (status FINALIZATION_DONE).
   */
  private static void finalizeOMUpgrade()
      throws IOException, InterruptedException, TimeoutException {

    // Trigger OM upgrade finalization. Ref: FinalizeUpgradeSubCommand#call
    final OzoneManagerProtocol omClient = client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    final String upgradeClientID = "Test-Upgrade-Client-" + UUID.randomUUID();
    UpgradeFinalization.StatusAndMessages finalizationResponse =
        omClient.finalizeUpgrade(upgradeClientID);

    // The status should transition as soon as the client call above returns
    assertTrue(isStarting(finalizationResponse.status()));

    // Wait for the finalization to be marked as done.
    // 10s timeout should be plenty.
    GenericTestUtils.waitFor(() -> {
      try {
        final UpgradeFinalization.StatusAndMessages progress =
            omClient.queryUpgradeFinalizationProgress(
                upgradeClientID, false, false);
        return isDone(progress.status());
      } catch (IOException e) {
        fail("Unexpected exception while waiting for "
            + "the OM upgrade to finalize: " + e.getMessage());
      }
      return false;
    }, 500, 10000);
  }

  @Test
  public void testDefaultS3Volume() throws Exception {
    final String bucketName = "bucket";

    // Default client not belonging to a tenant should end up in the S3 volume.
    ObjectStore store = client.getObjectStore();
    assertEquals(s3VolumeName, store.getS3Volume().getName());

    // Create bucket.
    store.createS3Bucket(bucketName);
    assertEquals(s3VolumeName,
        store.getS3Bucket(bucketName).getVolumeName());

    // Delete bucket.
    store.deleteS3Bucket(bucketName);
    assertS3BucketNotFound(store, bucketName);
  }

  @Test
  public void testS3TenantVolume() throws Exception {

    ObjectStore store = getStoreForAccessID(ACCESS_ID);

    store.createTenant(TENANT_ID);
    store.tenantAssignUserAccessId(USER_PRINCIPAL, TENANT_ID, ACCESS_ID);

    // S3 volume pointed to by the store should be for the tenant.
    assertEquals(TENANT_ID, store.getS3Volume().getName());

    // Create bucket in the tenant volume.
    store.createS3Bucket(BUCKET_NAME);
    OzoneBucket bucket = store.getS3Bucket(BUCKET_NAME);
    assertEquals(TENANT_ID, bucket.getVolumeName());

    // A different user should not see bucket, since they will be directed to
    // the s3 volume.
    ObjectStore store2 = getStoreForAccessID(UUID.randomUUID().toString());
    assertS3BucketNotFound(store2, BUCKET_NAME);

    // Delete bucket.
    store.deleteS3Bucket(BUCKET_NAME);
    assertS3BucketNotFound(store, BUCKET_NAME);

    store.tenantRevokeUserAccessId(ACCESS_ID);
    store.deleteTenant(TENANT_ID);
    store.deleteVolume(TENANT_ID);
  }

  /**
   * Checks that the bucket is not found using
   * {@link ObjectStore#getS3Bucket} and the designated S3 volume pointed to
   * by the ObjectStore.
   */
  private void assertS3BucketNotFound(ObjectStore store, String bucketName)
      throws IOException {
    try {
      store.getS3Bucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.BUCKET_NOT_FOUND) {
        throw ex;
      }
    }

    try {
      OzoneVolume volume = store.getS3Volume();
      volume.getBucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.BUCKET_NOT_FOUND) {
        throw ex;
      }
    }
  }

  private static ObjectStore getStoreForAccessID(String accessID)
      throws IOException {
    // Cluster provider will modify our provided configuration. We must use
    // this version to build the client.
    OzoneConfiguration conf = cluster.getOzoneManager().getConfiguration();
    // Manually construct an object store instead of using the cluster
    // provided one so we can specify the access ID.
    RpcClient rpcClient = new RpcClient(conf, null);
    // userPrincipal is set to be the same as accessId for the test
    rpcClient.setThreadLocalS3Auth(
        new S3Auth("unused1", "unused2", accessID, accessID));
    return new ObjectStore(conf, rpcClient);
  }

  @Test
  public void testOMRangerBGSyncRatisSetVersion()
      throws IOException, ServiceException {
    final long writtenVersion = 10L;

    cluster.getOzoneManager().getMultiTenantManager()
        .getOMRangerBGSyncService().setOMDBRangerServiceVersion(writtenVersion);

    String readBackVersionStr = cluster.getOzoneManager().getMetadataManager()
        .getMetaTable()
        .get(OzoneConsts.RANGER_OZONE_SERVICE_VERSION_KEY);
    long readBackVersion = Long.parseLong(readBackVersionStr);

    assertEquals(writtenVersion, readBackVersion);
  }

  @Test
  public void testTenantVolumeQuota() throws Exception {

    ObjectStore store = getStoreForAccessID(ACCESS_ID);

    // Create Tenant and check default quota
    store.createTenant(TENANT_ID);
    OzoneVolume volume;
    volume = store.getVolume(TENANT_ID);
    assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInNamespace());
    assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInBytes());

    long spaceQuota = 10;
    long namespaceQuota = 20;
    OzoneQuota quota = OzoneQuota.getOzoneQuota(spaceQuota, namespaceQuota);
    volume.setQuota(quota);

    // Check quota
    volume = store.getVolume(TENANT_ID);
    assertEquals(namespaceQuota, volume.getQuotaInNamespace());
    assertEquals(spaceQuota, volume.getQuotaInBytes());

    // Delete tenant and volume
    store.deleteTenant(TENANT_ID);
    store.deleteVolume(TENANT_ID);
  }

  @Test
  public void testRejectNonS3CompliantTenantIdCreationWithDefaultStrictS3True()
      throws Exception {
    ObjectStore store = getStoreForAccessID(ACCESS_ID);
    String[] nonS3CompliantTenantId =
        {"tenantid_underscore", "_tenantid___multi_underscore_", "tenantid_"};

    for (String tenantId : nonS3CompliantTenantId) {
      OMException e = assertThrows(
          OMException.class,
          () -> store.createTenant(tenantId));

      assertThat(e.getMessage())
          .contains("unsupported character")
          .contains("_");
    }
  }
}
