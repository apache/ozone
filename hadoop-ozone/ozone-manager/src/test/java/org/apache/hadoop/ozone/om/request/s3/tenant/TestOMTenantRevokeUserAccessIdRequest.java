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

package org.apache.hadoop.ozone.om.request.s3.tenant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.Collections;
import java.util.TreeSet;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.S3SecretLockedManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.S3SecretManagerImpl;
import org.apache.hadoop.ozone.om.TenantOp;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.multitenant.AuthorizerLock;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.s3.S3SecretCacheProvider;
import org.apache.hadoop.ozone.om.upgrade.OMVersionManager;
import org.apache.hadoop.ozone.om.upgrade.OMVersionManagerTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantRevokeUserAccessIdRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests STS revocation when a tenant access id is revoked. */
public class TestOMTenantRevokeUserAccessIdRequest {
  private static final String ACCESS_ID = "tenant$user";
  private static final String TENANT = "tenant";
  private static final String USER = "user";

  @TempDir
  private Path folder;
  private OzoneManager ozoneManager;
  private OmMetadataManagerImpl metadataManager;
  private OMMetrics metrics;
  private S3SecretManager secretManager;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, folder.toString());
    metadataManager = new OmMetadataManagerImpl(conf, ozoneManager);
    metrics = OMMetrics.create(conf);
    when(ozoneManager.getMetadataManager()).thenReturn(metadataManager);
    when(ozoneManager.getMetrics()).thenReturn(metrics);
    OMVersionManager versionManager = OMVersionManagerTestUtils.mockFinalizedOmVersionManager();
    when(ozoneManager.getVersionManager()).thenReturn(versionManager);
    when(ozoneManager.getAuditLogger()).thenReturn(mock(AuditLogger.class));
    secretManager = new S3SecretLockedManager(
        new S3SecretManagerImpl(metadataManager, S3SecretCacheProvider.IN_MEMORY.get(conf)), metadataManager.getLock());
    when(ozoneManager.getS3SecretManager()).thenReturn(secretManager);
    OMMultiTenantManager tenantManager = mock(OMMultiTenantManager.class);
    when(ozoneManager.getMultiTenantManager()).thenReturn(tenantManager);
    when(tenantManager.getTenantVolumeName(TENANT)).thenReturn(TENANT);
    when(tenantManager.getAuthorizerLock()).thenReturn(mock(AuthorizerLock.class));
    when(tenantManager.getAuthorizerOp()).thenReturn(mock(TenantOp.class));
    when(tenantManager.getCacheOp()).thenReturn(mock(TenantOp.class));
    metadataManager.getTenantAccessIdTable().put(ACCESS_ID, new OmDBAccessIdInfo.Builder()
        .setTenantId(TENANT).setUserPrincipal(USER).setIsAdmin(false).setIsDelegatedAdmin(false).build());
    metadataManager.getPrincipalToAccessIdsTable().put(USER, new OmDBUserPrincipalInfo.Builder()
        .setAccessIds(new TreeSet<>(Collections.singleton(ACCESS_ID))).build());
    secretManager.storeSecret(ACCESS_ID, S3SecretValue.of(ACCESS_ID, "old-secret"));
  }

  @AfterEach
  public void tearDown() throws Exception {
    metadataManager.stop();
    metrics.unRegister();
  }

  @Test
  public void testRevokeRecordsCutoffThatSurvivesNewSecret() throws Exception {
    OMRequest request = new OMTenantRevokeUserAccessIdRequest(revokeRequest()).preExecute(ozoneManager);
    assertTrue(request.hasRevokeSTSTokenRequest());
    long cutoff = request.getRevokeSTSTokenRequest().getRevocationTimeMillis();
    assertTrue(cutoff > 0);
    assertEquals(ACCESS_ID, request.getRevokeSTSTokenRequest().getOriginalAccessKeyId());
    revokeAndCommit(request);
    assertEquals(cutoff, metadataManager.getS3RevokedStsTokenTable().get(ACCESS_ID));
    assertEquals(cutoff, metadataManager.getS3RevokedStsTokenTable().getSkipCache(ACCESS_ID));
    secretManager.storeSecret(ACCESS_ID, S3SecretValue.of(ACCESS_ID, "new-secret"));
    assertTrue(secretManager.hasS3Secret(ACCESS_ID));
    assertEquals(cutoff, metadataManager.getS3RevokedStsTokenTable().get(ACCESS_ID));
  }

  @Test
  public void testRevokeKeepsHigherCutoff() throws Exception {
    OMRequest request = new OMTenantRevokeUserAccessIdRequest(revokeRequest()).preExecute(ozoneManager);
    long existing = request.getRevokeSTSTokenRequest().getRevocationTimeMillis() + 1000;
    metadataManager.getS3RevokedStsTokenTable().addCacheEntry(new CacheKey<>(ACCESS_ID), CacheValue.get(1L, existing));
    revokeAndCommit(request);
    assertEquals(existing, metadataManager.getS3RevokedStsTokenTable().get(ACCESS_ID));
    assertEquals(existing, metadataManager.getS3RevokedStsTokenTable().getSkipCache(ACCESS_ID));
  }

  @Test
  public void testReplayWithoutCutoff() throws Exception {
    revokeAndCommit(revokeRequest());
    assertNull(metadataManager.getS3RevokedStsTokenTable().get(ACCESS_ID));
    assertNull(metadataManager.getS3RevokedStsTokenTable().getSkipCache(ACCESS_ID));
  }

  private OMRequest revokeRequest() {
    return OMRequest.newBuilder().setCmdType(Type.TenantRevokeUserAccessId).setClientId("client")
        .setTenantRevokeUserAccessIdRequest(TenantRevokeUserAccessIdRequest.newBuilder()
            .setAccessId(ACCESS_ID).setTenantId(TENANT)).build();
  }

  private void revokeAndCommit(OMRequest request) throws Exception {
    OMClientResponse response = new OMTenantRevokeUserAccessIdRequest(request)
        .validateAndUpdateCache(ozoneManager, 2L);
    assertEquals(Status.OK, response.getOMResponse().getStatus());
    assertFalse(secretManager.hasS3Secret(ACCESS_ID));
    if (request.hasRevokeSTSTokenRequest()) {
      assertTrue(metadataManager.getS3RevokedStsTokenTable().get(ACCESS_ID)
          >= request.getRevokeSTSTokenRequest().getRevocationTimeMillis());
    }
    try (BatchOperation batch = metadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(metadataManager, batch);
      metadataManager.getStore().commitBatchOperation(batch);
    }
    assertNull(metadataManager.getSecret(ACCESS_ID));
  }
}
