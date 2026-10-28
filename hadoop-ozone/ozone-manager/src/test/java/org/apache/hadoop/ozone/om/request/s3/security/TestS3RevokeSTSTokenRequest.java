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

package org.apache.hadoop.ozone.om.request.s3.security;

import static org.apache.hadoop.security.authentication.util.KerberosName.DEFAULT_MECHANISM;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.ipc_.ExternalCall;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteRevokedSTSTokensRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link S3RevokeSTSTokenRequest} and {@link S3DeleteRevokedSTSTokensRequest}.
 */
public class TestS3RevokeSTSTokenRequest {

  private static final String TEST_KERBEROS_RULES =
      "RULE:[2:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" + "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" + "DEFAULT";

  @TempDir
  private Path folder;

  private OMMultiTenantManager omMultiTenantManager;
  private String kerberosMechanismBeforeTest;
  private String kerberosRulesBeforeTest;

  @BeforeEach
  public void setUp() throws Exception {
    kerberosMechanismBeforeTest = KerberosName.getRuleMechanism();
    kerberosRulesBeforeTest = KerberosName.getRules();
    KerberosName.setRuleMechanism(DEFAULT_MECHANISM);
    // Initialize KerberosName rules so that UGI short names derived from
    // principals like "alice@EXAMPLE.COM" are computed correctly.
    KerberosName.setRules(TEST_KERBEROS_RULES);

    // Multi-tenant manager mock used for tests that exercise the S3 multi-tenancy permission branch.
    omMultiTenantManager = mock(OMMultiTenantManager.class);
  }

  @AfterEach
  public void tearDown() {
    Server.getCurCall().remove();
    KerberosName.setRuleMechanism(kerberosMechanismBeforeTest);
    KerberosName.setRules(kerberosRulesBeforeTest);
  }

  @Test
  public void testPreExecuteFailsForNonOwnerOfOriginalAccessKey() throws Exception {
    // Verify that preExecute enforces permissions based on the request's original access key ID
    // and rejects revocation attempts from non-owners.
    final String originalAccessKeyId = "original-access-key-id";

    // An RPC call running another Kerberos identity should NOT be allowed to revoke the token whose original
    // access key id is different.
    final UserGroupInformation tempUgi = UserGroupInformation.createRemoteUser("another-kerberos-identity");
    Server.getCurCall().set(new StubCall(tempUgi));

    OMException ex;
    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      configureOzoneManagerForPreExecute(ozoneManager, originalAccessKeyId, true);
      when(ozoneManager.isS3Admin(any(UserGroupInformation.class))).thenReturn(false);

      final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(buildRevokeOmRequest(originalAccessKeyId));
      ex = assertThrows(OMException.class, () -> omClientRequest.preExecute(ozoneManager));
    }
    assertEquals(OMException.ResultCodes.USER_MISMATCH, ex.getResult());
  }

  @Test
  public void testPreExecuteFailsWithoutRpcUser() throws Exception {
    final String originalAccessKeyId = "original-access-key-id";

    OMException ex;
    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      configureOzoneManagerForPreExecute(ozoneManager, originalAccessKeyId, true);
      when(ozoneManager.isSecurityEnabled()).thenReturn(true);

      final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(buildRevokeOmRequest(originalAccessKeyId));
      ex = assertThrows(OMException.class, () -> omClientRequest.preExecute(ozoneManager));
    }
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());
  }

  @Test
  public void testPreExecuteSucceedsForOriginalAccessKeyOwner() throws Exception {
    // Verify that preExecute allows the owner of the original access key ID from the revoke request
    // to revoke the temporary credentials.
    final String originalAccessKeyId = "original-access-key-id";

    // Simulate RPC call running as originalAccessKeyId
    final UserGroupInformation originalUgi = UserGroupInformation.createRemoteUser(originalAccessKeyId);
    Server.getCurCall().set(new StubCall(originalUgi));

    final OzoneManager ozoneManager = mock(OzoneManager.class);
    configureOzoneManagerForPreExecute(ozoneManager, originalAccessKeyId, true);
    when(ozoneManager.isS3Admin(any(UserGroupInformation.class))).thenReturn(false);

    final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(buildRevokeOmRequest(originalAccessKeyId));
    final OMRequest result = omClientRequest.preExecute(ozoneManager);

    assertEquals(Type.RevokeSTSToken, result.getCmdType());
    assertTrue(result.getRevokeSTSTokenRequest().hasRevocationTimeMillis());
    assertEquals(originalAccessKeyId, result.getRevokeSTSTokenRequest().getOriginalAccessKeyId());
    assertTrue(result.getRevokeSTSTokenRequest().getRevocationTimeMillis() > 0L);
  }

  @Test
  public void testPreExecuteSucceedsForTenantAccessIdOwner() throws Exception {
    // When S3 multi-tenancy is enabled and the original access key id is assigned to a tenant, verify that
    // the tenant access ID owner is allowed to revoke the temporary credentials.
    final String tenantId = "finance";
    final String originalAccessKeyId = "alice@EXAMPLE.COM";

    // Caller short name "alice" should match the owner username returned from the multi-tenant manager.
    final UserGroupInformation callerUgi = UserGroupInformation.createRemoteUser(originalAccessKeyId);
    Server.getCurCall().set(new StubCall(callerUgi));

    final OzoneManager ozoneManager = mock(OzoneManager.class);
    configureOzoneManagerForPreExecute(ozoneManager, originalAccessKeyId, true);
    when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(true);
    when(ozoneManager.getMultiTenantManager()).thenReturn(omMultiTenantManager);

    // Original access key id is assigned to a tenant and owned by "alice".
    when(omMultiTenantManager.getTenantForAccessID(originalAccessKeyId)).thenReturn(Optional.of(tenantId));
    when(omMultiTenantManager.getUserNameGivenAccessId(originalAccessKeyId)).thenReturn("alice");
    // Not a tenant admin; ownership should be sufficient.
    when(omMultiTenantManager.isTenantAdmin(callerUgi, tenantId, false)).thenReturn(false);

    final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(buildRevokeOmRequest(originalAccessKeyId));
    final OMRequest result = omClientRequest.preExecute(ozoneManager);
    assertEquals(Type.RevokeSTSToken, result.getCmdType());
  }

  @Test
  public void testPreExecuteSucceedsForTenantAdmin() throws Exception {
    // When S3 multi-tenancy is enabled and the original access key id is assigned to a tenant, verify that a
    // tenant admin (who is not the owner) is allowed to revoke the temporary credentials.
    final String tenantId = "finance";
    final String originalAccessKeyId = "alice@EXAMPLE.COM";

    // Caller short name "bob" does not own the access ID but will be configured as tenant admin.
    final UserGroupInformation callerUgi = UserGroupInformation.createRemoteUser("bob@EXAMPLE.COM");
    Server.getCurCall().set(new StubCall(callerUgi));

    final OzoneManager ozoneManager = mock(OzoneManager.class);
    configureOzoneManagerForPreExecute(ozoneManager, originalAccessKeyId, true);
    when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(true);
    when(ozoneManager.getMultiTenantManager()).thenReturn(omMultiTenantManager);

    // Original access key id is assigned to a tenant and owned by "alice".
    when(omMultiTenantManager.getTenantForAccessID(originalAccessKeyId)).thenReturn(Optional.of(tenantId));
    when(omMultiTenantManager.getUserNameGivenAccessId(originalAccessKeyId)).thenReturn("alice");
    // Caller is configured as tenant admin so the check should pass.
    when(omMultiTenantManager.isTenantAdmin(callerUgi, tenantId, false)).thenReturn(true);

    final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(buildRevokeOmRequest(originalAccessKeyId));
    final OMRequest result = omClientRequest.preExecute(ozoneManager);
    assertEquals(Type.RevokeSTSToken, result.getCmdType());
  }

  @Test
  public void testPreExecuteFailsForNonOwnerNonAdminInTenant() throws Exception {
    // When S3 multi-tenancy is enabled and the original access key id is assigned to a tenant, verify that a
    // non-owner, non-admin caller is rejected.
    final String tenantId = "finance";
    final String originalAccessKeyId = "alice@EXAMPLE.COM";

    // Caller short name "carol" does not own the access ID and is not
    // configured as tenant admin.
    final UserGroupInformation callerUgi = UserGroupInformation.createRemoteUser("carol@EXAMPLE.COM");
    Server.getCurCall().set(new StubCall(callerUgi));

    final OMException ex;
    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      configureOzoneManagerForPreExecute(ozoneManager, originalAccessKeyId, true);
      when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(true);
      when(ozoneManager.getMultiTenantManager()).thenReturn(omMultiTenantManager);
      // Original access key id is assigned to a tenant and owned by "alice".
      when(omMultiTenantManager.getTenantForAccessID(originalAccessKeyId)).thenReturn(Optional.of(tenantId));
      when(omMultiTenantManager.getUserNameGivenAccessId(originalAccessKeyId)).thenReturn("alice");
      // Caller is not a tenant admin.
      when(omMultiTenantManager.isTenantAdmin(callerUgi, tenantId, false)).thenReturn(false);

      final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(buildRevokeOmRequest(originalAccessKeyId));
      ex = assertThrows(OMException.class, () -> omClientRequest.preExecute(ozoneManager));
    }
    assertEquals(OMException.ResultCodes.USER_MISMATCH, ex.getResult());
  }

  @Test
  public void testPreExecuteRejectsUnknownOriginalAccessKeyId() throws Exception {
    // Reject revocation when originalAccessKeyId has no S3 secret in RocksDB.
    final String originalAccessKeyId = "unknown-access-key-id";
    final UserGroupInformation callerUgi = UserGroupInformation.createRemoteUser(originalAccessKeyId);
    Server.getCurCall().set(new StubCall(callerUgi));

    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      final S3SecretManager s3SecretManager = configureOzoneManagerForPreExecute(
          ozoneManager, originalAccessKeyId, false);
      final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(buildRevokeOmRequest(originalAccessKeyId));
      final OMException ex = assertThrows(OMException.class, () -> omClientRequest.preExecute(ozoneManager));
      assertEquals(OMException.ResultCodes.ACCESS_ID_NOT_FOUND, ex.getResult());
      assertTrue(ex.getMessage().contains("does not exist"));
      assertTrue(ex.getMessage().contains(originalAccessKeyId));
      verify(s3SecretManager).hasS3Secret(originalAccessKeyId);
    }
  }

  @Test
  public void testPreExecuteRejectsUnknownOriginalAccessKeyIdForS3Admin() throws Exception {
    // S3 admins may revoke other principals' tokens, but not for unknown access key IDs.
    final String originalAccessKeyId = "unknown-access-key-id";
    final UserGroupInformation adminUgi = UserGroupInformation.createRemoteUser("om-admin");
    Server.getCurCall().set(new StubCall(adminUgi));

    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      final S3SecretManager s3SecretManager = configureOzoneManagerForPreExecute(
          ozoneManager, originalAccessKeyId, false);
      when(ozoneManager.isS3Admin(adminUgi)).thenReturn(true);

      final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(buildRevokeOmRequest(originalAccessKeyId));
      final OMException ex = assertThrows(OMException.class, () -> omClientRequest.preExecute(ozoneManager));
      assertEquals(OMException.ResultCodes.ACCESS_ID_NOT_FOUND, ex.getResult());
      assertTrue(ex.getMessage().contains("does not exist"));
      assertTrue(ex.getMessage().contains(originalAccessKeyId));
      verify(s3SecretManager).hasS3Secret(originalAccessKeyId);
    }
  }

  @Test
  public void testValidateAndUpdateCacheUpdatesCacheImmediately() {
    final String originalAccessKeyId = "original-access-key-id";
    final long revocationTimeMillis = 1_700_000_000_000L;

    final OzoneManager ozoneManager = mock(OzoneManager.class);
    final OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
    @SuppressWarnings("unchecked")
    final Table<String, Long> s3RevokedStsTokenTable = mock(Table.class);
    final ExecutionContext context = mock(ExecutionContext.class);
    final AuditLogger auditLogger = mock(AuditLogger.class);

    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(omMetadataManager.getS3RevokedStsTokenTable()).thenReturn(s3RevokedStsTokenTable);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);

    final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeRequest =
        OzoneManagerProtocolProtos.RevokeSTSTokenRequest.newBuilder()
            .setOriginalAccessKeyId(originalAccessKeyId)
            .setRevocationTimeMillis(revocationTimeMillis)
            .build();

    final OMRequest omRequest = OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.RevokeSTSToken)
        .setRevokeSTSTokenRequest(revokeRequest)
        .build();

    final S3RevokeSTSTokenRequest s3RevokeSTSTokenRequest = new S3RevokeSTSTokenRequest(omRequest);
    final OMClientResponse omClientResponse = s3RevokeSTSTokenRequest.validateAndUpdateCache(ozoneManager, context);

    assertEquals(OzoneManagerProtocolProtos.Status.OK, omClientResponse.getOMResponse().getStatus());
    verify(s3RevokedStsTokenTable).addCacheEntry(
        eq(new CacheKey<>(originalAccessKeyId)), any());
    assertNotNull(s3RevokeSTSTokenRequest.getAuditBuilder().getAuditMap());
    assertEquals(
        originalAccessKeyId, s3RevokeSTSTokenRequest.getAuditBuilder().getAuditMap().get(
            OzoneConsts.S3_REVOKESTSTOKEN_USER));
  }

  @Test
  public void testValidateAndUpdateCacheRejectsMissingRevocationTimeMillis() {
    final String originalAccessKeyId = "original-access-key-id";

    final OzoneManager ozoneManager = mock(OzoneManager.class);
    final OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
    @SuppressWarnings("unchecked")
    final Table<String, Long> s3RevokedStsTokenTable = mock(Table.class);
    final ExecutionContext context = mock(ExecutionContext.class);

    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(omMetadataManager.getS3RevokedStsTokenTable()).thenReturn(s3RevokedStsTokenTable);

    final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeRequest =
        OzoneManagerProtocolProtos.RevokeSTSTokenRequest.newBuilder()
            .setOriginalAccessKeyId(originalAccessKeyId)
            .build();

    final OMRequest omRequest = OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.RevokeSTSToken)
        .setRevokeSTSTokenRequest(revokeRequest)
        .build();

    final S3RevokeSTSTokenRequest s3RevokeSTSTokenRequest = new S3RevokeSTSTokenRequest(omRequest);
    final OMClientResponse omClientResponse =
        s3RevokeSTSTokenRequest.validateAndUpdateCache(ozoneManager, context);
    assertEquals(OzoneManagerProtocolProtos.Status.INTERNAL_ERROR, omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testPreExecuteRejectsClientSuppliedRevocationTimeMillis() throws Exception {
    final String originalAccessKeyId = "original-access-key-id";
    final UserGroupInformation callerUgi = UserGroupInformation.createRemoteUser(originalAccessKeyId);
    Server.getCurCall().set(new StubCall(callerUgi));

    final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeRequest =
        OzoneManagerProtocolProtos.RevokeSTSTokenRequest.newBuilder()
            .setOriginalAccessKeyId(originalAccessKeyId)
            .setRevocationTimeMillis(1_700_000_000_000L)
            .build();
    final OMRequest omRequest = OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.RevokeSTSToken)
        .setRevokeSTSTokenRequest(revokeRequest)
        .build();

    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      configureOzoneManagerForPreExecute(ozoneManager, originalAccessKeyId, true);
      final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(omRequest);
      final OMException ex = assertThrows(OMException.class, () -> omClientRequest.preExecute(ozoneManager));
      assertEquals(OMException.ResultCodes.INVALID_REQUEST, ex.getResult());
    }
  }

  @Test
  public void testPurgeKeepsNewerCutoffOfSameKey() throws Exception {
    // The cleanup scan read the older cutoff of keys a, b and c, then the purge is applied:
    // - a: a newer cutoff is applied before the purge, so the purge must skip it.
    // - b: the purge deletes it, then a newer cutoff is applied before the flush; the delete marker must not hide it.
    // - c: the purge deletes it and nothing follows; its delete marker leaving the cache shows the cleanup has run.
    final long olderCutoff = 1_700_000_000_000L;
    final long newerCutoff = olderCutoff + TimeUnit.HOURS.toMillis(13);
    final OmMetadataManagerImpl omMetadataManager = createMetadataManager();
    try {
      final OzoneManager ozoneManager = mockOzoneManager(omMetadataManager);
      flush(omMetadataManager, 1L, revoke(ozoneManager, 1L, "a", olderCutoff));
      flush(omMetadataManager, 2L, revoke(ozoneManager, 2L, "b", olderCutoff));
      flush(omMetadataManager, 3L, revoke(ozoneManager, 3L, "c", olderCutoff));
      final OMClientResponse newerRevokeOfA = revoke(ozoneManager, 4L, "a", newerCutoff);
      final OMClientResponse purge = new S3DeleteRevokedSTSTokensRequest(
          buildPurgeOmRequest(
              Arrays.asList("a", "b", "c"), Arrays.asList(olderCutoff, olderCutoff, olderCutoff)))
          .validateAndUpdateCache(ozoneManager, 5L);
      final OMClientResponse newerRevokeOfB = revoke(ozoneManager, 6L, "b", newerCutoff);
      flush(omMetadataManager, 4L, newerRevokeOfA);
      flush(omMetadataManager, 5L, purge);
      flush(omMetadataManager, 6L, newerRevokeOfB);

      final Table<String, Long> table = omMetadataManager.getS3RevokedStsTokenTable();
      assertThat(table.getSkipCache("a")).isEqualTo(newerCutoff);
      assertThat(table.getSkipCache("c")).isNull();
      waitFor(() -> !cachedKeys(table).contains("c"), 100, 5_000);
      for (String originalAccessKeyId : Arrays.asList("a", "b")) {
        assertThat(table.getIfExist(originalAccessKeyId)).as(originalAccessKeyId).isEqualTo(newerCutoff);
        assertThat(table.getSkipCache(originalAccessKeyId)).as(originalAccessKeyId).isEqualTo(newerCutoff);
      }
    } finally {
      omMetadataManager.stop();
    }
  }

  @Test
  public void testPurgeRemovesUnchangedCutoffFromCacheAndDB() throws Exception {
    final String originalAccessKeyId = "original-access-key-id";
    final long cutoff = 1_700_000_000_000L;
    final OmMetadataManagerImpl omMetadataManager = createMetadataManager();
    try {
      final OzoneManager ozoneManager = mockOzoneManager(omMetadataManager);
      flush(omMetadataManager, 1L, revoke(ozoneManager, 1L, originalAccessKeyId, cutoff));
      flush(omMetadataManager, 2L, new S3DeleteRevokedSTSTokensRequest(
          buildPurgeOmRequest(Collections.singletonList(originalAccessKeyId), Collections.singletonList(cutoff)))
          .validateAndUpdateCache(ozoneManager, 2L));

      assertRevokedEntryFullyRemoved(omMetadataManager.getS3RevokedStsTokenTable(), originalAccessKeyId);
    } finally {
      omMetadataManager.stop();
    }
  }

  @Test
  public void testPurgeWithoutCutoffsDeletesListedEntries() throws Exception {
    // A purge submitted without the cutoffs the scan read deletes the listed entries from the cache and the DB.
    final String originalAccessKeyId = "original-access-key-id";
    final long cutoff = 1_700_000_000_000L;
    final OmMetadataManagerImpl omMetadataManager = createMetadataManager();
    try {
      final OzoneManager ozoneManager = mockOzoneManager(omMetadataManager);
      flush(omMetadataManager, 1L, revoke(ozoneManager, 1L, originalAccessKeyId, cutoff));
      flush(omMetadataManager, 2L, new S3DeleteRevokedSTSTokensRequest(buildPurgeOmRequest(
          Collections.singletonList(originalAccessKeyId), Collections.emptyList()))
          .validateAndUpdateCache(ozoneManager, 2L));

      assertRevokedEntryFullyRemoved(omMetadataManager.getS3RevokedStsTokenTable(), originalAccessKeyId);
    } finally {
      omMetadataManager.stop();
    }
  }

  @Test
  public void testPurgeSkipsIdAlreadyMarkedForDeletion() throws Exception {
    // A second purge for the same id before flush sees the delete marker and does not delete again.
    final String originalAccessKeyId = "original-access-key-id";
    final long cutoff = 1_700_000_000_000L;
    final OmMetadataManagerImpl omMetadataManager = createMetadataManager();
    try {
      final OzoneManager ozoneManager = mockOzoneManager(omMetadataManager);
      flush(omMetadataManager, 1L, revoke(ozoneManager, 1L, originalAccessKeyId, cutoff));
      final OMRequest purgeOmRequest = buildPurgeOmRequest(
          Collections.singletonList(originalAccessKeyId), Collections.singletonList(cutoff));
      final OMClientResponse firstPurge = new S3DeleteRevokedSTSTokensRequest(purgeOmRequest)
          .validateAndUpdateCache(ozoneManager, 2L);
      final OMClientResponse secondPurge = new S3DeleteRevokedSTSTokensRequest(purgeOmRequest)
          .validateAndUpdateCache(ozoneManager, 3L);
      final Table<String, Long> table = omMetadataManager.getS3RevokedStsTokenTable();
      assertThat(table.getCacheValue(new CacheKey<>(originalAccessKeyId)).getEpoch()).isEqualTo(2L);
      flush(omMetadataManager, 2L, firstPurge);
      flush(omMetadataManager, 3L, secondPurge);

      assertRevokedEntryFullyRemoved(table, originalAccessKeyId);
    } finally {
      omMetadataManager.stop();
    }
  }

  @Test
  public void testPurgeWithMismatchedCutoffsDeletesNothing() throws Exception {
    // A cutoff list that does not match the key list is rejected with a status that does not terminate the OM,
    // and no entry is deleted from the cache or the DB.
    final long cutoff = 1_700_000_000_000L;
    final OmMetadataManagerImpl omMetadataManager = createMetadataManager();
    try {
      final OzoneManager ozoneManager = mockOzoneManager(omMetadataManager);
      flush(omMetadataManager, 1L, revoke(ozoneManager, 1L, "a", cutoff));
      flush(omMetadataManager, 2L, revoke(ozoneManager, 2L, "b", cutoff));
      // Note the size of the originalAccessKeyId list and the revocationCutoffTimeMillis list in the purge request
      // below is not the same, i.e. mismatched therefore nothing gets deleted
      final OMClientResponse purge = new S3DeleteRevokedSTSTokensRequest(
          buildPurgeOmRequest(Arrays.asList("a", "b"), Collections.singletonList(cutoff)))
          .validateAndUpdateCache(ozoneManager, 3L);
      flush(omMetadataManager, 3L, purge);

      assertThat(purge.getOMResponse().getStatus()).isEqualTo(OzoneManagerProtocolProtos.Status.INVALID_REQUEST);
      final Table<String, Long> table = omMetadataManager.getS3RevokedStsTokenTable();
      for (String originalAccessKeyId : Arrays.asList("a", "b")) {
        assertThat(table.getIfExist(originalAccessKeyId)).as(originalAccessKeyId).isEqualTo(cutoff);
        assertThat(table.getSkipCache(originalAccessKeyId)).as(originalAccessKeyId).isEqualTo(cutoff);
      }
    } finally {
      omMetadataManager.stop();
    }
  }

  private OmMetadataManagerImpl createMetadataManager() throws IOException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, folder.toAbsolutePath().toString());
    return new OmMetadataManagerImpl(conf, null);
  }

  private static OzoneManager mockOzoneManager(OMMetadataManager omMetadataManager) {
    final OzoneManager ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getAuditLogger()).thenReturn(mock(AuditLogger.class));
    return ozoneManager;
  }

  /**
   * Writes the response to the DB and cleans up the table cache, as the double buffer does.
   */
  private static void flush(OmMetadataManagerImpl omMetadataManager, long index, OMClientResponse response)
      throws IOException {
    try (BatchOperation batch = omMetadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(omMetadataManager, batch);
      omMetadataManager.getStore().commitBatchOperation(batch);
    }
    omMetadataManager.getS3RevokedStsTokenTable().cleanupCache(Collections.singletonList(index));
  }

  private static OMRequest buildReplicatedRevokeOmRequest(String originalAccessKeyId, long revocationTimeMillis) {
    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.RevokeSTSToken)
        .setRevokeSTSTokenRequest(OzoneManagerProtocolProtos.RevokeSTSTokenRequest.newBuilder()
            .setOriginalAccessKeyId(originalAccessKeyId)
            .setRevocationTimeMillis(revocationTimeMillis))
        .build();
  }

  private static OMRequest buildPurgeOmRequest(List<String> originalAccessKeyIds, List<Long> revocationTimeMillis) {
    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.DeleteRevokedSTSTokens)
        .setDeleteRevokedSTSTokensRequest(DeleteRevokedSTSTokensRequest.newBuilder()
            .addAllOriginalAccessKeyId(originalAccessKeyIds)
            .addAllRevocationTimeMillis(revocationTimeMillis))
        .build();
  }

  private static OMClientResponse revoke(OzoneManager ozoneManager, long index, String originalAccessKeyId,
      long revocationTimeMillis) {
    return new S3RevokeSTSTokenRequest(
        buildReplicatedRevokeOmRequest(originalAccessKeyId, revocationTimeMillis))
        .validateAndUpdateCache(ozoneManager, index);
  }

  private static Set<String> cachedKeys(Table<String, Long> table) {
    final Set<String> keys = new HashSet<>();
    table.cacheIterator().forEachRemaining(entry -> keys.add(entry.getKey().getCacheKey()));
    return keys;
  }

  private static void assertRevokedEntryFullyRemoved(Table<String, Long> table, String originalAccessKeyId)
      throws Exception {
    assertThat(table.getIfExist(originalAccessKeyId)).isNull();
    assertThat(table.getSkipCache(originalAccessKeyId)).isNull();
    // The full table cache evicts flushed delete markers in the background
    waitFor(() -> !table.cacheIterator().hasNext(), 100, 5_000);
  }

  private static OMRequest buildRevokeOmRequest(String originalAccessKeyId) {
    final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeRequest =
        OzoneManagerProtocolProtos.RevokeSTSTokenRequest.newBuilder()
            .setOriginalAccessKeyId(originalAccessKeyId)
            .build();

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.RevokeSTSToken)
        .setRevokeSTSTokenRequest(revokeRequest)
        .build();
  }

  private static S3SecretManager configureOzoneManagerForPreExecute(OzoneManager ozoneManager,
      String originalAccessKeyId, boolean hasSecret) throws IOException {
    when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(false);
    final S3SecretManager s3SecretManager = mock(S3SecretManager.class);
    when(ozoneManager.getS3SecretManager()).thenReturn(s3SecretManager);
    when(s3SecretManager.hasS3Secret(originalAccessKeyId)).thenReturn(hasSecret);
    return s3SecretManager;
  }

  private static final class StubCall extends ExternalCall<String> {
    private final UserGroupInformation ugi;

    StubCall(UserGroupInformation ugi) {
      super(null);
      this.ugi = ugi;
    }

    @Override
    public UserGroupInformation getRemoteUser() {
      return ugi;
    }
  }
}
