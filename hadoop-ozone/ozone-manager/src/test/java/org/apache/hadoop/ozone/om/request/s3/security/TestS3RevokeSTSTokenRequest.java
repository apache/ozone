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
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.ipc_.ExternalCall;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link S3RevokeSTSTokenRequest}.
 */
public class TestS3RevokeSTSTokenRequest {

  private static final String TEST_KERBEROS_RULES =
      "RULE:[2:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" + "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" + "DEFAULT";

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
            OzoneConsts.S3_STS_ORIGINAL_ACCESS_KEY_ID));
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
  public void testPreExecuteRejectsOverlongOriginalAccessKeyId() throws Exception {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < OzoneConsts.OZONE_MAXIMUM_ACCESS_ID_LENGTH; i++) {
      sb.append('a');
    }
    final UserGroupInformation callerUgi = UserGroupInformation.createRemoteUser("caller");
    Server.getCurCall().set(new StubCall(callerUgi));

    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      configureOzoneManagerForPreExecute(ozoneManager, sb.toString(), false);
      final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(buildRevokeOmRequest(sb.toString()));
      final OMException ex = assertThrows(OMException.class, () -> omClientRequest.preExecute(ozoneManager));
      assertEquals(OMException.ResultCodes.INVALID_REQUEST, ex.getResult());
    }
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
