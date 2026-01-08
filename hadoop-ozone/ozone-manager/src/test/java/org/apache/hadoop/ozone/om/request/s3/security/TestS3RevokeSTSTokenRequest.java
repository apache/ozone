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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ipc.ExternalCall;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.STSTokenSecretManager;
import org.apache.hadoop.ozone.security.SecretKeyTestClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link S3RevokeSTSTokenRequest}.
 */
public class TestS3RevokeSTSTokenRequest {

  private static final TestClock CLOCK = TestClock.newInstance();

  private STSTokenSecretManager stsTokenSecretManager;
  private SecretKeyClient secretKeyClient;
  private OMMultiTenantManager omMultiTenantManager;

  @BeforeEach
  public void setUp() throws Exception {
    // Initialize KerberosName rules so that UGI short names derived from
    // principals like "alice@EXAMPLE.COM" are computed correctly.
    KerberosName.setRuleMechanism(DEFAULT_MECHANISM);
    KerberosName.setRules(
        "RULE:[2:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" + "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" + "DEFAULT");

    secretKeyClient = new SecretKeyTestClient();
    stsTokenSecretManager = new STSTokenSecretManager(secretKeyClient);
    // Multi-tenant manager mock used for tests that exercise the S3 multi-tenancy permission branch.
    omMultiTenantManager = mock(OMMultiTenantManager.class);
  }

  @AfterEach
  public void tearDown() {
    Server.getCurCall().remove();
  }

  @Test
  public void testPreExecuteFailsForNonOwnerOfOriginalAccessKey() throws Exception {
    // Verify that preExecute enforces permissions based on the original access key id encoded in the STS token
    // and rejects revocation attempts from non-owners.
    final String tempAccessKeyId = "ASIA12345678";
    final String originalAccessKeyId = "original-access-key-id";
    final String sessionToken = createSessionToken(tempAccessKeyId, originalAccessKeyId);

    // An RPC call running another Kerberos identity should NOT be allowed to revoke the token whose original
    // access key id is different.
    final UserGroupInformation tempUgi = UserGroupInformation.createRemoteUser("another-kerberos-identity");
    Server.getCurCall().set(new StubCall(tempUgi));

    OMException ex;
    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(false);
      when(ozoneManager.isS3Admin(any(UserGroupInformation.class)))
          .thenReturn(false);
      when(ozoneManager.getSecretKeyClient()).thenReturn(secretKeyClient);

      final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeRequest =
          OzoneManagerProtocolProtos.RevokeSTSTokenRequest.newBuilder()
              .setSessionToken(sessionToken)
              .build();

      final OMRequest omRequest = OMRequest.newBuilder()
          .setClientId(UUID.randomUUID().toString())
          .setCmdType(Type.RevokeSTSToken)
          .setRevokeSTSTokenRequest(revokeRequest)
          .build();

      final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(omRequest);

      ex = assertThrows(OMException.class, () -> omClientRequest.preExecute(ozoneManager));
    }
    assertEquals(OMException.ResultCodes.USER_MISMATCH, ex.getResult());
  }

  @Test
  public void testPreExecuteSucceedsForOriginalAccessKeyOwner() throws Exception {
    // Verify that preExecute allows the owner of the original access key id (as encoded in the STS token)
    // to revoke the temporary credentials.
    final String tempAccessKeyId = "ASIA4567891230";
    final String originalAccessKeyId = "original-access-key-id";
    final String sessionToken = createSessionToken(tempAccessKeyId, originalAccessKeyId);

    // Simulate RPC call running as originalAccessKeyId
    final UserGroupInformation originalUgi = UserGroupInformation.createRemoteUser(originalAccessKeyId);
    Server.getCurCall().set(new StubCall(originalUgi));

    final OzoneManager ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(false);
    when(ozoneManager.isS3Admin(any(UserGroupInformation.class)))
        .thenReturn(false);
    when(ozoneManager.getSecretKeyClient()).thenReturn(secretKeyClient);

    final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeRequest =
        OzoneManagerProtocolProtos.RevokeSTSTokenRequest.newBuilder()
            .setSessionToken(sessionToken)
            .build();

    final OMRequest omRequest = OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.RevokeSTSToken)
        .setRevokeSTSTokenRequest(revokeRequest)
        .build();

    final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(omRequest);
    final OMRequest result = omClientRequest.preExecute(ozoneManager);
    assertEquals(Type.RevokeSTSToken, result.getCmdType());
  }

  @Test
  public void testPreExecuteSucceedsForTenantAccessIdOwner() throws Exception {
    // When S3 multi-tenancy is enabled and the original access key id is assigned to a tenant, verify that
    // the tenant access ID owner is allowed to revoke the temporary credentials.
    final String tenantId = "finance";
    final String originalAccessKeyId = "alice@EXAMPLE.COM";
    final String tempAccessKeyId = "ASIA123456789";
    final String sessionToken = createSessionToken(tempAccessKeyId, originalAccessKeyId);

    // Caller short name "alice" should match the owner username returned from the multi-tenant manager.
    final UserGroupInformation callerUgi = UserGroupInformation.createRemoteUser(originalAccessKeyId);
    Server.getCurCall().set(new StubCall(callerUgi));

    final OzoneManager ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(true);
    when(ozoneManager.getMultiTenantManager()).thenReturn(omMultiTenantManager);
    when(ozoneManager.getSecretKeyClient()).thenReturn(secretKeyClient);

    // Original access key id is assigned to a tenant and owned by "alice".
    when(omMultiTenantManager.getTenantForAccessID(originalAccessKeyId))
        .thenReturn(Optional.of(tenantId));
    when(omMultiTenantManager.getUserNameGivenAccessId(originalAccessKeyId))
        .thenReturn("alice");
    // Not a tenant admin; ownership should be sufficient.
    when(omMultiTenantManager.isTenantAdmin(callerUgi, tenantId, false))
        .thenReturn(false);

    final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeRequest =
        OzoneManagerProtocolProtos.RevokeSTSTokenRequest.newBuilder()
            .setSessionToken(sessionToken)
            .build();

    final OMRequest omRequest = OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.RevokeSTSToken)
        .setRevokeSTSTokenRequest(revokeRequest)
        .build();

    final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(omRequest);

    final OMRequest result = omClientRequest.preExecute(ozoneManager);
    assertEquals(Type.RevokeSTSToken, result.getCmdType());
  }

  @Test
  public void testPreExecuteSucceedsForTenantAdmin() throws Exception {
    // When S3 multi-tenancy is enabled and the original access key id is assigned to a tenant, verify that a
    // tenant admin (who is not the owner) is allowed to revoke the temporary credentials.
    final String tenantId = "finance";
    final String originalAccessKeyId = "alice@EXAMPLE.COM";
    final String tempAccessKeyId = "ASIA4567890123";
    final String sessionToken = createSessionToken(tempAccessKeyId, originalAccessKeyId);

    // Caller short name "bob" does not own the access ID but will be configured as tenant admin.
    final UserGroupInformation callerUgi = UserGroupInformation.createRemoteUser("bob@EXAMPLE.COM");
    Server.getCurCall().set(new StubCall(callerUgi));

    final OzoneManager ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(true);
    when(ozoneManager.getMultiTenantManager()).thenReturn(omMultiTenantManager);
    when(ozoneManager.getSecretKeyClient()).thenReturn(secretKeyClient);

    // Original access key id is assigned to a tenant and owned by "alice".
    when(omMultiTenantManager.getTenantForAccessID(originalAccessKeyId))
        .thenReturn(Optional.of(tenantId));
    when(omMultiTenantManager.getUserNameGivenAccessId(originalAccessKeyId))
        .thenReturn("alice");
    // Caller is configured as tenant admin so the check should pass.
    when(omMultiTenantManager.isTenantAdmin(callerUgi, tenantId, false))
        .thenReturn(true);

    final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeRequest =
        OzoneManagerProtocolProtos.RevokeSTSTokenRequest.newBuilder()
            .setSessionToken(sessionToken)
            .build();

    final OMRequest omRequest = OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.RevokeSTSToken)
        .setRevokeSTSTokenRequest(revokeRequest)
        .build();

    final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(omRequest);

    final OMRequest result = omClientRequest.preExecute(ozoneManager);
    assertEquals(Type.RevokeSTSToken, result.getCmdType());
  }

  @Test
  public void testPreExecuteFailsForNonOwnerNonAdminInTenant() throws Exception {
    // When S3 multi-tenancy is enabled and the original access key id is assigned to a tenant, verify that a
    // non-owner, non-admin caller is rejected.
    final String tenantId = "finance";
    final String originalAccessKeyId = "alice@EXAMPLE.COM";
    final String tempAccessKeyId = "ASIA123456789";
    final String sessionToken = createSessionToken(tempAccessKeyId, originalAccessKeyId);

    // Caller short name "carol" does not own the access ID and is not
    // configured as tenant admin.
    final UserGroupInformation callerUgi = UserGroupInformation.createRemoteUser("carol@EXAMPLE.COM");
    Server.getCurCall().set(new StubCall(callerUgi));

    final OMException ex;
    try (OzoneManager ozoneManager = mock(OzoneManager.class)) {
      when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(true);
      when(ozoneManager.getMultiTenantManager()).thenReturn(omMultiTenantManager);
      when(ozoneManager.getSecretKeyClient()).thenReturn(secretKeyClient);

      // Original access key id is assigned to a tenant and owned by "alice".
      when(omMultiTenantManager.getTenantForAccessID(originalAccessKeyId))
          .thenReturn(Optional.of(tenantId));
      when(omMultiTenantManager.getUserNameGivenAccessId(originalAccessKeyId))
          .thenReturn("alice");
      // Caller is not a tenant admin.
      when(omMultiTenantManager.isTenantAdmin(callerUgi, tenantId, false))
          .thenReturn(false);

      final OzoneManagerProtocolProtos.RevokeSTSTokenRequest revokeRequest =
          OzoneManagerProtocolProtos.RevokeSTSTokenRequest.newBuilder()
              .setSessionToken(sessionToken)
              .build();

      final OMRequest omRequest = OMRequest.newBuilder()
          .setClientId(UUID.randomUUID().toString())
          .setCmdType(Type.RevokeSTSToken)
          .setRevokeSTSTokenRequest(revokeRequest)
          .build();

      final OMClientRequest omClientRequest = new S3RevokeSTSTokenRequest(omRequest);

      ex = assertThrows(OMException.class, () -> omClientRequest.preExecute(ozoneManager));
    }
    assertEquals(OMException.ResultCodes.USER_MISMATCH, ex.getResult());
  }

  @Test
  public void testValidateAndUpdateCacheUpdatesCacheImmediately() throws Exception {
    final String tempAccessKeyId = "ASIA4567891230";
    final String originalAccessKeyId = "original-access-key-id";
    final String sessionToken = createSessionToken(tempAccessKeyId, originalAccessKeyId);

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
            .setSessionToken(sessionToken)
            .build();

    final OMRequest omRequest = OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.RevokeSTSToken)
        .setRevokeSTSTokenRequest(revokeRequest)
        .build();

    final S3RevokeSTSTokenRequest s3RevokeSTSTokenRequest = new S3RevokeSTSTokenRequest(omRequest);
    final OMClientResponse omClientResponse = s3RevokeSTSTokenRequest.validateAndUpdateCache(ozoneManager, context);

    assertEquals(OzoneManagerProtocolProtos.Status.OK, omClientResponse.getOMResponse().getStatus());
    verify(s3RevokedStsTokenTable).addCacheEntry(eq(new CacheKey<>(sessionToken)), any(CacheValue.class));
  }

  /**
   * Stub used to inject a remote user into the ProtobufRpcEngine.Server.getRemoteUser() thread-local.
   */
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

  private String createSessionToken(String tempAccessKeyId, String originalAccessKeyId) throws IOException {
    return stsTokenSecretManager.createSTSTokenString(
        tempAccessKeyId, originalAccessKeyId, "arn:aws:iam::123456789012:role/test-role", 3600,
        "test-secret-access-key", "test-session-policy", CLOCK);
  }
}
