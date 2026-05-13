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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_AUDIENCE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ISSUER_URI;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FEATURE_NOT_ENABLED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_EXPIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleWithWebIdentityResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateAssumeRoleWithWebIdentityRequest;
import org.apache.hadoop.ozone.security.STSTokenSecretManager;
import org.apache.hadoop.ozone.security.oidc.AuthCredentials;
import org.apache.hadoop.ozone.security.oidc.OidcAuthenticationException;
import org.apache.hadoop.ozone.security.oidc.OzoneIdentity;
import org.apache.hadoop.ozone.security.oidc.OzoneIdentityProvider;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for OM-authoritative AssumeRoleWithWebIdentity.
 */
public class TestS3AssumeRoleWithWebIdentityRequest {

  private static final String ROLE_ARN =
      "arn:aws:iam::123456789012:role/tomato";
  private static final String ROLE_SESSION_NAME = "tomato-session";
  private static final String PROVIDER_ID = "keycloak";
  private static final String REQUEST_ID = UUID.randomUUID().toString();
  private static final String RAW_JWT =
      "eyJhbGciOiJSUzI1NiJ9.sensitive-web-identity-token.signature";
  private static final String SESSION_POLICY = "session-policy";
  private static final String ISSUER =
      "https://keycloak.example.com/realms/ozone";
  private static final String AUDIENCE = "ozone";
  private static final String SUBJECT = "subject-123";
  private static final String USER = "tomato-user";
  private static final TestClock CLOCK =
      new TestClock(Instant.ofEpochSecond(1764819000), ZoneOffset.UTC);

  private OzoneManager ozoneManager;
  private OzoneConfiguration configuration;
  private IAccessAuthorizer accessAuthorizer;
  private ExecutionContext context;

  @BeforeEach
  public void setup() throws IOException {
    ozoneManager = mock(OzoneManager.class);
    configuration = new OzoneConfiguration();
    configuration.setBoolean(OZONE_STS_WEB_IDENTITY_ENABLED, true);
    configuration.set(OZONE_STS_WEB_IDENTITY_ISSUER_URI, ISSUER);
    configuration.set(OZONE_STS_WEB_IDENTITY_AUDIENCE, AUDIENCE);
    when(ozoneManager.getConfiguration()).thenReturn(configuration);
    when(ozoneManager.getOmRpcServerAddr()).thenReturn(
        new InetSocketAddress("localhost", 9876));
    when(ozoneManager.getAuditLogger()).thenReturn(mock(AuditLogger.class));

    accessAuthorizer = mock(IAccessAuthorizer.class);
    when(accessAuthorizer.generateAssumeRoleWithWebIdentitySessionPolicy(
        any(org.apache.hadoop.ozone.security.acl
            .AssumeRoleWithWebIdentityRequest.class)))
        .thenReturn(SESSION_POLICY);
    when(ozoneManager.getAccessAuthorizer()).thenReturn(accessAuthorizer);

    final STSTokenSecretManager stsTokenSecretManager =
        createTokenSecretManager();
    when(ozoneManager.getSTSTokenSecretManager()).thenReturn(
        stsTokenSecretManager);
    context = ExecutionContext.of(1L, null);
  }

  @Test
  public void testPreExecuteValidatesJwtAndStripsRawToken() throws Exception {
    final CapturingIdentityProvider identityProvider =
        new CapturingIdentityProvider(identity(3600));
    final S3AssumeRoleWithWebIdentityRequest request =
        new S3AssumeRoleWithWebIdentityRequest(externalRequest(3600), CLOCK,
            identityProvider);

    final OMRequest preExecuted = request.preExecute(ozoneManager);

    assertThat(identityProvider.getCapturedToken()).isEqualTo(RAW_JWT);
    assertThat(preExecuted.hasAssumeRoleWithWebIdentityRequest()).isFalse();
    assertThat(preExecuted.hasUpdateAssumeRoleWithWebIdentityRequest())
        .isTrue();
    assertThat(preExecuted.toString()).doesNotContain(RAW_JWT);
    assertThat(new String(preExecuted.toByteArray(), StandardCharsets.ISO_8859_1))
        .doesNotContain(RAW_JWT);

    final UpdateAssumeRoleWithWebIdentityRequest update =
        preExecuted.getUpdateAssumeRoleWithWebIdentityRequest();
    assertThat(update.getRoleArn()).isEqualTo(ROLE_ARN);
    assertThat(update.getRoleSessionName()).isEqualTo(ROLE_SESSION_NAME);
    assertThat(update.getDurationSeconds()).isEqualTo(3600);
    assertThat(update.getProviderId()).isEqualTo(PROVIDER_ID);
    assertThat(update.getRequestId()).isEqualTo(REQUEST_ID);
    assertThat(update.getEffectiveUser()).isEqualTo(USER);
    assertThat(update.getSubject()).isEqualTo(SUBJECT);
    assertThat(update.getIssuer()).isEqualTo(ISSUER);
    assertThat(update.getAudience()).isEqualTo(AUDIENCE);
    assertThat(update.getGroupsList()).containsExactly("ozone-tomato");
    assertThat(update.getRolesList()).containsExactly("role:writer");
    assertThat(update.getSessionPolicy()).isEqualTo(SESSION_POLICY);
    assertThat(update.getTempAccessKeyId()).startsWith("ASIA");
    assertThat(update.getSecretAccessKey()).hasSize(40);
    assertThat(update.getTokenFingerprint()).hasSize(64);
    assertThat(update.getCredentialExpirationEpochSeconds())
        .isEqualTo(CLOCK.instant().plusSeconds(3600).getEpochSecond());
  }

  @Test
  public void testAuthorizerReceivesMappedIdentityAndContext()
      throws Exception {
    final S3AssumeRoleWithWebIdentityRequest request =
        new S3AssumeRoleWithWebIdentityRequest(externalRequest(3600), CLOCK,
            new CapturingIdentityProvider(identity(3600)));

    request.preExecute(ozoneManager);

    final ArgumentCaptor<org.apache.hadoop.ozone.security.acl
        .AssumeRoleWithWebIdentityRequest> captor =
        ArgumentCaptor.forClass(org.apache.hadoop.ozone.security.acl
            .AssumeRoleWithWebIdentityRequest.class);
    verify(accessAuthorizer)
        .generateAssumeRoleWithWebIdentitySessionPolicy(captor.capture());

    final org.apache.hadoop.ozone.security.acl
        .AssumeRoleWithWebIdentityRequest captured = captor.getValue();
    assertThat(captured.getUser()).isEqualTo(USER);
    assertThat(captured.getGroups()).containsExactly("ozone-tomato");
    assertThat(captured.getRoles()).containsExactly("role:writer");
    assertThat(captured.getAction()).isEqualTo(
        org.apache.hadoop.ozone.security.acl
            .AssumeRoleWithWebIdentityRequest.ACTION);
    assertThat(captured.getRoleArn()).isEqualTo(ROLE_ARN);
    assertThat(captured.getRoleSessionName()).isEqualTo(ROLE_SESSION_NAME);
    assertThat(captured.getIssuer()).isEqualTo(ISSUER);
    assertThat(captured.getSubject()).isEqualTo(SUBJECT);
    assertThat(captured.getAudience()).isEqualTo(AUDIENCE);
    assertThat(captured.getProviderId()).isEqualTo(PROVIDER_ID);
  }

  @Test
  public void testValidateAndUpdateCacheUsesOnlySanitizedRequest()
      throws Exception {
    final CapturingIdentityProvider identityProvider =
        new CapturingIdentityProvider(identity(3600));
    final S3AssumeRoleWithWebIdentityRequest request =
        new S3AssumeRoleWithWebIdentityRequest(externalRequest(3600), CLOCK,
            identityProvider);
    final OMRequest preExecuted = request.preExecute(ozoneManager);

    final OzoneIdentityProvider replayProvider =
        credentials -> {
          throw new AssertionError("OIDC provider must not be called during "
              + "Ratis apply/replay");
        };
    final S3AssumeRoleWithWebIdentityRequest replayRequest =
        new S3AssumeRoleWithWebIdentityRequest(preExecuted,
            new TestClock(CLOCK.instant().plusSeconds(86400), ZoneOffset.UTC),
            replayProvider);

    final OMClientResponse clientResponse =
        replayRequest.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = clientResponse.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omResponse.hasAssumeRoleWithWebIdentityResponse()).isTrue();
    final AssumeRoleWithWebIdentityResponse response =
        omResponse.getAssumeRoleWithWebIdentityResponse();
    assertThat(response.getAccessKeyId()).startsWith("ASIA");
    assertThat(response.getSecretAccessKey()).hasSize(40);
    assertThat(response.getSessionToken()).isNotBlank();
    assertThat(response.getExpirationEpochSeconds())
        .isEqualTo(CLOCK.instant().plusSeconds(3600).getEpochSecond());
    assertThat(response.getAssumedRoleId()).contains(":" + ROLE_SESSION_NAME);
    assertThat(response.getSubjectFromWebIdentityToken()).isEqualTo(SUBJECT);
    assertThat(response.getAudience()).isEqualTo(AUDIENCE);
    assertThat(response.getProvider()).isEqualTo(PROVIDER_ID);

    verify(accessAuthorizer)
        .generateAssumeRoleWithWebIdentitySessionPolicy(any());
    verifyNoMoreInteractions(accessAuthorizer);
  }

  @Test
  public void testDurationIsClampedToWebIdentityTokenLifetime()
      throws Exception {
    final S3AssumeRoleWithWebIdentityRequest request =
        new S3AssumeRoleWithWebIdentityRequest(externalRequest(3600), CLOCK,
            new CapturingIdentityProvider(identity(1200)));

    final OMRequest preExecuted = request.preExecute(ozoneManager);

    assertThat(preExecuted.getUpdateAssumeRoleWithWebIdentityRequest()
        .getDurationSeconds()).isEqualTo(1200);
    assertThat(preExecuted.getUpdateAssumeRoleWithWebIdentityRequest()
        .getCredentialExpirationEpochSeconds())
        .isEqualTo(CLOCK.instant().plusSeconds(1200).getEpochSecond());
  }

  @Test
  public void testTokenExpiringBeforeMinimumDurationFailsClosed() {
    final S3AssumeRoleWithWebIdentityRequest request =
        new S3AssumeRoleWithWebIdentityRequest(externalRequest(3600), CLOCK,
            new CapturingIdentityProvider(identity(899)));

    assertThatThrownBy(() -> request.preExecute(ozoneManager))
        .isInstanceOf(OMException.class)
        .satisfies(e -> assertThat(((OMException) e).getResult())
            .isEqualTo(TOKEN_EXPIRED))
        .hasMessageContaining("expires before the minimum STS credential");
  }

  @Test
  public void testDisabledFeatureFailsBeforeTokenValidation() {
    configuration.setBoolean(OZONE_STS_WEB_IDENTITY_ENABLED, false);
    final CapturingIdentityProvider identityProvider =
        new CapturingIdentityProvider(identity(3600));
    final S3AssumeRoleWithWebIdentityRequest request =
        new S3AssumeRoleWithWebIdentityRequest(externalRequest(3600), CLOCK,
            identityProvider);

    assertThatThrownBy(() -> request.preExecute(ozoneManager))
        .isInstanceOf(OMException.class)
        .satisfies(e -> assertThat(((OMException) e).getResult())
            .isEqualTo(FEATURE_NOT_ENABLED));
    assertThat(identityProvider.getCapturedToken()).isNull();
  }

  private static OMRequest externalRequest(int durationSeconds) {
    return OMRequest.newBuilder()
        .setCmdType(Type.AssumeRoleWithWebIdentity)
        .setClientId("client-1")
        .setAssumeRoleWithWebIdentityRequest(
            org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
                .AssumeRoleWithWebIdentityRequest.newBuilder()
                .setRoleArn(ROLE_ARN)
                .setRoleSessionName(ROLE_SESSION_NAME)
                .setDurationSeconds(durationSeconds)
                .setProviderId(PROVIDER_ID)
                .setRequestId(REQUEST_ID)
                .setWebIdentityToken(RAW_JWT)
                .build())
        .build();
  }

  private static OzoneIdentity identity(long expiresInSeconds) {
    return OzoneIdentity.newBuilder()
        .setUsername(USER)
        .setSubject(SUBJECT)
        .setIssuer(ISSUER)
        .setGroups(set("ozone-tomato"))
        .setRoles(set("role:writer"))
        .setAuthenticatedAt(CLOCK.instant())
        .setExpiresAt(CLOCK.instant().plusSeconds(expiresInSeconds))
        .build();
  }

  private static Set<String> set(String... values) {
    return new LinkedHashSet<>(Arrays.asList(values));
  }

  private static STSTokenSecretManager createTokenSecretManager()
      throws IOException {
    final SecretKeySignerClient secretKeyClient =
        mock(SecretKeySignerClient.class);
    final ManagedSecretKey managedSecretKey = mock(ManagedSecretKey.class);
    final SecretKey secretKey = new SecretKeySpec(
        "testSecretKey".getBytes(StandardCharsets.UTF_8), "HmacSHA256");
    final UUID secretKeyId = UUID.randomUUID();

    when(secretKeyClient.getCurrentSecretKey()).thenReturn(managedSecretKey);
    when(managedSecretKey.getSecretKey()).thenReturn(secretKey);
    when(managedSecretKey.getId()).thenReturn(secretKeyId);
    when(managedSecretKey.sign(any(TokenIdentifier.class))).thenReturn(
        "signature".getBytes(StandardCharsets.UTF_8));
    return new STSTokenSecretManager(secretKeyClient);
  }

  private static final class CapturingIdentityProvider
      implements OzoneIdentityProvider {

    private final OzoneIdentity identity;
    private String capturedToken;

    private CapturingIdentityProvider(OzoneIdentity identity) {
      this.identity = identity;
    }

    @Override
    public OzoneIdentity authenticate(AuthCredentials credentials)
        throws OidcAuthenticationException {
      capturedToken = credentials.getBearerToken();
      return identity;
    }

    private String getCapturedToken() {
      return capturedToken;
    }
  }
}
