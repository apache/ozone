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

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.security.STSTokenSecretManager;
import org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.OzoneGrant;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

/**
 * Unit tests for S3AssumeRoleRequest.
 */
public class TestS3AssumeRoleRequest {

  private static final String ROLE_ARN_1 = "arn:aws:iam::123456789012:role/MyRole1";
  private static final String SESSION_NAME = "testSessionName";
  private static final String ORIGINAL_ACCESS_KEY_ID = "origAccessKeyId";
  private static final String TARGET_ROLE_NAME = "targetRole";
  private static final String SESSION_POLICY_VALUE = "session-policy";
  private static final String AWS_IAM_POLICY = "{\n" +
      "  \"Statement\": [{\n" +
      "    \"Effect\": \"Allow\",\n" +
      "    \"Action\": \"s3:*\",\n" +
      "    \"Resource\": \"arn:aws:s3:::*/*\"\n" +
      "  }]\n" +
      "}";

  private static final TestClock CLOCK = new TestClock(Instant.ofEpochMilli(1764819000), ZoneOffset.UTC);
  private static final String OM_HOST = "om-host";
  private static final InetAddress LOOPBACK_IP = InetAddress.getLoopbackAddress();
  private static final Set<OzoneGrant> EMPTY_GRANTS = Collections.singleton(new OzoneGrant(emptySet(), emptySet()));
  private static final String REQUEST_ID = UUID.randomUUID().toString();

  private static final Pattern ABC_PATTERN_32 = Pattern.compile("^[ABC]{32}$");
  private static final Pattern XYZ_PATTERN = Pattern.compile("^[XYZ]$");

  private OzoneManager ozoneManager;
  private ExecutionContext context;
  private IAccessAuthorizer accessAuthorizer;
  private AuditLogger auditLogger;

  @BeforeEach
  public void setup() throws IOException {
    ozoneManager = mock(OzoneManager.class);
    auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);

    final OzoneConfiguration configuration = new OzoneConfiguration();
    when(ozoneManager.getConfiguration()).thenReturn(configuration);
    when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(false);

    final SecretKeySignerClient secretKeyClient = mock(SecretKeySignerClient.class);
    final ManagedSecretKey managedSecretKey = mock(ManagedSecretKey.class);
    final SecretKey secretKey = new SecretKeySpec(
        "testSecretKey".getBytes(StandardCharsets.UTF_8), "HmacSHA256");
    final UUID secretKeyId = UUID.randomUUID();

    when(secretKeyClient.getCurrentSecretKey()).thenReturn(managedSecretKey);
    when(managedSecretKey.getSecretKey()).thenReturn(secretKey);
    when(managedSecretKey.getId()).thenReturn(secretKeyId);
    when(managedSecretKey.sign(any(TokenIdentifier.class))).thenReturn(
        "signature".getBytes(StandardCharsets.UTF_8));

    final STSTokenSecretManager stsTokenSecretManager = new STSTokenSecretManager(secretKeyClient);

    when(ozoneManager.getOmRpcServerAddr()).thenReturn(
        new InetSocketAddress("localhost", 9876));
    when(ozoneManager.getSTSTokenSecretManager()).thenReturn(stsTokenSecretManager);

    accessAuthorizer = mock(IAccessAuthorizer.class);
    when(ozoneManager.getAccessAuthorizer()).thenReturn(accessAuthorizer);
    when(accessAuthorizer.generateAssumeRoleSessionPolicy(any(
        org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.class)))
        .thenReturn(SESSION_POLICY_VALUE);

    context = ExecutionContext.of(1L, null);
  }

  @Test
  public void testInvalidDurationTooShort() {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(SESSION_NAME)
                .setDurationSeconds(899)  // less than 900
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse response = request.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.INVALID_REQUEST);
    assertThat(omResponse.getMessage()).isEqualTo(
        "Invalid Value: DurationSeconds must be between 900 and 43200 seconds");
    assertThat(omResponse.hasAssumeRoleResponse()).isFalse();
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testInvalidDurationTooLong() {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(SESSION_NAME)
                .setDurationSeconds(43201)  // more than 43200
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse response = request.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.INVALID_REQUEST);
    assertThat(omResponse.getMessage()).isEqualTo(
        "Invalid Value: DurationSeconds must be between 900 and 43200 seconds");
    assertThat(omResponse.hasAssumeRoleResponse()).isFalse();
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testValidDurationMaxBoundary() {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(SESSION_NAME)
                .setDurationSeconds(43200)  // exactly max
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse response = request.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omResponse.hasAssumeRoleResponse()).isTrue();
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testValidDurationMinBoundary() {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(SESSION_NAME)
                .setDurationSeconds(900)  // exactly min
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse response = request.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omResponse.hasAssumeRoleResponse()).isTrue();
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testMissingS3Authentication() {
    final OMRequest omRequest = OMRequest.newBuilder()  // note: not using baseOMRequestBuilder that has S3 auth
        .setCmdType(Type.AssumeRole)
        .setClientId("client-1")
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(SESSION_NAME)
                .setDurationSeconds(3600)
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse response = request.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.INVALID_REQUEST);
    assertThat(omResponse.getMessage()).isEqualTo("S3AssumeRoleRequest does not have S3 authentication");
    assertThat(omResponse.hasAssumeRoleResponse()).isFalse();
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testSuccessfulAssumeRoleGeneratesCredentials() {
    final int durationSeconds = 3600;
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(SESSION_NAME)
                .setDurationSeconds(durationSeconds)
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse clientResponse = request.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = clientResponse.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omResponse.hasAssumeRoleResponse()).isTrue();
    assertThat(omResponse.getCmdType()).isEqualTo(Type.AssumeRole);

    final AssumeRoleResponse assumeRoleResponse = omResponse.getAssumeRoleResponse();

    // AccessKeyId: prefix ASIA + 20 chars
    assertThat(assumeRoleResponse.getAccessKeyId()).startsWith("ASIA");
    assertThat(assumeRoleResponse.getAccessKeyId().length()).isEqualTo(24);  // 20 chars + 4 chars from ASIA

    // SecretAccessKey: 40 chars
    assertThat(assumeRoleResponse.getSecretAccessKey().length()).isEqualTo(40);

    // AssumedRoleId: prefix AROA + 16 chars, followed by ":" and sessionName
    assertThat(assumeRoleResponse.getAssumedRoleId())
        .startsWith("AROA")
        .contains(":" + SESSION_NAME);
    final int expectedAssumedRoleIdLength = 4 + 16 + 1 + SESSION_NAME.length(); // 4 for AROA, 16 chars, 1 for ":"
    assertThat(assumeRoleResponse.getAssumedRoleId().length()).isEqualTo(expectedAssumedRoleIdLength);

    // Verify expiration added durationSeconds
    final long expirationEpochSeconds = assumeRoleResponse.getExpirationEpochSeconds();
    assertThat(expirationEpochSeconds).isEqualTo(CLOCK.instant().getEpochSecond() + durationSeconds);
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testGenerateSecureRandomStringUsingChars() {
    final String chars = "ABC";
    final int length = 32;
    final String s = S3AssumeRoleRequest.generateSecureRandomStringUsingChars(
        chars, chars.length(), length);
    assertThat(s).hasSize(length).matches(ABC_PATTERN_32);

    // Test with length 0
    final String empty = S3AssumeRoleRequest.generateSecureRandomStringUsingChars(
        "ABC", 3, 0);
    assertThat(empty).isEmpty();

    // Test with length 1
    final String single = S3AssumeRoleRequest.generateSecureRandomStringUsingChars(
        "XYZ", 3, 1);
    assertThat(single).hasSize(1).matches(XYZ_PATTERN);
  }

  @Test
  public void testAssumeRoleCredentialsAreUnique() {
    // Test that multiple calls generate different credentials
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(SESSION_NAME)
                .setDurationSeconds(3600)
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request1 = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse response1 = request1.validateAndUpdateCache(ozoneManager, context);
    final S3AssumeRoleRequest request2 = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse response2 = request2.validateAndUpdateCache(ozoneManager, context);

    final AssumeRoleResponse assumeRoleResponse1 = response1.getOMResponse().getAssumeRoleResponse();
    final AssumeRoleResponse assumeRoleResponse2 = response2.getOMResponse().getAssumeRoleResponse();

    // Different access keys
    assertThat(assumeRoleResponse1.getAccessKeyId()).isNotEqualTo(assumeRoleResponse2.getAccessKeyId());

    // Different secret keys
    assertThat(assumeRoleResponse1.getSecretAccessKey()).isNotEqualTo(assumeRoleResponse2.getSecretAccessKey());

    // Different session tokens
    assertThat(assumeRoleResponse1.getSessionToken()).isNotEqualTo(assumeRoleResponse2.getSessionToken());

    // Different assumed role IDs
    assertThat(assumeRoleResponse1.getAssumedRoleId()).isNotEqualTo(assumeRoleResponse2.getAssumedRoleId());

    OMAuditLogger.log(request1.getAuditBuilder());
    OMAuditLogger.log(request2.getAuditBuilder());
    verify(auditLogger, times(2)).logWrite(any(AuditMessage.class));
  }

  @Test
  public void testAssumeRoleWithEmptySessionName() {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName("")
                .setDurationSeconds(3600)
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse response = request.validateAndUpdateCache(ozoneManager, context);
    assertThat(response.getOMResponse().getStatus()).isEqualTo(Status.INVALID_REQUEST);
    assertThat(response.getOMResponse().getMessage()).isEqualTo(
        "Value null at 'roleSessionName' failed to satisfy constraint: Member must not be null");
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testInvalidAssumeRoleSessionNameTooShort() {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName("T")   // Less than 2 characters
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse response = request.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.INVALID_REQUEST);
    assertThat(omResponse.getMessage()).isEqualTo(
        "Invalid RoleSessionName length 1: it must be 2-64 characters long and contain only alphanumeric " +
        "characters and +, =, ,, ., @, -");
    assertThat(omResponse.hasAssumeRoleResponse()).isFalse();
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testInvalidRoleSessionNameTooLong() {
    final String tooLongRoleSessionName = S3SecurityTestUtils.repeat('h', 70);
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(tooLongRoleSessionName)
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse response = request.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.INVALID_REQUEST);
    assertThat(omResponse.getMessage()).isEqualTo(
        "Invalid RoleSessionName length 70: it must be 2-64 characters long and contain only alphanumeric " +
        "characters and +, =, ,, ., @, -"
    );
    assertThat(omResponse.hasAssumeRoleResponse()).isFalse();
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testValidRoleSessionNameMaxLengthBoundary() {
    final String roleSessionName = S3SecurityTestUtils.repeat('g', 64);
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(roleSessionName)  // exactly max length
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse response = request.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omResponse.hasAssumeRoleResponse()).isTrue();
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testValidRoleSessionNameMinLengthBoundary() {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName("TT")   // exactly min length
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse response = request.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omResponse.hasAssumeRoleResponse()).isTrue();
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testAssumeRoleWithSessionPolicyPresent() {
    final String sessionPolicy = "{\"Version\":\"2012-10-17\",\"Statement\":[]}";
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(SESSION_NAME)
                .setDurationSeconds(3600)
                .setAwsIamSessionPolicy(sessionPolicy)
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMClientResponse response = request.validateAndUpdateCache(ozoneManager, context);
    assertThat(response.getOMResponse().getStatus()).isEqualTo(Status.OK);
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testGetSessionPolicyUsesDefaultVolumeWhenMultiTenantDisabled() throws Exception {
    when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(false);

    // Ensure s3v default volume was captured in the method invocation
    final org.apache.hadoop.ozone.security.acl.AssumeRoleRequest capturedAssumeRoleRequest =
        captureAssumeRoleRequest("s3v", "userNameA");

    assertThat(capturedAssumeRoleRequest.getHost()).isEqualTo(OM_HOST);
    assertThat(capturedAssumeRoleRequest.getIp()).isEqualTo(LOOPBACK_IP);
    assertThat(capturedAssumeRoleRequest.getTargetRoleName()).isEqualTo(TARGET_ROLE_NAME);
    assertThat(capturedAssumeRoleRequest.getGrants()).isEqualTo(EMPTY_GRANTS);
  }

  @Test
  public void testGetSessionPolicyResolvesIamPolicyWithTenantVolume() throws Exception {
    when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(true);

    final OMMultiTenantManager multiTenantManager = mock(OMMultiTenantManager.class);
    when(ozoneManager.getMultiTenantManager()).thenReturn(multiTenantManager);
    when(multiTenantManager.getTenantForAccessID(ORIGINAL_ACCESS_KEY_ID)).thenReturn(Optional.of("tenant-a"));
    when(multiTenantManager.getTenantVolumeName("tenant-a")).thenReturn("tenant-a-volume");

    // Ensure "tenant-a-volume" was captured in the method invocation
    final org.apache.hadoop.ozone.security.acl.AssumeRoleRequest capturedAssumeRoleRequest =
        captureAssumeRoleRequest("tenant-a-volume", "userNameA");

    assertThat(capturedAssumeRoleRequest.getHost()).isEqualTo(OM_HOST);
    assertThat(capturedAssumeRoleRequest.getIp()).isEqualTo(LOOPBACK_IP);
    assertThat(capturedAssumeRoleRequest.getTargetRoleName()).isEqualTo(TARGET_ROLE_NAME);
    assertThat(capturedAssumeRoleRequest.getGrants()).isEqualTo(EMPTY_GRANTS);
  }

  @Test
  public void testGetSessionPolicyFallsBackToDefaultVolumeWhenTenantMissing() throws Exception {
    when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(true);

    final OMMultiTenantManager multiTenantManager = mock(OMMultiTenantManager.class);
    when(ozoneManager.getMultiTenantManager()).thenReturn(multiTenantManager);
    when(multiTenantManager.getTenantForAccessID(ORIGINAL_ACCESS_KEY_ID)).thenReturn(Optional.empty());

    // Ensure s3v default volume was captured in the method invocation since tenant was missing
    final org.apache.hadoop.ozone.security.acl.AssumeRoleRequest capturedAssumeRoleRequest =
        captureAssumeRoleRequest("s3v", "userNameB");

    verify(multiTenantManager, never()).getTenantVolumeName(any());
    assertThat(capturedAssumeRoleRequest.getHost()).isEqualTo(OM_HOST);
    assertThat(capturedAssumeRoleRequest.getIp()).isEqualTo(LOOPBACK_IP);
    assertThat(capturedAssumeRoleRequest.getTargetRoleName()).isEqualTo(TARGET_ROLE_NAME);
    assertThat(capturedAssumeRoleRequest.getGrants()).isEqualTo(EMPTY_GRANTS);
  }

  @Test
  public void testGetSessionPolicyWithBlankAwsPolicyCapturesNullGrants() throws Exception {
    when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(false);

    final String awsIamPolicy = null;
    try (MockedStatic<IamSessionPolicyResolver> resolverMock = mockStatic(IamSessionPolicyResolver.class)) {
      final String result = new S3AssumeRoleRequest(baseOmRequestBuilder().build(), CLOCK)
          .getSessionPolicy(
              ozoneManager, ORIGINAL_ACCESS_KEY_ID, awsIamPolicy, OM_HOST, LOOPBACK_IP,
              UserGroupInformation.createRemoteUser("userNameC"), TARGET_ROLE_NAME);

      assertThat(result).isEqualTo(SESSION_POLICY_VALUE);

      // Ensure IamSessionPolicyResolver was never invoked since awsIamPolicy is null
      resolverMock.verifyNoInteractions();
    }

    final ArgumentCaptor<org.apache.hadoop.ozone.security.acl.AssumeRoleRequest> captor =
        ArgumentCaptor.forClass(org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.class);
    verify(accessAuthorizer).generateAssumeRoleSessionPolicy(captor.capture());

    final org.apache.hadoop.ozone.security.acl.AssumeRoleRequest capturedAssumeRoleRequest = captor.getValue();
    assertThat(capturedAssumeRoleRequest.getHost()).isEqualTo(OM_HOST);
    assertThat(capturedAssumeRoleRequest.getIp()).isEqualTo(LOOPBACK_IP);
    assertThat(capturedAssumeRoleRequest.getTargetRoleName()).isEqualTo(TARGET_ROLE_NAME);
    assertThat(capturedAssumeRoleRequest.getGrants()).isNull();
  }

  private org.apache.hadoop.ozone.security.acl.AssumeRoleRequest captureAssumeRoleRequest(String volumeName,
      String userName) throws Exception {
    try (MockedStatic<IamSessionPolicyResolver> resolverMock = mockStatic(IamSessionPolicyResolver.class)) {
      resolverMock.when(() -> IamSessionPolicyResolver.resolve(
              AWS_IAM_POLICY, volumeName, IamSessionPolicyResolver.AuthorizerType.RANGER))
          .thenReturn(EMPTY_GRANTS);

      final String result = new S3AssumeRoleRequest(baseOmRequestBuilder().build(), CLOCK)
          .getSessionPolicy(
              ozoneManager, ORIGINAL_ACCESS_KEY_ID, AWS_IAM_POLICY, OM_HOST, LOOPBACK_IP,
              UserGroupInformation.createRemoteUser(userName), TARGET_ROLE_NAME);

      assertThat(result).isEqualTo(SESSION_POLICY_VALUE);
      resolverMock.verify(() -> IamSessionPolicyResolver.resolve(
          AWS_IAM_POLICY, volumeName, IamSessionPolicyResolver.AuthorizerType.RANGER));
    }

    final ArgumentCaptor<org.apache.hadoop.ozone.security.acl.AssumeRoleRequest> captor =
        ArgumentCaptor.forClass(org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.class);
    verify(accessAuthorizer).generateAssumeRoleSessionPolicy(captor.capture());
    return captor.getValue();
  }

  private static OMRequest.Builder baseOmRequestBuilder() {
    return OMRequest.newBuilder()
        .setCmdType(Type.AssumeRole)
        .setClientId("client-1")
        .setS3Authentication(
            S3Authentication.newBuilder()
                .setAccessId(ORIGINAL_ACCESS_KEY_ID)
        );
  }

  private void assertMarkForAuditCalled(S3AssumeRoleRequest request) {
    OMAuditLogger.log(request.getAuditBuilder());
    verify(auditLogger).logWrite(any(AuditMessage.class));
  }
}


