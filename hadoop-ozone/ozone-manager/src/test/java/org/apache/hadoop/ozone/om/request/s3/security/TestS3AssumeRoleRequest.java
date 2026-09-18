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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateAssumeRoleRequest;
import org.apache.hadoop.ozone.security.STSTokenSecretManager;
import org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.OzoneGrant;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ozone.test.MockClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

/**
 * Unit tests for S3AssumeRoleRequest.
 */
public class TestS3AssumeRoleRequest {

  private static final String ROLE_ARN_1 = "arn:aws:iam::123456789012:role/MyRole1";
  private static final String ROLE_ARN_2 = "arn:aws:iam::123456789012:role/MyRole2";
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

  private static final MockClock CLOCK = new MockClock(Instant.ofEpochMilli(1764819000), ZoneOffset.UTC);
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
    final OMException exception = assertThrows(OMException.class, () -> request.preExecute(ozoneManager));

    assertThat(exception.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(exception.getMessage()).isEqualTo(
        "Invalid Value: DurationSeconds must be between 900 and 43200 seconds");
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
    final OMException exception = assertThrows(OMException.class, () -> request.preExecute(ozoneManager));

    assertThat(exception.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(exception.getMessage()).isEqualTo(
        "Invalid Value: DurationSeconds must be between 900 and 43200 seconds");
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testValidDurationMaxBoundary() throws IOException {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(SESSION_NAME)
                .setDurationSeconds(43200)  // exactly max
                .setRequestId(REQUEST_ID)
        ).build();

    // Call preExecute first to generate credentials
    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMRequest preExecutedRequest = request.preExecute(ozoneManager);

    assertLeaderGeneratedAssumeRoleFields(preExecutedRequest, 43200);

    final S3AssumeRoleRequest requestWithCredentials = new S3AssumeRoleRequest(preExecutedRequest, CLOCK);
    final OMClientResponse response = requestWithCredentials.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omResponse.hasAssumeRoleResponse()).isTrue();
    verify(accessAuthorizer).generateAssumeRoleSessionPolicy(
        any(org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.class));
    assertMarkForAuditCalled(requestWithCredentials);
  }

  @Test
  public void testValidDurationMinBoundary() throws IOException {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(SESSION_NAME)
                .setDurationSeconds(900)  // exactly min
                .setRequestId(REQUEST_ID)
        ).build();

    // Call preExecute first to generate credentials
    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMRequest preExecutedRequest = request.preExecute(ozoneManager);

    assertLeaderGeneratedAssumeRoleFields(preExecutedRequest, 900);

    final S3AssumeRoleRequest requestWithCredentials = new S3AssumeRoleRequest(preExecutedRequest, CLOCK);
    final OMClientResponse response = requestWithCredentials.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omResponse.hasAssumeRoleResponse()).isTrue();
    verify(accessAuthorizer).generateAssumeRoleSessionPolicy(
        any(org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.class));
    assertMarkForAuditCalled(requestWithCredentials);
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
    final OMException exception = assertThrows(OMException.class, () -> request.preExecute(ozoneManager));

    assertThat(exception.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(exception.getMessage()).isEqualTo("S3AssumeRoleRequest does not have S3 authentication");
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testSuccessfulAssumeRoleGeneratesCredentials() throws IOException {
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
    // Call preExecute first to generate credentials
    final OMRequest preExecutedRequest = request.preExecute(ozoneManager);

    assertLeaderGeneratedAssumeRoleFields(preExecutedRequest, durationSeconds);

    final S3AssumeRoleRequest requestWithCredentials = new S3AssumeRoleRequest(preExecutedRequest, CLOCK);
    final OMClientResponse clientResponse = requestWithCredentials.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = clientResponse.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omResponse.hasAssumeRoleResponse()).isTrue();
    assertThat(omResponse.getCmdType()).isEqualTo(Type.AssumeRole);

    final AssumeRoleResponse assumeRoleResponse = omResponse.getAssumeRoleResponse();
    assertThat(assumeRoleResponse.getSessionToken()).isEqualTo(
        preExecutedRequest.getUpdateAssumeRoleRequest().getSessionToken());

    // AccessKeyId: prefix ASIA + 20 chars
    assertThat(assumeRoleResponse.getAccessKeyId()).startsWith("ASIA");
    assertThat(assumeRoleResponse.getAccessKeyId().length()).isEqualTo(24);  // 20 chars + 4 chars from ASIA

    // SecretAccessKey: 40 chars
    assertThat(assumeRoleResponse.getSecretAccessKey().length()).isEqualTo(40);

    // AssumedRoleId: prefix AROA + 16 chars, followed by ":" and sessionName
    final String expectedRoleId = S3AssumeRoleRequest.generateDeterministicRoleId(ROLE_ARN_1);
    assertThat(assumeRoleResponse.getAssumedRoleId())
        .isEqualTo(expectedRoleId + ":" + SESSION_NAME);

    // Verify expiration added durationSeconds
    final long expirationEpochSeconds = assumeRoleResponse.getExpirationEpochSeconds();
    assertThat(expirationEpochSeconds).isEqualTo(CLOCK.instant().getEpochSecond() + durationSeconds);
    assertMarkForAuditCalled(requestWithCredentials);
  }

  @Test
  public void testGenerateDeterministicRoleId() {
    final String roleId1 = S3AssumeRoleRequest.generateDeterministicRoleId(ROLE_ARN_1);
    final String roleId2 = S3AssumeRoleRequest.generateDeterministicRoleId(ROLE_ARN_1);
    final String roleId3 = S3AssumeRoleRequest.generateDeterministicRoleId(ROLE_ARN_2);

    assertThat(roleId1).startsWith("AROA").hasSize(4 + 16);
    assertThat(roleId1).isEqualTo(roleId2);
    assertThat(roleId1).isNotEqualTo(roleId3);
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
  public void testAssumeRoleCredentialsAreUnique() throws IOException {
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
    // Call preExecute first to generate credentials
    final OMRequest preExecutedRequest1 = request1.preExecute(ozoneManager);
    final S3AssumeRoleRequest requestWithCredentials1 = new S3AssumeRoleRequest(preExecutedRequest1, CLOCK);
    final OMClientResponse response1 = requestWithCredentials1.validateAndUpdateCache(ozoneManager, context);
    
    final S3AssumeRoleRequest request2 = new S3AssumeRoleRequest(omRequest, CLOCK);
    // Call preExecute again to generate different credentials
    final OMRequest preExecutedRequest2 = request2.preExecute(ozoneManager);
    final S3AssumeRoleRequest requestWithCredentials2 = new S3AssumeRoleRequest(preExecutedRequest2, CLOCK);
    final OMClientResponse response2 = requestWithCredentials2.validateAndUpdateCache(ozoneManager, context);

    final AssumeRoleResponse assumeRoleResponse1 = response1.getOMResponse().getAssumeRoleResponse();
    final AssumeRoleResponse assumeRoleResponse2 = response2.getOMResponse().getAssumeRoleResponse();

    // Different access keys
    assertThat(assumeRoleResponse1.getAccessKeyId()).isNotEqualTo(assumeRoleResponse2.getAccessKeyId());

    // Different secret keys
    assertThat(assumeRoleResponse1.getSecretAccessKey()).isNotEqualTo(assumeRoleResponse2.getSecretAccessKey());

    // Different session tokens
    assertThat(assumeRoleResponse1.getSessionToken()).isNotEqualTo(assumeRoleResponse2.getSessionToken());

    // Same assumed role ID for the same role and session name
    assertThat(assumeRoleResponse1.getAssumedRoleId()).isEqualTo(assumeRoleResponse2.getAssumedRoleId());

    // Different role ARN yields a different assumed role ID
    final OMRequest omRequestDifferentRole = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_2)
                .setRoleSessionName(SESSION_NAME)
                .setDurationSeconds(3600)
                .setRequestId(REQUEST_ID)
        ).build();
    final S3AssumeRoleRequest request3 = new S3AssumeRoleRequest(omRequestDifferentRole, CLOCK);
    final OMRequest preExecutedRequest3 = request3.preExecute(ozoneManager);
    final S3AssumeRoleRequest requestWithCredentials3 = new S3AssumeRoleRequest(preExecutedRequest3, CLOCK);
    final OMClientResponse response3 = requestWithCredentials3.validateAndUpdateCache(ozoneManager, context);
    final AssumeRoleResponse assumeRoleResponse3 = response3.getOMResponse().getAssumeRoleResponse();
    assertThat(assumeRoleResponse1.getAssumedRoleId()).isNotEqualTo(assumeRoleResponse3.getAssumedRoleId());

    OMAuditLogger.log(requestWithCredentials1.getAuditBuilder());
    OMAuditLogger.log(requestWithCredentials2.getAuditBuilder());
    OMAuditLogger.log(requestWithCredentials3.getAuditBuilder());
    verify(auditLogger, times(3)).logWrite(any(AuditMessage.class));
  }

  @Test
  public void testValidateAndUpdateCacheDoesNotCallAuthorizer() throws IOException {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(SESSION_NAME)
                .setDurationSeconds(3600)
                .setRequestId(REQUEST_ID)
        ).build();

    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMRequest preExecutedRequest = request.preExecute(ozoneManager);
    verify(accessAuthorizer).generateAssumeRoleSessionPolicy(
        any(org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.class));

    final S3AssumeRoleRequest requestWithCredentials = new S3AssumeRoleRequest(preExecutedRequest, CLOCK);
    requestWithCredentials.validateAndUpdateCache(ozoneManager, context);

    verify(accessAuthorizer, times(1)).generateAssumeRoleSessionPolicy(
        any(org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.class));
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
    final OMException exception = assertThrows(OMException.class, () -> request.preExecute(ozoneManager));
    assertThat(exception.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(exception.getMessage()).isEqualTo(
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
    final OMException exception = assertThrows(OMException.class, () -> request.preExecute(ozoneManager));

    assertThat(exception.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(exception.getMessage()).isEqualTo(
        "Invalid RoleSessionName length 1: it must be 2-64 characters long and contain only alphanumeric " +
        "characters and +, =, ,, ., @, -");
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
    final OMException exception = assertThrows(OMException.class, () -> request.preExecute(ozoneManager));

    assertThat(exception.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(exception.getMessage()).isEqualTo(
        "Invalid RoleSessionName length 70: it must be 2-64 characters long and contain only alphanumeric " +
        "characters and +, =, ,, ., @, -"
    );
    assertMarkForAuditCalled(request);
  }

  @Test
  public void testValidRoleSessionNameMaxLengthBoundary() throws IOException {
    final String roleSessionName = S3SecurityTestUtils.repeat('g', 64);
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(roleSessionName)  // exactly max length
                .setRequestId(REQUEST_ID)
        ).build();

    // Call preExecute first to generate credentials
    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMRequest preExecutedRequest = request.preExecute(ozoneManager);
    final S3AssumeRoleRequest requestWithCredentials = new S3AssumeRoleRequest(preExecutedRequest, CLOCK);
    final OMClientResponse response = requestWithCredentials.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omResponse.hasAssumeRoleResponse()).isTrue();
    assertMarkForAuditCalled(requestWithCredentials);
  }

  @Test
  public void testValidRoleSessionNameMinLengthBoundary() throws IOException {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName("TT")   // exactly min length
                .setRequestId(REQUEST_ID)
        ).build();

    // Call preExecute first to generate credentials
    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMRequest preExecutedRequest = request.preExecute(ozoneManager);
    final S3AssumeRoleRequest requestWithCredentials = new S3AssumeRoleRequest(preExecutedRequest, CLOCK);
    final OMClientResponse response = requestWithCredentials.validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omResponse.hasAssumeRoleResponse()).isTrue();
    assertMarkForAuditCalled(requestWithCredentials);
  }

  @Test
  public void testAssumeRoleWithSessionPolicyPresent() throws IOException {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(
            AssumeRoleRequest.newBuilder()
                .setRoleArn(ROLE_ARN_1)
                .setRoleSessionName(SESSION_NAME)
                .setDurationSeconds(3600)
                .setAwsIamSessionPolicy(AWS_IAM_POLICY)
                .setRequestId(REQUEST_ID)
        ).build();

    // Call preExecute first to generate credentials
    final S3AssumeRoleRequest request = new S3AssumeRoleRequest(omRequest, CLOCK);
    final OMRequest preExecutedRequest = request.preExecute(ozoneManager);
    final S3AssumeRoleRequest requestWithCredentials = new S3AssumeRoleRequest(preExecutedRequest, CLOCK);
    final OMClientResponse response = requestWithCredentials.validateAndUpdateCache(ozoneManager, context);
    assertThat(response.getOMResponse().getStatus()).isEqualTo(Status.OK);
    assertMarkForAuditCalled(requestWithCredentials);
  }

  @Test
  public void testMalformedSessionPolicyDoesNotIssueCredentials() throws IOException {
    final String sessionPolicy = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:GetObject\",\n" +
        "    \"Action\": \"s3:*\",\n" +
        "    \"Resource\": \"arn:aws:s3:::bucket1/*\"\n" +
        "  }]\n" +
        "}";
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
    final OMException exception = assertThrows(OMException.class, () -> request.preExecute(ozoneManager));

    assertThat(exception.getResult()).isEqualTo(OMException.ResultCodes.MALFORMED_POLICY_DOCUMENT);
    assertThat(exception.getMessage()).isEqualTo("IAM session policy: Duplicate field 'Action' in session policy");
    verify(accessAuthorizer, never()).generateAssumeRoleSessionPolicy(
        any(org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.class));
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

  @Test
  public void testResolveGrantsAgainstBucketLinksLeavesNonLinkGrantsUnchanged() throws IOException {
    final Set<OzoneGrant> grants = Collections.singleton(
        new OzoneGrant(
            objectsOf(
                obj(OzoneObj.ResourceType.VOLUME, "s3v", null, null),
                obj(OzoneObj.ResourceType.BUCKET, "s3v", "mybucket", null),
                obj(OzoneObj.ResourceType.KEY, "s3v", "mybucket", "*")),
            EnumSet.of(ACLType.READ), Collections.singleton("GetObject")));

    final Set<OzoneGrant> result = S3AssumeRoleRequest.resolveGrantsAgainstBucketLinks(
        grants, (volume, bucket) -> new ResolvedBucket(
            volume, bucket, volume, bucket, "owner", BucketLayout.OBJECT_STORE));

    assertThat(result).isEqualTo(grants);
  }

  @Test
  public void testResolveGrantsAgainstBucketLinksRewritesToSourceForSameVolumeLink() throws IOException {
    final Set<OzoneGrant> grants = Collections.singleton(
        new OzoneGrant(
            objectsOf(
                obj(OzoneObj.ResourceType.VOLUME, "s3v", null, null),
                obj(OzoneObj.ResourceType.BUCKET, "s3v", "s3v-iceberg", null),
                obj(OzoneObj.ResourceType.KEY, "s3v", "s3v-iceberg", "*")),
            EnumSet.of(ACLType.READ), Collections.singleton("GetObject")));

    final Set<OzoneGrant> result = S3AssumeRoleRequest.resolveGrantsAgainstBucketLinks(
        grants, (volume, bucket) -> resolved(
            volume, bucket, "s3v", "iceberg", Pair.of("s3v", "s3v-iceberg")));

    final IOzoneObj linkBucket = obj(OzoneObj.ResourceType.BUCKET, "s3v", "s3v-iceberg", null);
    final Set<IOzoneObj> allObjects = allObjectsIn(result);
    // Key and bucket are anchored to the link's source.
    assertThat(allObjects).contains(obj(OzoneObj.ResourceType.KEY, "s3v", "iceberg", "*"));
    assertThat(allObjects).contains(obj(OzoneObj.ResourceType.BUCKET, "s3v", "iceberg", null));
    // The link key is gone; only a READ on the link bucket remains, for following the link.
    assertThat(allObjects).doesNotContain(obj(OzoneObj.ResourceType.KEY, "s3v", "s3v-iceberg", "*"));
    assertThat(allObjects).contains(linkBucket);
    // Same volume, so no extra source-volume grant is added.
    assertThat(allObjects).doesNotContain(obj(OzoneObj.ResourceType.VOLUME, "iceberg", null, null));

    final OzoneGrant followGrant = grantContaining(result, linkBucket);
    assertThat(followGrant.getPermissions()).containsExactly(ACLType.READ);
    assertThat(followGrant.getS3Actions()).isEmpty();
  }

  @Test
  public void testResolveGrantsAgainstBucketLinksRewritesPrefixOnSameVolumeLink() throws IOException {
    final Set<OzoneGrant> grants = Collections.singleton(
        new OzoneGrant(
            objectsOf(
                prefixObj("s3v", "s3v-iceberg", "folder/")),
            EnumSet.of(ACLType.READ, ACLType.LIST), Collections.singleton("ListBucket")));

    final Set<OzoneGrant> result = S3AssumeRoleRequest.resolveGrantsAgainstBucketLinks(
        grants, (volume, bucket) -> resolved(
            volume, bucket, "s3v", "iceberg", Pair.of("s3v", "s3v-iceberg")));

    final IOzoneObj linkBucket = obj(OzoneObj.ResourceType.BUCKET, "s3v", "s3v-iceberg", null);
    final Set<IOzoneObj> allObjects = allObjectsIn(result);
    assertThat(allObjects).contains(prefixObj("s3v", "iceberg", "folder/"));
    assertThat(allObjects).doesNotContain(prefixObj("s3v", "s3v-iceberg", "folder/"));
    assertThat(allObjects).contains(linkBucket);

    final OzoneGrant followGrant = grantContaining(result, linkBucket);
    assertThat(followGrant.getPermissions()).containsExactly(ACLType.READ);
    assertThat(followGrant.getS3Actions()).isEmpty();
  }

  @Test
  public void testResolveGrantsAgainstBucketLinksAddsSourceVolumeReadForCrossVolumeLink() throws IOException {
    final Set<OzoneGrant> grants = Collections.singleton(
        new OzoneGrant(
            objectsOf(
                obj(OzoneObj.ResourceType.VOLUME, "s3v", null, null),
                obj(OzoneObj.ResourceType.BUCKET, "s3v", "s3v-iceberg", null),
                obj(OzoneObj.ResourceType.KEY, "s3v", "s3v-iceberg", "*")),
            EnumSet.of(ACLType.READ), Collections.singleton("GetObject")));

    final Set<OzoneGrant> result = S3AssumeRoleRequest.resolveGrantsAgainstBucketLinks(
        grants, (volume, bucket) -> resolved(
            volume, bucket, "tenantvol", "iceberg", Pair.of("s3v", "s3v-iceberg")));

    final IOzoneObj linkBucket = obj(OzoneObj.ResourceType.BUCKET, "s3v", "s3v-iceberg", null);
    final IOzoneObj sourceVolume = obj(OzoneObj.ResourceType.VOLUME, "tenantvol", null, null);
    final Set<IOzoneObj> allObjects = allObjectsIn(result);
    assertThat(allObjects).contains(obj(OzoneObj.ResourceType.KEY, "tenantvol", "iceberg", "*"));
    assertThat(allObjects).contains(linkBucket);
    assertThat(allObjects).contains(sourceVolume);

    final OzoneGrant followGrant = grantContaining(result, sourceVolume);
    assertThat(followGrant.getPermissions()).containsExactly(ACLType.READ);
    assertThat(followGrant.getObjects()).contains(linkBucket);
  }

  @Test
  public void testResolveGrantsAgainstBucketLinksSkipsWildcardBuckets() throws IOException {
    final Set<OzoneGrant> grants = Collections.singleton(
        new OzoneGrant(
            objectsOf(
                obj(OzoneObj.ResourceType.VOLUME, "s3v", null, null),
                obj(OzoneObj.ResourceType.BUCKET, "s3v", "*", null),
                obj(OzoneObj.ResourceType.KEY, "s3v", "*", "*")),
            EnumSet.of(ACLType.READ), Collections.singleton("GetObject")));

    final Set<OzoneGrant> result = S3AssumeRoleRequest.resolveGrantsAgainstBucketLinks(
        grants, (volume, bucket) -> {
          throw new AssertionError("link resolver must not be called for wildcard buckets");
        });

    assertThat(result).isEqualTo(grants);
  }

  @Test
  public void testResolveGrantsAgainstBucketLinksLeavesDanglingBucketsUnchanged() throws IOException {
    final Set<OzoneGrant> grants = Collections.singleton(
        new OzoneGrant(
            objectsOf(
                obj(OzoneObj.ResourceType.KEY, "s3v", "danglingBucket", "*")),
            EnumSet.of(ACLType.READ), Collections.singleton("GetObject")));

    final Set<OzoneGrant> result = S3AssumeRoleRequest.resolveGrantsAgainstBucketLinks(
        grants, (volume, bucket) -> new ResolvedBucket(volume, bucket, null, null, null, null));

    assertThat(result).isEqualTo(grants);
  }

  @Test
  public void testResolveGrantsAgainstBucketLinksRewritesChainedSameVolumeLink() throws IOException {
    final Set<OzoneGrant> grants = Collections.singleton(
        new OzoneGrant(
            objectsOf(
                obj(OzoneObj.ResourceType.BUCKET, "s3v", "linkA", null),
                obj(OzoneObj.ResourceType.KEY, "s3v", "linkA", "*")),
            EnumSet.of(ACLType.READ), Collections.singleton("GetObject")));

    final Set<OzoneGrant> result = S3AssumeRoleRequest.resolveGrantsAgainstBucketLinks(
        grants, (volume, bucket) -> resolved(
            volume, bucket, "s3v", "source", Pair.of("s3v", "linkA"), Pair.of("s3v", "linkB")));

    final IOzoneObj linkA = obj(OzoneObj.ResourceType.BUCKET, "s3v", "linkA", null);
    final IOzoneObj linkB = obj(OzoneObj.ResourceType.BUCKET, "s3v", "linkB", null);
    final Set<IOzoneObj> allObjects = allObjectsIn(result);
    assertThat(allObjects).contains(obj(OzoneObj.ResourceType.KEY, "s3v", "source", "*"));
    assertThat(allObjects).contains(linkA);
    assertThat(allObjects).contains(linkB);
    assertThat(allObjects).doesNotContain(obj(OzoneObj.ResourceType.VOLUME, "s3v", null, null));
  }

  @Test
  public void testResolveGrantsAgainstBucketLinksAddsVolumeReadForChainedCrossVolumeHop() throws IOException {
    final Set<OzoneGrant> grants = Collections.singleton(
        new OzoneGrant(
            objectsOf(
                obj(OzoneObj.ResourceType.BUCKET, "s3v", "linkA", null),
                obj(OzoneObj.ResourceType.KEY, "s3v", "linkA", "*")),
            EnumSet.of(ACLType.READ), Collections.singleton("GetObject")));

    final Set<OzoneGrant> result = S3AssumeRoleRequest.resolveGrantsAgainstBucketLinks(
        grants, (volume, bucket) -> resolved(
            volume, bucket, "tenant", "source", Pair.of("s3v", "linkA"), Pair.of("tenant", "linkB")));

    final IOzoneObj linkA = obj(OzoneObj.ResourceType.BUCKET, "s3v", "linkA", null);
    final IOzoneObj linkB = obj(OzoneObj.ResourceType.BUCKET, "tenant", "linkB", null);
    final IOzoneObj tenantVolume = obj(OzoneObj.ResourceType.VOLUME, "tenant", null, null);
    final Set<IOzoneObj> allObjects = allObjectsIn(result);
    assertThat(allObjects).contains(obj(OzoneObj.ResourceType.KEY, "tenant", "source", "*"));
    assertThat(allObjects).contains(linkA);
    assertThat(allObjects).contains(linkB);
    assertThat(allObjects).contains(tenantVolume);
    assertThat(allObjects).doesNotContain(obj(OzoneObj.ResourceType.VOLUME, "s3v", null, null));
  }

  @Test
  public void testGetSessionPolicyRewritesLinkBucketGrantsToSource() throws Exception {
    when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(false);

    final Set<OzoneGrant> resolverGrants = Collections.singleton(
        new OzoneGrant(
            objectsOf(
                obj(OzoneObj.ResourceType.VOLUME, "s3v", null, null),
                obj(OzoneObj.ResourceType.BUCKET, "s3v", "s3v-iceberg", null),
                obj(OzoneObj.ResourceType.KEY, "s3v", "s3v-iceberg", "*")),
            EnumSet.of(ACLType.READ), Collections.singleton("GetObject")));

    when(ozoneManager.resolveBucketLink(Pair.of("s3v", "s3v-iceberg"), true, false))
        .thenReturn(resolved("s3v", "s3v-iceberg", "s3v", "iceberg", Pair.of("s3v", "s3v-iceberg")));

    try (MockedStatic<IamSessionPolicyResolver> resolverMock = mockStatic(IamSessionPolicyResolver.class)) {
      resolverMock.when(() -> IamSessionPolicyResolver.resolve(
              AWS_IAM_POLICY, "s3v", IamSessionPolicyResolver.AuthorizerType.RANGER))
          .thenReturn(resolverGrants);

      final String result = new S3AssumeRoleRequest(baseOmRequestBuilder().build(), CLOCK)
          .getSessionPolicy(
              ozoneManager, ORIGINAL_ACCESS_KEY_ID, AWS_IAM_POLICY, OM_HOST, LOOPBACK_IP,
              UserGroupInformation.createRemoteUser("userNameLink"), TARGET_ROLE_NAME);
      // Ensure no exception was thrown and that the method actually delegated to generateAssumeRoleSessionPolicy
      // and returned its value (not null, not something else).
      assertThat(result).isEqualTo(SESSION_POLICY_VALUE);
    }

    final ArgumentCaptor<org.apache.hadoop.ozone.security.acl.AssumeRoleRequest> captor = ArgumentCaptor.forClass(
        org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.class);
    verify(accessAuthorizer).generateAssumeRoleSessionPolicy(captor.capture());

    final Set<IOzoneObj> allObjects = allObjectsIn(captor.getValue().getGrants());
    assertThat(allObjects).contains(obj(OzoneObj.ResourceType.KEY, "s3v", "iceberg", "*"));
    assertThat(allObjects).contains(obj(OzoneObj.ResourceType.BUCKET, "s3v", "s3v-iceberg", null));
    assertThat(allObjects).doesNotContain(obj(OzoneObj.ResourceType.KEY, "s3v", "s3v-iceberg", "*"));

    verify(ozoneManager).resolveBucketLink(Pair.of("s3v", "s3v-iceberg"), true, false);
  }

  @SafeVarargs
  private static ResolvedBucket resolved(String requestedVol, String requestedBucket, String realVol, String realBucket,
      Pair<String, String>... linkChain) {
    return new ResolvedBucket(
        requestedVol, requestedBucket, realVol, realBucket, "owner",
        BucketLayout.OBJECT_STORE, Arrays.asList(linkChain));
  }

  private static IOzoneObj obj(OzoneObj.ResourceType type, String volume, String bucket, String key) {
    final OzoneObjInfo.Builder builder = OzoneObjInfo.Builder.newBuilder()
        .setResType(type)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volume);
    if (bucket != null) {
      builder.setBucketName(bucket);
    }
    if (key != null) {
      builder.setKeyName(key);
    }
    return builder.build();
  }

  @SuppressWarnings("SameParameterValue")
  private static IOzoneObj prefixObj(String volume, String bucket, String prefix) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setPrefixName(prefix)
        .build();
  }

  private static Set<IOzoneObj> objectsOf(IOzoneObj... objects) {
    return new LinkedHashSet<>(Arrays.asList(objects));
  }

  private static Set<IOzoneObj> allObjectsIn(Set<OzoneGrant> grants) {
    final Set<IOzoneObj> all = new LinkedHashSet<>();
    for (OzoneGrant grant : grants) {
      all.addAll(grant.getObjects());
    }
    return all;
  }

  private static OzoneGrant grantContaining(Set<OzoneGrant> grants, IOzoneObj object) {
    return grants.stream()
        .filter(grant -> grant.getObjects().contains(object))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No grant contains " + object));
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

  private void assertLeaderGeneratedAssumeRoleFields(OMRequest preExecutedRequest, int durationSeconds) {
    assertThat(preExecutedRequest.hasUpdateAssumeRoleRequest()).isTrue();
    final UpdateAssumeRoleRequest updateAssumeRoleRequest = preExecutedRequest.getUpdateAssumeRoleRequest();
    assertThat(updateAssumeRoleRequest.getTempAccessKeyId()).startsWith("ASIA");
    assertThat(updateAssumeRoleRequest.getSecretAccessKey()).isNotEmpty();
    assertThat(updateAssumeRoleRequest.getRoleId()).startsWith("AROA");
    assertThat(updateAssumeRoleRequest.getSessionToken()).isNotEmpty();
    assertThat(updateAssumeRoleRequest.getExpirationEpochSeconds())
        .isEqualTo(CLOCK.instant().getEpochSecond() + durationSeconds);
  }

  private void assertMarkForAuditCalled(S3AssumeRoleRequest request) {
    OMAuditLogger.log(request.getAuditBuilder());
    verify(auditLogger).logWrite(any(AuditMessage.class));
  }
}


