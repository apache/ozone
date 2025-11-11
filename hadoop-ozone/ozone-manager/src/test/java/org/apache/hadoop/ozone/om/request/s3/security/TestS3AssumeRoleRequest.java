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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.regex.Pattern;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for S3AssumeRoleRequest.
 */
public class TestS3AssumeRoleRequest {

  private static final String ROLE_ARN_1 = "arn:aws:iam::123456789012:role/MyRole1";
  private static final String ROLE_ARN_2 = "arn:aws:iam::123456789012:role/path/anotherLevel/Role2";
  private static final String SESSION_NAME = "testSessionName";
  private static final String ORIGINAL_ACCESS_KEY_ID = "origAccessKeyId";

  private OzoneManager ozoneManager;
  private ExecutionContext context;

  @BeforeEach
  public void setup() {
    ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.getOmRpcServerAddr())
        .thenReturn(new InetSocketAddress("localhost", 9876));
    context = ExecutionContext.of(1L, null);
  }

  @Test
  public void testInvalidDurationTooShort() {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(AssumeRoleRequest.newBuilder()
            .setRoleArn(ROLE_ARN_1)
            .setRoleSessionName(SESSION_NAME)
            .setDurationSeconds(899)  // less than 900
        ).build();

    final OMClientResponse response = new S3AssumeRoleRequest(omRequest)
        .validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.INVALID_REQUEST);
    assertThat(omResponse.getMessage()).isEqualTo("Duration: 899 is not valid");
    assertThat(omResponse.hasAssumeRoleResponse()).isFalse();
  }

  @Test
  public void testInvalidDurationTooLong() {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(AssumeRoleRequest.newBuilder()
            .setRoleArn(ROLE_ARN_1)
            .setRoleSessionName(SESSION_NAME)
            .setDurationSeconds(43201)) // more than 43200
        .build();

    final OMClientResponse response = new S3AssumeRoleRequest(omRequest)
        .validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.INVALID_REQUEST);
    assertThat(omResponse.getMessage()).isEqualTo("Duration: 43201 is not valid");
    assertThat(omResponse.hasAssumeRoleResponse()).isFalse();
  }

  @Test
  public void testValidDurationMaxBoundary() {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(AssumeRoleRequest.newBuilder()
            .setRoleArn(ROLE_ARN_1)
            .setRoleSessionName(SESSION_NAME)
            .setDurationSeconds(43200)  // exactly max
        ).build();

    final OMClientResponse response = new S3AssumeRoleRequest(omRequest)
        .validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omResponse.hasAssumeRoleResponse()).isTrue();
  }

  @Test
  public void testValidDurationMinBoundary() {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(AssumeRoleRequest.newBuilder()
            .setRoleArn(ROLE_ARN_1)
            .setRoleSessionName(SESSION_NAME)
            .setDurationSeconds(900)  // exactly min
        ).build();

    final OMClientResponse response = new S3AssumeRoleRequest(omRequest)
        .validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omResponse.hasAssumeRoleResponse()).isTrue();
  }

  @Test
  public void testMissingS3Authentication() {
    final OMRequest omRequest = OMRequest.newBuilder()  // note: not using baseOMRequestBuilder that has S3 auth
        .setCmdType(Type.AssumeRole)
        .setClientId("client-1")
        .setAssumeRoleRequest(AssumeRoleRequest.newBuilder()
            .setRoleArn(ROLE_ARN_1)
            .setRoleSessionName(SESSION_NAME)
            .setDurationSeconds(3600)
        ).build();

    final OMClientResponse response = new S3AssumeRoleRequest(omRequest)
        .validateAndUpdateCache(ozoneManager, context);
    final OMResponse omResponse = response.getOMResponse();

    assertThat(omResponse.getStatus()).isEqualTo(Status.INVALID_REQUEST);
    assertThat(omResponse.getMessage()).isEqualTo("S3AssumeRoleRequest does not have S3 authentication");
    assertThat(omResponse.hasAssumeRoleResponse()).isFalse();
  }

  @Test
  public void testSuccessfulAssumeRoleGeneratesCredentials() {
    final int durationSeconds = 3600;
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(AssumeRoleRequest.newBuilder()
            .setRoleArn(ROLE_ARN_1)
            .setRoleSessionName(SESSION_NAME)
            .setDurationSeconds(durationSeconds)
        ).build();

    final long before = Instant.now().getEpochSecond();
    final OMClientResponse clientResponse =
        new S3AssumeRoleRequest(omRequest).validateAndUpdateCache(ozoneManager, context);
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

    // Expiration around now + durationSeconds (allow small skew)
    final long after = Instant.now().getEpochSecond();
    final long expirationEpochSeconds = assumeRoleResponse.getExpirationEpochSeconds();
    assertThat(expirationEpochSeconds).isBetween(before + durationSeconds - 1, after + durationSeconds + 1);
  }

  @Test
  public void testValidateAndExtractRoleNameFromArnSuccessCases() throws OMException {
    assertThat(S3AssumeRoleRequest.validateAndExtractRoleNameFromArn(ROLE_ARN_1)).isEqualTo("MyRole1");

    assertThat(S3AssumeRoleRequest.validateAndExtractRoleNameFromArn(ROLE_ARN_2)).isEqualTo("Role2");

    // Path name right at 511-char max boundary
    final String arnPrefixLen511 = repeat('p', 510) + "/"; // 510 chars + '/' = 511
    final String arnMaxPath = "arn:aws:iam::123456789012:role/" + arnPrefixLen511 + "RoleB";
    assertThat(S3AssumeRoleRequest.validateAndExtractRoleNameFromArn(arnMaxPath)).isEqualTo("RoleB");

    // Role name right at 64-char max boundary
    final String roleName64 = repeat('A', 64);
    final String arn64 = "arn:aws:iam::123456789012:role/" + roleName64;
    assertThat(S3AssumeRoleRequest.validateAndExtractRoleNameFromArn(arn64)).isEqualTo(roleName64);
  }

  @Test
  public void testValidateAndExtractRoleNameFromArnFailureCases() {
    // Improper structure
    final OMException e1 = assertThrows(OMException.class,
        () -> S3AssumeRoleRequest.validateAndExtractRoleNameFromArn("roleNoSlashNorColons")
    );
    assertThat(e1.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e1.getMessage()).isEqualTo("Invalid role ARN: roleNoSlashNorColons");

    // Null
    final OMException e2 = assertThrows(OMException.class,
        () -> S3AssumeRoleRequest.validateAndExtractRoleNameFromArn(null)
    );
    assertThat(e2.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e2.getMessage()).isEqualTo("Role ARN is required");

    // String without role name
    final OMException e3 = assertThrows(OMException.class,
        () -> S3AssumeRoleRequest.validateAndExtractRoleNameFromArn("arn:aws:iam::123456789012:role/")
    );
    assertThat(e3.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e3.getMessage()).isEqualTo("Invalid role ARN: missing role name");

    // No role resource and no role name
    final OMException e4 = assertThrows(OMException.class,
        () -> S3AssumeRoleRequest.validateAndExtractRoleNameFromArn("arn:aws:iam::123456789012")
    );
    assertThat(e4.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e4.getMessage()).isEqualTo("Invalid role ARN: arn:aws:iam::123456789012");

    // No role resource but contains role name
    final OMException e5 = assertThrows(OMException.class,
        () -> S3AssumeRoleRequest.validateAndExtractRoleNameFromArn("arn:aws:iam::123456789012:WebRole")
    );
    assertThat(e5.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e5.getMessage()).isEqualTo("Invalid role ARN: arn:aws:iam::123456789012:WebRole");

    // Empty string
    final OMException e6 = assertThrows(OMException.class,
        () -> S3AssumeRoleRequest.validateAndExtractRoleNameFromArn("")
    );
    assertThat(e6.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e6.getMessage()).isEqualTo("Role ARN is required");

    // String with only slash
    final OMException e7 = assertThrows(OMException.class,
        () -> S3AssumeRoleRequest.validateAndExtractRoleNameFromArn("/")
    );
    assertThat(e7.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e7.getMessage()).isEqualTo("Role ARN length: 1 is not valid");

    // String with only whitespace
    final OMException e8 = assertThrows(OMException.class,
        () -> S3AssumeRoleRequest.validateAndExtractRoleNameFromArn("     ")
    );
    assertThat(e8.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e8.getMessage()).isEqualTo("Role ARN is required");

    // Path name too long (> 511 characters)
    final String arnPrefixLen512 = repeat('q', 511) + "/"; // 511 chars + '/' = 512
    final String arnTooLongPath = "arn:aws:iam::123456789012:role/" + arnPrefixLen512 + "RoleA";
    final OMException e9 = assertThrows(OMException.class,
        () -> S3AssumeRoleRequest.validateAndExtractRoleNameFromArn(arnTooLongPath)
    );
    assertThat(e9.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e9.getMessage()).isEqualTo("Role path length must be between 1 and 512 characters");

    // Otherwise valid role ending in /
    final OMException e10 = assertThrows(OMException.class,
        () -> S3AssumeRoleRequest.validateAndExtractRoleNameFromArn("arn:aws:iam::123456789012:role/MyRole/")
    );
    assertThat(e10.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e10.getMessage()).isEqualTo("Invalid role ARN: missing role name");

    // 65-char role name
    final String roleName65 = repeat('B', 65);
    final String roleArn65 = "arn:aws:iam::123456789012:role/" + roleName65;
    final OMException e11 = assertThrows(OMException.class,
        () -> S3AssumeRoleRequest.validateAndExtractRoleNameFromArn(roleArn65)
    );
    assertThat(e11.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e11.getMessage()).isEqualTo("Invalid role name: " + roleName65);
  }

  @Test
  public void testGenerateSecureRandomStringUsingChars() {
    final String chars = "ABC";
    final int length = 32;
    final String s = S3AssumeRoleRequest.generateSecureRandomStringUsingChars(chars,
        chars.length(),
        length
    );
    assertThat(s).hasSize(length).matches(Pattern.compile("^[ABC]{" + length + "}$"));

    // Test with length 0
    final String empty = S3AssumeRoleRequest.generateSecureRandomStringUsingChars("ABC",
        3,
        0
    );
    assertThat(empty).isEmpty();

    // Test with length 1
    final String single = S3AssumeRoleRequest.generateSecureRandomStringUsingChars("XYZ",
        3,
        1
    );
    assertThat(single).hasSize(1).matches(Pattern.compile("^[XYZ]$"));
  }

  @Test
  public void testAssumeRoleCredentialsAreUnique() {
    // Test that multiple calls generate different credentials
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(AssumeRoleRequest.newBuilder()
            .setRoleArn(ROLE_ARN_1)
            .setRoleSessionName(SESSION_NAME)
            .setDurationSeconds(3600)
        ).build();

    final OMClientResponse response1 = new S3AssumeRoleRequest(omRequest)
        .validateAndUpdateCache(ozoneManager, context);
    final OMClientResponse response2 = new S3AssumeRoleRequest(omRequest)
        .validateAndUpdateCache(ozoneManager, context);

    final AssumeRoleResponse assumeRoleResponse1 = response1.getOMResponse().getAssumeRoleResponse();
    final AssumeRoleResponse assumeRoleResponse2 = response2.getOMResponse().getAssumeRoleResponse();

    // Different access keys
    assertThat(assumeRoleResponse1.getAccessKeyId()).isNotEqualTo(assumeRoleResponse2.getAccessKeyId());

    // Different secret keys
    assertThat(assumeRoleResponse1.getSecretAccessKey()).isNotEqualTo(assumeRoleResponse2.getSecretAccessKey());

    // Different assumed role IDs
    assertThat(assumeRoleResponse1.getAssumedRoleId()).isNotEqualTo(assumeRoleResponse2.getAssumedRoleId());
  }

  @Test
  public void testAssumeRoleWithEmptySessionName() {
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(AssumeRoleRequest.newBuilder()
            .setRoleArn(ROLE_ARN_1)
            .setRoleSessionName("")
            .setDurationSeconds(3600)
        ).build();

    final OMClientResponse response = new S3AssumeRoleRequest(omRequest)
        .validateAndUpdateCache(ozoneManager, context);
    assertThat(response.getOMResponse().getStatus()).isEqualTo(Status.INVALID_REQUEST);
    assertThat(response.getOMResponse().getMessage()).isEqualTo("RoleSessionName:  is not valid");
  }

  @Test
  public void testAssumeRoleWithSessionPolicyPresent() {
    final String sessionPolicy = "{\"Version\":\"2012-10-17\",\"Statement\":[]}";
    final OMRequest omRequest = baseOmRequestBuilder()
        .setAssumeRoleRequest(AssumeRoleRequest.newBuilder()
            .setRoleArn(ROLE_ARN_1)
            .setRoleSessionName(SESSION_NAME)
            .setDurationSeconds(3600)
            .setAwsIamSessionPolicy(sessionPolicy))
        .build();

    final OMClientResponse response =
        new S3AssumeRoleRequest(omRequest).validateAndUpdateCache(ozoneManager, context);
    assertThat(response.getOMResponse().getStatus()).isEqualTo(Status.OK);
  }

  private static OMRequest.Builder baseOmRequestBuilder() {
    return OMRequest.newBuilder()
        .setCmdType(Type.AssumeRole)
        .setClientId("client-1")
        .setS3Authentication(S3Authentication.newBuilder()
            .setAccessId(ORIGINAL_ACCESS_KEY_ID)
        );
  }

  private static String repeat(char c, int count) {
    final StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append(c);
    }
    return sb.toString();
  }
}


