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

import static org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.OzoneGrant;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.om.OzoneAclUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3AssumeRoleResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Handles S3AssumeRoleRequest request.
 */
public class S3AssumeRoleRequest extends OMClientRequest {

  private static final SecureRandom SECURE_RANDOM;

  static {
    SecureRandom secureRandom;
    try {
      // Prefer non-blocking native PRNG where available
      secureRandom = SecureRandom.getInstance("NativePRNGNonBlocking");
    } catch (Exception e) {
      // Fallback to default SecureRandom implementation
      secureRandom = new SecureRandom();
    }
    SECURE_RANDOM = secureRandom;
  }

  private static final int MIN_TOKEN_EXPIRATION_SECONDS = 900;    // 15 minutes in seconds
  private static final int MAX_TOKEN_EXPIRATION_SECONDS = 43200;  // 12 hours in seconds
  private static final int STS_ACCESS_KEY_ID_LENGTH = 20;
  private static final int STS_SECRET_ACCESS_KEY_LENGTH = 40;
  private static final int STS_ROLE_ID_LENGTH = 16;
  private static final String ASSUME_ROLE_ID_PREFIX = "AROA";
  private static final int ASSUME_ROLE_SESSION_NAME_MIN_LENGTH = 2;
  private static final int ASSUME_ROLE_SESSION_NAME_MAX_LENGTH = 64;
  private static final String CHARS_FOR_ACCESS_KEY_IDS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final int CHARS_FOR_ACCESS_KEY_IDS_LENGTH = CHARS_FOR_ACCESS_KEY_IDS.length();
  private static final String CHARS_FOR_SECRET_ACCESS_KEYS = CHARS_FOR_ACCESS_KEY_IDS +
      "abcdefghijklmnopqrstuvwxyz/+";
  private static final int CHARS_FOR_SECRET_ACCESS_KEYS_LENGTH = CHARS_FOR_SECRET_ACCESS_KEYS.length();
  public static final String STS_TOKEN_PREFIX = "ASIA";

  private final Clock clock;

  public S3AssumeRoleRequest(OMRequest omRequest, Clock clock) {
    super(omRequest);
    this.clock = clock;
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final OMRequest omRequest = getOmRequest();
    final AssumeRoleRequest assumeRoleRequest = omRequest.getAssumeRoleRequest();
    final int durationSeconds = assumeRoleRequest.getDurationSeconds();

    // Validate duration
    if (durationSeconds < MIN_TOKEN_EXPIRATION_SECONDS || durationSeconds > MAX_TOKEN_EXPIRATION_SECONDS) {
      final OMException omException = new OMException(
          "Duration must be between " + MIN_TOKEN_EXPIRATION_SECONDS + " and " + MAX_TOKEN_EXPIRATION_SECONDS,
          OMException.ResultCodes.INVALID_REQUEST);
      return new S3AssumeRoleResponse(
          createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), omException));
    }

    // Validate role session name
    final String roleSessionName = assumeRoleRequest.getRoleSessionName();
    final S3AssumeRoleResponse roleSessionNameErrorResponse = validateRoleSessionName(roleSessionName, omRequest);
    if (roleSessionNameErrorResponse != null) {
      return roleSessionNameErrorResponse;
    }

    final String roleArn = assumeRoleRequest.getRoleArn();
    try {
      // Validate role ARN and extract role
      final String targetRoleName = AwsRoleArnValidator.validateAndExtractRoleNameFromArn(roleArn);

      if (!omRequest.hasS3Authentication()) {
        final String msg = "S3AssumeRoleRequest does not have S3 authentication";
        final OMException omException = new OMException(msg, OMException.ResultCodes.INVALID_REQUEST);
        return new S3AssumeRoleResponse(
            createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), omException));
      }

      // Generate temporary AWS credentials using cryptographically strong SecureRandom
      final String tempAccessKeyId = STS_TOKEN_PREFIX + generateSecureRandomStringUsingChars(
          CHARS_FOR_ACCESS_KEY_IDS, CHARS_FOR_ACCESS_KEY_IDS_LENGTH, STS_ACCESS_KEY_ID_LENGTH);
      final String secretAccessKey = generateSecureRandomStringUsingChars(
          CHARS_FOR_SECRET_ACCESS_KEYS, CHARS_FOR_SECRET_ACCESS_KEYS_LENGTH, STS_SECRET_ACCESS_KEY_LENGTH);
      final String sessionToken = generateSessionToken(
          targetRoleName, omRequest, ozoneManager, assumeRoleRequest, secretAccessKey, tempAccessKeyId);

      // Generate AssumedRoleId for response
      final String roleId = ASSUME_ROLE_ID_PREFIX + generateSecureRandomStringUsingChars(
          CHARS_FOR_ACCESS_KEY_IDS, CHARS_FOR_ACCESS_KEY_IDS_LENGTH, STS_ROLE_ID_LENGTH);
      final String assumedRoleId = roleId + ":" + roleSessionName;

      // Calculate expiration of session token
      final long expirationEpochSeconds = clock.instant().plusSeconds(durationSeconds).getEpochSecond();

      final AssumeRoleResponse.Builder responseBuilder = AssumeRoleResponse.newBuilder()
          .setAccessKeyId(tempAccessKeyId)
          .setSecretAccessKey(secretAccessKey)
          .setSessionToken(sessionToken)
          .setExpirationEpochSeconds(expirationEpochSeconds)
          .setAssumedRoleId(assumedRoleId);

      return new S3AssumeRoleResponse(
          OmResponseUtil.getOMResponseBuilder(omRequest)
              .setAssumeRoleResponse(responseBuilder.build())
              .build());
    } catch (OMException e) {
      return new S3AssumeRoleResponse(createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), e));
    } catch (IOException e) {
      final OMException omException = new OMException(
          "Failed to generate STS token for role: " + roleArn, e, OMException.ResultCodes.INTERNAL_ERROR);
      return new S3AssumeRoleResponse(
          createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), omException));
    }
  }

  /**
   * Ensures RoleSessionName is valid.
   */
  private S3AssumeRoleResponse validateRoleSessionName(String roleSessionName, OMRequest omRequest) {
    if (StringUtils.isBlank(roleSessionName)) {
      final OMException omException = new OMException(
          "RoleSessionName is required", OMException.ResultCodes.INVALID_REQUEST);
      return new S3AssumeRoleResponse(
          createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), omException));
    }
    if (roleSessionName.length() < ASSUME_ROLE_SESSION_NAME_MIN_LENGTH ||
        roleSessionName.length() > ASSUME_ROLE_SESSION_NAME_MAX_LENGTH) {
      final OMException omException = new OMException(
          "RoleSessionName length must be between " + ASSUME_ROLE_SESSION_NAME_MIN_LENGTH + " and " +
          ASSUME_ROLE_SESSION_NAME_MAX_LENGTH, OMException.ResultCodes.INVALID_REQUEST);
      return new S3AssumeRoleResponse(
          createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), omException));
    }
    return null;
  }

  /**
   * Generates session token using components from the AssumeRoleRequest.
   */
  private String generateSessionToken(String targetRoleName, OMRequest omRequest,
      OzoneManager ozoneManager, AssumeRoleRequest assumeRoleRequest, String secretAccessKey,
      String tempAccessKeyId) throws IOException {

    InetAddress remoteIp = ProtobufRpcEngine.Server.getRemoteIp();
    if (remoteIp == null) {
      remoteIp = ozoneManager.getOmRpcServerAddr().getAddress();
    }

    final String hostName = remoteIp != null ? remoteIp.getHostName() : ozoneManager.getOmRpcServerAddr().getHostName();

    // Determine the caller's access key ID - this will be referred to as the original
    // access key id.  When STS tokens are used, the tokens will be authorized as
    // the kerberos principal associated to the original access key id, in conjunction with the
    // role permissions and optional AWS IAM session policy permissions.
    final String originalAccessKeyId = omRequest.getS3Authentication().getAccessId();

    final String principal = OzoneAclUtils.accessIdToUserPrincipal(originalAccessKeyId);
    final UserGroupInformation ugi = UserGroupInformation.createRemoteUser(principal);

    final String roleArn = assumeRoleRequest.getRoleArn();
    final String sessionPolicy = getSessionPolicy(
        ozoneManager, originalAccessKeyId, assumeRoleRequest.getAwsIamSessionPolicy(), hostName, remoteIp, ugi,
        targetRoleName);

    return ozoneManager.getSTSTokenSecretManager().createSTSTokenString(
        tempAccessKeyId, originalAccessKeyId, roleArn, assumeRoleRequest.getDurationSeconds(), secretAccessKey,
        sessionPolicy, clock);
  }

  /**
   * Calls utility to convert IAM Policy to Ozone nomenclature and uses this output as input
   * to IAccessAuthorizer.generateAssumeRoleSessionPolicy() which is currently only implemented
   * by RangerOzoneAuthorizer.
   */
  @VisibleForTesting
  String getSessionPolicy(OzoneManager ozoneManager, String originalAccessKeyId, String awsIamPolicy,
      String hostName, InetAddress remoteIp, UserGroupInformation ugi, String targetRoleName) throws IOException {

    final String volumeName;
    if (ozoneManager.isS3MultiTenancyEnabled()) {
      final Optional<String> tenantOpt = ozoneManager.getMultiTenantManager()
          .getTenantForAccessID(originalAccessKeyId);
      if (tenantOpt.isPresent()) {
        volumeName = ozoneManager.getMultiTenantManager()
            .getTenantVolumeName(tenantOpt.get());
      } else {
        volumeName = HddsClientUtils.getDefaultS3VolumeName(ozoneManager.getConfiguration());
      }
    } else {
      volumeName = HddsClientUtils.getDefaultS3VolumeName(ozoneManager.getConfiguration());
    }

    final Set<OzoneGrant> grants = StringUtils.isBlank(awsIamPolicy) ?
        null :
        IamSessionPolicyResolver.resolve(awsIamPolicy, volumeName, IamSessionPolicyResolver.AuthorizerType.RANGER);

    return ozoneManager.getAccessAuthorizer().generateAssumeRoleSessionPolicy(
        new org.apache.hadoop.ozone.security.acl.AssumeRoleRequest(
            hostName, remoteIp, ugi, targetRoleName, grants));
  }

  /**
   * Generates a cryptographically strong String of the supplied stringLength using supplied chars.
   */
  @VisibleForTesting
  static String generateSecureRandomStringUsingChars(String chars, int charsLength, int stringLength) {
    final StringBuilder sb = new StringBuilder(stringLength);
    for (int i = 0; i < stringLength; i++) {
      sb.append(chars.charAt(SECURE_RANDOM.nextInt(charsLength)));
    }
    return sb.toString();
  }
}
