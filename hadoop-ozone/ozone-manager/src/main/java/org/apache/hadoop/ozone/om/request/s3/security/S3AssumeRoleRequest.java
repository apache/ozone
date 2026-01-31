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
import com.google.common.base.Strings;
import java.io.IOException;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneAclUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.AwsRoleArnValidator;
import org.apache.hadoop.ozone.om.helpers.S3STSUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3AssumeRoleResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateAssumeRoleRequest;
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

  private static final int STS_ACCESS_KEY_ID_LENGTH = 20;
  private static final int STS_SECRET_ACCESS_KEY_LENGTH = 40;
  private static final int STS_ROLE_ID_LENGTH = 16;
  private static final String ASSUME_ROLE_ID_PREFIX = "AROA";
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
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final AssumeRoleRequest assumeRoleRequest = getOmRequest().getAssumeRoleRequest();

    // Brief overview of flow:
    // The STS Endpoint makes the AssumeRole call, which when received by OM leader (via this method),
    // it will generate the temporary credentials (tempAccessKeyId, secretAccessKey) and roleId.
    // The original AssumeRole request is converted to an UpdateAssumeRoleRequest with the generated
    // credentials. This update request will be submitted to Ratis and the credentials
    // created by the leader will be replicated across all OMs.  All OMs in
    // HA mode therefore will have identical audit logs with the same tempAccessKeyId.

    // Generate temporary AWS credentials using cryptographically strong SecureRandom
    final String tempAccessKeyId = STS_TOKEN_PREFIX + generateSecureRandomStringUsingChars(
        CHARS_FOR_ACCESS_KEY_IDS, CHARS_FOR_ACCESS_KEY_IDS_LENGTH, STS_ACCESS_KEY_ID_LENGTH);
    final String secretAccessKey = generateSecureRandomStringUsingChars(
        CHARS_FOR_SECRET_ACCESS_KEYS, CHARS_FOR_SECRET_ACCESS_KEYS_LENGTH, STS_SECRET_ACCESS_KEY_LENGTH);
    final String roleId = ASSUME_ROLE_ID_PREFIX + generateSecureRandomStringUsingChars(
        CHARS_FOR_ACCESS_KEY_IDS, CHARS_FOR_ACCESS_KEY_IDS_LENGTH, STS_ROLE_ID_LENGTH);

    // Build UpdateAssumeRoleRequest with leader-generated credentials
    final UpdateAssumeRoleRequest.Builder updateAssumeRoleRequestBuilder =
        UpdateAssumeRoleRequest.newBuilder()
            .setRoleArn(assumeRoleRequest.getRoleArn())
            .setRoleSessionName(assumeRoleRequest.getRoleSessionName())
            .setDurationSeconds(assumeRoleRequest.getDurationSeconds())
            .setRequestId(assumeRoleRequest.getRequestId())
            .setTempAccessKeyId(tempAccessKeyId)
            .setSecretAccessKey(secretAccessKey)
            .setRoleId(roleId);

    if (assumeRoleRequest.hasAwsIamSessionPolicy()) {
      updateAssumeRoleRequestBuilder.setAwsIamSessionPolicy(assumeRoleRequest.getAwsIamSessionPolicy());
    }

    // Build new OMRequest with both original and update requests
    final OMRequest.Builder omRequest = OMRequest.newBuilder()
        .setUserInfo(getUserInfo())
        .setCmdType(getOmRequest().getCmdType())
        .setClientId(getOmRequest().getClientId())
        .setAssumeRoleRequest(assumeRoleRequest)
        .setUpdateAssumeRoleRequest(updateAssumeRoleRequestBuilder.build());

    if (getOmRequest().hasS3Authentication()) {
      omRequest.setS3Authentication(getOmRequest().getS3Authentication());
    }

    if (getOmRequest().hasTraceID()) {
      omRequest.setTraceID(getOmRequest().getTraceID());
    }

    return omRequest.build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final OMRequest omRequest = getOmRequest();
    final AssumeRoleRequest assumeRoleRequest = omRequest.getAssumeRoleRequest();
    final UpdateAssumeRoleRequest updateAssumeRoleRequest = omRequest.getUpdateAssumeRoleRequest();

    final int durationSeconds = assumeRoleRequest.getDurationSeconds();
    final String roleSessionName = assumeRoleRequest.getRoleSessionName();
    final String roleArn = assumeRoleRequest.getRoleArn();
    final String awsIamSessionPolicy = assumeRoleRequest.getAwsIamSessionPolicy();
    final String requestId = assumeRoleRequest.getRequestId();

    // Extract leader-generated credentials and roleId from UpdateAssumeRoleRequest
    final String tempAccessKeyId = updateAssumeRoleRequest.getTempAccessKeyId();
    final String secretAccessKey = updateAssumeRoleRequest.getSecretAccessKey();
    final String roleId = updateAssumeRoleRequest.getRoleId();

    final Map<String, String> auditMap = new HashMap<>();
    final AuditLogger auditLogger = ozoneManager.getAuditLogger();
    final OzoneManagerProtocolProtos.UserInfo userInfo = omRequest.getUserInfo();
    S3STSUtils.addAssumeRoleAuditParams(
        auditMap, roleArn, roleSessionName, awsIamSessionPolicy, durationSeconds, requestId);

    Exception exception = null;
    OMClientResponse omClientResponse;
    try {
      // Validate duration
      S3STSUtils.validateDuration(durationSeconds);

      // Validate role session name
      S3STSUtils.validateRoleSessionName(roleSessionName);

      // Validate role ARN and extract role
      final String targetRoleName = AwsRoleArnValidator.validateAndExtractRoleNameFromArn(roleArn);

      // Note: The IamSessionPolicyResolver validates the awsIamPolicy length internally

      if (!omRequest.hasS3Authentication()) {
        throw new OMException(
            "S3AssumeRoleRequest does not have S3 authentication", OMException.ResultCodes.INVALID_REQUEST);
      }

      // Generate session token using leader-generated credentials
      final String sessionToken = generateSessionToken(
          targetRoleName, omRequest, ozoneManager, assumeRoleRequest, secretAccessKey, tempAccessKeyId);

      // Generate AssumedRoleId for response using leader-generated roleId
      final String assumedRoleId = roleId + ":" + roleSessionName;

      // Calculate expiration of session token
      final long expirationEpochSeconds = clock.instant().plusSeconds(durationSeconds).getEpochSecond();

      // Add tempAccessKeyId to the log so it can be determined which permanent user created the tempAccessKeyId
      auditMap.put("tempAccessKeyId", tempAccessKeyId);

      final AssumeRoleResponse.Builder responseBuilder = AssumeRoleResponse.newBuilder()
          .setAccessKeyId(tempAccessKeyId)
          .setSecretAccessKey(secretAccessKey)
          .setSessionToken(sessionToken)
          .setExpirationEpochSeconds(expirationEpochSeconds)
          .setAssumedRoleId(assumedRoleId);

      omClientResponse = new S3AssumeRoleResponse(
          OmResponseUtil.getOMResponseBuilder(omRequest)
              .setAssumeRoleResponse(responseBuilder.build())
              .build());
    } catch (OMException e) {
      exception = e;
      omClientResponse = new S3AssumeRoleResponse(
          createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), e));
    } catch (IOException e) {
      final OMException omException = new OMException(
          "Failed to generate STS token for role: " + roleArn, e, OMException.ResultCodes.INTERNAL_ERROR);
      exception = omException;
      omClientResponse = new S3AssumeRoleResponse(
          createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), omException));
    }

    // Audit log
    markForAudit(auditLogger, buildAuditMessage(OMAction.S3_ASSUME_ROLE, auditMap, exception, userInfo));

    return omClientResponse;
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

    final Set<OzoneGrant> grants = Strings.isNullOrEmpty(awsIamPolicy) ?
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
