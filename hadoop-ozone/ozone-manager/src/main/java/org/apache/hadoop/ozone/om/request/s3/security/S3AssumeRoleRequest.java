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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.time.Instant;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Handles S3AssumeRoleRequest request.
 */
public class S3AssumeRoleRequest extends OMClientRequest {

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  private static final int MIN_TOKEN_EXPIRATION_SECONDS = 900;    // 15 minutes in seconds
  private static final int MAX_TOKEN_EXPIRATION_SECONDS = 43200;  // 12 hours in seconds
  private static final String STS_TOKEN_PREFIX = "ASIA";
  private static final int STS_ACCESS_KEY_ID_LENGTH = 20;
  private static final int STS_SECRET_ACCESS_KEY_LENGTH = 40;
  private static final int STS_ROLE_ID_LENGTH = 16;
  private static final String ASSUME_ROLE_ID_PREFIX = "AROA";
  private static final int ASSUME_ROLE_NAME_MAX_LENGTH = 64;
  private static final int ASSUME_ROLE_ARN_MIN_LENGTH = 20;
  private static final int ASSUME_ROLE_ARN_MAX_LENGTH = 2048;
  private static final String CHARS_FOR_ACCESS_KEY_IDS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final int CHARS_FOR_ACCESS_KEY_IDS_LENGTH = CHARS_FOR_ACCESS_KEY_IDS.length();
  private static final String CHARS_FOR_SECRET_ACCESS_KEYS = CHARS_FOR_ACCESS_KEY_IDS +
      "abcdefghijklmnopqrstuvwxyz/+";
  private static final int CHARS_FOR_SECRET_ACCESS_KEYS_LENGTH = CHARS_FOR_SECRET_ACCESS_KEYS.length();

  public S3AssumeRoleRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
                                                 ExecutionContext context) {
    final OMRequest omRequest = getOmRequest();
    final AssumeRoleRequest assumeRoleRequest = omRequest.getAssumeRoleRequest();
    final int durationSeconds = assumeRoleRequest.getDurationSeconds();

    // Validate duration
    if (durationSeconds < MIN_TOKEN_EXPIRATION_SECONDS ||
        durationSeconds > MAX_TOKEN_EXPIRATION_SECONDS) {
      final OMException omException = new OMException("Duration: " + durationSeconds +
          " is not valid",
          OMException.ResultCodes.INVALID_REQUEST
      );
      return new S3AssumeRoleResponse(
          createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), omException)
      );
    }

    // Validate role session name
    final String roleSessionName = assumeRoleRequest.getRoleSessionName();
    if (StringUtils.isBlank(roleSessionName)) {
      final OMException omException = new OMException("RoleSessionName: " + roleSessionName +
          " is not valid",
          OMException.ResultCodes.INVALID_REQUEST
      );
      return new S3AssumeRoleResponse(
          createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), omException)
      );
    }

    final String roleArn = assumeRoleRequest.getRoleArn();
    try {
      // Validate role ARN and extract role
      final String targetRoleName = validateAndExtractRoleNameFromArn(roleArn);

      if (!omRequest.hasS3Authentication()) {
        final String msg = "S3AssumeRoleRequest does not have S3 authentication";
        final OMException omException = new OMException(msg, OMException.ResultCodes.INVALID_REQUEST);
        return new S3AssumeRoleResponse(
            createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), omException)
        );
      }

      // Generate temporary AWS credentials using cryptographically strong SecureRandom
      final String tempAccessKeyId = STS_TOKEN_PREFIX +
          generateSecureRandomStringUsingChars(CHARS_FOR_ACCESS_KEY_IDS,
              CHARS_FOR_ACCESS_KEY_IDS_LENGTH,
              STS_ACCESS_KEY_ID_LENGTH
          );
      final String secretAccessKey = generateSecureRandomStringUsingChars(CHARS_FOR_SECRET_ACCESS_KEYS,
          CHARS_FOR_SECRET_ACCESS_KEYS_LENGTH,
          STS_SECRET_ACCESS_KEY_LENGTH
      );
      final String sessionToken = generateSessionToken(targetRoleName,
          omRequest,
          ozoneManager,
          assumeRoleRequest,
          secretAccessKey);

      // Generate AssumedRoleId for response
      final String roleId = ASSUME_ROLE_ID_PREFIX +
          generateSecureRandomStringUsingChars(CHARS_FOR_ACCESS_KEY_IDS,
              CHARS_FOR_ACCESS_KEY_IDS_LENGTH,
              STS_ROLE_ID_LENGTH
          );
      final String assumedRoleId = roleId + ":" + roleSessionName;

      // Calculate expiration of session token
      final long expirationEpochSeconds = Instant.now().plusSeconds(durationSeconds).getEpochSecond();

      final AssumeRoleResponse.Builder responseBuilder = AssumeRoleResponse.newBuilder()
          .setAccessKeyId(tempAccessKeyId)
          .setSecretAccessKey(secretAccessKey)
          .setSessionToken(sessionToken)
          .setExpirationEpochSeconds(expirationEpochSeconds)
          .setAssumedRoleId(assumedRoleId);

      return new S3AssumeRoleResponse(
          OmResponseUtil.getOMResponseBuilder(omRequest)
              .setAssumeRoleResponse(responseBuilder.build())
              .build()
      );

    } catch (OMException e) {
      return new S3AssumeRoleResponse(
          createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), e)
      );
    } catch (IOException e) {
      final OMException omException = new OMException("Failed to generate STS token for role: " + roleArn,
          e,
          OMException.ResultCodes.INTERNAL_ERROR
      );
      return new S3AssumeRoleResponse(
          createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), omException)
      );
    }
  }

  /**
   * Generates session token using components from the AssumeRoleRequest.
   */
  private String generateSessionToken(String targetRoleName,
                                      OMRequest omRequest,
                                      OzoneManager ozoneManager,
                                      AssumeRoleRequest assumeRoleRequest,
                                      String secretAccessKey) throws IOException {

    InetAddress remoteIp = ProtobufRpcEngine.Server.getRemoteIp();
    if (remoteIp == null) {
      remoteIp = ozoneManager.getOmRpcServerAddr().getAddress();
    }

    final String hostName = remoteIp != null ?
        remoteIp.getHostName() :
        ozoneManager.getOmRpcServerAddr().getHostName();

    // Determine the caller's access key ID - this will be referred to as the original
    // access key id.  When STS tokens are used, the tokens will be authorized as
    // the kerberos principal associated to the original access key id, in conjunction with the
    // role permissions and optional AWS IAM session policy permissions.
    final String originalAccessKeyId = omRequest.getS3Authentication().getAccessId();

    final String principal = OzoneAclUtils.accessIdToUserPrincipal(originalAccessKeyId);
    final UserGroupInformation ugi = UserGroupInformation.createRemoteUser(principal);

    final String roleArn = assumeRoleRequest.getRoleArn();
    final String sessionPolicy = getSessionPolicy(ozoneManager,
        originalAccessKeyId,
        assumeRoleRequest.getAwsIamSessionPolicy(),
        hostName,
        remoteIp,
        ugi,
        targetRoleName
    );

    // TODO sts - generate a real STS token in a future PR that incorporates the components above
    return originalAccessKeyId + ":" + roleArn + ":" + assumeRoleRequest.getDurationSeconds() +
        ":" + secretAccessKey + ":" + sessionPolicy;
  }

  /**
   * Calls utility to convert IAM Policy to Ozone nomenclature and uses this output as input
   * to IAccessAuthorizer.generateAssumeRoleSessionPolicy() which is currently only implemented
   * by RangerOzoneAuthorizer.
   */
  private String getSessionPolicy(OzoneManager ozoneManager,
                                  String originalAccessKeyId,
                                  String awsIamPolicy,
                                  String hostName,
                                  InetAddress remoteIp,
                                  UserGroupInformation ugi,
                                  String targetRoleName) throws IOException {
    // TODO sts - implement in a future PR
    return null;
  }

  /**
   * Generates a cryptographically strong String of the supplied stringLength using supplied chars.
   */
  @VisibleForTesting
  static String generateSecureRandomStringUsingChars(String chars,
                                                     int charsLength,
                                                     int stringLength) {
    final StringBuilder sb = new StringBuilder(stringLength);
    for (int i = 0; i < stringLength; i++) {
      sb.append(chars.charAt(SECURE_RANDOM.nextInt(charsLength)));
    }
    return sb.toString();
  }

  /**
   * Extract the role name from an AWS-style role ARN, falling back to the
   * full ARN if parsing is not possible. Examples:
   * arn:aws:iam::123456789012:role/RoleA -> RoleA
   * arn:aws:iam::123456789012:role/path/RoleB -> RoleB
   */
  @VisibleForTesting
  static String validateAndExtractRoleNameFromArn(String roleArn) throws OMException {
    if (StringUtils.isBlank(roleArn)) {
      throw new OMException("Role ARN is required", OMException.ResultCodes.INVALID_REQUEST);
    }

    final int roleArnLength = roleArn.length();
    if (roleArnLength < ASSUME_ROLE_ARN_MIN_LENGTH || roleArnLength > ASSUME_ROLE_ARN_MAX_LENGTH) {
      throw new OMException("Role ARN length: " + roleArnLength +
          " is not valid",
          OMException.ResultCodes.INVALID_REQUEST
      );
    }


    // Expected format: arn:aws:iam::123456789012:role/[optional path segments/]RoleName
    if (!roleArn.startsWith("arn:aws:iam::")) {
      throw new OMException("Invalid role ARN: " + roleArn, OMException.ResultCodes.INVALID_REQUEST);
    }

    // Split ARN into parts: arn:aws:iam::accountId:role/path/name
    final String[] parts = roleArn.split(":", 6);
    if (parts.length < 6 || !parts[5].startsWith("role/")) {
      throw new OMException("Invalid role ARN: " + roleArn, OMException.ResultCodes.INVALID_REQUEST);
    }

    // Validate account ID (12 digits)
    final String accountId = parts[4];
    if (accountId.length() != 12 || !isAllDigits(accountId)) {
      throw new OMException("Invalid AWS account ID in ARN", OMException.ResultCodes.INVALID_REQUEST);
    }

    // Extract role name (last segment after last slash)
    final String rolePath = parts[5].substring(5); // Skip "role/"
    if (rolePath.isEmpty() || rolePath.endsWith("/")) {
      throw new OMException("Invalid role ARN: missing role name", OMException.ResultCodes.INVALID_REQUEST);
    }

    final String[] pathSegments = rolePath.split("/");
    final String roleName = pathSegments[pathSegments.length - 1];

    // Validate role name
    if (roleName.isEmpty() ||
        roleName.length() > ASSUME_ROLE_NAME_MAX_LENGTH ||
        hasCharNotAllowedInIamRoleArn(roleName)) {
      throw new OMException("Invalid role name: " + roleName, OMException.ResultCodes.INVALID_REQUEST);
    }

    // Validate path segments if present
    if (pathSegments.length > 1) {
      final String pathPrefix = rolePath.substring(0, rolePath.lastIndexOf('/') + 1);
      if (pathPrefix.length() > 511) {
        throw new OMException("Role path length must be between 1 and 512 characters",
            OMException.ResultCodes.INVALID_REQUEST);
      }
      for (String segment : pathSegments) {
        if (segment.isEmpty() || hasCharNotAllowedInIamRoleArn(segment)) {
          throw new OMException("Invalid role path segment: " + segment, OMException.ResultCodes.INVALID_REQUEST);
        }
      }
    }

    return roleName;
  }

  /**
   * Checks if all the characters in a String are numbers.
   */
  private static boolean isAllDigits(String s) {
    for (int i = 0; i < s.length(); i++) {
      if (!Character.isDigit(s.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if supplied string contains a char that is not allowed in IAM Role ARN.
   */
  private static boolean hasCharNotAllowedInIamRoleArn(String s) {
    for (int i = 0; i < s.length(); i++) {
      if (!isCharAllowedInIamRoleArn(s.charAt(i))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if the supplied chars is allowed in IAM Role ARN.
   */
  private static boolean isCharAllowedInIamRoleArn(char c) {
    return (c >= 'A' && c <= 'Z')
        || (c >= 'a' && c <= 'z')
        || (c >= '0' && c <= '9')
        || c == '+' || c == '=' || c == ',' || c == '.' || c == '@' || c == '_' || c == '-';
  }
}
