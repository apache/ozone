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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;

import com.google.common.base.Strings;
import java.util.Map;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/**
 * Utility class containing constants and validation methods shared by STS endpoint and OzoneManager processing.
 */
@Immutable
public final class S3STSUtils {
  // STS API constants
  public static final int DEFAULT_DURATION_SECONDS = 3600;    // 1 hour
  public static final int MAX_DURATION_SECONDS = 43200;       // 12 hours
  public static final int MIN_DURATION_SECONDS = 900;         // 15 minutes

  public static final int ASSUME_ROLE_SESSION_NAME_MIN_LENGTH = 2;
  public static final int ASSUME_ROLE_SESSION_NAME_MAX_LENGTH = 64;

  // AWS limit for session policy is 2048 characters
  public static final int MAX_SESSION_POLICY_LENGTH = 2048;

  private S3STSUtils() {
  }

  /**
   * Adds standard AssumeRole audit params.
   */
  public static void addAssumeRoleAuditParams(Map<String, String> auditParams, String roleArn, String roleSessionName,
      String awsIamSessionPolicy, int duration, String requestId) {

    auditParams.put("action", "AssumeRole");
    auditParams.put("roleArn", roleArn);
    auditParams.put("roleSessionName", roleSessionName);
    auditParams.put("duration", String.valueOf(duration));
    auditParams.put("isPolicyIncluded", Strings.isNullOrEmpty(awsIamSessionPolicy) ? "N" : "Y");
    auditParams.put("requestId", requestId);
  }

  /**
   * Validates the duration in seconds.
   * @param durationSeconds duration in seconds
   * @return validated duration
   * @throws OMException if duration is invalid
   */
  public static int validateDuration(Integer durationSeconds) throws OMException {
    if (durationSeconds == null) {
      return DEFAULT_DURATION_SECONDS;
    }

    if (durationSeconds < MIN_DURATION_SECONDS || durationSeconds > MAX_DURATION_SECONDS) {
      throw new OMException(
          "Invalid Value: DurationSeconds must be between " + MIN_DURATION_SECONDS + " and " + MAX_DURATION_SECONDS +
          " seconds", INVALID_REQUEST);
    }

    return durationSeconds;
  }

  /**
   * Validates the role session name.
   * @param roleSessionName role session name
   * @throws OMException if role session name is invalid
   */
  public static void validateRoleSessionName(String roleSessionName) throws OMException {
    if (Strings.isNullOrEmpty(roleSessionName)) {
      throw new OMException(
          "Value null at 'roleSessionName' failed to satisfy constraint: Member must not be null", INVALID_REQUEST);
    }

    final int roleSessionNameLength = roleSessionName.length();
    if (roleSessionNameLength < ASSUME_ROLE_SESSION_NAME_MIN_LENGTH ||
        roleSessionNameLength > ASSUME_ROLE_SESSION_NAME_MAX_LENGTH) {
      throw new OMException("Invalid RoleSessionName length " + roleSessionNameLength + ": it must be " +
          ASSUME_ROLE_SESSION_NAME_MIN_LENGTH + "-" + ASSUME_ROLE_SESSION_NAME_MAX_LENGTH + " characters long and " +
          "contain only alphanumeric characters and +, =, ,, ., @, -", INVALID_REQUEST);
    }

    // AWS allows: alphanumeric, +, =, ,, ., @, -
    // Pattern: [\w+=,.@-]*
    // Don't use regex for performance reasons
    for (int i = 0; i < roleSessionNameLength; i++) {
      final char c = roleSessionName.charAt(i);
      if (!isRoleSessionNameChar(c)) {
        throw new OMException("Invalid character '" + c + "' in RoleSessionName: it must be " +
            ASSUME_ROLE_SESSION_NAME_MIN_LENGTH + "-" + ASSUME_ROLE_SESSION_NAME_MAX_LENGTH + " characters long and " +
            "contain only alphanumeric characters and +, =, ,, ., @, -", INVALID_REQUEST);
      }
    }
  }

  private static boolean isRoleSessionNameChar(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
        c == '_' || c == '+' || c == '=' || c == ',' || c == '.' || c == '@' || c == '-';
  }

  /**
   * Validates the session policy length.
   * @param awsIamSessionPolicy session policy
   * @throws OMException if policy length is invalid
   */
  public static void validateSessionPolicy(String awsIamSessionPolicy) throws OMException {
    if (awsIamSessionPolicy != null && awsIamSessionPolicy.length() > MAX_SESSION_POLICY_LENGTH) {
      throw new OMException(
          "Value '" + awsIamSessionPolicy + "' at 'policy' failed to satisfy constraint: Member " +
              "must have length less than or equal to " + MAX_SESSION_POLICY_LENGTH, INVALID_REQUEST);
    }
  }

  /**
   * Generates the assumed role user ARN.
   * @param validRoleArn                  valid role ARN
   * @param roleSessionName               role session name
   * @return assumed role user ARN
   */
  public static String toAssumedRoleUserArn(String validRoleArn, String roleSessionName) {
    // We already know the roleArn is valid, so perform the conversion for assumed role user arn format
    // RoleArn format: arn:aws:iam::<account-id>:role/<role-name>
    // Assumed role user arn format: arn:aws:sts::<account-id>:assumed-role/<role-name>/<role-session-name>
    final String[] parts = splitRoleArnWithoutRegex(validRoleArn);

    final String partition = parts[1];
    final String accountId = parts[4];
    final String resource = parts[5];
    final String roleName = resource.substring("role/".length());

    //noinspection StringBufferReplaceableByString
    final StringBuilder stringBuilder = new StringBuilder("arn:");
    stringBuilder.append(partition);
    stringBuilder.append(":sts::");
    stringBuilder.append(accountId);
    stringBuilder.append(":assumed-role/");
    stringBuilder.append(roleName);
    stringBuilder.append('/');
    stringBuilder.append(roleSessionName);
    return stringBuilder.toString();
  }

  private static String[] splitRoleArnWithoutRegex(String roleArn) {
    final String[] parts = new String[6];
    int start = 0;
    for (int i = 0; i < 5; i++) {
      final int end = roleArn.indexOf(':', start);
      parts[i] = roleArn.substring(start, end);
      start = end + 1;
    }
    parts[5] = roleArn.substring(start);
    return parts;
  }
}
