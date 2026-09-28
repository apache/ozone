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

import com.google.common.base.Strings;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/**
 * Validator for AWS IAM Role ARNs and extracts the role name from them.
 */
public final class AwsRoleArnValidator {

  private static final int ASSUME_ROLE_NAME_MAX_LENGTH = 64;
  private static final int ASSUME_ROLE_ARN_MIN_LENGTH = 20;
  private static final int ASSUME_ROLE_ARN_MAX_LENGTH = 2048;

  private AwsRoleArnValidator() {
  }

  /**
   * Extract the role name from an AWS-style role ARN, falling back to the
   * full ARN if parsing is not possible.
   * Examples:
   * <pre>{@code
   * arn:aws:iam::123456789012:role/RoleA -> RoleA
   * arn:aws:iam::123456789012:role/path/RoleB -> RoleB
   * }</pre>
   *
   * @param roleArn the AWS role ARN to validate and extract from
   * @return the extracted role name
   * @throws OMException if the ARN is invalid
   */
  public static String validateAndExtractRoleNameFromArn(String roleArn) throws OMException {
    if (Strings.isNullOrEmpty(roleArn)) {
      throw new OMException(
          "Value null at 'roleArn' failed to satisfy constraint: Member must not be null",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    final int roleArnLength = roleArn.length();
    if (roleArnLength < ASSUME_ROLE_ARN_MIN_LENGTH || roleArnLength > ASSUME_ROLE_ARN_MAX_LENGTH) {
      throw new OMException(
          "Role ARN length must be between " + ASSUME_ROLE_ARN_MIN_LENGTH + " and " +
          ASSUME_ROLE_ARN_MAX_LENGTH, OMException.ResultCodes.INVALID_REQUEST);
    }

    // Expected format: arn:aws:iam::123456789012:role/[optional path segments/]RoleName
    if (!roleArn.startsWith("arn:aws:iam::")) {
      throw new OMException(
          "Invalid role ARN (does not start with arn:aws:iam::): " + roleArn, OMException.ResultCodes.INVALID_REQUEST);
    }

    // Split ARN into parts: arn:aws:iam::accountId:role/path/name
    final String[] parts = roleArn.split(":", 6);
    if (parts.length < 6 || !parts[5].startsWith("role/")) {
      throw new OMException(
          "Invalid role ARN (unexpected field count): " + roleArn, OMException.ResultCodes.INVALID_REQUEST);
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
    if (roleName.isEmpty() || roleName.length() > ASSUME_ROLE_NAME_MAX_LENGTH ||
        hasCharNotAllowedInIamRoleArn(roleName)) {
      throw new OMException("Invalid role name: " + roleName, OMException.ResultCodes.INVALID_REQUEST);
    }

    // Validate path segments if present
    if (pathSegments.length > 1) {
      final String pathPrefix = rolePath.substring(0, rolePath.lastIndexOf('/') + 1);
      if (pathPrefix.length() > 511) {
        throw new OMException(
            "Role path length must be between 1 and 512 characters", OMException.ResultCodes.INVALID_REQUEST);
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
      if (!isCharAllowedInIamRoleArn(s.codePointAt(i))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if the supplied char is allowed in IAM Role ARN.
   * Pattern: [\u0009\u000A\u000D\u0020-\u007E\u0085\u00A0-\uD7FF\uE000-\uFFFD\u10000-\u10FFFF]+
   */
  private static boolean isCharAllowedInIamRoleArn(int c) {
    return c == 0x09 || c == 0x0A || c == 0x0D || (c >= 0x20 && c <= 0x7E) || c == 0x85 || (c >= 0xA0 && c <= 0xD7FF) ||
        (c >= 0xE000 && c <= 0xFFFD) || (c >= 0x10000 && c <= 0x10FFFF);
  }
}

