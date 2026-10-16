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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for AwsRoleArnValidator.
 */
public class TestAwsRoleArnValidator {

  private static final String ROLE_ARN_1 = "arn:aws:iam::123456789012:role/MyRole1";
  private static final String ROLE_ARN_2 = "arn:aws:iam::123456789012:role/path/anotherLevel/Role2";

  @Test
  public void testValidateAndExtractRoleNameFromArnSuccessCases() throws OMException {
    assertThat(AwsRoleArnValidator.validateAndExtractRoleNameFromArn(ROLE_ARN_1)).isEqualTo("MyRole1");

    assertThat(AwsRoleArnValidator.validateAndExtractRoleNameFromArn(ROLE_ARN_2)).isEqualTo("Role2");

    // Path name right at 511-char max boundary
    final String arnPrefixLen511 = StringUtils.repeat('p', 510) + "/"; // 510 chars + '/' = 511
    final String arnMaxPath = "arn:aws:iam::123456789012:role/" + arnPrefixLen511 + "RoleB";
    assertThat(AwsRoleArnValidator.validateAndExtractRoleNameFromArn(arnMaxPath)).isEqualTo("RoleB");

    // Role name right at 64-char max boundary
    final String roleName64 = StringUtils.repeat('A', 64);
    final String arn64 = "arn:aws:iam::123456789012:role/" + roleName64;
    assertThat(AwsRoleArnValidator.validateAndExtractRoleNameFromArn(arn64)).isEqualTo(roleName64);
  }

  @Test
  public void testValidateAndExtractRoleNameFromArnFailureCases() {
    // Improper structure
    final OMException e1 = assertThrows(
        OMException.class, () -> AwsRoleArnValidator.validateAndExtractRoleNameFromArn("roleNoSlashNorColons"));
    assertThat(e1.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e1.getMessage()).isEqualTo(
        "Invalid role ARN (does not start with arn:aws:iam::): roleNoSlashNorColons");

    // Null
    final OMException e2 = assertThrows(
        OMException.class, () -> AwsRoleArnValidator.validateAndExtractRoleNameFromArn(null));
    assertThat(e2.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e2.getMessage()).isEqualTo(
        "Value null at 'roleArn' failed to satisfy constraint: Member must not be null");

    // String without role name
    final OMException e3 = assertThrows(
        OMException.class,
        () -> AwsRoleArnValidator.validateAndExtractRoleNameFromArn("arn:aws:iam::123456789012:role/"));
    assertThat(e3.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e3.getMessage()).isEqualTo("Invalid role ARN: missing role name");

    // No role resource and no role name
    final OMException e4 = assertThrows(
        OMException.class,
        () -> AwsRoleArnValidator.validateAndExtractRoleNameFromArn("arn:aws:iam::123456789012"));
    assertThat(e4.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e4.getMessage()).isEqualTo(
        "Invalid role ARN (unexpected field count): arn:aws:iam::123456789012");

    // No role resource but contains role name
    final OMException e5 = assertThrows(
        OMException.class,
        () -> AwsRoleArnValidator.validateAndExtractRoleNameFromArn("arn:aws:iam::123456789012:WebRole"));
    assertThat(e5.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e5.getMessage()).isEqualTo(
        "Invalid role ARN (unexpected field count): arn:aws:iam::123456789012:WebRole");

    // Empty string
    final OMException e6 = assertThrows(
        OMException.class, () -> AwsRoleArnValidator.validateAndExtractRoleNameFromArn(""));
    assertThat(e6.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e6.getMessage()).isEqualTo(
        "Value null at 'roleArn' failed to satisfy constraint: Member must not be null");

    // String with only slash
    final OMException e7 = assertThrows(
        OMException.class, () -> AwsRoleArnValidator.validateAndExtractRoleNameFromArn("/"));
    assertThat(e7.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e7.getMessage()).isEqualTo("Role ARN length must be between 20 and 2048");

    // String with only whitespace
    final OMException e8 = assertThrows(
        OMException.class, () -> AwsRoleArnValidator.validateAndExtractRoleNameFromArn("     "));
    assertThat(e8.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e8.getMessage()).isEqualTo("Role ARN length must be between 20 and 2048");

    // Path name too long (> 511 characters)
    final String arnPrefixLen512 = StringUtils.repeat('q', 511) + "/"; // 511 chars + '/' = 512
    final String arnTooLongPath = "arn:aws:iam::123456789012:role/" + arnPrefixLen512 + "RoleA";
    final OMException e9 = assertThrows(
        OMException.class, () -> AwsRoleArnValidator.validateAndExtractRoleNameFromArn(arnTooLongPath));
    assertThat(e9.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e9.getMessage()).isEqualTo("Role path length must be between 1 and 512 characters");

    // Otherwise valid role ending in /
    final OMException e10 = assertThrows(
        OMException.class,
        () -> AwsRoleArnValidator.validateAndExtractRoleNameFromArn("arn:aws:iam::123456789012:role/MyRole/"));
    assertThat(e10.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e10.getMessage()).isEqualTo("Invalid role ARN: missing role name");  // MyRole/ is considered a path

    // 65-char role name
    final String roleName65 = StringUtils.repeat('B', 65);
    final String roleArn65 = "arn:aws:iam::123456789012:role/" + roleName65;
    final OMException e11 = assertThrows(
        OMException.class, () -> AwsRoleArnValidator.validateAndExtractRoleNameFromArn(roleArn65));
    assertThat(e11.getResult()).isEqualTo(OMException.ResultCodes.INVALID_REQUEST);
    assertThat(e11.getMessage()).isEqualTo("Invalid role name: " + roleName65);
  }
}
