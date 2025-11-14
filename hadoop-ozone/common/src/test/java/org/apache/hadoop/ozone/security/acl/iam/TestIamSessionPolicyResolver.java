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

package org.apache.hadoop.ozone.security.acl.iam;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link IamSessionPolicyResolver}.
 * */
public class TestIamSessionPolicyResolver {

  private static final String VOLUME = "s3v";

  @Test
  public void testUnsupportedConditionOperatorThrows() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::b\",\n" +
        "    \"Condition\": { \"StringLike\": { \"s3:prefix\": \"x/*\" } }\n" +
        "  }]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "Unsupported Condition operator - StringLike", NOT_SUPPORTED_OPERATION);
  }

  @Test
  public void testUnsupportedConditionAttributeThrows() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::b\",\n" +
        "    \"Condition\": { \"StringEquals\": { \"aws:SourceArn\": \"arn:aws:s3:::d\" } }\n" +
        "  }]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "Unsupported Condition attribute - aws:SourceArn", NOT_SUPPORTED_OPERATION);
  }

  @Test
  public void testUnsupportedEffectThrows() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Deny\",\n" +                       // unsupported effect
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::proj-*\"\n" +
        "  }]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "Unsupported Effect - Deny", NOT_SUPPORTED_OPERATION);
  }

  @Test
  public void testInvalidJsonWithoutStatementThrows() {
    final String json = "{\n" +
        "  \"RandomAttribute\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::b\",\n" +
        "    \"Condition\": { \"StringEquals\": { \"s3:prefix\": \"x/*\" } }\n" +
        "  }]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "Invalid policy JSON - missing Statement", INVALID_REQUEST);
  }

  @Test
  public void testInvalidEffectThrows() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": [\"Allow\"],\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::bucket1\"\n" +
        "  }]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "Invalid Effect in JSON policy (must be a String) - [\"Allow\"]",
        INVALID_REQUEST);
  }

  @Test
  public void testMissingEffectInStatementThrows() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::bucket1\"\n" +
        "  }]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "Effect is missing from JSON policy", INVALID_REQUEST);
  }

  @Test
  public void testInvalidNumberOfConditionsThrows() {
    final String json = "{\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": \"s3:ListBucket\",\n" +
        "      \"Resource\": \"arn:aws:s3:::b\",\n" +
        "      \"Condition\": [\n" +
        "        {\n" +
        "          \"StringEquals\": {\n" +
        "            \"aws:SourceArn\": \"arn:aws:s3:::d\"\n" +
        "          }\n" +
        "        },\n" +
        "        {\n" +
        "          \"StringEquals\": {\n" +
        "            \"aws:SourceArn\": \"arn:aws:s3:::e\"\n" +
        "          }\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "Only one Condition is supported", NOT_SUPPORTED_OPERATION);
  }

  @Test
  public void testInvalidConditionThrows() {
    final String json = "{\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": \"s3:ListBucket\",\n" +
        "      \"Resource\": \"arn:aws:s3:::b\",\n" +
        "      \"Condition\": [\"RandomCondition\"]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "Invalid Condition (must have operator StringEquals and attribute " +
        "s3:prefix) - [\"RandomCondition\"]", INVALID_REQUEST);
  }

  @Test
  public void testInvalidConditionAttributeMissingStringEqualsThrows() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::b\",\n" +
        "    \"Condition\": { \"StringEquals\": null }\n" +
        "  }]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "Missing Condition attribute - StringEquals", INVALID_REQUEST);
  }

  @Test
  public void testInvalidConditionAttributeStructureThrows() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::b\",\n" +
        "    \"Condition\": { \"StringEquals\": [{ \"s3:prefix\": \"folder/\" }] }\n" +
        "  }]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "Invalid Condition attribute structure - [{\"s3:prefix\":\"folder/\"}]",
        INVALID_REQUEST);
  }

  @Test
  public void testInvalidJsonThrows() {
    final String invalidJson = "{[{{}]\"\"";

    expectResolveThrowsForBothAuthorizers(
        invalidJson, "Invalid policy JSON (most likely JSON structure is incorrect)",
        INVALID_REQUEST);
  }

  @Test
  public void testJsonExceedsMaxLengthThrows() {
    final String json = createJsonStringLargerThan2048Characters();

    expectResolveThrowsForBothAuthorizers(
        json, "Invalid policy JSON - exceeds maximum length of 2048 characters",
        INVALID_REQUEST);
  }

  @Test
  public void testJsonAtMaxLengthSucceeds() throws OMException {
    // Create a JSON string that is exactly 2048 characters
    final String json = create2048CharJsonString();
    assertThat(json.length()).isEqualTo(2048);

    // Must not throw an exception
    IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
  }

  private static void expectResolveThrows(String json,
      IamSessionPolicyResolver.AuthorizerType authorizerType, String expectedMessage,
      OMException.ResultCodes expectedCode) {
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, authorizerType);
      throw new AssertionError("Expected exception not thrown");
    } catch (OMException ex) {
      assertThat(ex.getMessage()).isEqualTo(expectedMessage);
      assertThat(ex.getResult()).isEqualTo(expectedCode);
    }
  }
  
  private static void expectResolveThrowsForBothAuthorizers(String json,
      String expectedMessage, OMException.ResultCodes expectedCode) {
    expectResolveThrows(json, IamSessionPolicyResolver.AuthorizerType.NATIVE, expectedMessage, expectedCode);
    expectResolveThrows(json, IamSessionPolicyResolver.AuthorizerType.RANGER, expectedMessage, expectedCode);
  }

  private static String createJsonStringLargerThan2048Characters() {
    final StringBuilder jsonBuilder = new StringBuilder();
    jsonBuilder.append("{\n");
    jsonBuilder.append("  \"Statement\": [{\n");
    jsonBuilder.append("    \"Effect\": \"Allow\",\n");
    jsonBuilder.append("    \"Action\": \"s3:ListBucket\",\n");
    jsonBuilder.append("    \"Resource\": \"arn:aws:s3:::");
    // Add enough characters to exceed 2048
    while (jsonBuilder.length() < 2048) {
      jsonBuilder.append("very-long-bucket-name-that-exceeds-the-limit-");
    }
    jsonBuilder.append("\"\n");
    jsonBuilder.append("  }]\n");
    jsonBuilder.append('}');

    return jsonBuilder.toString();
  }

  private static String create2048CharJsonString() {
    final StringBuilder jsonBuilder = new StringBuilder();
    jsonBuilder.append("{\n");
    jsonBuilder.append("  \"Statement\": [{\n");
    jsonBuilder.append("    \"Effect\": \"Allow\",\n");
    jsonBuilder.append("    \"Action\": \"s3:ListBucket\",\n");
    jsonBuilder.append("    \"Resource\": \"arn:aws:s3:::");
    // Add characters to reach exactly 2048 (accounting for closing brackets and newlines)
    // Closing part: "\"\n  }]\n}" = 8 chars
    while (jsonBuilder.length() < 2048 - 8) {
      jsonBuilder.append('a');
    }
    jsonBuilder.append("\"\n  }]\n}");

    return jsonBuilder.toString();
  }
}

