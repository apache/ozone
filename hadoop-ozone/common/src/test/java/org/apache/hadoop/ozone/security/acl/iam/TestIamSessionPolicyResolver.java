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
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.AuthorizerType.NATIVE;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.AuthorizerType.RANGER;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.buildCaseInsensitiveS3ActionMap;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.mapPolicyActionsToS3Actions;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.S3Action;
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
        json, "Unsupported Condition key name - aws:SourceArn", NOT_SUPPORTED_OPERATION);
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
        json, "Invalid Condition (must have operator StringEquals and key name " +
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
        json, "Missing Condition operator - StringEquals", INVALID_REQUEST);
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
        json, "Invalid Condition operator value structure - [{\"s3:prefix\":\"folder/\"}]",
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
    IamSessionPolicyResolver.resolve(json, VOLUME, NATIVE);
    IamSessionPolicyResolver.resolve(json, VOLUME, RANGER);
  }

  @Test
  public void testConditionKeyMustBeCaseInsensitive() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::b\",\n" +
        "    \"Condition\": { \"StringEquals\": { \"S3:PRefiX\": \"x/*\" } }\n" +
        "  }]\n" +
        "}";

    // Must not throw exception
    IamSessionPolicyResolver.resolve(json, VOLUME, NATIVE);
    IamSessionPolicyResolver.resolve(json, VOLUME, RANGER);
  }

  @Test
  public void testEffectMustBeCaseSensitive() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"aLLOw\",\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::b\",\n" +
        "    \"Condition\": { \"StringEquals\": { \"s3:prefix\": \"x/*\" } }\n" +
        "  }]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "Unsupported Effect - aLLOw", NOT_SUPPORTED_OPERATION);
  }

  @Test
  public void testBuildCaseInsensitiveS3ActionMapMatchesConstant() {
    assertThat(buildCaseInsensitiveS3ActionMap()).isEqualTo(IamSessionPolicyResolver.S3_ACTION_MAP_CI);
  }

  @Test
  public void testBuildCaseInsensitiveS3ActionMap() {
    final Map<String, Set<S3Action>> caseInsensitiveS3ActionMap = buildCaseInsensitiveS3ActionMap();
    
    // Verify that individual S3 actions are present
    assertThat(caseInsensitiveS3ActionMap).containsKeys(
        "s3:listbucket", "s3:getobject", "s3:putobject", "s3:deleteobject", "s3:createbucket", "s3:listallmybuckets");

    // Verify that wildcard actions are present
    assertThat(caseInsensitiveS3ActionMap).containsKeys(
        "s3:*", "s3:get*", "s3:put*", "s3:list*", "s3:delete*", "s3:create*");

    // Verify s3:Get* contains Get actions
    final Set<S3Action> getActions = caseInsensitiveS3ActionMap.get("s3:get*");
    assertThat(getActions).containsOnly(
        S3Action.GET_OBJECT, S3Action.GET_BUCKET_ACL, S3Action.GET_BUCKET_LOCATION, S3Action.GET_OBJECT_TAGGING);

    // Verify s3:Put* contains Put actions
    final Set<S3Action> putActions = caseInsensitiveS3ActionMap.get("s3:put*");
    assertThat(putActions).containsOnly(
        S3Action.PUT_OBJECT, S3Action.PUT_OBJECT_TAGGING, S3Action.PUT_BUCKET_ACL);

    // Verify s3:List* contains List actions
    final Set<S3Action> listActions = caseInsensitiveS3ActionMap.get("s3:list*");
    assertThat(listActions).containsOnly(
        S3Action.LIST_BUCKET, S3Action.LIST_ALL_MY_BUCKETS, S3Action.LIST_BUCKET_MULTIPART_UPLOADS,
        S3Action.LIST_MULTIPART_UPLOAD_PARTS);

    // Verify s3:Delete* contains Delete actions
    final Set<S3Action> deleteActions = caseInsensitiveS3ActionMap.get("s3:delete*");
    assertThat(deleteActions).containsOnly(
        S3Action.DELETE_OBJECT, S3Action.DELETE_BUCKET, S3Action.DELETE_OBJECT_TAGGING);

    // Verify s3:Create* contains Create actions
    final Set<S3Action> createActions = caseInsensitiveS3ActionMap.get("s3:create*");
    assertThat(createActions).containsOnly(S3Action.CREATE_BUCKET);
  }

  @Test
  public void testBuildCaseInsensitiveS3ActionMapIndividualActionsContainSingleEntry() {
    final Map<String, Set<S3Action>> actionMap = buildCaseInsensitiveS3ActionMap();
    
    // Individual actions should map to a set with exactly one entry
    final Set<S3Action> listBucketAction = actionMap.get("s3:listbucket");
    assertThat(listBucketAction).hasSize(1);
    
    final Set<S3Action> getObjectAction = actionMap.get("s3:getobject");
    assertThat(getObjectAction).hasSize(1);
  }

  @Test
  public void testMapPolicyActionsToS3ActionsWithNullReturnsEmpty() {
    final Set<S3Action> result = mapPolicyActionsToS3Actions(null);
    assertThat(result).isEmpty();
  }

  @Test
  public void testMapPolicyActionsToS3ActionsWithEmptyListReturnsEmpty() {
    final Set<S3Action> result = mapPolicyActionsToS3Actions(Collections.emptySet());
    assertThat(result).isEmpty();
  }

  @Test
  public void testMapPolicyActionsToS3ActionsWithSingleActionMapsCorrectly() {
    final Set<S3Action> listBucket = mapPolicyActionsToS3Actions(Collections.singleton("s3:ListBucket"));
    assertThat(listBucket).containsOnly(S3Action.LIST_BUCKET);

    // Ensure case-insensitive action works
    final Set<S3Action> listBucketCi = mapPolicyActionsToS3Actions(Collections.singleton("S3:ListBuCKet"));
    assertThat(listBucketCi).containsOnly(S3Action.LIST_BUCKET);

    final Set<S3Action> deleteObject = mapPolicyActionsToS3Actions(Collections.singleton("s3:DeleteObject"));
    assertThat(deleteObject).containsOnly(S3Action.DELETE_OBJECT);

    // Ensure case-insensitive action works
    final Set<S3Action> deleteObjectCi = mapPolicyActionsToS3Actions(Collections.singleton("S3:DeLETeObjeCT"));
    assertThat(deleteObjectCi).containsOnly(S3Action.DELETE_OBJECT);
  }

  @Test
  public void testMapPolicyActionsToS3ActionsWithMultipleActionsMapAllCorrectly() {
    final Set<S3Action> result = mapPolicyActionsToS3Actions(strSet("s3:ListBucket", "s3:GetObject", "s3:PutObject"));
    assertThat(result).containsOnly(S3Action.LIST_BUCKET, S3Action.GET_OBJECT, S3Action.PUT_OBJECT);
  }

  @Test
  public void testMapPolicyActionsToS3ActionsWithWildcardExpansion() {
    final Set<S3Action> result = mapPolicyActionsToS3Actions(Collections.singleton("s3:Get*"));
    assertThat(result).containsOnly(S3Action.GET_OBJECT, S3Action.GET_BUCKET_ACL, S3Action.GET_BUCKET_LOCATION,
        S3Action.GET_OBJECT_TAGGING);

    // Ensure it is case-insensitive
    final Set<S3Action> resultCi = mapPolicyActionsToS3Actions(Collections.singleton("s3:gET*"));
    assertThat(resultCi).containsOnly(S3Action.GET_OBJECT, S3Action.GET_BUCKET_ACL, S3Action.GET_BUCKET_LOCATION,
        S3Action.GET_OBJECT_TAGGING);
  }

  @Test
  public void testMapPolicyActionsToS3ActionsWithS3StarReturnsAll() {
    final Set<S3Action> result = mapPolicyActionsToS3Actions(Collections.singleton("s3:*"));
    assertThat(result).containsOnly(S3Action.ALL_S3);

    final Set<S3Action> resultCi = mapPolicyActionsToS3Actions(Collections.singleton("S3:*"));
    assertThat(resultCi).containsOnly(S3Action.ALL_S3);
  }

  @Test
  public void testMapPolicyActionsToS3ActionsIgnoresUnsupportedActions() {
    final Set<S3Action> result = mapPolicyActionsToS3Actions(strSet("s3:GetAccelerateConfiguration", "s3:GetObject"));
    // Unsupported action should be silently ignored
    assertThat(result).containsOnly(S3Action.GET_OBJECT);
  }

  @Test
  public void testMapPolicyActionsToS3ActionsWithOnlyUnsupportedActionsReturnsEmpty() {
    final Set<S3Action> result = mapPolicyActionsToS3Actions(
        strSet("s3:GetAccelerateConfiguration", "s3:PutBucketVersioning"));
    assertThat(result).isEmpty();
  }

  @Test
  public void testMapPolicyActionsToS3ActionsDeduplicatesResults() {
    final Set<S3Action> result = mapPolicyActionsToS3Actions(strSet("s3:Get*", "s3:GetObject", "s3:GetBucketAcl"));
    assertThat(result).containsOnly(S3Action.GET_OBJECT, S3Action.GET_BUCKET_ACL, S3Action.GET_BUCKET_LOCATION,
        S3Action.GET_OBJECT_TAGGING);
  }

  @Test
  public void testMapPolicyActionsToS3ActionsHandlesMultipleWildcards() {
    final Set<S3Action> result = mapPolicyActionsToS3Actions(strSet("s3:Get*", "s3:Put*"));
    assertThat(result).containsOnly(S3Action.GET_OBJECT, S3Action.GET_BUCKET_ACL, S3Action.GET_BUCKET_LOCATION,
        S3Action.GET_OBJECT_TAGGING, S3Action.PUT_OBJECT, S3Action.PUT_OBJECT_TAGGING, S3Action.PUT_BUCKET_ACL);
  }

  @Test
  public void testMapPolicyActionsToS3ActionsWithS3StarIgnoresOtherActions() {
    final Set<S3Action> result = mapPolicyActionsToS3Actions(strSet("s3:*", "s3:GetObject", "s3:PutObject"));
    // When s3:* is present, it should return only the ALL_S3 action
    assertThat(result).containsOnly(S3Action.ALL_S3);
  }

  private static Set<String> strSet(String... strs) {
    final Set<String> s = new LinkedHashSet<>();
    Collections.addAll(s, strs);
    return s;
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

  private static void expectResolveThrowsForBothAuthorizers(String json, String expectedMessage,
      OMException.ResultCodes expectedCode) {
    expectResolveThrows(json, NATIVE, expectedMessage, expectedCode);
    expectResolveThrows(json, RANGER, expectedMessage, expectedCode);
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

