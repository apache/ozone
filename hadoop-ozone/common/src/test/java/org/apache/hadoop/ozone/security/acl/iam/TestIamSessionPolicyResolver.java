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
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ_ACL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE_ACL;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.AuthorizerType.NATIVE;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.AuthorizerType.RANGER;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.S3ResourceType;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.buildCaseInsensitiveS3ActionMap;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.createPathsAndPermissions;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.mapPolicyActionsToS3Actions;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.validateAndCategorizeResources;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.AssumeRoleRequest;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
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
        json, "Invalid policy JSON - exceeds maximum length of 2048 characters", INVALID_REQUEST);
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

  @Test
  public void testValidateAndCategorizeResourcesWithWildcard() throws OMException {
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, Collections.singleton("*")),
        "Wildcard bucket patterns are not supported for Ozone native authorizer", NOT_SUPPORTED_OPERATION);

    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("*"));
    assertThat(resultRanger).containsOnly(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.ANY, "*", null, null));
  }

  @Test
  public void testValidateAndCategorizeResourcesWithSingleBucket() throws OMException {
    final IamSessionPolicyResolver.ResourceSpec expectedResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.BUCKET, "my-bucket", null, null);

    final Set<IamSessionPolicyResolver.ResourceSpec> resultNative = validateAndCategorizeResources(
        NATIVE, Collections.singleton("arn:aws:s3:::my-bucket"));
    assertThat(resultNative).containsOnly(expectedResourceSpec);

    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::my-bucket"));
    assertThat(resultRanger).containsOnly(expectedResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketWildcard() throws OMException {
    final IamSessionPolicyResolver.ResourceSpec expectedResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.BUCKET_WILDCARD, "my-bucket*", null, null);

    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, Collections.singleton("arn:aws:s3:::my-bucket*")),
        "Wildcard bucket patterns are not supported for Ozone native authorizer",
        NOT_SUPPORTED_OPERATION);

    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::my-bucket*"));
    assertThat(resultRanger).containsOnly(expectedResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketWildcardAndExactObjectKey() throws OMException {
    final IamSessionPolicyResolver.ResourceSpec expectedResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_EXACT, "*", null, "myKey.txt");

    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, Collections.singleton("arn:aws:s3:::*/myKey.txt")),
        "Wildcard bucket patterns are not supported for Ozone native authorizer",
        NOT_SUPPORTED_OPERATION);

    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::*/myKey.txt"));
    assertThat(resultRanger).containsOnly(expectedResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketWildcardAndObjectWildcard() throws OMException {
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, Collections.singleton("arn:aws:s3:::*/*")),
        "Wildcard bucket patterns are not supported for Ozone native authorizer",
        NOT_SUPPORTED_OPERATION);

    final IamSessionPolicyResolver.ResourceSpec expectedResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_PREFIX_WILDCARD, "*", "*", null);
    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::*/*"));
    assertThat(resultRanger).containsOnly(expectedResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketAndExactObjectKey() throws OMException {
    final IamSessionPolicyResolver.ResourceSpec expectedResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_EXACT, "bucket1", null, "key.txt");

    final Set<IamSessionPolicyResolver.ResourceSpec> resultNative = validateAndCategorizeResources(
        NATIVE, Collections.singleton("arn:aws:s3:::bucket1/key.txt"));
    assertThat(resultNative).containsOnly(expectedResourceSpec);

    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::bucket1/key.txt"));
    assertThat(resultRanger).containsOnly(expectedResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketAndExactObjectKeyWithPath() throws OMException {
    final IamSessionPolicyResolver.ResourceSpec expectedResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_EXACT, "bucket2", null, "path/folder/nested/key.txt");

    final Set<IamSessionPolicyResolver.ResourceSpec> resultNative = validateAndCategorizeResources(
        NATIVE, Collections.singleton("arn:aws:s3:::bucket2/path/folder/nested/key.txt"));
    assertThat(resultNative).containsOnly(expectedResourceSpec);

    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::bucket2/path/folder/nested/key.txt"));
    assertThat(resultRanger).containsOnly(expectedResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketAndObjectPrefixAndEmpty() throws OMException {
    final IamSessionPolicyResolver.ResourceSpec expectedNativeResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_PREFIX, "bucket3", "", null);
    final Set<IamSessionPolicyResolver.ResourceSpec> resultNative = validateAndCategorizeResources(
        NATIVE, Collections.singleton("arn:aws:s3:::bucket3/*"));
    assertThat(resultNative).containsOnly(expectedNativeResourceSpec);

    final IamSessionPolicyResolver.ResourceSpec expectedRangerResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket3", "*", null);
    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::bucket3/*"));
    assertThat(resultRanger).containsOnly(expectedRangerResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketAndObjectPrefixAndEmptyWithPath() throws OMException {
    final IamSessionPolicyResolver.ResourceSpec expectedNativeResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_PREFIX, "bucket3", "path/b/", null);
    final Set<IamSessionPolicyResolver.ResourceSpec> resultNative = validateAndCategorizeResources(
        NATIVE, Collections.singleton("arn:aws:s3:::bucket3/path/b/*"));
    assertThat(resultNative).containsOnly(expectedNativeResourceSpec);

    final IamSessionPolicyResolver.ResourceSpec expectedRangerResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket3", "path/b/*", null);
    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::bucket3/path/b/*"));
    assertThat(resultRanger).containsOnly(expectedRangerResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketAndObjectPrefixAndNonEmpty() throws OMException {
    final IamSessionPolicyResolver.ResourceSpec expectedNativeResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_PREFIX, "bucket3", "test", null);
    final Set<IamSessionPolicyResolver.ResourceSpec> resultNative = validateAndCategorizeResources(
        NATIVE, Collections.singleton("arn:aws:s3:::bucket3/test*"));
    assertThat(resultNative).containsOnly(expectedNativeResourceSpec);

    final IamSessionPolicyResolver.ResourceSpec expectedRangerResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket3", "test*", null);
    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::bucket3/test*"));
    assertThat(resultRanger).containsOnly(expectedRangerResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketAndObjectPrefixAndNonEmptyWithPath() throws OMException {
    final IamSessionPolicyResolver.ResourceSpec expectedNativeResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_PREFIX, "bucket", "a/b/test", null);
    final Set<IamSessionPolicyResolver.ResourceSpec> resultNative = validateAndCategorizeResources(
        NATIVE, Collections.singleton("arn:aws:s3:::bucket/a/b/test*"));
    assertThat(resultNative).containsOnly(expectedNativeResourceSpec);

    final IamSessionPolicyResolver.ResourceSpec expectedRangerResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket", "a/b/test*", null);
    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::bucket/a/b/test*"));
    assertThat(resultRanger).containsOnly(expectedRangerResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketAndObjectPrefixWildcardNotAtEnd() throws OMException {
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, Collections.singleton("arn:aws:s3:::bucket3/*.log")),
        "Wildcard prefix patterns are not supported for Ozone native authorizer if wildcard is not " +
        "at the end", NOT_SUPPORTED_OPERATION);

    final IamSessionPolicyResolver.ResourceSpec expectedRangerResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket3", "*.log", null);
    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::bucket3/*.log"));
    assertThat(resultRanger).containsOnly(expectedRangerResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketAndObjectPrefixWildcardNotAtEndWithPath() throws OMException {
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, Collections.singleton("arn:aws:s3:::bucket/a/q/*.ps")),
        "Wildcard prefix patterns are not supported for Ozone native authorizer if wildcard is not " +
        "at the end", NOT_SUPPORTED_OPERATION);

    final IamSessionPolicyResolver.ResourceSpec expectedRangerResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket", "a/q/*.ps", null);
    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::bucket/a/q/*.ps"));
    assertThat(resultRanger).containsOnly(expectedRangerResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketAndObjectPrefixWildcardOneAtEndAndOneNotAtEnd()
      throws OMException {
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, Collections.singleton("arn:aws:s3:::bucket3/*key*")),
        "Wildcard prefix patterns are not supported for Ozone native authorizer if wildcard is not " +
            "at the end", NOT_SUPPORTED_OPERATION);

    final IamSessionPolicyResolver.ResourceSpec expectedRangerResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket3", "*key*", null);
    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::bucket3/*key*"));
    assertThat(resultRanger).containsOnly(expectedRangerResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketAndObjectPrefixWildcardOneAtEndAndOneNotAtEndWithPath()
      throws OMException {
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, Collections.singleton("arn:aws:s3:::bucket3/a/b/t/*key*")),
        "Wildcard prefix patterns are not supported for Ozone native authorizer if wildcard is not " +
            "at the end", NOT_SUPPORTED_OPERATION);

    final IamSessionPolicyResolver.ResourceSpec expectedRangerResourceSpec = new IamSessionPolicyResolver.ResourceSpec(
        S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket3", "a/b/t/*key*", null);
    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::bucket3/a/b/t/*key*"));
    assertThat(resultRanger).containsOnly(expectedRangerResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithMultipleResources() throws OMException {
    final Set<IamSessionPolicyResolver.ResourceSpec> resultNative = validateAndCategorizeResources(
        NATIVE, strSet("arn:aws:s3:::bucket1", "arn:aws:s3:::bucket2/*", "arn:aws:s3:::bucket3/key.txt"));
    assertThat(resultNative).containsOnly(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET, "bucket1", null, null),
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_PREFIX, "bucket2", "", null),
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_EXACT, "bucket3", null, "key.txt"));

    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, strSet("arn:aws:s3:::bucket1", "arn:aws:s3:::bucket2/*", "arn:aws:s3:::bucket3/key.txt"));
    assertThat(resultRanger).containsOnly(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET, "bucket1", null, null),
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket2", "*", null),
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_EXACT, "bucket3", null, "key.txt"));
  }

  @Test
  public void testValidateAndCategorizeResourcesWithInvalidArnThrows() {
    final String invalidArn = "arn:aws:ec2:::bucket";
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, Collections.singleton(invalidArn)),
        "Unsupported Resource Arn - " + invalidArn, NOT_SUPPORTED_OPERATION);
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(RANGER, Collections.singleton(invalidArn)),
        "Unsupported Resource Arn - " + invalidArn, NOT_SUPPORTED_OPERATION);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithArnWithNoBucketThrows() {
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, Collections.singleton("arn:aws:s3:::")),
        "Invalid Resource Arn - arn:aws:s3:::", INVALID_REQUEST);
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(RANGER, Collections.singleton("arn:aws:s3:::")),
        "Invalid Resource Arn - arn:aws:s3:::", INVALID_REQUEST);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithNoResourcesThrows() {
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, Collections.emptySet()),
        "No Resource(s) found in policy", INVALID_REQUEST);
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(RANGER, Collections.emptySet()),
        "No Resource(s) found in policy", INVALID_REQUEST);
  }

  @Test
  public void testCreatePathsAndPermissionsWithResourceAny() {
    // This also tests that acls are deduplicated across different resource types
    final Set<S3Action> actions = Stream.of(S3Action.LIST_ALL_MY_BUCKETS, S3Action.LIST_BUCKET, S3Action.GET_OBJECT)
        .collect(Collectors.toSet()); // actions at volume, bucket and key levels
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.ANY, "*", null, null));

    expectIllegalArgumentException(
        () -> createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, Collections.emptySet()),
        "ResourceSpec type ANY not supported for OzoneNativeAuthorizer");

    final Set<AssumeRoleRequest.OzoneGrant> resultRanger = createPathsAndPermissions(
        VOLUME, RANGER, actions, resourceSpecs, Collections.emptySet());
    final Set<IOzoneObj> readAndListObjects = objSet(volume(), bucket("*")); // volume, bucket level have READ, LIST
    final Set<IOzoneObj> readObject = objSet(key("*", "*")); // key level has READ
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new AssumeRoleRequest.OzoneGrant(readAndListObjects, acls(READ, LIST)),
        new AssumeRoleRequest.OzoneGrant(readObject, acls(READ)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithBucketResource() {
    final Set<S3Action> actions = Collections.singleton(IamSessionPolicyResolver.S3Action.LIST_BUCKET);
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET, "bucket1", null, null));
    final Set<IOzoneObj> readAndListObject = objSet(bucket("bucket1"));
    final Set<IOzoneObj> readVolume = objSet(volume());

    final Set<AssumeRoleRequest.OzoneGrant> resultNative = createPathsAndPermissions(
        VOLUME, NATIVE, actions, resourceSpecs, Collections.emptySet());
    assertThat(resultNative).containsExactlyInAnyOrder(
        new AssumeRoleRequest.OzoneGrant(readAndListObject, acls(READ, LIST)),
        new AssumeRoleRequest.OzoneGrant(readVolume, acls(READ)));

    final Set<AssumeRoleRequest.OzoneGrant> resultRanger = createPathsAndPermissions(
        VOLUME, RANGER, actions, resourceSpecs, Collections.emptySet());
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new AssumeRoleRequest.OzoneGrant(readAndListObject, acls(READ, LIST)),
        new AssumeRoleRequest.OzoneGrant(readVolume, acls(READ)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithBucketWildcardResource() {
    final Set<S3Action> actions = Collections.singleton(IamSessionPolicyResolver.S3Action.PUT_BUCKET_ACL);
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET_WILDCARD, "bucket1*", null, null));
    final Set<IOzoneObj> writeAclObject = objSet(bucket("bucket1*"));
    final Set<IOzoneObj> readVolume = objSet(volume());

    expectIllegalArgumentException(
        () -> createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, Collections.emptySet()),
        "ResourceSpec type BUCKET_WILDCARD not supported for OzoneNativeAuthorizer");

    final Set<AssumeRoleRequest.OzoneGrant> resultRanger = createPathsAndPermissions(
        VOLUME, RANGER, actions, resourceSpecs, Collections.emptySet());
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new AssumeRoleRequest.OzoneGrant(writeAclObject, acls(WRITE_ACL)),
        new AssumeRoleRequest.OzoneGrant(readVolume, acls(READ)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithBucketsWildcardResourceAll() {
    // For AWS IAM, s3:ListAllMyBuckets supports both "*" and "arn:aws:s3:::*" as
    // Resource values.  The "*" case is covered by testCreatePathsAndPermissionsWithResourceAny.
    // This test ensures that "arn:aws:s3:::*" (parsed as BUCKET_WILDCARD with bucket="*")
    // also grants the expected volume-level permissions for ListAllMyBuckets.
    final Set<S3Action> actions = Stream.of(S3Action.LIST_ALL_MY_BUCKETS, S3Action.LIST_BUCKET)
        .collect(Collectors.toSet());
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET_WILDCARD, "*", null, null));

    expectIllegalArgumentException(
        () -> createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, Collections.emptySet()),
        "ResourceSpec type BUCKET_WILDCARD not supported for OzoneNativeAuthorizer");

    final Set<AssumeRoleRequest.OzoneGrant> resultRanger = createPathsAndPermissions(
        VOLUME, RANGER, actions, resourceSpecs, Collections.emptySet());

    // Both the volume and the wildcard bucket should end up with READ + LIST permissions.
    final Set<IOzoneObj> readAndListObjects = objSet(volume(), bucket("*"));
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new AssumeRoleRequest.OzoneGrant(readAndListObjects, acls(READ, LIST)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithObjectExactResource() {
    final Set<S3Action> actions = Collections.singleton(S3Action.GET_OBJECT);
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_EXACT, "bucket1", null, "key.txt"));
    final Set<IOzoneObj> readObjects = objSet(key("bucket1", "key.txt"), bucket("bucket1"), volume());

    final Set<AssumeRoleRequest.OzoneGrant> resultNative = createPathsAndPermissions(
        VOLUME, NATIVE, actions, resourceSpecs, Collections.emptySet());
    assertThat(resultNative).containsExactly(new AssumeRoleRequest.OzoneGrant(readObjects, acls(READ)));

    final Set<AssumeRoleRequest.OzoneGrant> resultRanger = createPathsAndPermissions(
        VOLUME, RANGER, actions, resourceSpecs, Collections.emptySet());
    assertThat(resultRanger).containsExactly(new AssumeRoleRequest.OzoneGrant(readObjects, acls(READ)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithObjectPrefixResource() {
    final Set<S3Action> actions = Collections.singleton(S3Action.GET_OBJECT);

    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_PREFIX, "bucket1", "prefix/", null));
    final Set<IOzoneObj> nativeReadObjects = objSet(prefix("bucket1", "prefix/"), bucket("bucket1"), volume());
    final Set<AssumeRoleRequest.OzoneGrant> resultNative = createPathsAndPermissions(
        VOLUME, NATIVE, actions, resourceSpecs, Collections.emptySet());
    assertThat(resultNative).containsExactly(new AssumeRoleRequest.OzoneGrant(nativeReadObjects, acls(READ)));

    expectIllegalArgumentException(
        () -> createPathsAndPermissions(VOLUME, RANGER, actions, resourceSpecs, Collections.emptySet()),
        "ResourceSpec type OBJECT_PREFIX not supported for RangerOzoneAuthorizer");
  }

  @Test
  public void testCreatePathsAndPermissionsWithObjectPrefixWildcardResource() {
    final Set<S3Action> actions = Collections.singleton(S3Action.GET_OBJECT);
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket1", "prefix/*", null));

    expectIllegalArgumentException(
        () -> createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, Collections.emptySet()),
        "ResourceSpec type OBJECT_PREFIX_WILDCARD not supported for OzoneNativeAuthorizer");

    final Set<IOzoneObj> rangerReadObjects = objSet(key("bucket1", "prefix/*"), bucket("bucket1"), volume());
    final Set<AssumeRoleRequest.OzoneGrant> resultRanger = createPathsAndPermissions(
        VOLUME, RANGER, actions, resourceSpecs, Collections.emptySet());
    assertThat(resultRanger).containsExactly(new AssumeRoleRequest.OzoneGrant(rangerReadObjects, acls(READ)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithConditionPrefixesForObjectActionMustIgnoreConditionPrefixes() {
    final Set<S3Action> actions = Collections.singleton(S3Action.GET_OBJECT);
    final Set<String> prefixes = strSet("folder1/", "folder2/");

    final Set<IamSessionPolicyResolver.ResourceSpec> nativeResourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_PREFIX, "bucket1", "", null));
    final Set<IOzoneObj> nativeReadObjects = objSet(prefix("bucket1", ""), bucket("bucket1"), volume());
    final Set<AssumeRoleRequest.OzoneGrant> resultNative = createPathsAndPermissions(
        VOLUME, NATIVE, actions, nativeResourceSpecs, prefixes);
    assertThat(resultNative).containsExactly(new AssumeRoleRequest.OzoneGrant(nativeReadObjects, acls(READ)));

    final Set<IamSessionPolicyResolver.ResourceSpec> rangerResourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket1", "*", null));
    final Set<IOzoneObj> rangerReadObjects = objSet(key("bucket1", "*"), bucket("bucket1"), volume());
    final Set<AssumeRoleRequest.OzoneGrant> resultRanger = createPathsAndPermissions(
        VOLUME, RANGER, actions, rangerResourceSpecs, prefixes);
    assertThat(resultRanger).containsExactly(new AssumeRoleRequest.OzoneGrant(rangerReadObjects, acls(READ)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithConditionPrefixesForBucketActionWhenActionIsListBucket() {
    final Set<S3Action> actions = Collections.singleton(S3Action.LIST_BUCKET);
    final Set<String> prefixes = strSet("folder1/", "folder2/");

    final Set<IamSessionPolicyResolver.ResourceSpec> nativeResourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET, "bucket1", null, null));
    final Set<IOzoneObj> nativeReadObjects = objSet(
        prefix("bucket1", "folder1/"), prefix("bucket1", "folder2/"), volume());
    final Set<IOzoneObj> nativeReadAndListObject = objSet(bucket("bucket1"));
    final Set<AssumeRoleRequest.OzoneGrant> resultNative = createPathsAndPermissions(
        VOLUME, NATIVE, actions, nativeResourceSpecs, prefixes);
    assertThat(resultNative).containsExactlyInAnyOrder(
        new AssumeRoleRequest.OzoneGrant(nativeReadObjects, acls(READ)),
        new AssumeRoleRequest.OzoneGrant(nativeReadAndListObject, acls(READ, LIST)));

    final Set<IamSessionPolicyResolver.ResourceSpec> rangerResourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET, "bucket1", null, null));
    final Set<IOzoneObj> rangerReadObjects = objSet(
        key("bucket1", "folder1/"), key("bucket1", "folder2/"), volume());
    final Set<IOzoneObj> rangerReadAndListObject = objSet(bucket("bucket1"));
    final Set<AssumeRoleRequest.OzoneGrant> resultRanger = createPathsAndPermissions(
        VOLUME, RANGER, actions, rangerResourceSpecs, prefixes);
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new AssumeRoleRequest.OzoneGrant(rangerReadObjects, acls(READ)),
        new AssumeRoleRequest.OzoneGrant(rangerReadAndListObject, acls(READ, LIST)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithConditionPrefixesForBucketActionWhenActionIsNotListBucket() {
    final Set<S3Action> actions = Collections.singleton(S3Action.GET_BUCKET_ACL);
    final Set<String> prefixes = strSet("folder1/", "folder2/");
    final Set<IOzoneObj> readObject = objSet(volume());
    final Set<IOzoneObj> readAndReadAclObject = objSet(bucket("bucket1"));

    final Set<IamSessionPolicyResolver.ResourceSpec> nativeResourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET, "bucket1", null, null));
    final Set<AssumeRoleRequest.OzoneGrant> resultNative = createPathsAndPermissions(
        VOLUME, NATIVE, actions, nativeResourceSpecs, prefixes);
    assertThat(resultNative).containsExactlyInAnyOrder(
        new AssumeRoleRequest.OzoneGrant(readObject, acls(READ)),
        new AssumeRoleRequest.OzoneGrant(readAndReadAclObject, acls(READ, READ_ACL)));

    final Set<IamSessionPolicyResolver.ResourceSpec> rangerResourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET, "bucket1", null, null));
    final Set<AssumeRoleRequest.OzoneGrant> resultRanger = createPathsAndPermissions(
        VOLUME, RANGER, actions, rangerResourceSpecs, prefixes);
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new AssumeRoleRequest.OzoneGrant(readObject, acls(READ)),
        new AssumeRoleRequest.OzoneGrant(readAndReadAclObject, acls(READ, READ_ACL)));
  }

  // TODO sts - add more createPathsAndPermissions tests in the next PR

  private static void expectIllegalArgumentException(Runnable runnable, String expectedMessage) {
    try {
      runnable.run();
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo(expectedMessage);
    }
  }

  private static void expectOMExceptionWithCode(RunnableThrowingOMException runnable, String expectedMessage,
      OMException.ResultCodes expectedCode) {
    try {
      runnable.run();
      throw new AssertionError("Expected exception not thrown");
    } catch (OMException ex) {
      assertThat(ex.getMessage()).isEqualTo(expectedMessage);
      assertThat(ex.getResult()).isEqualTo(expectedCode);
    }
  }

  @FunctionalInterface
  private interface RunnableThrowingOMException {
    void run() throws OMException;
  }

  private static IOzoneObj key(String bucket, String key) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(VOLUME)
        .setBucketName(bucket)
        .setKeyName(key)
        .build();
  }

  private static IOzoneObj volume() {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.VOLUME)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(VOLUME)
        .build();
  }

  private static IOzoneObj bucket(String bucket) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(VOLUME)
        .setBucketName(bucket)
        .build();
  }

  private static IOzoneObj prefix(String bucket, String prefix) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(VOLUME)
        .setBucketName(bucket)
        .setPrefixName(prefix)
        .build();
  }

  private static Set<IOzoneObj> objSet(IOzoneObj... objs) {
    final Set<IOzoneObj> s = new LinkedHashSet<>();
    Collections.addAll(s, objs);
    return s;
  }

  private static Set<ACLType> acls(ACLType... types) {
    final Set<ACLType> s = new LinkedHashSet<>();
    Collections.addAll(s, types);
    return s;
  }

  private static Set<String> strSet(String... strs) {
    final Set<String> s = new LinkedHashSet<>();
    Collections.addAll(s, strs);
    return s;
  }

  private static void expectResolveThrows(String json, IamSessionPolicyResolver.AuthorizerType authorizerType,
      String expectedMessage, OMException.ResultCodes expectedCode) {
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

