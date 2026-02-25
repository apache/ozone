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

import static java.util.Collections.emptySet;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.MALFORMED_POLICY_DOCUMENT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.OzoneGrant;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.DELETE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ_ACL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE_ACL;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.AuthorizerType.NATIVE;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.AuthorizerType.RANGER;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.S3ResourceType;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.buildCaseInsensitiveS3ActionMap;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.createPathsAndPermissions;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.groupObjectsByAcls;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.mapPolicyActionsToS3Actions;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.resolve;
import static org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver.validateAndCategorizeResources;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
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
        "    \"Condition\": { \"StringNotEqualsIgnoreCase\": { \"s3:prefix\": \"x/*\" } }\n" +
        "  }]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "IAM session policy: Unsupported Condition operator - StringNotEqualsIgnoreCase",
        NOT_SUPPORTED_OPERATION);
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
        json, "IAM session policy: Unsupported Condition key name - aws:SourceArn", NOT_SUPPORTED_OPERATION);
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
        json, "IAM session policy: Unsupported Effect - Deny", NOT_SUPPORTED_OPERATION);
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
        json, "IAM session policy: Invalid policy JSON - missing Statement", MALFORMED_POLICY_DOCUMENT);
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
        json, "IAM session policy: Invalid Effect in JSON policy (must be a String) - [\"Allow\"]",
        MALFORMED_POLICY_DOCUMENT);
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
        json, "IAM session policy: Effect is missing from JSON policy", MALFORMED_POLICY_DOCUMENT);
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
        json, "IAM session policy: Only one Condition is supported", NOT_SUPPORTED_OPERATION);
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
        json, "IAM session policy: Invalid Condition (must have operator StringEquals or StringLike and key name " +
        "s3:prefix) - [\"RandomCondition\"]", MALFORMED_POLICY_DOCUMENT);
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
        json, "IAM session policy: Missing Condition operator value for StringEquals",
        MALFORMED_POLICY_DOCUMENT);
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
        json, "IAM session policy: Invalid Condition operator value structure - [{\"s3:prefix\":\"folder/\"}]",
        MALFORMED_POLICY_DOCUMENT);
  }

  @Test
  public void testInvalidJsonThrows() {
    final String invalidJson = "{[{{}]\"\"";

    expectResolveThrowsForBothAuthorizers(
        invalidJson, "IAM session policy: Invalid policy JSON (most likely JSON structure is incorrect)",
        MALFORMED_POLICY_DOCUMENT);
  }

  @Test
  public void testJsonExceedsMaxLengthThrows() {
    final String json = createJsonStringLargerThan2048Characters();

    expectResolveThrowsForBothAuthorizers(
        json, "IAM session policy: Invalid policy JSON - exceeds maximum length of 2048 characters",
        MALFORMED_POLICY_DOCUMENT);
  }

  @Test
  public void testJsonAtMaxLengthSucceeds() throws OMException {
    // Create a JSON string that is exactly 2048 characters
    final String json = create2048CharJsonString();
    assertThat(json.length()).isEqualTo(2048);

    // Must not throw an exception
    resolve(json, VOLUME, NATIVE);
    resolve(json, VOLUME, RANGER);
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
    resolve(json, VOLUME, NATIVE);
    resolve(json, VOLUME, RANGER);
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
        json, "IAM session policy: Unsupported Effect - aLLOw", NOT_SUPPORTED_OPERATION);
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
    final Set<S3Action> result = mapPolicyActionsToS3Actions(emptySet());
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
        "IAM session policy: Wildcard bucket patterns are not supported for Ozone native authorizer",
        NOT_SUPPORTED_OPERATION);

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
        "IAM session policy: Wildcard bucket patterns are not supported for Ozone native authorizer",
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
        "IAM session policy: Wildcard bucket patterns are not supported for Ozone native authorizer",
        NOT_SUPPORTED_OPERATION);

    final Set<IamSessionPolicyResolver.ResourceSpec> resultRanger = validateAndCategorizeResources(
        RANGER, Collections.singleton("arn:aws:s3:::*/myKey.txt"));
    assertThat(resultRanger).containsOnly(expectedResourceSpec);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithBucketWildcardAndObjectWildcard() throws OMException {
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, Collections.singleton("arn:aws:s3:::*/*")),
        "IAM session policy: Wildcard bucket patterns are not supported for Ozone native authorizer",
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
        "IAM session policy: Wildcard prefix patterns are not supported for Ozone native authorizer " +
          "if wildcard is not at the end", NOT_SUPPORTED_OPERATION);

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
        "IAM session policy: Wildcard prefix patterns are not supported for Ozone native authorizer if " +
        "wildcard is not at the end", NOT_SUPPORTED_OPERATION);

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
        "IAM session policy: Wildcard prefix patterns are not supported for Ozone native authorizer " +
            "if wildcard is not at the end", NOT_SUPPORTED_OPERATION);

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
        "IAM session policy: Wildcard prefix patterns are not supported for Ozone native authorizer " +
            "if wildcard is not at the end", NOT_SUPPORTED_OPERATION);

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
        "IAM session policy: Unsupported Resource Arn - " + invalidArn, NOT_SUPPORTED_OPERATION);
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(RANGER, Collections.singleton(invalidArn)),
        "IAM session policy: Unsupported Resource Arn - " + invalidArn, NOT_SUPPORTED_OPERATION);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithArnWithNoBucketThrows() {
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, Collections.singleton("arn:aws:s3:::")),
        "IAM session policy: Invalid Resource Arn - arn:aws:s3:::", MALFORMED_POLICY_DOCUMENT);
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(RANGER, Collections.singleton("arn:aws:s3:::")),
        "IAM session policy: Invalid Resource Arn - arn:aws:s3:::", MALFORMED_POLICY_DOCUMENT);
  }

  @Test
  public void testValidateAndCategorizeResourcesWithNoResourcesThrows() {
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(NATIVE, emptySet()), "IAM session policy: No Resource(s) found in policy",
        MALFORMED_POLICY_DOCUMENT);
    expectOMExceptionWithCode(
        () -> validateAndCategorizeResources(RANGER, emptySet()), "IAM session policy: No Resource(s) found in policy",
        MALFORMED_POLICY_DOCUMENT);
  }

  @Test
  public void testCreatePathsAndPermissionsWithResourceAny() {
    // This also tests that acls are deduplicated across different resource types
    final Set<S3Action> actions = Stream.of(S3Action.LIST_ALL_MY_BUCKETS, S3Action.LIST_BUCKET, S3Action.GET_OBJECT)
        .collect(Collectors.toSet()); // actions at volume, bucket and key levels
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.ANY, "*", null, null));

    expectIllegalArgumentException(
        () -> createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, emptySet(), new LinkedHashMap<>()),
        "ResourceSpec type ANY not supported for OzoneNativeAuthorizer");

    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, RANGER, actions, resourceSpecs, emptySet(), objToAclsMapRanger);
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    final Set<IOzoneObj> readAndListObjects = objSet(volume(), bucket("*")); // volume, bucket level have READ, LIST
    final Set<IOzoneObj> readObject = objSet(key("*", "*")); // key level has READ
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new OzoneGrant(readAndListObjects, acls(READ, LIST)),
        new OzoneGrant(readObject, acls(READ)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithBucketResourceThatIsListBucket() {
    final Set<S3Action> actions = Collections.singleton(IamSessionPolicyResolver.S3Action.LIST_BUCKET);
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET, "bucket1", null, null));
    final Set<IOzoneObj> readAndListObject = objSet(bucket("bucket1"));

    final Map<IOzoneObj, Set<ACLType>> objToAclsMapNative = new LinkedHashMap<>();
    final Set<IOzoneObj> nativeReadObjects = objSet(volume(), prefix("bucket1", ""));
    createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, emptySet(), objToAclsMapNative);
    final Set<OzoneGrant> resultNative = groupObjectsByAcls(objToAclsMapNative);
    assertThat(resultNative).containsExactlyInAnyOrder(
        new OzoneGrant(readAndListObject, acls(READ, LIST)), new OzoneGrant(nativeReadObjects, acls(READ)));

    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    final Set<IOzoneObj> rangerReadObjects = objSet(volume(), key("bucket1", "*"));
    createPathsAndPermissions(VOLUME, RANGER, actions, resourceSpecs, emptySet(), objToAclsMapRanger);
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new OzoneGrant(readAndListObject, acls(READ, LIST)), new OzoneGrant(rangerReadObjects, acls(READ)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithBucketResourceThatIsNotListBucket() {
    final Set<S3Action> actions = Collections.singleton(S3Action.CREATE_BUCKET);
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET, "bucket1", null, null));
    final Set<IOzoneObj> createObject = objSet(bucket("bucket1"));
    final Set<IOzoneObj> readObject = objSet(volume());

    final Map<IOzoneObj, Set<ACLType>> objToAclsMapNative = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, emptySet(), objToAclsMapNative);
    final Set<OzoneGrant> resultNative = groupObjectsByAcls(objToAclsMapNative);
    assertThat(resultNative).containsExactlyInAnyOrder(
        new OzoneGrant(createObject, acls(CREATE)), new OzoneGrant(readObject, acls(READ)));

    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, RANGER, actions, resourceSpecs, emptySet(), objToAclsMapRanger);
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new OzoneGrant(createObject, acls(CREATE)), new OzoneGrant(readObject, acls(READ)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithBucketWildcardResource() {
    final Set<S3Action> actions = Collections.singleton(IamSessionPolicyResolver.S3Action.PUT_BUCKET_ACL);
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET_WILDCARD, "bucket1*", null, null));
    final Set<IOzoneObj> writeAclObject = objSet(bucket("bucket1*"));
    final Set<IOzoneObj> readVolume = objSet(volume());

    expectIllegalArgumentException(
        () -> createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, emptySet(), new LinkedHashMap<>()),
        "ResourceSpec type BUCKET_WILDCARD not supported for OzoneNativeAuthorizer");

    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, RANGER, actions, resourceSpecs, emptySet(), objToAclsMapRanger);
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new OzoneGrant(writeAclObject, acls(WRITE_ACL)), new OzoneGrant(readVolume, acls(READ)));
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
        () -> createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, emptySet(), new LinkedHashMap<>()),
        "ResourceSpec type BUCKET_WILDCARD not supported for OzoneNativeAuthorizer");

    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, RANGER, actions, resourceSpecs, emptySet(), objToAclsMapRanger);

    // Both the volume and the wildcard bucket should end up with READ + LIST permissions.
    // We also need READ access on the keys
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    final Set<IOzoneObj> readAndListObjects = objSet(volume(), bucket("*"));
    final Set<IOzoneObj> readObjects = objSet(key("*", "*"));
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new OzoneGrant(readAndListObjects, acls(READ, LIST)), new OzoneGrant(readObjects, acls(READ)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithObjectExactResource() {
    final Set<S3Action> actions = Collections.singleton(S3Action.GET_OBJECT);
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_EXACT, "bucket1", null, "key.txt"));
    final Set<IOzoneObj> readObjects = objSet(key("bucket1", "key.txt"), bucket("bucket1"), volume());

    final Map<IOzoneObj, Set<ACLType>> objToAclsMapNative = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, emptySet(), objToAclsMapNative);
    final Set<OzoneGrant> resultNative = groupObjectsByAcls(objToAclsMapNative);
    assertThat(resultNative).containsExactly(new OzoneGrant(readObjects, acls(READ)));

    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, RANGER, actions, resourceSpecs, emptySet(), objToAclsMapRanger);
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    assertThat(resultRanger).containsExactly(new OzoneGrant(readObjects, acls(READ)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithObjectPrefixResource() {
    final Set<S3Action> actions = Collections.singleton(S3Action.GET_OBJECT);

    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_PREFIX, "bucket1", "prefix/", null));
    final Set<IOzoneObj> nativeReadObjects = objSet(prefix("bucket1", "prefix/"), bucket("bucket1"), volume());
    final Map<IOzoneObj, Set<ACLType>> objToAclsMapNative = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, emptySet(), objToAclsMapNative);
    final Set<OzoneGrant> resultNative = groupObjectsByAcls(objToAclsMapNative);
    assertThat(resultNative).containsExactly(new OzoneGrant(nativeReadObjects, acls(READ)));

    expectIllegalArgumentException(
        () -> createPathsAndPermissions(VOLUME, RANGER, actions, resourceSpecs, emptySet(), new LinkedHashMap<>()),
        "ResourceSpec type OBJECT_PREFIX not supported for RangerOzoneAuthorizer");
  }

  @Test
  public void testCreatePathsAndPermissionsWithObjectPrefixWildcardResource() {
    final Set<S3Action> actions = Collections.singleton(S3Action.GET_OBJECT);
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket1", "prefix/*", null));

    expectIllegalArgumentException(
        () -> createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, emptySet(), new LinkedHashMap<>()),
        "ResourceSpec type OBJECT_PREFIX_WILDCARD not supported for OzoneNativeAuthorizer");

    final Set<IOzoneObj> rangerReadObjects = objSet(key("bucket1", "prefix/*"), bucket("bucket1"), volume());
    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, RANGER, actions, resourceSpecs, emptySet(), objToAclsMapRanger);
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    assertThat(resultRanger).containsExactly(new OzoneGrant(rangerReadObjects, acls(READ)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithConditionPrefixesForObjectActionMustIgnoreConditionPrefixes() {
    final Set<S3Action> actions = Collections.singleton(S3Action.GET_OBJECT);
    final Set<String> prefixes = strSet("folder1/", "folder2/");

    final Set<IamSessionPolicyResolver.ResourceSpec> nativeResourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_PREFIX, "bucket1", "", null));
    final Map<IOzoneObj, Set<ACLType>> objToAclsMapNative = new LinkedHashMap<>();
    final Set<IOzoneObj> nativeReadObjects = objSet(prefix("bucket1", ""), bucket("bucket1"), volume());
    createPathsAndPermissions(VOLUME, NATIVE, actions, nativeResourceSpecs, prefixes, objToAclsMapNative);
    final Set<OzoneGrant> resultNative = groupObjectsByAcls(objToAclsMapNative);
    assertThat(resultNative).containsExactly(new OzoneGrant(nativeReadObjects, acls(READ)));

    final Set<IamSessionPolicyResolver.ResourceSpec> rangerResourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket1", "*", null));
    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    final Set<IOzoneObj> rangerReadObjects = objSet(key("bucket1", "*"), bucket("bucket1"), volume());
    createPathsAndPermissions(VOLUME, RANGER, actions, rangerResourceSpecs, prefixes, objToAclsMapRanger);
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    assertThat(resultRanger).containsExactly(new OzoneGrant(rangerReadObjects, acls(READ)));
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
    final Map<IOzoneObj, Set<ACLType>> objToAclsMapNative = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, NATIVE, actions, nativeResourceSpecs, prefixes, objToAclsMapNative);
    final Set<OzoneGrant> resultNative = groupObjectsByAcls(objToAclsMapNative);
    assertThat(resultNative).containsExactlyInAnyOrder(
        new OzoneGrant(nativeReadObjects, acls(READ)), new OzoneGrant(nativeReadAndListObject, acls(READ, LIST)));

    final Set<IamSessionPolicyResolver.ResourceSpec> rangerResourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET, "bucket1", null, null));
    final Set<IOzoneObj> rangerReadObjects = objSet(
        key("bucket1", "folder1/"), key("bucket1", "folder2/"), volume());
    final Set<IOzoneObj> rangerReadAndListObject = objSet(bucket("bucket1"));
    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, RANGER, actions, rangerResourceSpecs, prefixes, objToAclsMapRanger);
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new OzoneGrant(rangerReadObjects, acls(READ)), new OzoneGrant(rangerReadAndListObject, acls(READ, LIST)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithConditionPrefixesForBucketActionWhenActionIsNotListBucket() {
    final Set<S3Action> actions = Collections.singleton(S3Action.GET_BUCKET_ACL);
    final Set<String> prefixes = strSet("folder1/", "folder2/");
    final Set<IOzoneObj> readObject = objSet(volume());
    final Set<IOzoneObj> readAndReadAclObject = objSet(bucket("bucket1"));

    final Set<IamSessionPolicyResolver.ResourceSpec> nativeResourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET, "bucket1", null, null));
    final Map<IOzoneObj, Set<ACLType>> objToAclsMapNative = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, NATIVE, actions, nativeResourceSpecs, prefixes, objToAclsMapNative);
    final Set<OzoneGrant> resultNative = groupObjectsByAcls(objToAclsMapNative);
    assertThat(resultNative).containsExactlyInAnyOrder(
        new OzoneGrant(readObject, acls(READ)), new OzoneGrant(readAndReadAclObject, acls(READ, READ_ACL)));

    final Set<IamSessionPolicyResolver.ResourceSpec> rangerResourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET, "bucket1", null, null));
    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, RANGER, actions, rangerResourceSpecs, prefixes, objToAclsMapRanger);
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new OzoneGrant(readObject, acls(READ)), new OzoneGrant(readAndReadAclObject, acls(READ, READ_ACL)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithNoMappedActions() {
    final Set<S3Action> actions = emptySet();

    final Set<IamSessionPolicyResolver.ResourceSpec> nativeResourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_PREFIX, "bucket1", null, null));
    final Map<IOzoneObj, Set<ACLType>> objToAclsMapNative = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, NATIVE, actions, nativeResourceSpecs, emptySet(), objToAclsMapNative);
    final Set<OzoneGrant> resultNative = groupObjectsByAcls(objToAclsMapNative);
    assertThat(resultNative).isEmpty();

    final Set<IamSessionPolicyResolver.ResourceSpec> rangerResourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_PREFIX_WILDCARD, "bucket1", null, null));
    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, RANGER, actions, rangerResourceSpecs, emptySet(), objToAclsMapRanger);
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    assertThat(resultRanger).isEmpty();
  }

  @Test
  public void testCreatePathsAndPermissionsWithNoMappedResources() {
    final Set<S3Action> actions = Collections.singleton(S3Action.GET_OBJECT);
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = emptySet();

    final Map<IOzoneObj, Set<ACLType>> objToAclsMapNative = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, emptySet(), objToAclsMapNative);
    final Set<OzoneGrant> resultNative = groupObjectsByAcls(objToAclsMapNative);
    assertThat(resultNative).isEmpty();

    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, RANGER, actions, resourceSpecs, emptySet(), objToAclsMapRanger);
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    assertThat(resultRanger).isEmpty();
  }

  @Test
  public void testCreatePathsAndPermissionsDeduplicatesAcrossSameResourceTypes() {
    final Set<S3Action> actions = Stream.of(
        S3Action.GET_OBJECT, S3Action.GET_OBJECT_TAGGING, S3Action.DELETE_OBJECT, S3Action.DELETE_OBJECT_TAGGING)
        .collect(Collectors.toSet());
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Collections.singleton(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_EXACT, "bucket1", null, "key.txt"));
    final Set<IOzoneObj> readAndDeleteObject = objSet(key("bucket1", "key.txt"));
    final Set<IOzoneObj> readObjects = objSet(bucket("bucket1"), volume());

    final Map<IOzoneObj, Set<ACLType>> objToAclsMapNative = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, emptySet(), objToAclsMapNative);
    final Set<OzoneGrant> resultNative = groupObjectsByAcls(objToAclsMapNative);
    assertThat(resultNative).containsExactlyInAnyOrder(
        new OzoneGrant(readAndDeleteObject, acls(READ, DELETE)), new OzoneGrant(readObjects, acls(READ)));

    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, RANGER, actions, resourceSpecs, emptySet(), objToAclsMapRanger);
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new OzoneGrant(readAndDeleteObject, acls(READ, DELETE)), new OzoneGrant(readObjects, acls(READ)));
  }

  @Test
  public void testCreatePathsAndPermissionsWithAllS3ActionsOverridesAnyOtherAction() {
    final Set<S3Action> actions = Stream.of(
        S3Action.ALL_S3, S3Action.GET_OBJECT, S3Action.DELETE_OBJECT, S3Action.LIST_BUCKET)
        .collect(Collectors.toSet());
    final Set<IamSessionPolicyResolver.ResourceSpec> resourceSpecs = Stream.of(
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.OBJECT_EXACT, "bucket1", null, "key.txt"),
        new IamSessionPolicyResolver.ResourceSpec(S3ResourceType.BUCKET, "bucket2", null, null))
        .collect(Collectors.toSet());
    final Set<IOzoneObj> allObjects = objSet(key("bucket1", "key.txt"), bucket("bucket2"));

    final Set<IOzoneObj> nativeReadObjects = objSet(volume(), bucket("bucket1"), prefix("bucket2", ""));
    final Map<IOzoneObj, Set<ACLType>> objToAclsMapNative = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, NATIVE, actions, resourceSpecs, emptySet(), objToAclsMapNative);
    final Set<OzoneGrant> resultNative = groupObjectsByAcls(objToAclsMapNative);
    assertThat(resultNative).containsExactlyInAnyOrder(
        new OzoneGrant(allObjects, acls(ALL)), new OzoneGrant(nativeReadObjects, acls(READ)));

    final Set<IOzoneObj> rangerReadObjects = objSet(volume(), bucket("bucket1"), key("bucket2", "*"));
    final Map<IOzoneObj, Set<ACLType>> objToAclsMapRanger = new LinkedHashMap<>();
    createPathsAndPermissions(VOLUME, RANGER, actions, resourceSpecs, emptySet(), objToAclsMapRanger);
    final Set<OzoneGrant> resultRanger = groupObjectsByAcls(objToAclsMapRanger);
    assertThat(resultRanger).containsExactlyInAnyOrder(
        new OzoneGrant(allObjects, acls(ALL)), new OzoneGrant(rangerReadObjects, acls(READ)));
  }

  @Test
  public void testDeduplicatesAcrossMultipleStatementsWhenSameStatementsArePresent() throws OMException {
    final String json = "{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:GetBucketAcl\",\n" +
        "        \"s3:PutBucketAcl\",\n" +
        "        \"s3:ListBucket\"\n" +
        "      ],\n" +
        "      \"Resource\": \"arn:aws:s3:::my-bucket\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:GetBucketAcl\",\n" +
        "        \"s3:PutBucketAcl\",\n" +
        "        \"s3:ListBucket\"\n" +
        "      ],\n" +
        "      \"Resource\": \"arn:aws:s3:::my-bucket\"\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: bucket READ, LIST, READ_ACL, WRITE_ACL; volume and prefix "" READ
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));
    final Set<ACLType> bucketAcls = acls(READ, LIST, READ_ACL, WRITE_ACL);
    expectedResolvedNative.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedNative.add(new OzoneGrant(objSet(volume(), prefix("my-bucket", "")), acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: bucket READ, LIST, READ_ACL, WRITE_ACL; volume and key "*"  READ
    expectedResolvedRanger.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume(), key("my-bucket", "*")), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testDeduplicatesAcrossMultipleStatementsForSameActionsButDifferentResource() throws OMException {
    final String json = "{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:GetBucketAcl\",\n" +
        "        \"s3:PutBucketAcl\",\n" +
        "        \"s3:ListBucket\"\n" +
        "      ],\n" +
        "      \"Resource\": \"arn:aws:s3:::my-bucket\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:GetBucketAcl\",\n" +
        "        \"s3:PutBucketAcl\",\n" +
        "        \"s3:ListBucket\"\n" +
        "      ],\n" +
        "      \"Resource\": \"arn:aws:s3:::my-bucket2\"\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: bucket READ, LIST, READ_ACL, WRITE_ACL; volume and prefix "" READ
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"), bucket("my-bucket2"));
    final Set<ACLType> bucketAcls = acls(READ, LIST, READ_ACL, WRITE_ACL);
    expectedResolvedNative.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedNative.add(new OzoneGrant(
        objSet(volume(), prefix("my-bucket2", ""), prefix("my-bucket", "")), acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: bucket READ, LIST, READ_ACL, WRITE_ACL; volume and key "*" READ
    expectedResolvedRanger.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedRanger.add(new OzoneGrant(
        objSet(volume(), key("my-bucket2", "*"), key("my-bucket", "*")), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testDeduplicatesAcrossMultipleStatementsForDifferentActionsButSameResource() throws OMException {
    final String json = "{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:GetBucketAcl\",\n" +
        "        \"s3:PutBucketAcl\",\n" +
        "        \"s3:ListBucket\"\n" +
        "      ],\n" +
        "      \"Resource\": \"arn:aws:s3:::my-bucket\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:GetBucketAcl\",\n" +
        "        \"s3:CreateBucket\"\n" +
        "      ],\n" +
        "      \"Resource\": \"arn:aws:s3:::my-bucket\"\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: bucket READ, LIST, READ_ACL, WRITE_ACL, CREATE; volume, prefix "" READ
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));
    final Set<ACLType> bucketAcls = acls(READ, LIST, READ_ACL, WRITE_ACL, CREATE);
    expectedResolvedNative.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedNative.add(new OzoneGrant(objSet(volume(), prefix("my-bucket", "")), acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: bucket READ, LIST, READ_ACL, WRITE_ACL, CREATE; volume, key "*" READ
    expectedResolvedRanger.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume(), key("my-bucket", "*")), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testDeduplicatesAcrossMultipleStatementsWhenAllActionPresent() throws OMException {
    final String json = "{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:GetBucketAcl\",\n" +
        "        \"s3:PutBucketAcl\",\n" +
        "        \"s3:ListBucket\"\n" +
        "      ],\n" +
        "      \"Resource\": \"arn:aws:s3:::my-bucket\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": \"s3:*\",\n" +
        "      \"Resource\": \"arn:aws:s3:::my-bucket\"\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: bucket ALL (instead of individual actions); volume and prefix "" READ
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));
    final Set<ACLType> bucketAcls = acls(ALL);
    expectedResolvedNative.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedNative.add(new OzoneGrant(objSet(volume(), prefix("my-bucket", "")), acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: bucket ALL (instead of individual actions); volume and key "*" READ
    expectedResolvedRanger.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume(), key("my-bucket", "*")), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testAllowGetPutOnKey() throws OMException {
    final String json = "{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": [\"s3:GetObject\", \"s3:PutObject\"],\n" +
        "    \"Resource\": \"arn:aws:s3:::my-bucket/folder/file.txt\"\n" +
        "  }]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedFromBothAuthorizers = new LinkedHashSet<>();
    // Expected: READ, CREATE, WRITE on key; bucket READ; volume READ
    final Set<IOzoneObj> keySet = objSet(key("my-bucket", "folder/file.txt"));
    final Set<ACLType> keyAcls = acls(READ, CREATE, WRITE);
    expectedResolvedFromBothAuthorizers.add(new OzoneGrant(objSet(volume(), bucket("my-bucket")), acls(READ)));
    expectedResolvedFromBothAuthorizers.add(new OzoneGrant(keySet, keyAcls));

    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedFromBothAuthorizers);
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedFromBothAuthorizers);
  }

  @Test
  public void testAllActionsForKey() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:*\",\n" +
        "    \"Resource\": \"arn:aws:s3:::my-bucket/*\"\n" +
        "  }]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: all key ACLs on prefix "" under bucket; bucket READ, volume READ
    final Set<IOzoneObj> keyPrefixSet = objSet(prefix("my-bucket", ""));
    final Set<ACLType> allKeyAcls = acls(ALL);
    expectedResolvedNative.add(new OzoneGrant(keyPrefixSet, allKeyAcls));
    expectedResolvedNative.add(new OzoneGrant(objSet(volume(), bucket("my-bucket")), acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    // Expected for Ranger: all key acls for resource type KEY with key name "*"
    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    final Set<IOzoneObj> rangerKeySet = objSet(key("my-bucket", "*"));
    expectedResolvedRanger.add(new OzoneGrant(rangerKeySet, allKeyAcls));
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume(), bucket("my-bucket")), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testAllActionsForBucket() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:*\",\n" +
        "    \"Resource\": \"arn:aws:s3:::my-bucket\"\n" +
        "  }]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: all Bucket ACLs for bucket; volume, prefix "" READ
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));
    final Set<ACLType> allBucketAcls = acls(ALL);
    expectedResolvedNative.add(new OzoneGrant(objSet(volume(), prefix("my-bucket", "")), acls(READ)));
    expectedResolvedNative.add(new OzoneGrant(bucketSet, allBucketAcls));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    // Expected for Ranger: all Bucket ACLs for bucket; volume, key "*" READ
    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume(), key("my-bucket", "*")), acls(READ)));
    expectedResolvedRanger.add(new OzoneGrant(bucketSet, allBucketAcls));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testMultipleResourcesInSeparateStatements() throws OMException {
    final String json = "{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:GetBucketAcl\",\n" +
        "        \"s3:PutBucketAcl\",\n" +
        "        \"s3:ListBucket\"\n" +
        "      ],\n" +
        "      \"Resource\": \"arn:aws:s3:::my-bucket\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": \"s3:*\",\n" +
        "      \"Resource\": \"arn:aws:s3:::my-bucket/*\"\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: bucket READ, LIST, READ_ACL, WRITE_ACL; volume READ
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));
    final Set<ACLType> bucketAcls = acls(READ, LIST, READ_ACL, WRITE_ACL);
    expectedResolvedNative.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedNative.add(new OzoneGrant(objSet(volume()), acls(READ)));
    // Expected for native: all key ACLs on prefix "" under bucket
    final Set<IOzoneObj> keyPrefixSet = objSet(prefix("my-bucket", ""));
    final Set<ACLType> keyAllAcls = acls(ALL);
    expectedResolvedNative.add(new OzoneGrant(keyPrefixSet, keyAllAcls));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: bucket READ, LIST, READ_ACL, WRITE_ACL; volume READ
    expectedResolvedRanger.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume()), acls(READ)));
    // Expected for Ranger: all key acls for resource type KEY with key name "*"
    final Set<IOzoneObj> rangerKeySet = objSet(key("my-bucket", "*"));
    expectedResolvedRanger.add(new OzoneGrant(rangerKeySet, keyAllAcls));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testMultipleResourcesInOneStatement() throws OMException {
    final String json = "{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:*\"\n" +
        "      ],\n" +
        "      \"Resource\": [\n" +
        "        \"arn:aws:s3:::my-bucket\",\n" +
        "        \"arn:aws:s3:::my-bucket/*\"\n" +
        "      ]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: all for bucket and key acls; volume READ
    final Set<IOzoneObj> resourceSetNative = objSet(bucket("my-bucket"), prefix("my-bucket", ""));
    expectedResolvedNative.add(new OzoneGrant(resourceSetNative, acls(ALL)));
    expectedResolvedNative.add(new OzoneGrant(objSet(volume()), acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: all for bucket and key acls; volume READ
    final Set<IOzoneObj> resourceSetRanger = objSet(bucket("my-bucket"), key("my-bucket", "*"));
    expectedResolvedRanger.add(new OzoneGrant(resourceSetRanger, acls(ALL)));
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume()), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testMultipleResourcesWithDifferentBucketsAndDeepPathsInOneStatement() throws OMException {
    final String json = "{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:*\"\n" +
        "      ],\n" +
        "      \"Resource\": [\n" +
        "        \"arn:aws:s3:::my-bucket/team/folder1/security/*\",\n" +
        "        \"arn:aws:s3:::my-bucket2/team/folder2/misc/*\"\n" +
        "      ]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: all key ACLs on prefix "team/folder1/security/" under
    // my-bucket and all key ACLs on prefix "team/folder2/misc/" under my-bucket2; bucket READ; volume READ
    final Set<IOzoneObj> keyPrefixSet = objSet(
        prefix("my-bucket", "team/folder1/security/"), prefix("my-bucket2", "team/folder2/misc/"));
    final Set<ACLType> keyAllAcls = acls(ALL);
    expectedResolvedNative.add(new OzoneGrant(keyPrefixSet, keyAllAcls));
    expectedResolvedNative.add(new OzoneGrant(objSet(volume(), bucket("my-bucket"), bucket("my-bucket2")), acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: all key acls for resource type KEY with key name
    // "team/folder1/security/*" under my-bucket and "team/folder2/misc/*" under my-bucket2; bucket READ; volume READ
    final Set<IOzoneObj> rangerKeySet = objSet(
        key("my-bucket", "team/folder1/security/*"), key("my-bucket2", "team/folder2/misc/*"));
    expectedResolvedRanger.add(new OzoneGrant(rangerKeySet, keyAllAcls));
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume(), bucket("my-bucket"), bucket("my-bucket2")), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testUnsupportedActionIgnoredWhenItIsTheOnlyAction() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:ReplicateObject\",\n" +         // unsupported action
        "    \"Resource\": \"*\"\n" +
        "  }]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    assertThat(resolvedFromNativeAuthorizer).isEmpty();
    assertThat(resolvedFromRangerAuthorizer).isEmpty();
  }

  @Test
  public void testUnsupportedResourceArnThrows() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:dynamodb:us-east-2:123456789012:table/example-table\"\n" +
        "  }]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "IAM session policy: Unsupported Resource Arn - " +
        "arn:aws:dynamodb:us-east-2:123456789012:table/example-table", NOT_SUPPORTED_OPERATION);
  }

  @Test
  public void testListBucketWithWildcard() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::proj-*\"\n" +
        "  }]\n" +
        "}";

    // Wildcards on bucket are not supported for Native authorizer
    expectBucketWildcardUnsupportedExceptionForNativeAuthorizer(json);

    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);
    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: bucket READ and LIST on wildcard pattern; volume and key "*" READ
    final Set<IOzoneObj> bucketSet = objSet(bucket("proj-*"));
    final Set<ACLType> bucketAcls = acls(READ, LIST);
    expectedResolvedRanger.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume(), key("proj-*", "*")), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testListBucketOperationsWithNoPrefixes() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:ListBucket\",\n" +
        "        \"s3:ListBucketMultipartUploads\"\n" +
        "      ],\n" +
        "      \"Resource\": \"arn:aws:s3:::proj\"\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: bucket READ and LIST; volume, prefix "" READ
    final Set<IOzoneObj> bucketSet = objSet(bucket("proj"));
    final Set<ACLType> bucketAcls = acls(READ, LIST);
    final Set<IOzoneObj> nativeReadObjects = objSet(volume(), prefix("proj", ""));
    expectedResolvedNative.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedNative.add(new OzoneGrant(nativeReadObjects, acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    // Expected for Ranger: bucket READ and LIST; volume, key "*" READ
    final Set<IOzoneObj> rangerReadObjects = objSet(volume(), key("proj", "*"));
    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    expectedResolvedRanger.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedRanger.add(new OzoneGrant(rangerReadObjects, acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testIgnoresUnsupportedActionsWhenSupportedActionsAreIncluded() throws OMException {
    final String json = "{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Sid\": \"AllowListingOfDataLakeFolder\",\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:GetAccelerateConfiguration\",\n" +    // unsupported action
        "        \"s3:GetBucketAcl\",\n" +
        "        \"s3:GetObject\",\n" +                     // object-level action not applied for bucket
        "        \"s3:GetObjectAcl\",\n" +                  // unsupported action
        "        \"s3:ListBucket\",\n" +
        "        \"s3:ListBucketMultipartUploads\"\n" +
        "      ],\n" +
        "      \"Resource\": \"arn:aws:s3:::bucket1\",\n" +
        "      \"Condition\": {\n" +
        "        \"StringEquals\": {\n" +
        "          \"s3:prefix\": [ \"team/folder\", \"team/folder/*\" ]\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();

    // Expected for native: READ, LIST, READ_ACL bucket acls; volume and prefixes "team/folder", "team/folder/" READ
    final Set<IOzoneObj> bucketSet = objSet(bucket("bucket1"));
    final Set<ACLType> bucketAcls = acls(READ, LIST, READ_ACL);
    expectedResolvedNative.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedNative.add(new OzoneGrant(
        objSet(volume(), prefix("bucket1", "team/folder"), prefix("bucket1", "team/folder/")), acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: READ, LIST, READ_ACL bucket acls; volume and keys "team/folder" and "team/folder/*" READ
    expectedResolvedRanger.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedRanger.add(new OzoneGrant(
        objSet(volume(), key("bucket1", "team/folder"), key("bucket1", "team/folder/*")), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testMultiplePrefixesWithWildcards() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:GetObject\",\n" +
        "    \"Resource\": \"arn:aws:s3:::logs/*\",\n" +
        "    \"Condition\": { \"StringEquals\": { \"s3:prefix\": [\"a/*\", \"b/*\"] } }\n" +
        "  }]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: READ acl on prefix "" (condition prefixes are ignored); bucket READ; volume READ;
    final Set<IOzoneObj> readObjectsNative = objSet(prefix("logs", ""), bucket("logs"), volume());
    expectedResolvedNative.add(new OzoneGrant(readObjectsNative, acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: READ acl on key "*" (condition prefixes are ignored)
    final Set<IOzoneObj> keySet = objSet(key("logs", "*"), bucket("logs"), volume());
    expectedResolvedRanger.add(new OzoneGrant(keySet, acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testObjectResourceWithWildcardInMiddle() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:GetObject\",\n" +
        "    \"Resource\": \"arn:aws:s3:::logs/file*.log\"\n" +
        "  }]\n" +
        "}";

    // Wildcards in middle of object resource are not supported for Native authorizer
    expectResolveThrows(
        json, NATIVE, "IAM session policy: Wildcard prefix patterns are not supported for Ozone native " +
        "authorizer if wildcard is not at the end", NOT_SUPPORTED_OPERATION);

    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);
    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: READ acl on key "file*.log", bucket READ, volume READ
    final Set<IOzoneObj> readObjectsRanger = objSet(key("logs", "file*.log"), bucket("logs"), volume());
    expectedResolvedRanger.add(new OzoneGrant(readObjectsRanger, acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testObjectResourceWithPrefixWildcard() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:GetObject\",\n" +
        "    \"Resource\": \"arn:aws:s3:::myBucket/file*\"\n" +
        "  }]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: READ acl on prefix "file" under bucket, bucket READ, volume READ
    final Set<IOzoneObj> readObjectsNative = objSet(prefix("myBucket", "file"), bucket("myBucket"), volume());
    expectedResolvedNative.add(new OzoneGrant(readObjectsNative, acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: READ acl on key "file*", bucket READ, volume READ
    final Set<IOzoneObj> readObjectsRanger = objSet(key("myBucket", "file*"), bucket("myBucket"), volume());
    expectedResolvedRanger.add(new OzoneGrant(readObjectsRanger, acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testBucketActionOnAllResources() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:ListAllMyBuckets\",\n" +
        "        \"s3:ListBucket\"\n" +
        "      ],\n" +
        "    \"Resource\": \"*\"\n" +
        "  }]\n" +
        "}";

    // Wildcards on bucket are not supported for Native authorizer
    expectBucketWildcardUnsupportedExceptionForNativeAuthorizer(json);

    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);
    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: READ and LIST on volume and bucket (wildcard), READ on key "*"
    final Set<IOzoneObj> resourceSet = objSet(volume(), bucket("*"));
    expectedResolvedRanger.add(new OzoneGrant(resourceSet, acls(READ, LIST)));
    expectedResolvedRanger.add(new OzoneGrant(objSet(key("*", "*")), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testObjectActionOnAllResources() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:PutObject\",\n" +
        "    \"Resource\": \"*\"\n" +
        "  }]\n" +
        "}";

    // Wildcards on bucket are not supported for Native authorizer
    expectBucketWildcardUnsupportedExceptionForNativeAuthorizer(json);

    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);
    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: CREATE and WRITE key acls on wildcard pattern, bucket READ, volume READ
    final Set<IOzoneObj> keySet = objSet(key("*", "*"));
    final Set<ACLType> keyAcls = acls(CREATE, WRITE);
    expectedResolvedRanger.add(new OzoneGrant(keySet, keyAcls));
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume(), bucket("*")), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testAllActionsOnAllResources() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:*\",\n" +
        "    \"Resource\": \"*\"\n" +
        "  }]\n" +
        "}";

    // Wildcards on bucket are not supported for Native authorizer
    expectBucketWildcardUnsupportedExceptionForNativeAuthorizer(json);

    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);
    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: READ, LIST acl on volume, ALL acl bucket (wildcard) and key (wildcard)
    final Set<IOzoneObj> resourceSet = objSet(bucket("*"), key("*", "*"));
    expectedResolvedRanger.add(new OzoneGrant(resourceSet, acls(ALL)));
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume()), acls(READ, LIST)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testAllActionsOnAllBucketResources() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:*\",\n" +
        "    \"Resource\": \"arn:aws:s3:::*\"\n" +
        "  }]\n" +
        "}";

    // Wildcards on bucket are not supported for Native authorizer
    expectBucketWildcardUnsupportedExceptionForNativeAuthorizer(json);

    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);
    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: ALL bucket acls on wildcard pattern, volume READ, key "*" READ
    final Set<IOzoneObj> bucketSet = objSet(bucket("*"));
    final Set<ACLType> bucketAcls = acls(ALL);
    expectedResolvedRanger.add(new OzoneGrant(bucketSet, bucketAcls));
    expectedResolvedRanger.add(new OzoneGrant(objSet(key("*", "*")), acls(READ)));
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume()), acls(READ, LIST)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testAllActionsOnAllObjectResources() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:*\",\n" +
        "    \"Resource\": \"arn:aws:s3:::*/*\"\n" +
        "  }]\n" +
        "}";

    // Wildcards on bucket are not supported for Native authorizer
    expectBucketWildcardUnsupportedExceptionForNativeAuthorizer(json);

    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);
    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: ALL key acls on wildcard pattern; bucket READ; volume READ
    final Set<IOzoneObj> keySet = objSet(key("*", "*"));
    final Set<ACLType> keyAcls = acls(ALL);
    expectedResolvedRanger.add(new OzoneGrant(keySet, keyAcls));
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume(), bucket("*")), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testWildcardActionGroupGetStar() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:Get*\",\n" +
        "      \"Resource\": [\n" +
        "        \"arn:aws:s3:::my-bucket\",\n" +
        "        \"arn:aws:s3:::my-bucket/*\"\n" +
        "      ]\n" +
        "  }]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: bucket READ, READ_ACL acls
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));
    final Set<ACLType> bucketAcls = acls(READ, READ_ACL);
    expectedResolvedNative.add(new OzoneGrant(bucketSet, bucketAcls));
    // Expected for native: READ acl on prefix "" under bucket; volume READ
    final Set<IOzoneObj> readObjectsNative = objSet(prefix("my-bucket", ""), volume());
    expectedResolvedNative.add(new OzoneGrant(readObjectsNative, acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: bucket READ, READ_ACL acls
    expectedResolvedRanger.add(new OzoneGrant(bucketSet, bucketAcls));
    // Expected for Ranger: READ key acl for resource type KEY with key name "*"; volume READ
    final Set<IOzoneObj> readObjectsRanger = objSet(key("my-bucket", "*"), volume());
    expectedResolvedRanger.add(new OzoneGrant(readObjectsRanger, acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testWildcardActionGroupListStar() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:List*\",\n" +
        "      \"Resource\": [\n" +
        "        \"arn:aws:s3:::my-bucket\",\n" +
        "        \"arn:aws:s3:::my-bucket/*\"\n" +  // ListMultipartUploadParts has READ effect on file/object resources
        "      ]\n" +
        "  }]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: READ, LIST bucket acls
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));
    final Set<ACLType> bucketAcls = acls(READ, LIST);
    expectedResolvedNative.add(new OzoneGrant(bucketSet, bucketAcls));
    // Expected for native: READ acl on prefix "" under bucket; volume READ
    final Set<IOzoneObj> readObjectsNative = objSet(prefix("my-bucket", ""), volume());
    expectedResolvedNative.add(new OzoneGrant(readObjectsNative, acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: READ, LIST bucket acls
    expectedResolvedRanger.add(new OzoneGrant(bucketSet, bucketAcls));
    // Expected for Ranger: READ key acl for resource type KEY with key name "*"; volume READ
    final Set<IOzoneObj> readObjectsRanger = objSet(key("my-bucket", "*"), volume());
    expectedResolvedRanger.add(new OzoneGrant(readObjectsRanger, acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testWildcardActionGroupPutStar() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:Put*\",\n" +
        "      \"Resource\": [\n" +
        "        \"arn:aws:s3:::my-bucket\",\n" +
        "        \"arn:aws:s3:::my-bucket/*\"\n" +
        "      ]\n" +
        "  }]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: bucket READ, WRITE_ACL acl
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));
    final Set<ACLType> bucketAcl = acls(READ, WRITE_ACL);
    expectedResolvedNative.add(new OzoneGrant(bucketSet, bucketAcl));
    // Expected for native: CREATE, WRITE acls on prefix "" under bucket
    final Set<IOzoneObj> keyPrefixSet = objSet(prefix("my-bucket", ""));
    final Set<ACLType> keyAcls = acls(CREATE, WRITE);
    expectedResolvedNative.add(new OzoneGrant(keyPrefixSet, keyAcls));
    // Expected for native: volume READ
    expectedResolvedNative.add(new OzoneGrant(objSet(volume()), acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: bucket READ, WRITE_ACL acl
    expectedResolvedRanger.add(new OzoneGrant(bucketSet, bucketAcl));
    // Expected for Ranger: CREATE, WRITE key acls for resource type KEY with key name "*"
    final Set<IOzoneObj> rangerKeySet = objSet(key("my-bucket", "*"));
    expectedResolvedRanger.add(new OzoneGrant(rangerKeySet, keyAcls));
    // Expected for Ranger: volume READ
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume()), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testWildcardActionGroupDeleteStar() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:Delete*\",\n" +
        "      \"Resource\": [\n" +
        "        \"arn:aws:s3:::my-bucket\",\n" +
        "        \"arn:aws:s3:::my-bucket/*\"\n" +
        "      ]\n" +
        "  }]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    final Set<OzoneGrant> expectedResolvedNative = new LinkedHashSet<>();
    // Expected for native: DELETE on prefix "" under bucket; bucket READ, DELETE; volume READ
    final Set<IOzoneObj> resourceSetNative = objSet(prefix("my-bucket", ""));
    expectedResolvedNative.add(new OzoneGrant(resourceSetNative, acls(DELETE)));
    expectedResolvedNative.add(new OzoneGrant(objSet(bucket("my-bucket")), acls(READ, DELETE)));
    expectedResolvedNative.add(new OzoneGrant(objSet(volume()), acls(READ)));
    assertThat(resolvedFromNativeAuthorizer).isEqualTo(expectedResolvedNative);

    final Set<OzoneGrant> expectedResolvedRanger = new LinkedHashSet<>();
    // Expected for Ranger: DELETE on resource type KEY with key name "*"; bucket READ, DELETE; volume READ
    final Set<IOzoneObj> resourceSetRanger = objSet(key("my-bucket", "*"));
    expectedResolvedRanger.add(new OzoneGrant(resourceSetRanger, acls(DELETE)));
    expectedResolvedRanger.add(new OzoneGrant(objSet(bucket("my-bucket")), acls(READ, DELETE)));
    expectedResolvedRanger.add(new OzoneGrant(objSet(volume()), acls(READ)));
    assertThat(resolvedFromRangerAuthorizer).isEqualTo(expectedResolvedRanger);
  }

  @Test
  public void testMismatchedActionAndResourceReturnsEmpty() throws OMException {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:GetObject\",\n" +             // object-level action
        "    \"Resource\": \"arn:aws:s3:::my-bucket\"\n" +  // bucket-level resource
        "  }]\n" +
        "}";

    final Set<OzoneGrant> resolvedFromNativeAuthorizer = resolve(json, VOLUME, NATIVE);
    final Set<OzoneGrant> resolvedFromRangerAuthorizer = resolve(json, VOLUME, RANGER);

    // Ensure what we got is what we expected
    assertThat(resolvedFromNativeAuthorizer).isEmpty();
    assertThat(resolvedFromRangerAuthorizer).isEmpty();
  }

  @Test
  public void testInvalidResourceArnThrows() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::\"\n" +
        "  }]\n" +
        "}";

    expectResolveThrowsForBothAuthorizers(
        json, "IAM session policy: Invalid Resource Arn - arn:aws:s3:::", MALFORMED_POLICY_DOCUMENT);
  }

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
      resolve(json, VOLUME, authorizerType);
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

  /**
   * Ensure resources containing wildcards in buckets throw an Exception
   * when the OzoneNativeAuthorizer is used.
   */
  private static void expectBucketWildcardUnsupportedExceptionForNativeAuthorizer(String json) {
    try {
      resolve(json, VOLUME, NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (OMException ex) {
      assertThat(ex.getMessage()).isEqualTo(
          "IAM session policy: Wildcard bucket patterns are not supported for Ozone native authorizer");
      assertThat(ex.getResult()).isEqualTo(NOT_SUPPORTED_OPERATION);
    }
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

