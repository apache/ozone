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

import static java.util.Collections.singleton;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.AssumeRoleRequest;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;

/**
 * Resolves a limited subset of AWS IAM session policies into Ozone ACL grants,
 * according to either the RangerOzoneAuthorizer or OzoneNativeAuthorizer constructs.
 * <p>
 * Here are some differences between the RangerOzoneAuthorizer and OzoneNativeAuthorizer:
 *    - RangerOzoneAuthorizer doesn't currently use ResourceType.PREFIX, whereas OzoneNativeAuthorizer does.
 *    - OzoneNativeAuthorizer doesn't allow wildcards in bucket names (ex. ResourceArn `arn:aws:s3:::*`,
 *    `arn:aws:s3:::bucket*` or `*`), whereas RangerOzoneAuthorizer does.
 *    - For OzoneNativeAuthorizer, certain object wildcards are accepted.   For example, ResourceArn
 *    `arn:aws:s3:::myBucket/*` and `arn:aws:s3:::myBucket/folder/logs/*` are accepted but not
 *    `arn:aws:s3:::myBucket/file*.txt`.
 * <p>
 * The only supported ResourceArn has prefix arn:aws:s3::: - all others will throw
 * OMException with NOT_SUPPORTED_OPERATION.
 * <p>
 * The only supported Condition operator is StringEquals - all others will throw
 * OMException with NOT_SUPPORTED_OPERATION.  Furthermore, only one Condition is supported in a
 * statement.  The value StringEquals is case-sensitive per the
 * <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html">
 * AWS spec</a>.
 * <p>
 * The only supported Condition key name is s3:prefix - all others will throw
 * OMException with NOT_SUPPORTED_OPERATION.  s3:prefix is case-insensitive per the
 * <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition.html">AWS spec</a>.
 * <p>
 * The only supported Effect is Allow - all others will throw OMException with NOT_SUPPORTED_OPERATION.  This
 * value is case-sensitive per the
 * <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_effect.html">AWS spec</a>.
 * <p>
 * If a (currently) unsupported S3 action is requested, such as s3:GetAccelerateConfiguration,
 * it will be silently ignored.
 * <p>
 * Supported wildcard expansions in Actions are: s3:*, s3:Get*, s3:Put*, s3:List*,
 * s3:Create*, and s3:Delete*.
 */
public final class IamSessionPolicyResolver {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // JSON length is limited per AWS policy.  See https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
  // under Policy section.
  private static final int MAX_JSON_LENGTH = 2048;

  // Used to group actions into s3:Get*, s3:Put*, s3:List*, s3:Delete*, s3:Create*
  private static final String[] S3_ACTION_PREFIXES = {"s3:Get", "s3:Put", "s3:List", "s3:Delete", "s3:Create"};

  @VisibleForTesting
  static final Map<String, Set<S3Action>> S3_ACTION_MAP_CI = buildCaseInsensitiveS3ActionMap();

  private IamSessionPolicyResolver() {
  }

  /**
   * Resolves an S3 IAM session policy in the form of a JSON String to a data structure comprising
   * the IOzoneObjs and permissions that IAM policy grants (if any).
   * <p>
   * Each entry represents a path (such as /s3v/bucket1 or /s3v/bucket1/*) and a set of
   * permissions (such as READ, LIST, CREATE).
   * <p>
   * The OzoneObj can be different depending on the AuthorizerType (see main Javadoc at top of file
   * for examples).
   * <p>
   *
   * @param policyJson     the IAM session policy
   * @param volumeName     the volume under which the resource(s) live.  This may not be s3v in
   *                       multi-tenant scenarios
   * @param authorizerType whether the IOzoneObjs should be generated for use by the
   *                       RangerOzoneAuthorizer or the OzoneNativeAuthorizer
   * @return the data structure comprising the paths and permission pairings that
   * the session policy grants (if any)
   * @throws OMException if the policy JSON is invalid, malformed, or contains unsupported features
   */
  public static Set<AssumeRoleRequest.OzoneGrant> resolve(String policyJson, String volumeName,
      AuthorizerType authorizerType) throws OMException {

    validateInputParameters(policyJson, volumeName, authorizerType);

    final Set<AssumeRoleRequest.OzoneGrant> result = new LinkedHashSet<>();

    // Parse JSON into set of statements
    final Set<JsonNode> statements = parseJsonAndRetrieveStatements(policyJson);

    for (JsonNode stmt : statements) {
      validateEffectInJsonStatement(stmt);

      final Set<String> actions = readStringOrArray(stmt.get("Action"));
      final Set<String> resources = readStringOrArray(stmt.get("Resource"));

      // Parse prefixes from conditions, if any
      final Set<String> prefixes = parsePrefixesFromConditions(stmt);

      // Map actions to S3Action enum if possible
      final Set<S3Action> mappedS3Actions = mapPolicyActionsToS3Actions(actions);
      if (mappedS3Actions.isEmpty()) {
        // No actions recognized - no need to look at Resources for this Statement
        continue;
      }

      // Categorize resources according to bucket resource, object resource, etc
      final Set<ResourceSpec> resourceSpecs = validateAndCategorizeResources(authorizerType, resources);

      // For each action, map to Ozone objects (paths) and acls based on resource specs and prefixes
      final Set<AssumeRoleRequest.OzoneGrant> stmtResults = createPathsAndPermissions(
          volumeName, authorizerType, mappedS3Actions, resourceSpecs, prefixes);

      result.addAll(stmtResults);
    }

    return result;
  }

  /**
   * Ensures required input parameters are supplied.
   */
  private static void validateInputParameters(String policyJson, String volumeName,
      AuthorizerType authorizerType) throws OMException {
    if (StringUtils.isBlank(policyJson)) {
      throw new OMException("The IAM session policy JSON is required", INVALID_REQUEST);
    }

    if (StringUtils.isBlank(volumeName)) {
      throw new OMException("The volume name is required", INVALID_REQUEST);
    }

    Objects.requireNonNull(authorizerType, "The authorizer type is required");

    if (policyJson.length() > MAX_JSON_LENGTH) {
      throw new OMException("Invalid policy JSON - exceeds maximum length of " +
          MAX_JSON_LENGTH + " characters", INVALID_REQUEST);
    }
  }

  /**
   * Parses IAM session policy and retrieve the statement(s).
   */
  private static Set<JsonNode> parseJsonAndRetrieveStatements(String policyJson) throws OMException {
    final JsonNode root;
    try {
      root = MAPPER.readTree(policyJson);
    } catch (Exception e) {
      throw new OMException("Invalid policy JSON (most likely JSON structure is incorrect)", e, INVALID_REQUEST);
    }

    final JsonNode statementsNode = root.path("Statement");
    if (statementsNode.isMissingNode()) {
      throw new OMException("Invalid policy JSON - missing Statement", INVALID_REQUEST);
    }

    final Set<JsonNode> statements = new HashSet<>();

    if (statementsNode.isArray()) {
      statementsNode.forEach(statements::add);
    } else {
      statements.add(statementsNode);
    }
    return statements;
  }

  /**
   * Parses Effect from IAM session policy and ensures it is valid and supported.
   */
  private static void validateEffectInJsonStatement(JsonNode statement) throws OMException {
    final JsonNode effectNode = statement.get("Effect");
    if (effectNode != null) {
      if (effectNode.isTextual()) {
        final String effect = effectNode.asText();
        if (!"Allow".equals(effect)) {
          throw new OMException("Unsupported Effect - " + effect, NOT_SUPPORTED_OPERATION);
        }
        return;
      }

      throw new OMException(
          "Invalid Effect in JSON policy (must be a String) - " + effectNode, INVALID_REQUEST);
    }

    throw new OMException("Effect is missing from JSON policy", INVALID_REQUEST);
  }

  /**
   * Reads a JsonNode and converts to a Set of String, if the node represents
   * a textual value or an array of textual values.  Otherwise, returns
   * an empty List.
   */
  private static Set<String> readStringOrArray(JsonNode node) {
    if (node == null || node.isMissingNode() || node.isNull()) {
      return Collections.emptySet();
    }
    if (node.isTextual()) {
      return Collections.singleton(node.asText());
    }
    if (node.isArray()) {
      final Set<String> set = new HashSet<>();
      node.forEach(n -> {
        if (n.isTextual()) {
          set.add(n.asText());
        }
      });
      return set;
    }

    return Collections.emptySet();
  }

  /**
   * Parses and returns prefixes from Conditions (if any).  Also validates
   * that if there is a Condition, there is only one and that the Condition
   * operator and key name are supported.
   * <p>
   * Only the StringEquals operator and s3:prefix key name are supported.
   */
  private static Set<String> parsePrefixesFromConditions(JsonNode stmt) throws OMException {
    Set<String> prefixes = Collections.emptySet();
    final JsonNode cond = stmt.get("Condition");
    if (cond != null && !cond.isMissingNode() && !cond.isNull()) {
      if (cond.size() != 1) {
        throw new OMException("Only one Condition is supported", NOT_SUPPORTED_OPERATION);
      }

      if (!cond.isObject()) {
        throw new OMException(
            "Invalid Condition (must have operator StringEquals " + "and key name s3:prefix) - " +
            cond, INVALID_REQUEST);
      }

      final String operator = cond.fieldNames().next();
      if (!"StringEquals".equals(operator)) {
        throw new OMException("Unsupported Condition operator - " + operator, NOT_SUPPORTED_OPERATION);
      }

      final JsonNode operatorValue = cond.get("StringEquals");
      if ("null".equals(operatorValue.asText())) {
        throw new OMException("Missing Condition operator - StringEquals", INVALID_REQUEST);
      }

      if (!operatorValue.isObject()) {
        throw new OMException("Invalid Condition operator value structure - " + operatorValue, INVALID_REQUEST);
      }

      final String keyName = operatorValue.fieldNames().hasNext() ? operatorValue.fieldNames().next() : null;
      if (!"s3:prefix".equalsIgnoreCase(keyName)) {
        throw new OMException("Unsupported Condition key name - " + keyName, NOT_SUPPORTED_OPERATION);
      }

      prefixes = readStringOrArray(operatorValue.get(keyName));
    }

    return prefixes;
  }

  /**
   * Builds a case-insensitive S3Action map by lowercasing keys.  This map is used for mapping policy actions to
   * S3Action enum values.  This map is built once and cached statically.
   */
  @VisibleForTesting
  static Map<String, Set<S3Action>> buildCaseInsensitiveS3ActionMap() {
    final Map<String, Set<S3Action>> ciMap = new LinkedHashMap<>();
    for (S3Action sa : S3Action.values()) {
      // Exact action mapping
      ciMap.put(sa.name.toLowerCase(), singleton(sa));

      // Group into s3:Get*, s3:Put*, s3:List*, s3:Delete*, s3:Create* based on action name prefix
      for (String prefix : S3_ACTION_PREFIXES) {
        if (sa.name.startsWith(prefix)) {
          final String wildcardKey = (prefix + "*").toLowerCase();
          ciMap.computeIfAbsent(wildcardKey, k -> new LinkedHashSet<>()).add(sa);
          break;
        }
      }
    }
    return Collections.unmodifiableMap(ciMap);
  }

  /**
   * Maps actions from JSON IAM policy to S3Action enum in order to determine what the
   * permissions should be.
   */
  @VisibleForTesting
  static Set<S3Action> mapPolicyActionsToS3Actions(Set<String> actions) {
    if (actions == null || actions.isEmpty()) {
      return Collections.emptySet();
    }

    // Map the actions from the IAM policy to S3Action
    final Set<S3Action> mappedActions = new LinkedHashSet<>();
    for (String action : actions) {
      if ("s3:*".equalsIgnoreCase(action)) {
        return EnumSet.of(S3Action.ALL_S3);
      }

      // Unsupported actions are silently ignored
      final Set<S3Action> s3Actions = S3_ACTION_MAP_CI.get(action.toLowerCase());
      if (s3Actions != null) {
        mappedActions.addAll(s3Actions);
      }
    }

    return mappedActions;
  }

  /**
   * Iterates over resources in IAM policy and determines whether it is a bucket resource,
   * an object resource, a prefix or a wildcard.  The categorization can be different
   * depending on whether the AuthorizerType is Ranger (for RangerOzoneAuthorizer) or
   * native (for OzoneNativeAuthorizer).  See main Javadoc at top of file for more
   * examples of these differences.
   * <p>
   * It also validates that the Resource Arn(s) are valid and supported.
   */
  private static Set<ResourceSpec> validateAndCategorizeResources(AuthorizerType authorizerType,
      Set<String> resources) throws OMException {
    // TODO implement in future PR
    return Collections.emptySet();
  }

  /**
   * Iterates over all resources, finds applicable actions (if any) and constructs
   * entries pairing sets of IOzoneObjs with the requisite permissions granted (if any).
   */
  private static Set<AssumeRoleRequest.OzoneGrant> createPathsAndPermissions(String volumeName,
      AuthorizerType authorizerType, Set<S3Action> mappedS3Actions, Set<ResourceSpec> resourceSpecs,
      Set<String> prefixes) {
    // TODO implement in future PR
    return Collections.emptySet();
  }

  /**
   * The authorizer type, whether for OzoneNativeAuthorizer or RangerOzoneAuthorizer.
   * The IOzoneObjs generated differ in certain cases depending on the type.
   * See main Javadoc at top of file for differences.
   */
  public enum AuthorizerType {
    NATIVE,
    RANGER
  }

  /**
   * The type of resource the S3 action applies to.
   */
  private enum ActionKind {
    VOLUME,
    BUCKET,
    OBJECT,
    ALL
  }

  /**
   * Utility to help categorize IAM policy resources, whether for bucket, key, wildcards, etc.
   */
  private static final class ResourceSpec {
    // TODO implement in future PR
  }

  @VisibleForTesting
  enum S3Action {
    // Volume-scope
    // Used for ListBuckets api
    LIST_ALL_MY_BUCKETS("s3:ListAllMyBuckets", ActionKind.VOLUME, EnumSet.of(ACLType.READ, ACLType.LIST)),

    // Bucket-scope
    CREATE_BUCKET("s3:CreateBucket", ActionKind.BUCKET, EnumSet.of(ACLType.CREATE)),
    DELETE_BUCKET("s3:DeleteBucket", ActionKind.BUCKET, EnumSet.of(ACLType.DELETE)),
    GET_BUCKET_ACL("s3:GetBucketAcl", ActionKind.BUCKET, EnumSet.of(ACLType.READ, ACLType.READ_ACL)),
    GET_BUCKET_LOCATION("s3:GetBucketLocation", ActionKind.BUCKET, EnumSet.of(ACLType.READ)),
    // Used for HeadBucket, ListObjects and ListObjectsV2 apis
    LIST_BUCKET("s3:ListBucket", ActionKind.BUCKET, EnumSet.of(ACLType.READ, ACLType.LIST)),
    // Used for ListMultipartUploads API
    LIST_BUCKET_MULTIPART_UPLOADS(
        "s3:ListBucketMultipartUploads",
        ActionKind.BUCKET,
        EnumSet.of(ACLType.READ, ACLType.LIST)
    ),
    PUT_BUCKET_ACL("s3:PutBucketAcl", ActionKind.BUCKET, EnumSet.of(ACLType.WRITE_ACL)),

    // Object-scope
    ABORT_MULTIPART_UPLOAD("s3:AbortMultipartUpload", ActionKind.OBJECT, EnumSet.of(ACLType.DELETE)),
    // Used for DeleteObject (when versionId parameter is not supplied),
    // DeleteObjects (when versionId parameter is not supplied) APIs
    DELETE_OBJECT("s3:DeleteObject", ActionKind.OBJECT, EnumSet.of(ACLType.DELETE)),
    DELETE_OBJECT_TAGGING("s3:DeleteObjectTagging", ActionKind.OBJECT, EnumSet.of(ACLType.DELETE)),
    // Used for DeleteObject (when versionId parameter is supplied),
    // DeleteObjects (when versionId parameter is supplied) APIs
    DELETE_OBJECT_VERSION("s3:DeleteObjectVersion", ActionKind.OBJECT, EnumSet.of(ACLType.DELETE)),
    // Used for HeadObject, CopyObject (for source bucket), GetObject (without versionId parameter) APIs
    GET_OBJECT("s3:GetObject", ActionKind.OBJECT, EnumSet.of(ACLType.READ)),
    GET_OBJECT_TAGGING("s3:GetObjectTagging", ActionKind.OBJECT, EnumSet.of(ACLType.READ)),
    // Used for GetObject API when versionId parameter is supplied
    GET_OBJECT_VERSION("s3:GetObjectVersion", ActionKind.OBJECT, EnumSet.of(ACLType.READ)),
    // Used for ListParts API
    LIST_MULTIPART_UPLOAD_PARTS("s3:ListMultipartUploadParts", ActionKind.OBJECT, EnumSet.of(ACLType.READ)),
    // Used for CreateMultipartUpload, UploadPart, CompleteMultipartUpload,
    // CopyObject (for destination bucket), PutObject APIs
    PUT_OBJECT("s3:PutObject", ActionKind.OBJECT, EnumSet.of(ACLType.CREATE, ACLType.WRITE)),
    PUT_OBJECT_TAGGING("s3:PutObjectTagging", ActionKind.OBJECT, EnumSet.of(ACLType.WRITE)),
    // Used for PutObjectTagging (with versionId parameter) API
    PUT_OBJECT_VERSION_TAGGING("s3:PutObjectVersionTagging", ActionKind.OBJECT, EnumSet.of(ACLType.WRITE)),

    // Wildcard all
    ALL_S3("s3:*", ActionKind.ALL, EnumSet.of(ACLType.ALL));

    private final String name;
    private final ActionKind kind;
    private final Set<ACLType> perms;

    S3Action(String name, ActionKind kind, Set<ACLType> perms) {
      this.name = name;
      this.kind = kind;
      this.perms = perms;
    }
  }
}
