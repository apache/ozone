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
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.MALFORMED_POLICY_DOCUMENT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.DELETE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ_ACL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE_ACL;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;

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
 * The only supported Condition operators are StringEquals and StringLike - all others will throw
 * OMException with NOT_SUPPORTED_OPERATION.  Furthermore, only one Condition is supported in a
 * statement.  The value for both StringEquals and StringLike is case-sensitive per the
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
 * it will be silently ignored.  Similarly, if an invalid S3 action is requested, it will be silently ignored.
 * <p>
 * Supported wildcard expansions in Actions are: s3:*, s3:Get*, s3:Put*, s3:List*,
 * s3:Create*, and s3:Delete*.
 */
public final class IamSessionPolicyResolver {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String AWS_S3_ARN_PREFIX = "arn:aws:s3:::";

  // JSON length is limited per AWS policy.  See https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
  // under Policy section.
  private static final int MAX_JSON_LENGTH = 2048;

  // Used to group actions into s3:Get*, s3:Put*, s3:List*, s3:Delete*, s3:Create*
  private static final String[] S3_ACTION_PREFIXES = {"s3:Get", "s3:Put", "s3:List", "s3:Delete", "s3:Create"};

  private static final String ERROR_PREFIX = "IAM session policy: ";

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

    // Accumulate ACLs across ALL statements using a single map to allow
    // cross-statement deduplication and ALL-permission collapsing.
    final Map<IOzoneObj, Set<ACLType>> objToAclsMap = new LinkedHashMap<>();

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
      createPathsAndPermissions(volumeName, authorizerType, mappedS3Actions, resourceSpecs, prefixes, objToAclsMap);
    }

    // Group accumulated objects by their ACL sets to create final result
    return groupObjectsByAcls(objToAclsMap);
  }

  /**
   * Ensures required input parameters are supplied.
   */
  private static void validateInputParameters(String policyJson, String volumeName,
      AuthorizerType authorizerType) throws OMException {
    if (StringUtils.isBlank(policyJson)) {
      throw new OMException(ERROR_PREFIX + "The IAM session policy JSON is required", INTERNAL_ERROR);
    }

    if (StringUtils.isBlank(volumeName)) {
      throw new OMException(ERROR_PREFIX + "The volume name is required", INTERNAL_ERROR);
    }

    Objects.requireNonNull(authorizerType, "The authorizer type is required");

    if (policyJson.length() > MAX_JSON_LENGTH) {
      throw new OMException(
          ERROR_PREFIX + "Invalid policy JSON - exceeds maximum length of " + MAX_JSON_LENGTH + " characters",
          MALFORMED_POLICY_DOCUMENT);
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
      throw new OMException(
          ERROR_PREFIX + "Invalid policy JSON (most likely JSON structure is incorrect)", e, MALFORMED_POLICY_DOCUMENT);
    }

    final JsonNode statementsNode = root.path("Statement");
    if (statementsNode.isMissingNode()) {
      throw new OMException(ERROR_PREFIX + "Invalid policy JSON - missing Statement", MALFORMED_POLICY_DOCUMENT);
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
          throw new OMException(ERROR_PREFIX + "Unsupported Effect - " + effect, NOT_SUPPORTED_OPERATION);
        }
        return;
      }

      throw new OMException(
          ERROR_PREFIX + "Invalid Effect in JSON policy (must be a String) - " + effectNode, MALFORMED_POLICY_DOCUMENT);
    }

    throw new OMException(ERROR_PREFIX + "Effect is missing from JSON policy", MALFORMED_POLICY_DOCUMENT);
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
        throw new OMException(ERROR_PREFIX + "Only one Condition is supported", NOT_SUPPORTED_OPERATION);
      }

      if (!cond.isObject()) {
        throw new OMException(
            ERROR_PREFIX + "Invalid Condition (must have operator StringEquals or StringLike " +
            "and key name s3:prefix) - " + cond, MALFORMED_POLICY_DOCUMENT);
      }

      final String operator = cond.fieldNames().next();
      if (!"StringEquals".equals(operator) && !"StringLike".equals(operator)) {
        throw new OMException(ERROR_PREFIX + "Unsupported Condition operator - " + operator, NOT_SUPPORTED_OPERATION);
      }

      final JsonNode operatorValue = cond.get(operator);
      if ("null".equals(operatorValue.asText())) {
        throw new OMException(
            ERROR_PREFIX + "Missing Condition operator value for " + operator, MALFORMED_POLICY_DOCUMENT);
      }

      if (!operatorValue.isObject()) {
        throw new OMException(
            ERROR_PREFIX + "Invalid Condition operator value structure - " + operatorValue, MALFORMED_POLICY_DOCUMENT);
      }

      final String keyName = operatorValue.fieldNames().hasNext() ? operatorValue.fieldNames().next() : null;
      if (!"s3:prefix".equalsIgnoreCase(keyName)) {
        throw new OMException(ERROR_PREFIX + "Unsupported Condition key name - " + keyName, NOT_SUPPORTED_OPERATION);
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
   * Validates that wildcard bucket patterns are not used with native authorizer.
   */
  private static void validateNativeAuthorizerBucketPattern(AuthorizerType authorizerType, String bucket)
      throws OMException {
    if (authorizerType == AuthorizerType.NATIVE && bucket.contains("*")) {
      throw new OMException(
          ERROR_PREFIX + "Wildcard bucket patterns are not supported for Ozone native authorizer",
          NOT_SUPPORTED_OPERATION);
    }
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
  @VisibleForTesting
  static Set<ResourceSpec> validateAndCategorizeResources(AuthorizerType authorizerType,
      Set<String> resources) throws OMException {
    final Set<ResourceSpec> resourceSpecs = new HashSet<>();
    if (resources.isEmpty()) {
      throw new OMException(ERROR_PREFIX + "No Resource(s) found in policy", MALFORMED_POLICY_DOCUMENT);
    }
    for (String resource : resources) {
      if ("*".equals(resource)) {
        validateNativeAuthorizerBucketPattern(authorizerType, "*");
        resourceSpecs.add(ResourceSpec.any());
        continue;
      }

      if (!resource.startsWith(AWS_S3_ARN_PREFIX)) {
        throw new OMException(ERROR_PREFIX + "Unsupported Resource Arn - " + resource, NOT_SUPPORTED_OPERATION);
      }

      final String suffix = resource.substring(AWS_S3_ARN_PREFIX.length());
      if (suffix.isEmpty()) {
        throw new OMException(ERROR_PREFIX + "Invalid Resource Arn - " + resource, MALFORMED_POLICY_DOCUMENT);
      }

      ResourceSpec spec = parseResourceSpec(suffix);

      // This scenario can happen in the case of arn:aws:s3:::*/* or arn:aws:s3:::*/test.txt for
      // examples
      validateNativeAuthorizerBucketPattern(authorizerType, spec.bucket);

      if (authorizerType == AuthorizerType.NATIVE && spec.type == S3ResourceType.OBJECT_PREFIX_WILDCARD) {
        final String specPrefixExceptLastChar = spec.prefix.substring(0, spec.prefix.length() - 1);
        if (spec.prefix.endsWith("*") && !specPrefixExceptLastChar.contains("*")) {
          spec = ResourceSpec.objectPrefix(spec.bucket, specPrefixExceptLastChar);
        } else {
          throw new OMException(
              ERROR_PREFIX + "Wildcard prefix patterns are not supported for Ozone native authorizer if " +
              "wildcard is not at the end", NOT_SUPPORTED_OPERATION);
        }
      }
      resourceSpecs.add(spec);
    }
    return resourceSpecs;
  }

  /**
   * Iterates over all resources, finds applicable actions (if any) and constructs
   * entries pairing sets of IOzoneObjs with the requisite permissions granted (if any).
   */
  @VisibleForTesting
  static void createPathsAndPermissions(String volumeName, AuthorizerType authorizerType, Set<S3Action> mappedS3Actions,
      Set<ResourceSpec> resourceSpecs, Set<String> prefixes, Map<IOzoneObj, Set<ACLType>> objToAclsMap) {
    // Process each resource spec with the given actions
    for (ResourceSpec resourceSpec : resourceSpecs) {
      processResourceSpecWithActions(volumeName, authorizerType, mappedS3Actions, resourceSpec, prefixes, objToAclsMap);
    }
  }

  /**
   * Groups objects by their ACL sets.
   */
  @VisibleForTesting
  static Set<AssumeRoleRequest.OzoneGrant> groupObjectsByAcls(Map<IOzoneObj, Set<ACLType>> objToAclsMap) {
    final Map<Set<ACLType>, Set<IOzoneObj>> groupMap = new LinkedHashMap<>();

    // Group objects by their ACL sets only (across resource types)
    objToAclsMap.forEach((obj, acls) ->
        groupMap.computeIfAbsent(acls, k -> new LinkedHashSet<>()).add(obj));

    // Convert to result format, filtering out entries with empty ACLs
    final Set<AssumeRoleRequest.OzoneGrant> result = new LinkedHashSet<>();
    groupMap.forEach((key, objs) -> {
      if (!key.isEmpty()) {
        result.add(new AssumeRoleRequest.OzoneGrant(objs, key));
      }
    });

    return result;
  }

  /**
   * Processes a single ResourceSpec with given actions and adds resulting
   * IOzoneObj to ACLType mappings to the provided map.
   */
  private static void processResourceSpecWithActions(String volumeName, AuthorizerType authorizerType,
      Set<S3Action> mappedS3Actions, ResourceSpec resourceSpec, Set<String> prefixes,
      Map<IOzoneObj, Set<ACLType>> objToAclsMap) {

    // Process based on ResourceSpec type
    switch (resourceSpec.type) {
    case ANY:
      Preconditions.checkArgument(
          authorizerType != AuthorizerType.NATIVE,
          "ResourceSpec type ANY not supported for OzoneNativeAuthorizer");
      processResourceTypeAny(volumeName, mappedS3Actions, objToAclsMap);
      break;
    case BUCKET:
      processBucketResource(volumeName, mappedS3Actions, resourceSpec, prefixes, authorizerType, objToAclsMap);
      break;
    case BUCKET_WILDCARD:
      Preconditions.checkArgument(
          authorizerType != AuthorizerType.NATIVE,
          "ResourceSpec type BUCKET_WILDCARD not supported for OzoneNativeAuthorizer");
      processBucketResource(volumeName, mappedS3Actions, resourceSpec, prefixes, authorizerType, objToAclsMap);
      break;
    case OBJECT_EXACT:
      processObjectExactResource(volumeName, mappedS3Actions, resourceSpec, objToAclsMap);
      break;
    case OBJECT_PREFIX:
      Preconditions.checkArgument(
          authorizerType != AuthorizerType.RANGER,
          "ResourceSpec type OBJECT_PREFIX not supported for RangerOzoneAuthorizer");
      processObjectPrefixResource(volumeName, authorizerType, mappedS3Actions, resourceSpec, objToAclsMap);
      break;
    case OBJECT_PREFIX_WILDCARD:
      Preconditions.checkArgument(
          authorizerType != AuthorizerType.NATIVE,
          "ResourceSpec type OBJECT_PREFIX_WILDCARD not supported for OzoneNativeAuthorizer");
      processObjectPrefixResource(volumeName, authorizerType, mappedS3Actions, resourceSpec, objToAclsMap);
      break;
    default:
      throw new IllegalStateException("Unexpected resourceSpec type found: " + resourceSpec.type);
    }
  }

  /**
   * Handles ResourceType.ANY (*).
   * Example: "Resource": "*"
   */
  private static void processResourceTypeAny(String volumeName, Set<S3Action> mappedS3Actions,
      Map<IOzoneObj, Set<ACLType>> objToAclsMap) {
    for (S3Action action : mappedS3Actions) {
      addAclsForObj(objToAclsMap, volumeObj(volumeName), action.volumePerms);
      addAclsForObj(objToAclsMap, bucketObj(volumeName, "*"), action.bucketPerms);
      addAclsForObj(objToAclsMap, keyObj(volumeName, "*", "*"), action.objectPerms);
    }
  }

  /**
   * Handles BUCKET and BUCKET_WILDCARD resource types.
   * Example: "Resource": "arn:aws:s3:::my-bucket" or "Resource": "arn:aws:s3:::my-bucket*" or
   *          "Resource": "arn:aws:s3:::*"
   */
  private static void processBucketResource(String volumeName, Set<S3Action> mappedS3Actions,
      ResourceSpec resourceSpec, Set<String> prefixes, AuthorizerType authorizerType,
      Map<IOzoneObj, Set<ACLType>> objToAclsMap) {
    for (S3Action action : mappedS3Actions) {
      // The s3:ListAllMyBuckets action can use either "*" or
      // "arn:aws:s3:::*" as its Resource.  The former is already handled via the
      // ResourceSpec.ANY path.  The latter is parsed as a BUCKET_WILDCARD with a
      // bucket name of "*".  To align with AWS, make sure that in this
      // specific case we also grant the volume-level permissions for volume-scoped
      // actions (currently s3:ListAllMyBuckets).
      if (action.kind == ActionKind.BUCKET ||
          (action.kind == ActionKind.VOLUME && "*".equals(resourceSpec.bucket))) { // this handles s3:ListAllMyBuckets
        addAclsForObj(objToAclsMap, volumeObj(volumeName), action.volumePerms);
        addAclsForObj(objToAclsMap, bucketObj(volumeName, resourceSpec.bucket), action.bucketPerms);
      } else if (action == S3Action.ALL_S3) {
        // For s3:*, ALL should only apply at the bucket level; grant READ at volume for navigation
        // However, resource "arn:aws:s3:::*" can apply to volume as well (as explained above)
        // If the bucket is "*", include the volumePerms, otherwise just include READ for navigation.
        if ("*".equals(resourceSpec.bucket)) {
          addAclsForObj(objToAclsMap, volumeObj(volumeName), action.volumePerms);
        } else {
          addAclsForObj(objToAclsMap, volumeObj(volumeName), EnumSet.of(READ));
        }
        addAclsForObj(objToAclsMap, bucketObj(volumeName, resourceSpec.bucket), action.bucketPerms);
      }

      if (action == S3Action.LIST_BUCKET || action == S3Action.ALL_S3) {
        // If condition prefixes are present, these would constrain the object permissions if the action
        // is s3:ListBucket or s3:* (which includes s3:ListBucket)
        if (prefixes != null && !prefixes.isEmpty()) {
          for (String prefix : prefixes) {
            createObjectResourcesFromConditionPrefix(
                volumeName, authorizerType, resourceSpec, prefix, objToAclsMap, EnumSet.of(READ));
          }
        } else {
          // No condition prefixes, but we need READ access to all objects, so use "*" as the prefix
          createObjectResourcesFromConditionPrefix(
              volumeName, authorizerType, resourceSpec, "*", objToAclsMap, EnumSet.of(READ));
        }
      }
    }
  }

  /**
   * Handles OBJECT_EXACT resource type.
   * Example: "Resource": "arn:aws:s3:::my-bucket/file.txt"
   */
  private static void processObjectExactResource(String volumeName, Set<S3Action> mappedS3Actions,
      ResourceSpec resourceSpec, Map<IOzoneObj, Set<ACLType>> objToAclsMap) {
    for (S3Action action : mappedS3Actions) {
      if (action.kind == ActionKind.OBJECT) {
        addAclsForObj(objToAclsMap, volumeObj(volumeName), action.volumePerms);
        addAclsForObj(objToAclsMap, bucketObj(volumeName, resourceSpec.bucket), action.bucketPerms);
        addAclsForObj(objToAclsMap, keyObj(volumeName, resourceSpec.bucket, resourceSpec.key), action.objectPerms);
      } else if (action == S3Action.ALL_S3) {
        addAclsForObj(objToAclsMap, volumeObj(volumeName), EnumSet.of(READ));
        // For s3:*, ALL should only apply at the object level; grant READ at bucket level for navigation
        addAclsForObj(objToAclsMap, bucketObj(volumeName, resourceSpec.bucket), EnumSet.of(READ));
        addAclsForObj(objToAclsMap, keyObj(volumeName, resourceSpec.bucket, resourceSpec.key), action.objectPerms);
      }
    }
  }

  /**
   * Handles OBJECT_PREFIX and OBJECT_PREFIX_WILDCARD resource types.
   * Prefixes can be specified in the Resource itself as in the example below, or via an s3:prefix Condition.
   * Example: "Resource": "arn:aws:s3:::my-bucket/path/folder"
   */
  private static void processObjectPrefixResource(String volumeName, AuthorizerType authorizerType,
      Set<S3Action> mappedS3Actions, ResourceSpec resourceSpec, Map<IOzoneObj, Set<ACLType>> objToAclsMap) {
    for (S3Action action : mappedS3Actions) {
      // Object actions apply to prefix/key resources
      if (action.kind == ActionKind.OBJECT) {
        addAclsForObj(objToAclsMap, volumeObj(volumeName), action.volumePerms);
        addAclsForObj(objToAclsMap, bucketObj(volumeName, resourceSpec.bucket), action.bucketPerms);
      } else if (action == S3Action.ALL_S3) {
        addAclsForObj(objToAclsMap, volumeObj(volumeName), EnumSet.of(READ));
        // For s3:*, ALL should only apply at the object/prefix level; grant READ at bucket level for navigation
        addAclsForObj(objToAclsMap, bucketObj(volumeName, resourceSpec.bucket), EnumSet.of(READ));
      }

      // Handle the resource prefix itself (e.g., my-bucket/*)
      createObjectResourcesFromResourcePrefix(
          volumeName, authorizerType, resourceSpec, objToAclsMap, action.objectPerms);
    }
  }

  /**
   * Creates object resources from resource prefix (e.g., my-bucket/*).
   */
  private static void createObjectResourcesFromResourcePrefix(String volumeName, AuthorizerType authorizerType,
      ResourceSpec resourceSpec, Map<IOzoneObj, Set<ACLType>> objToAclsMap, Set<ACLType> acls) {
    if (authorizerType == AuthorizerType.NATIVE) {
      final IOzoneObj prefixObj = prefixObj(volumeName, resourceSpec.bucket, resourceSpec.prefix);
      addAclsForObj(objToAclsMap, prefixObj, acls);
    } else {
      final IOzoneObj keyObj = keyObj(volumeName, resourceSpec.bucket, resourceSpec.prefix);
      addAclsForObj(objToAclsMap, keyObj, acls);
    }
  }

  /**
   * Creates object resources from condition prefixes (i.e. the s3:prefix conditions).
   */
  private static void createObjectResourcesFromConditionPrefix(String volumeName, AuthorizerType authorizerType,
      ResourceSpec resourceSpec, String conditionPrefix, Map<IOzoneObj, Set<ACLType>> objToAclsMap, Set<ACLType> acls) {
    if (authorizerType == AuthorizerType.NATIVE) {
      // For native authorizer, use PREFIX resource type with normalized prefix.
      // Map "x" in condition list prefix to "x". Map "x/*" in condition list prefix to "x/".
      // Map "*" in condition list prefix to "".
      final String normalizedPrefix;
      if (conditionPrefix != null && conditionPrefix.endsWith("*")) {
        normalizedPrefix = conditionPrefix.substring(0, conditionPrefix.length() - 1);
      } else {
        normalizedPrefix = conditionPrefix;
      }
      final IOzoneObj prefixObj = prefixObj(volumeName, resourceSpec.bucket, normalizedPrefix);
      addAclsForObj(objToAclsMap, prefixObj, acls);
    } else {
      // For Ranger authorizer, use KEY resource type with original prefix
      // Map "x" in condition list prefix to "x".  Map "x/*" in condition list prefix to "x/*".
      // Map "* in condition list prefix to "*".
      final IOzoneObj keyObj = keyObj(volumeName, resourceSpec.bucket, conditionPrefix);
      addAclsForObj(objToAclsMap, keyObj, acls);
    }
  }

  /**
   * Helper method to add ACLs for an IOzoneObj, merging with existing ACLs if present.
   * If ALL permission is present, no other permissions are added.
   */
  private static void addAclsForObj(Map<IOzoneObj, Set<ACLType>> objToAclsMap, IOzoneObj obj, Set<ACLType> acls) {
    if (acls != null && !acls.isEmpty()) {
      final OzoneObj ozoneObj = (OzoneObj) obj;
      final Set<ACLType> existingAcls = objToAclsMap.computeIfAbsent(ozoneObj, k -> EnumSet.noneOf(ACLType.class));

      // If ALL is already present, don't add other permissions
      if (existingAcls.contains(ACLType.ALL)) {
        return;
      }

      // If we're about to add ALL, remove all other permissions first
      if (acls.contains(ACLType.ALL)) {
        existingAcls.clear();
        existingAcls.add(ACLType.ALL);
      } else {
        // Only add permissions if ALL is not already present
        existingAcls.addAll(acls);
      }
    }
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
   * The categorization possibilities of Resources in the IAM policy.
   */
  @VisibleForTesting
  enum S3ResourceType {
    ANY,                    // Ranger authorizer solely uses this
    BUCKET,
    BUCKET_WILDCARD,        // Ranger authorizer solely uses this
    OBJECT_PREFIX,          // Native authorizer solely uses this
    OBJECT_PREFIX_WILDCARD, // Ranger authorizer solely uses this. We initially categorize all resources with
                            // wildcard (*) as OBJECT_PREFIX_WILDCARD, but if the wildcard is not at the end, and
                            // Native authorizer is being used, an error is thrown.  If the wildcard is at the end,
                            // then the categorization will use OBJECT_PREFIX for native authorizer instead and remove
                            // the wildcard.
    OBJECT_EXACT
  }

  /**
   * Utility to help categorize IAM policy resources, whether for bucket, key, wildcards, etc.
   */
  @VisibleForTesting
  static final class ResourceSpec {
    private final S3ResourceType type;
    private final String bucket;
    private final String prefix; // for OBJECT_PREFIX or OBJECT_PREFIX_WILDCARD only, otherwise null
    private final String key; // for OBJECT_EXACT only, otherwise null

    @VisibleForTesting
    ResourceSpec(S3ResourceType type, String bucket, String prefix, String key) {
      this.type = type;
      this.bucket = bucket;
      this.prefix = prefix;
      this.key = key;
    }

    static ResourceSpec any() {
      return new ResourceSpec(S3ResourceType.ANY, "*", null, null);
    }

    static ResourceSpec bucket(String bucket) {
      return new ResourceSpec(
          bucket.contains("*") ? S3ResourceType.BUCKET_WILDCARD : S3ResourceType.BUCKET, bucket, null, null);
    }

    static ResourceSpec objectExact(String bucket, String key) {
      return new ResourceSpec(S3ResourceType.OBJECT_EXACT, bucket, null, key);
    }

    static ResourceSpec objectPrefix(String bucket, String prefix) {
      return new ResourceSpec(
          prefix.contains("*") ? S3ResourceType.OBJECT_PREFIX_WILDCARD : S3ResourceType.OBJECT_PREFIX, bucket,
          prefix, null);
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final ResourceSpec that = (ResourceSpec) o;
      return type == that.type && Objects.equals(bucket, that.bucket) && Objects.equals(prefix, that.prefix) &&
          Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, bucket, prefix, key);
    }

    @Override
    public String toString() {
      return "ResourceSpec{" + "type=" + type + ", bucket='" + bucket + '\'' + ", prefix='" + prefix + '\'' +
          ", key='" + key + '\'' + '}';
    }
  }

  /**
   * Parses and categorizes the ResourceArn.
   * <p>
   * Suffix parameter can be:
   * -> bucket
   * -> bucket/* (prefix in OzoneNativeAuthorizer or wildcard key in RangerOzoneAuthorizer)
   * -> bucket/deep/path/* (prefix in OzoneNativeAuthorizer or wildcard key in RangerOzoneAuthorizer)
   * -> bucket/key or bucket/prefix/key (exact key)
   */
  private static ResourceSpec parseResourceSpec(String suffix) {

    final int slashIndex = suffix.indexOf('/');
    if (slashIndex < 0) {
      return ResourceSpec.bucket(suffix);
    }

    final String bucket = suffix.substring(0, slashIndex);
    final String rest = suffix.substring(slashIndex + 1);
    if (rest.contains("*")) {
      return ResourceSpec.objectPrefix(bucket, rest);
    }

    return ResourceSpec.objectExact(bucket, rest);
  }

  @VisibleForTesting
  enum S3Action {
    // Volume-scope
    // Used for ListBuckets api
    LIST_ALL_MY_BUCKETS("s3:ListAllMyBuckets", ActionKind.VOLUME, EnumSet.of(READ, LIST),
        EnumSet.noneOf(ACLType.class), EnumSet.noneOf(ACLType.class)),

    // Bucket-scope
    CREATE_BUCKET("s3:CreateBucket", ActionKind.BUCKET, EnumSet.of(READ), EnumSet.of(CREATE),
        EnumSet.noneOf(ACLType.class)),
    DELETE_BUCKET("s3:DeleteBucket", ActionKind.BUCKET, EnumSet.of(READ), EnumSet.of(DELETE),
        EnumSet.noneOf(ACLType.class)),
    GET_BUCKET_ACL("s3:GetBucketAcl", ActionKind.BUCKET, EnumSet.of(READ), EnumSet.of(READ, READ_ACL),
        EnumSet.noneOf(ACLType.class)),
    GET_BUCKET_LOCATION("s3:GetBucketLocation", ActionKind.BUCKET, EnumSet.of(READ), EnumSet.of(READ),
        EnumSet.noneOf(ACLType.class)),
    // Used for HeadBucket, ListObjects and ListObjectsV2 apis
    LIST_BUCKET("s3:ListBucket", ActionKind.BUCKET, EnumSet.of(READ), EnumSet.of(READ, LIST), EnumSet.of(READ)),
    // Used for ListMultipartUploads API
    LIST_BUCKET_MULTIPART_UPLOADS("s3:ListBucketMultipartUploads", ActionKind.BUCKET, EnumSet.of(READ),
        EnumSet.of(READ, LIST), EnumSet.noneOf(ACLType.class)),
    PUT_BUCKET_ACL("s3:PutBucketAcl", ActionKind.BUCKET, EnumSet.of(READ), EnumSet.of(WRITE_ACL),
        EnumSet.noneOf(ACLType.class)),

    // Object-scope
    ABORT_MULTIPART_UPLOAD("s3:AbortMultipartUpload", ActionKind.OBJECT, EnumSet.of(READ), EnumSet.of(READ),
        EnumSet.of(DELETE)),
    // Used for DeleteObject (when versionId parameter is not supplied),
    // DeleteObjects (when versionId parameter is not supplied) APIs
    DELETE_OBJECT("s3:DeleteObject", ActionKind.OBJECT, EnumSet.of(READ), EnumSet.of(READ), EnumSet.of(DELETE)),
    DELETE_OBJECT_TAGGING("s3:DeleteObjectTagging", ActionKind.OBJECT, EnumSet.of(READ), EnumSet.of(READ),
        EnumSet.of(DELETE)),
    // Used for HeadObject, CopyObject (for source bucket), GetObject (without versionId parameter) APIs
    GET_OBJECT("s3:GetObject", ActionKind.OBJECT, EnumSet.of(READ), EnumSet.of(READ), EnumSet.of(READ)),
    GET_OBJECT_TAGGING("s3:GetObjectTagging", ActionKind.OBJECT, EnumSet.of(READ), EnumSet.of(READ), EnumSet.of(READ)),
    // Used for ListParts API
    LIST_MULTIPART_UPLOAD_PARTS("s3:ListMultipartUploadParts", ActionKind.OBJECT, EnumSet.of(READ), EnumSet.of(READ),
        EnumSet.of(READ)),
    // Used for CreateMultipartUpload, UploadPart, CompleteMultipartUpload,
    // CopyObject (for destination bucket), PutObject APIs
    PUT_OBJECT("s3:PutObject", ActionKind.OBJECT, EnumSet.of(READ), EnumSet.of(READ),
        EnumSet.of(CREATE, ACLType.WRITE)),
    PUT_OBJECT_TAGGING("s3:PutObjectTagging", ActionKind.OBJECT, EnumSet.of(READ), EnumSet.of(READ),
        EnumSet.of(ACLType.WRITE)),

    // Wildcard all
    ALL_S3("s3:*", ActionKind.ALL, EnumSet.of(READ, LIST), EnumSet.of(ACLType.ALL), EnumSet.of(ACLType.ALL));

    private final String name;
    private final ActionKind kind;
    private final Set<ACLType> volumePerms;
    private final Set<ACLType> bucketPerms;
    private final Set<ACLType> objectPerms;

    S3Action(String name, ActionKind kind, Set<ACLType> volumePerms, Set<ACLType> bucketPerms,
        Set<ACLType> objectPerms) {
      this.name = name;
      this.kind = kind;
      this.volumePerms = volumePerms;
      this.bucketPerms = bucketPerms;
      this.objectPerms = objectPerms;
    }
  }

  /**
   * Creates an OzoneObjInfo.Builder based on supplied parameters.
   */
  private static OzoneObjInfo.Builder obj(OzoneObj.ResourceType type, String volumeName, String bucketName) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(type)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .setBucketName(bucketName);
  }

  /**
   * Creates IOzoneObj with ResourceType BUCKET.
   */
  private static IOzoneObj bucketObj(String volumeName, String bucketName) {
    return obj(OzoneObj.ResourceType.BUCKET, volumeName, bucketName).build();
  }

  /**
   * Creates IOzoneObj with ResourceType KEY.
   */
  private static IOzoneObj keyObj(String volumeName, String bucketName, String keyName) {
    return obj(OzoneObj.ResourceType.KEY, volumeName, bucketName)
        .setKeyName(keyName)
        .build();
  }

  /**
   * Creates IOzoneObj with ResourceType PREFIX.
   */
  private static IOzoneObj prefixObj(String volumeName, String bucketName, String prefixName) {
    return obj(OzoneObj.ResourceType.PREFIX, volumeName, bucketName)
        .setPrefixName(prefixName)
        .build();
  }

  /**
   * Creates IOzoneObj with ResourceType VOLUME.
   */
  private static IOzoneObj volumeObj(String volumeName) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.VOLUME)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .build();
  }
}
