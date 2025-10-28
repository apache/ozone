/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.security.acl.iam;

import static java.util.Collections.singleton;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;

/**
 * Resolves a limited subset of AWS IAM session policies into Ozone ACL grants,
 * according to either the RangerOzoneAuthorizer or OzoneNativeAuthorizer constructs.
 * RangerOzoneAuthorizer doesn't currently use ResourceType.PREFIX for example,
 * whereas OzoneNativeAuthorizer does.  Also, OzoneNativeAuthorizer doesn't allow
 * wildcards (*) in bucket names, whereas RangerOzoneAuthorizer does.
 * <p>
 * The only supported ResourceArn has prefix arn:aws:s3::: - all others will throw
 * UnsupportedOperationException.
 * <p>
 * The only supported Condition operator is StringEquals - all others will throw
 * UnsupportedOperationException.  Furthermore, only one Condition is supported in a
 * statement.
 * <p>
 * The only supported Condition operation is s3:prefix - all others will throw
 * UnsupportedOperationException.
 * <p>
 * The only supported Effect is Allow - all others will be silently ignored.
 * <p>
 * If a (currently) unsupported S3 action is requested, such as s3:GetAccelerateConfiguration,
 * it will be silently ignored.
 * <p>
 * Supported wildcard expansions in Actions are: s3:*, s3:Get*, s3:Put*, s3:List*,
 * s3:Create*, and s3:Delete*.
 */
public final class IamSessionPolicyResolver {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String AWS_S3_ARN_PREFIX = "arn:aws:s3:::";

  private static final int MAX_NUM_OF_STATEMENTS = 100;
  private static final int MAX_NUM_OF_ACTIONS = 100;
  private static final int MAX_NUM_OF_RESOURCES = 100;

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
   *
   * @param policyJson     the IAM session policy
   * @param volumeName     the volume under which the resource(s) live.  This may not be s3v in
   *                       multi-tenant scenarios
   * @param authorizerType whether the IOzoneObjs should be generated for use by the
   *                       RangerOzoneAuthorizer or the OzoneNativeAuthorizer
   * @return the data structure comprising the paths and permission pairings that
   * the session policy grants (if any)
   */
  public static Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolve(
      String policyJson,
      String volumeName,
      AuthorizerType authorizerType
  ) {

    validateInputParameters(policyJson, volumeName, authorizerType);

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> result =
        new LinkedHashSet<>();

    // Parse JSON into list of statements
    final List<JsonNode> statements = parseJsonAndRetrieveStatements(policyJson);

    for (JsonNode stmt : statements) {
      validateEffectInJsonStatement(stmt);

      final List<String> actions = readStringOrArray(stmt.get("Action"));
      if (actions.size() > MAX_NUM_OF_ACTIONS) {
        throw new IllegalArgumentException("Invalid policy JSON - too many Actions. Max is: " +
            MAX_NUM_OF_ACTIONS);
      }
      final List<String> resources = readStringOrArray(stmt.get("Resource"));
      if (resources.size() > MAX_NUM_OF_RESOURCES) {
        throw new IllegalArgumentException("Invalid policy JSON - too many Resources. Max is: " +
            MAX_NUM_OF_RESOURCES);
      }

      // Parse prefixes from conditions, if any
      final List<String> prefixes = parsePrefixesFromConditions(stmt);

      // Map actions to S3Action enum if possible
      final Set<S3Action> mappedS3Actions = mapPolicyActionsToS3Actions(actions);
      if (mappedS3Actions.isEmpty()) {
        // No actions recognized - no need to look at Resources for this Statement
        continue;
      }

      // Categorize resources according to bucket resource, object resource, etc
      final List<ResourceSpec> resourceSpecs = validateAndCategorizeResources(authorizerType, resources);

      // For each action, map to Ozone objects (paths) and acls based on resource specs and prefixes
      final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> stmtResults =
          createPathsAndPermissions(volumeName, authorizerType, mappedS3Actions, resourceSpecs, prefixes);

      result.addAll(stmtResults);
    }

    return result;
  }

  /**
   * Ensures required input parameters are supplied.
   */
  private static void validateInputParameters(String policyJson,
                                              String volumeName,
                                              AuthorizerType authorizerType) {
    if (StringUtils.isBlank(policyJson)) {
      throw new IllegalArgumentException("The IAM session policy JSON is required");
    }

    if (StringUtils.isBlank(volumeName)) {
      throw new IllegalArgumentException("The volume name is required");
    }

    Objects.requireNonNull(authorizerType, "The authorizer type is required");
  }

  /**
   * Parses IAM session policy and retrieve the statement(s).
   */
  private static List<JsonNode> parseJsonAndRetrieveStatements(String policyJson) {
    final JsonNode root;
    try {
      root = MAPPER.readTree(policyJson);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid policy JSON (most likely JSON structure is incorrect)", e);
    }

    final JsonNode statementsNode = root.path("Statement");
    if (statementsNode.isMissingNode()) {
      throw new IllegalArgumentException("Invalid policy JSON - missing Statement");
    }
    final List<JsonNode> statements = new ArrayList<>();

    if (statementsNode.size() > MAX_NUM_OF_STATEMENTS) {
      throw new IllegalArgumentException("Invalid policy JSON - too many Statements. Max is: " +
          MAX_NUM_OF_STATEMENTS);
    }

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
  private static void validateEffectInJsonStatement(JsonNode statement) {
    final JsonNode effectNode = statement.get("Effect");
    if (effectNode != null) {
      if (effectNode.isTextual()) {
        final String effect = effectNode.asText();
        if (!"Allow".equalsIgnoreCase(effect)) {
          throw new UnsupportedOperationException("Unsupported Effect - " + effect);
        }
        return;
      }

      throw new IllegalArgumentException("Invalid Effect in JSON policy (must be a String) - " +
          effectNode
      );
    }

    throw new IllegalArgumentException("Effect is missing from JSON policy");
  }

  /**
   * Reads a JsonNode and converts to a List of String, if the node represents
   * a textual value or an array of textual values.  Otherwise, returns
   * an empty List.
   */
  private static List<String> readStringOrArray(JsonNode node) {
    if (node == null || node.isMissingNode() || node.isNull()) {
      return Collections.emptyList();
    }
    if (node.isTextual()) {
      return Collections.singletonList(node.asText());
    }
    if (node.isArray()) {
      final List<String> list = new ArrayList<>();
      node.forEach(n -> {
        if (n.isTextual()) {
          list.add(n.asText());
        }
      });
      return list;
    }

    return Collections.emptyList();
  }

  /**
   * Parses and returns prefixes from Conditions (if any).  Also validates
   * that if there is a Condition, there is only one and that the Condition
   * operator and attribute are supported.
   * <p>
   * Only the StringEquals operator and s3:prefix attribute are supported.
   */
  private static List<String> parsePrefixesFromConditions(JsonNode stmt) {
    List<String> prefixes = Collections.emptyList();
    final JsonNode cond = stmt.get("Condition");
    if (cond != null && !cond.isMissingNode() && !cond.isNull()) {
      if (cond.size() != 1) {
        throw new UnsupportedOperationException("Only one Condition is supported");
      }

      if (!cond.isObject()) {
        throw new IllegalArgumentException("Invalid Condition (must have operator StringEquals " +
            "and attribute s3:prefix) - " + cond
        );
      }

      final String operator = cond.fieldNames().next();
      if (!"StringEquals".equals(operator)) {
        throw new UnsupportedOperationException("Unsupported Condition operator - " + operator);
      }

      final JsonNode attribute = cond.get("StringEquals");
      if ("null".equals(attribute.asText())) {
        throw new IllegalArgumentException("Missing Condition attribute - StringEquals");
      }

      if (!attribute.isObject()) {
        throw new IllegalArgumentException("Invalid Condition attribute structure - " + attribute);
      }

      final String attributeFieldName = attribute.fieldNames().hasNext() ? attribute.fieldNames().next() : null;
      if (!"s3:prefix".equals(attributeFieldName)) {
        throw new UnsupportedOperationException("Unsupported Condition attribute - " + attributeFieldName);
      }

      prefixes = readStringOrArray(attribute.get("s3:prefix"));
    }

    return prefixes;
  }

  /**
   * Maps actions from JSON IAM policy to S3Action enum in order to determine what the
   * permissions should be.
   */
  private static Set<S3Action> mapPolicyActionsToS3Actions(List<String> actions) {
    if (actions == null || actions.isEmpty()) {
      return Collections.emptySet();
    }

    // Preprocess the S3Action enum values get the S3Actions keyed by the action
    // name or grouped by prefixes like s3:Get* or s3:Put*
    final Map<String, Set<S3Action>> s3ActionMap = new LinkedHashMap<>();
    for (S3Action sa : S3Action.values()) {
      s3ActionMap.put(sa.name, singleton(sa));

      // Also group into s3:Get*, s3:Put*, s3:List* depending on the action name
      if (sa.name.startsWith("s3:Get")) {
        s3ActionMap.computeIfAbsent("s3:Get*", k -> new LinkedHashSet<>()).add(sa);
      } else if (sa.name.startsWith("s3:Put")) {
        s3ActionMap.computeIfAbsent("s3:Put*", k -> new LinkedHashSet<>()).add(sa);
      } else if (sa.name.startsWith("s3:List")) {
        s3ActionMap.computeIfAbsent("s3:List*", k -> new LinkedHashSet<>()).add(sa);
      } else if (sa.name.startsWith("s3:Delete")) {
        s3ActionMap.computeIfAbsent("s3:Delete*", k -> new LinkedHashSet<>()).add(sa);
      } else if (sa.name.startsWith("s3:Create")) {
        s3ActionMap.computeIfAbsent("s3:Create*", k -> new LinkedHashSet<>()).add(sa);
      }
    }

    // Map the actions from the IAM policy to S3Action
    final Set<S3Action> mappedActions = new LinkedHashSet<>();
    for (String action : actions) {
      if ("s3:*".equalsIgnoreCase(action)) {
        return EnumSet.of(S3Action.ALL_S3);
      }

      // Unsupported actions are silently ignored
      if (s3ActionMap.containsKey(action)) {
        mappedActions.addAll(s3ActionMap.get(action));
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
  private static List<ResourceSpec> validateAndCategorizeResources(AuthorizerType authorizerType,
                                                                   List<String> resources) {
    final List<ResourceSpec> resourceSpecs = new ArrayList<>();
    for (String resource : resources) {
      if ("*".equals(resource)) {
        if (authorizerType == AuthorizerType.NATIVE) {
          throw new UnsupportedOperationException("Wildcard bucket patterns are not " +
              "supported for Ozone native authorizer");
        }
        resourceSpecs.add(ResourceSpec.any());
        continue;
      }

      if (!resource.startsWith(AWS_S3_ARN_PREFIX)) {
        throw new UnsupportedOperationException("Unsupported Resource Arn - " + resource);
      }

      final String suffix = resource.substring(AWS_S3_ARN_PREFIX.length());
      if (suffix.isEmpty()) {
        throw new IllegalArgumentException("Invalid Resource Arn - " + resource);
      }

      ResourceSpec spec = parseResourceSpec(suffix);
      if (authorizerType == AuthorizerType.NATIVE && spec.type == S3ResourceType.BUCKET_WILDCARD) {
        throw new UnsupportedOperationException("Wildcard bucket patterns are not " +
            "supported for Ozone native authorizer");
      }

      // This scenario can happen in the case of arn:aws:s3:::*/* or arn:aws:s3:::*/test.txt for
      // examples
      if (authorizerType == AuthorizerType.NATIVE && spec.bucket.contains("*")) {
        throw new UnsupportedOperationException("Wildcard bucket patterns are not " +
            "supported for Ozone native authorizer");
      }

      if (authorizerType == AuthorizerType.NATIVE && spec.type == S3ResourceType.OBJECT_PREFIX_WILDCARD) {
        if (spec.prefix.endsWith("*")) {
          spec = ResourceSpec.objectPrefix(spec.bucket,
              spec.prefix.substring(0, spec.prefix.length() - 1)
          );
        } else {
          throw new UnsupportedOperationException("Wildcard prefix patterns are not " +
              "supported for Ozone native authorizer if wildcard is not at the end");
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
  private static Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> createPathsAndPermissions(
      String volumeName,
      AuthorizerType authorizerType,
      Set<S3Action> mappedS3Actions,
      List<ResourceSpec> resourceSpecs,
      List<String> prefixes
  ) {

    // Create map to collect IOzoneObj to ACLType mappings
    final Map<IOzoneObj, Set<ACLType>> objToAclsMap = new LinkedHashMap<>();

    // Canonicalization map to deduplicate logically equivalent IOzoneObj
    // without relying on equals/hashCode
    final Map<String, IOzoneObj> canonicalObjBySignature = new LinkedHashMap<>();

    // Process each resource spec with the given actions
    for (ResourceSpec resourceSpec : resourceSpecs) {
      processResourceSpecWithActions(volumeName,
          authorizerType,
          mappedS3Actions,
          resourceSpec,
          prefixes,
          canonicalObjBySignature,
          objToAclsMap
      );
    }

    // Group objects by their ACL sets to create proper entries
    return groupObjectsByAcls(objToAclsMap);
  }

  /**
   * Groups objects by their ACL sets.
   */
  private static Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> groupObjectsByAcls(
      Map<IOzoneObj, Set<ACLType>> objToAclsMap
  ) {

    final Map<GroupingKey, Set<IOzoneObj>> groupMap = new LinkedHashMap<>();

    for (Map.Entry<IOzoneObj, Set<ACLType>> entry : objToAclsMap.entrySet()) {
      final IOzoneObj obj = entry.getKey();
      final Set<ACLType> acls = entry.getValue();

      // Create grouping key based on ACLs and resource type
      final GroupingKey key = new GroupingKey(acls, ((OzoneObj)obj).getResourceType());
      groupMap.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(obj);
    }

    // Convert to result format
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> result =
        new LinkedHashSet<>();
    for (Map.Entry<GroupingKey, Set<IOzoneObj>> entry : groupMap.entrySet()) {
      final GroupingKey key = entry.getKey();
      final Set<IOzoneObj> objs = entry.getValue();

      if (!key.acls.isEmpty() && !objs.isEmpty()) {
        result.add(new AbstractMap.SimpleImmutableEntry<>(objs, key.acls));
      }
    }

    return result;
  }

  /**
   * Key for grouping objects by ACLs and resource type.
   */
  private static class GroupingKey {
    private final Set<ACLType> acls;
    private final OzoneObj.ResourceType resourceType;
    
    GroupingKey(Set<ACLType> acls, OzoneObj.ResourceType resourceType) {
      this.acls = acls;
      this.resourceType = resourceType;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final GroupingKey that = (GroupingKey) obj;
      return Objects.equals(acls, that.acls) && resourceType == that.resourceType;
    }
    
    @Override
    public int hashCode() {
      return Objects.hash(acls, resourceType);
    }
  }

  /**
   * Processes a single ResourceSpec with given actions and adds resulting
   * IOzoneObj to ACLType mappings to the provided map.
   */
  private static void processResourceSpecWithActions(String volumeName,
                                                     AuthorizerType authorizerType,
                                                     Set<S3Action> mappedS3Actions,
                                                     ResourceSpec resourceSpec,
                                                     List<String> prefixes,
                                                     Map<String, IOzoneObj> canonicalObjBySignature,
                                                     Map<IOzoneObj, Set<ACLType>> objToAclsMap) {
    
    // Process based on ResourceSpec type
    switch (resourceSpec.type) {
    case ANY:
      processResourceTypeAny(volumeName,
          mappedS3Actions,
          canonicalObjBySignature,
          objToAclsMap
      );
      break;
    case BUCKET:
    case BUCKET_WILDCARD:
      processBucketResource(volumeName,
          mappedS3Actions,
          resourceSpec,
          canonicalObjBySignature,
          objToAclsMap
      );
      break;
    case OBJECT_EXACT:
      processObjectExactResource(volumeName,
          mappedS3Actions,
          resourceSpec,
          canonicalObjBySignature,
          objToAclsMap
      );
      break;
    case OBJECT_PREFIX:
    case OBJECT_PREFIX_WILDCARD:
      processObjectPrefixResource(volumeName,
          authorizerType,
          mappedS3Actions,
          resourceSpec,
          prefixes,
          canonicalObjBySignature,
          objToAclsMap
      );
      break;
    default:
      throw new IllegalStateException("Unexpected resourceSpec type found: " + resourceSpec.type);
    }
  }

  /**
   * Handles ResourceType.ANY (*).
   * Example: "Resource": "*"
   */
  private static void processResourceTypeAny(String volumeName,
                                             Set<S3Action> mappedS3Actions,
                                             Map<String, IOzoneObj> canonicalObjBySignature,
                                             Map<IOzoneObj, Set<ACLType>> objToAclsMap) {
    
    for (S3Action action : mappedS3Actions) {
      if (action.kind == ActionKind.VOLUME || action == S3Action.ALL_S3) {
        final IOzoneObj volumeObj = volumeObj(volumeName);
        final Set<ACLType> volumeAcls = action == S3Action.ALL_S3 ?
            EnumSet.of(ACLType.ALL) :
            action.volumePerms;
        addAclsForObj(objToAclsMap, canonicalObjBySignature, volumeObj, volumeAcls);
      }

      if (action.kind == ActionKind.BUCKET || action == S3Action.ALL_S3) {
        final IOzoneObj bucketObj = bucketObj(volumeName, "*");
        final Set<ACLType> bucketAcls = action == S3Action.ALL_S3 ?
            EnumSet.of(ACLType.ALL) :
            action.bucketPerms;
        addAclsForObj(objToAclsMap, canonicalObjBySignature, bucketObj, bucketAcls);
      }

      if (action.kind == ActionKind.OBJECT || action == S3Action.ALL_S3) {
        final IOzoneObj keyObj = keyObj(volumeName, "*", "*");
        final Set<ACLType> objectAcls = action == S3Action.ALL_S3 ?
            EnumSet.of(ACLType.ALL) :
            action.objectPerms;
        addAclsForObj(objToAclsMap, canonicalObjBySignature, keyObj, objectAcls);
      }
    }
  }

  /**
   * Handles BUCKET and BUCKET_WILDCARD resource types.
   * Example: "Resource": "arn:aws:s3:::my-bucket"
   */
  private static void processBucketResource(String volumeName,
                                            Set<S3Action> mappedS3Actions,
                                            ResourceSpec resourceSpec,
                                            Map<String, IOzoneObj> canonicalObjBySignature,
                                            Map<IOzoneObj, Set<ACLType>> objToAclsMap) {
    for (S3Action action : mappedS3Actions) {
      if (action.kind == ActionKind.BUCKET || action == S3Action.ALL_S3) {
        final IOzoneObj bucketObj = bucketObj(volumeName, resourceSpec.bucket);
        final Set<ACLType> bucketAcls = action == S3Action.ALL_S3 ?
            EnumSet.of(ACLType.ALL) :
            action.bucketPerms;
        addAclsForObj(objToAclsMap, canonicalObjBySignature, bucketObj, bucketAcls);
      }
    }
  }

  /**
   * Handles OBJECT_EXACT resource type.
   * Example: "Resource": "arn:aws:s3:::my-bucket/file.txt"
   */
  private static void processObjectExactResource(String volumeName,
                                                 Set<S3Action> mappedS3Actions,
                                                 ResourceSpec resourceSpec,
                                                 Map<String, IOzoneObj> canonicalObjBySignature,
                                                 Map<IOzoneObj, Set<ACLType>> objToAclsMap) {
    
    for (S3Action action : mappedS3Actions) {
      if (action.kind == ActionKind.OBJECT || action == S3Action.ALL_S3) {
        final IOzoneObj keyObj = keyObj(volumeName, resourceSpec.bucket, resourceSpec.key);
        final Set<ACLType> objectAcls = action == S3Action.ALL_S3 ?
            EnumSet.of(ACLType.ALL) :
            action.objectPerms;
        addAclsForObj(objToAclsMap, canonicalObjBySignature, keyObj, objectAcls);
      }
    }
  }

  /**
   * Handles OBJECT_PREFIX and OBJECT_PREFIX_WILDCARD resource types.
   * Example: "Resource": "arn:aws:s3:::my-bucket/path/folder"
   */
  private static void processObjectPrefixResource(String volumeName,
                                                  AuthorizerType authorizerType,
                                                  Set<S3Action> mappedS3Actions,
                                                  ResourceSpec resourceSpec,
                                                  List<String> prefixes,
                                                  Map<String, IOzoneObj> canonicalObjBySignature,
                                                  Map<IOzoneObj, Set<ACLType>> objToAclsMap) {

    for (S3Action action : mappedS3Actions) {
      // Object actions apply to prefix/key resources
      if (action.kind == ActionKind.OBJECT || action == S3Action.ALL_S3) {
        final Set<ACLType> objectAcls = action == S3Action.ALL_S3 ?
            EnumSet.of(ACLType.ALL) :
            action.objectPerms;

        if (prefixes != null && !prefixes.isEmpty()) {
          // Handle specific prefixes from conditions
          for (String prefix : prefixes) {
            createObjectResourcesFromConditionPrefix(volumeName,
                authorizerType,
                resourceSpec,
                prefix,
                canonicalObjBySignature,
                objToAclsMap,
                objectAcls
            );
          }
        } else {
          // Handle the resource prefix itself (e.g., my-bucket/*)
          createObjectResourcesFromResourcePrefix(volumeName,
              authorizerType,
              resourceSpec,
              canonicalObjBySignature,
              objToAclsMap,
              objectAcls
          );
        }
      }
    }
  }

  /**
   * Creates object resources from resource prefix (e.g., my-bucket/*).
   */
  private static void createObjectResourcesFromResourcePrefix(
      String volumeName,
      AuthorizerType authorizerType,
      ResourceSpec resourceSpec,
      Map<String, IOzoneObj> canonicalObjBySignature,
      Map<IOzoneObj, Set<ACLType>> objToAclsMap,
      Set<ACLType> acls
  ) {
    
    if (authorizerType == AuthorizerType.NATIVE) {
      final IOzoneObj prefixObj = prefixObj(volumeName, resourceSpec.bucket, resourceSpec.prefix);
      addAclsForObj(objToAclsMap, canonicalObjBySignature, prefixObj, acls);
    } else {
      final IOzoneObj keyObj = keyObj(volumeName, resourceSpec.bucket, resourceSpec.prefix);
      addAclsForObj(objToAclsMap, canonicalObjBySignature, keyObj, acls);
    }
  }

  /**
   * Creates object resources from condition prefixes (i.e. the s3:prefix conditions).
   */
  private static void createObjectResourcesFromConditionPrefix(
      String volumeName,
      AuthorizerType authorizerType,
      ResourceSpec resourceSpec,
      String conditionPrefix,
      Map<String, IOzoneObj> canonicalObjBySignature,
      Map<IOzoneObj, Set<ACLType>> objToAclsMap,
      Set<ACLType> acls
  ) {
    
    if (authorizerType == AuthorizerType.NATIVE) {
      // For native authorizer, use PREFIX resource type with normalized prefix.
      // Map "x" in condition list prefix to "x". Map "x/*" in condition list prefix to "x/".
      final String normalizedPrefix;
      if (conditionPrefix != null && conditionPrefix.endsWith("/*")) {
        final String base = conditionPrefix.substring(0, conditionPrefix.length() - 2);
        normalizedPrefix = base + "/";
      } else {
        normalizedPrefix = conditionPrefix;
      }
      final IOzoneObj prefixObj = prefixObj(volumeName, resourceSpec.bucket, normalizedPrefix);
      addAclsForObj(objToAclsMap, canonicalObjBySignature, prefixObj, acls);
    } else {
      // For Ranger authorizer, use KEY resource type with original prefix
      // Map "x" in condition list prefix to "x".  Map "x/*" in condition list prefix to "x/*".
      final IOzoneObj keyObj = keyObj(volumeName, resourceSpec.bucket, conditionPrefix);
      addAclsForObj(objToAclsMap, canonicalObjBySignature, keyObj, acls);
    }
  }

  /**
   * Helper method to add ACLs for an IOzoneObj, merging with existing ACLs if present.
   */
  private static void addAclsForObj(Map<IOzoneObj, Set<ACLType>> objToAclsMap,
                                    Map<String, IOzoneObj> canonicalObjBySignature,
                                    IOzoneObj obj,
                                    Set<ACLType> acls) {
    if (acls != null && !acls.isEmpty()) {
      // Compute string representation for deduplication (resource type + store type + full path)
      final OzoneObj ozoneObj = (OzoneObj) obj;
      final String signature = ozoneObj.getResourceType() + "|" + ozoneObj.getStoreType() +
          "|" + ozoneObj.getPath();
      final IOzoneObj canonical = canonicalObjBySignature.computeIfAbsent(signature, s -> obj);
      objToAclsMap.computeIfAbsent(canonical, k -> EnumSet.noneOf(ACLType.class)).addAll(acls);
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

  private enum ActionKind {
    VOLUME,
    BUCKET,
    OBJECT,
    ALL
  }

  private enum S3ResourceType {
    ANY,
    BUCKET,
    BUCKET_WILDCARD,
    OBJECT_PREFIX,
    OBJECT_PREFIX_WILDCARD,
    OBJECT_EXACT
  }

  /**
   * Utility to help categorize IAM policy resources, whether for bucket, key, wildcards, etc.
   */
  private static final class ResourceSpec {
    private final S3ResourceType type;
    private final String bucket;
    private final String prefix; // for OBJECT_PREFIX or OBJECT_PREFIX_WILDCARD only, otherwise null
    private final String key; // for OBJECT_EXACT only, otherwise null

    private ResourceSpec(S3ResourceType type,
                         String bucket,
                         String prefix,
                         String key) {
      this.type = type;
      this.bucket = bucket;
      this.prefix = prefix;
      this.key = key;
    }

    static ResourceSpec any() {
      return new ResourceSpec(S3ResourceType.ANY, "*", null, null);
    }

    static ResourceSpec bucket(String bucket) {
      return new ResourceSpec(bucket.contains("*") ? S3ResourceType.BUCKET_WILDCARD :
          S3ResourceType.BUCKET,
          bucket,
          null,
          null
      );
    }

    static ResourceSpec objectExact(String bucket, String key) {
      return new ResourceSpec(S3ResourceType.OBJECT_EXACT, bucket, null, key);
    }

    static ResourceSpec objectPrefix(String bucket, String prefix) {
      return new ResourceSpec(prefix.contains("*") ? S3ResourceType.OBJECT_PREFIX_WILDCARD :
          S3ResourceType.OBJECT_PREFIX,
          bucket,
          prefix,
          null
      );
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

  /**
   * Represents S3 actions and requisite permissions required and at what level.
   */
  private enum S3Action {
    // Volume-scope
    // Used for ListBuckets api
    LIST_ALL_MY_BUCKETS("s3:ListAllMyBuckets",
        ActionKind.VOLUME,
        EnumSet.of(ACLType.READ, ACLType.LIST),
        EnumSet.noneOf(ACLType.class),
        EnumSet.noneOf(ACLType.class)
    ),

    // Bucket-scope
    CREATE_BUCKET("s3:CreateBucket",
        ActionKind.BUCKET,
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.CREATE),
        EnumSet.noneOf(ACLType.class)
    ),
    DELETE_BUCKET("s3:DeleteBucket",
        ActionKind.BUCKET,
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.DELETE),
        EnumSet.noneOf(ACLType.class)
    ),
    GET_BUCKET_ACL("s3:GetBucketAcl",
        ActionKind.BUCKET,
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.READ, ACLType.READ_ACL),
        EnumSet.noneOf(ACLType.class)
    ),
    GET_BUCKET_LOCATION("s3:GetBucketLocation",
        ActionKind.BUCKET,
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.READ),
        EnumSet.noneOf(ACLType.class)
    ),
    // Used for HeadBucket, ListObjects and ListObjectsV2 apis
    LIST_BUCKET("s3:ListBucket",
        ActionKind.BUCKET,
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.READ, ACLType.LIST),
        EnumSet.noneOf(ACLType.class)
    ),
    // Used for ListMultipartUploads API
    LIST_BUCKET_MULTIPART_UPLOADS(
        "s3:ListBucketMultipartUploads",
        ActionKind.BUCKET,
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.READ, ACLType.LIST),
        EnumSet.noneOf(ACLType.class)
    ),
    PUT_BUCKET_ACL("s3:PutBucketAcl",
        ActionKind.BUCKET,
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.WRITE_ACL),
        EnumSet.noneOf(ACLType.class)
    ),

    // Object-scope
    ABORT_MULTIPART_UPLOAD("s3:AbortMultipartUpload",
        ActionKind.OBJECT,
        EnumSet.noneOf(ACLType.class),
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.DELETE)
    ),
    // Used for DeleteObject (when versionId parameter is not supplied),
    // DeleteObjects (when versionId parameter is not supplied) APIs
    DELETE_OBJECT("s3:DeleteObject",
        ActionKind.OBJECT,
        EnumSet.noneOf(ACLType.class),
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.DELETE)
    ),
    DELETE_OBJECT_TAGGING("s3:DeleteObjectTagging",
        ActionKind.OBJECT,
        EnumSet.noneOf(ACLType.class),
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.DELETE)
    ),
    // Used for DeleteObject (when versionId parameter is supplied),
    // DeleteObjects (when versionId parameter is supplied) APIs
    DELETE_OBJECT_VERSION("s3:DeleteObjectVersion",
        ActionKind.OBJECT,
        EnumSet.noneOf(ACLType.class),
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.DELETE)
    ),
    // Used for HeadObject, CopyObject (for source bucket), GetObject (without versionId parameter) APIs
    GET_OBJECT("s3:GetObject",
        ActionKind.OBJECT,
        EnumSet.noneOf(ACLType.class),
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.READ)
    ),
    GET_OBJECT_TAGGING("s3:GetObjectTagging",
        ActionKind.OBJECT,
        EnumSet.noneOf(ACLType.class),
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.READ)
    ),
    // Used for GetObject API when versionId parameter is supplied
    GET_OBJECT_VERSION("s3:GetObjectVersion",
        ActionKind.OBJECT,
        EnumSet.noneOf(ACLType.class),
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.READ)
    ),
    // Used for ListParts API
    LIST_MULTIPART_UPLOAD_PARTS("s3:ListMultipartUploadParts",
        ActionKind.OBJECT,
        EnumSet.noneOf(ACLType.class),
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.READ)
    ),
    // Used for CreateMultipartUpload, UploadPart, CompleteMultipartUpload,
    // CopyObject (for destination bucket), PutObject APIs
    PUT_OBJECT("s3:PutObject",
        ActionKind.OBJECT,
        EnumSet.noneOf(ACLType.class),
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.CREATE, ACLType.WRITE)
    ),
    PUT_OBJECT_TAGGING("s3:PutObjectTagging",
        ActionKind.OBJECT,
        EnumSet.noneOf(ACLType.class),
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.WRITE)
    ),
    // Used for PutObjectTagging (with versionId parameter) API
    PUT_OBJECT_VERSION_TAGGING("s3:PutObjectVersionTagging",
        ActionKind.OBJECT,
        EnumSet.noneOf(ACLType.class),
        EnumSet.noneOf(ACLType.class),
        EnumSet.of(ACLType.WRITE)
    ),

    // Wildcard all
    ALL_S3("s3:*",
        ActionKind.ALL,
        EnumSet.of(ACLType.ALL),
        EnumSet.of(ACLType.ALL),
        EnumSet.of(ACLType.ALL)
    );

    private final String name;
    private final ActionKind kind;
    private final Set<ACLType> volumePerms;
    private final Set<ACLType> bucketPerms;
    private final Set<ACLType> objectPerms;

    S3Action(String name,
             ActionKind kind,
             Set<ACLType> volumePerms,
             Set<ACLType> bucketPerms,
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
  private static OzoneObjInfo.Builder obj(OzoneObj.ResourceType type,
                                          String volumeName,
                                          String bucketName) {
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


