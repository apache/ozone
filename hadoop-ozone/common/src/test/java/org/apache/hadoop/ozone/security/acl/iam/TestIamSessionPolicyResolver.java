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

import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.DELETE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ_ACL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE_ACL;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link IamSessionPolicyResolver}.
 * */
public class TestIamSessionPolicyResolver {

  private static final String VOLUME = "s3v";

  @Test
  public void testAllowGetPutOnKey() {
    final String json = "{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": [\"s3:GetObject\", \"s3:PutObject\"],\n" +
        "    \"Resource\": \"arn:aws:s3:::my-bucket/folder/file.txt\"\n" +
        "  }]\n" +
        "}";

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedFromBothAuthorizers =
        new LinkedHashSet<>();

    // Expected: READ, CREATE, WRITE on key
    final Set<IOzoneObj> keySet = objSet(key("my-bucket", "folder/file.txt"));

    final Set<ACLType> keyAcls = acls(READ, CREATE, WRITE);

    expectedResolvedFromBothAuthorizers.add(new AbstractMap.SimpleImmutableEntry<>(keySet, keyAcls));

    final Set<String> expectResolvedStringifiedSet = expectedResolvedFromBothAuthorizers
        .stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet());

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectResolvedStringifiedSet);

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectResolvedStringifiedSet);
  }

  @Test
  public void testAllActionsForKey() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:*\",\n" +
        "    \"Resource\": \"arn:aws:s3:::my-bucket/*\"\n" +
        "  }]\n" +
        "}";

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedNative =
        new LinkedHashSet<>();

    // Expected for native: all key ACLs on prefix "" under bucket
    final Set<IOzoneObj> keyPrefixSet = objSet(prefix("my-bucket", ""));

    final Set<ACLType> allKeyAcls = acls(ALL);
    
    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(keyPrefixSet, allKeyAcls));

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedNative.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );

    // Expected for Ranger: all key acls for resource type KEY with key name "*"
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();
    
    final Set<IOzoneObj> rangerKeySet = objSet(key("my-bucket", "*"));
    
    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(rangerKeySet, allKeyAcls));
    
    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testAllActionsForBucket() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:*\",\n" +
        "    \"Resource\": \"arn:aws:s3:::my-bucket\"\n" +
        "  }]\n" +
        "}";

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedFromBothAuthorizers =
        new LinkedHashSet<>();

    // Expected: all Bucket ACLs for bucket
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));

    final Set<ACLType> allBucketAcls = acls(ALL);

    expectedResolvedFromBothAuthorizers.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, allBucketAcls));

    final Set<String> expectResolvedStringifiedSet = expectedResolvedFromBothAuthorizers
        .stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet());

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectResolvedStringifiedSet);

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectResolvedStringifiedSet);
  }

  @Test
  public void testMultipleResourcesInSeparateStatements() {
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

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedNative =
        new LinkedHashSet<>();

    // Expected for native: bucket READ, LIST, READ_ACL, WRITE_ACL
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));

    final Set<ACLType> bucketAcls = acls(READ, LIST, READ_ACL, WRITE_ACL);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Expected for native: all key ACLs on prefix "" under bucket
    final Set<IOzoneObj> keyPrefixSet = objSet(prefix("my-bucket", ""));

    final Set<ACLType> keyAllAcls = acls(ALL);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(keyPrefixSet, keyAllAcls));

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedNative.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: bucket READ, LIST, READ_ACL, WRITE_ACL
    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Expected for Ranger: all key acls for resource type KEY with key name "*"
    final Set<IOzoneObj> rangerKeySet = objSet(key("my-bucket", "*"));

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(rangerKeySet, keyAllAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testMultipleResourcesInOneStatement() {
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

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedNative =
        new LinkedHashSet<>();

    // Expected for native: all bucket acls
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));

    final Set<ACLType> bucketAcls = acls(ALL);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Expected for native: all key ACLs on prefix "" under bucket
    final Set<IOzoneObj> keyPrefixSet = objSet(prefix("my-bucket", ""));

    final Set<ACLType> keyAllAcls = acls(ALL);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(keyPrefixSet, keyAllAcls));

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedNative.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: all bucket acls
    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Expected for Ranger: all key acls for resource type KEY with key name "*"
    final Set<IOzoneObj> rangerKeySet = objSet(key("my-bucket", "*"));

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(rangerKeySet, keyAllAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testMultipleResourcesWithDifferentBucketsAndDeepPathsInOneStatement() {
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

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedNative =
        new LinkedHashSet<>();

    // Expected for native: all key ACLs on prefix "team/folder1/security/" under
    // my-bucket and all key ACLs on prefix "team/folder2/misc/" under my-bucket2
    final Set<IOzoneObj> keyPrefixSet = objSet(
        prefix("my-bucket", "team/folder1/security/"),
        prefix("my-bucket2", "team/folder2/misc/")
    );

    final Set<ACLType> keyAllAcls = acls(ALL);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(keyPrefixSet, keyAllAcls));

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedNative.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: all key acls for resource type KEY with key name
    // "team/folder1/security/*" under my-bucket and "team/folder2/misc/*" under
    // my-bucket2
    final Set<IOzoneObj> rangerKeySet = objSet(
        key("my-bucket", "team/folder1/security/*"),
        key("my-bucket2", "team/folder2/misc/*")
    );

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(rangerKeySet, keyAllAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testUnsupportedActionIgnoredWhenItIsTheOnlyAction() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:ReplicateObject\",\n" +         // unsupported action
        "    \"Resource\": \"*\"\n" +
        "  }]\n" +
        "}";

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.isEmpty());

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.isEmpty());
  }

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

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (UnsupportedOperationException ex) {
      assertThat(ex.getMessage()).isEqualTo("Unsupported Condition operator - StringLike");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (UnsupportedOperationException ex) {
      assertThat(ex.getMessage()).isEqualTo("Unsupported Condition operator - StringLike");
    }
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

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (UnsupportedOperationException ex) {
      assertThat(ex.getMessage()).isEqualTo("Unsupported Condition attribute - aws:SourceArn");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (UnsupportedOperationException ex) {
      assertThat(ex.getMessage()).isEqualTo("Unsupported Condition attribute - aws:SourceArn");
    }
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

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (UnsupportedOperationException ex) {
      assertThat(ex.getMessage()).isEqualTo("Unsupported Resource Arn - " +
          "arn:aws:dynamodb:us-east-2:123456789012:table/example-table");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (UnsupportedOperationException ex) {
      assertThat(ex.getMessage()).isEqualTo("Unsupported Resource Arn - " +
          "arn:aws:dynamodb:us-east-2:123456789012:table/example-table");
    }
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

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (UnsupportedOperationException ex) {
      assertThat(ex.getMessage()).isEqualTo("Unsupported Effect - Deny");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (UnsupportedOperationException ex) {
      assertThat(ex.getMessage()).isEqualTo("Unsupported Effect - Deny");
    }
  }

  @Test
  public void testListBucketWithWildcard() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::proj-*\"\n" +
        "  }]\n" +
        "}";

    // Wildcards on bucket are not supported for Native authorizer
    expectBucketWildcardUnsupportedExceptionForNativeAuthorizer(json);

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: bucket READ and LIST on wildcard pattern
    final Set<IOzoneObj> bucketSet = objSet(bucket("proj-*"));

    final Set<ACLType> bucketAcls = acls(READ, LIST);

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testListBucketOperationsWithNoPrefixes() {
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

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedFromBothAuthorizers =
        new LinkedHashSet<>();

    // Expected: bucket READ and LIST
    final Set<IOzoneObj> bucketSet = objSet(bucket("proj"));

    final Set<ACLType> bucketAcls = acls(READ, LIST);

    expectedResolvedFromBothAuthorizers.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    final Set<String> expectResolvedStringifiedSet = expectedResolvedFromBothAuthorizers
        .stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet());

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectResolvedStringifiedSet);

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectResolvedStringifiedSet);
  }

  @Test
  public void testIgnoresUnsupportedActionsWhenSupportedActionsAreIncluded() {
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

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedNative =
        new LinkedHashSet<>();

    // Expected for native: READ, LIST, READ_ACL bucket acls
    // (Even though there are prefixes, the resource is a bucket resource, so GetBucketAcl and
    // ListBucket would apply)
    final Set<IOzoneObj> bucketSet = objSet(bucket("bucket1"));

    final Set<ACLType> bucketAcls = acls(READ, LIST, READ_ACL);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedNative.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: READ, LIST, READ_ACL bucket acls
    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testMultiplePrefixesWithWildcards() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:GetObject\",\n" +
        "    \"Resource\": \"arn:aws:s3:::logs/*\",\n" +
        "    \"Condition\": { \"StringEquals\": { \"s3:prefix\": [\"a/*\", \"b/*\"] } }\n" +
        "  }]\n" +
        "}";

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedNative =
        new LinkedHashSet<>();

    // Expected for native: READ acl on prefixes "a" and "b"
    final Set<IOzoneObj> keyPrefixSet = objSet(
        prefix("logs", "a/"),
        prefix("logs", "b/")
    );

    final Set<ACLType> prefixAcls = acls(READ);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(keyPrefixSet, prefixAcls));

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedNative.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: READ acl on keys "a/*" and "b/*"
    final Set<IOzoneObj> keySet = objSet(
        key("logs", "a/*"),
        key("logs", "b/*")
    );

    final Set<ACLType> keyAcls = acls(READ);

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(keySet, keyAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testObjectResourceWithWildcardInMiddle() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:GetObject\",\n" +
        "    \"Resource\": \"arn:aws:s3:::logs/file*.log\"\n" +
        "  }]\n" +
        "}";

    // Wildcards in middle of object resource are not supported for Native authorizer
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (UnsupportedOperationException ex) {
      assertThat(ex.getMessage()).isEqualTo(
          "Wildcard prefix patterns are not supported for Ozone native " +
              "authorizer if wildcard is not at the end"
      );
    }

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: READ acl on key "file*.log"
    final Set<IOzoneObj> keySet = objSet(key("logs", "file*.log"));

    final Set<ACLType> keyAcls = acls(READ);

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(keySet, keyAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testBucketActionOnAllResources() {
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

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: volume READ and LIST on volumeName
    final Set<IOzoneObj> volumeSet = objSet(volume());

    final Set<ACLType> volumeAcls = acls(READ, LIST);

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(volumeSet, volumeAcls));

    // Expected for Ranger: bucket READ and LIST on wildcard pattern
    final Set<IOzoneObj> bucketSet = objSet(bucket("*"));

    final Set<ACLType> bucketAcls = acls(READ, LIST);

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testObjectActionOnAllResources() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:PutObject\",\n" +
        "    \"Resource\": \"*\"\n" +
        "  }]\n" +
        "}";

    // Wildcards on bucket are not supported for Native authorizer
    expectBucketWildcardUnsupportedExceptionForNativeAuthorizer(json);

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: CREATE and WRITE key acls on wildcard pattern
    final Set<IOzoneObj> keySet = objSet(key("*", "*"));

    final Set<ACLType> keyAcls = acls(CREATE, WRITE);

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(keySet, keyAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testAllActionsOnAllResources() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:*\",\n" +
        "    \"Resource\": \"*\"\n" +
        "  }]\n" +
        "}";

    // Wildcards on bucket are not supported for Native authorizer
    expectBucketWildcardUnsupportedExceptionForNativeAuthorizer(json);

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: ALL volume acls on volumeName
    final Set<IOzoneObj> volumeSet = objSet(volume());

    final Set<ACLType> volumeAcls = acls(ALL);

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(volumeSet, volumeAcls));

    // Expected for Ranger: ALL bucket acls on wildcard pattern
    final Set<IOzoneObj> bucketSet = objSet(bucket("*"));

    final Set<ACLType> bucketAcls = acls(ALL);

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Expected for Ranger: ALL key acls on wildcard pattern
    final Set<IOzoneObj> keySet = objSet(key("*", "*"));

    final Set<ACLType> keyAcls = acls(ALL);

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(keySet, keyAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testAllActionsOnAllBucketResources() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:*\",\n" +
        "    \"Resource\": \"arn:aws:s3:::*\"\n" +
        "  }]\n" +
        "}";

    // Wildcards on bucket are not supported for Native authorizer
    expectBucketWildcardUnsupportedExceptionForNativeAuthorizer(json);

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: ALL bucket acls on wildcard pattern
    final Set<IOzoneObj> bucketSet = objSet(bucket("*"));

    final Set<ACLType> bucketAcls = acls(ALL);

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testAllActionsOnAllObjectResources() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:*\",\n" +
        "    \"Resource\": \"arn:aws:s3:::*/*\"\n" +
        "  }]\n" +
        "}";

    // Wildcards on bucket are not supported for Native authorizer
    expectBucketWildcardUnsupportedExceptionForNativeAuthorizer(json);

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: ALL key acls on wildcard pattern
    final Set<IOzoneObj> keySet = objSet(key("*", "*"));

    final Set<ACLType> keyAcls = acls(ALL);

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(keySet, keyAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testWildcardActionGroupGetStar() {
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

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedNative =
        new LinkedHashSet<>();

    // Expected for native: bucket READ, READ_ACL acls
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));

    final Set<ACLType> bucketAcls = acls(READ, READ_ACL);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Expected for native: READ acl on prefix "" under bucket
    final Set<IOzoneObj> keyPrefixSet = objSet(prefix("my-bucket", ""));

    final Set<ACLType> keyAcls = acls(READ);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(keyPrefixSet, keyAcls));

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedNative.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: bucket READ, READ_ACL acls
    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Expected for Ranger: READ key acl for resource type KEY with key name "*"
    final Set<IOzoneObj> rangerKeySet = objSet(key("my-bucket", "*"));

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(rangerKeySet, keyAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testWildcardActionGroupListStar() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:List*\",\n" +
        "      \"Resource\": [\n" +
        "        \"arn:aws:s3:::my-bucket\",\n" +
        "        \"arn:aws:s3:::my-bucket/*\"\n" +    // ListMultipartUploadParts has READ effect
                                                      // on file/object resources
        "      ]\n" +
        "  }]\n" +
        "}";

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedNative =
        new LinkedHashSet<>();

    // Expected for native: READ, LIST bucket acls
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));

    final Set<ACLType> bucketAcls = acls(READ, LIST);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Expected for native: READ acl on prefix "" under bucket
    final Set<IOzoneObj> keyPrefixSet = objSet(prefix("my-bucket", ""));

    final Set<ACLType> keyAcl = acls(READ);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(keyPrefixSet, keyAcl));

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedNative.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: READ, LIST bucket acls
    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcls));

    // Expected for Ranger: READ key acl for resource type KEY with key name "*"
    final Set<IOzoneObj> rangerKeySet = objSet(key("my-bucket", "*"));

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(rangerKeySet, keyAcl));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testWildcardActionGroupPutStar() {
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

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedNative =
        new LinkedHashSet<>();

    // Expected for native: bucket WRITE_ACL acl
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));

    final Set<ACLType> bucketAcl = acls(WRITE_ACL);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcl));

    // Expected for native: CREATE, WRITE acls on prefix "" under bucket
    final Set<IOzoneObj> keyPrefixSet = objSet(prefix("my-bucket", ""));

    final Set<ACLType> keyAcls = acls(CREATE, WRITE);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(keyPrefixSet, keyAcls));

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedNative.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: bucket WRITE_ACL acl
    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcl));

    // Expected for Ranger: CREATE, WRITE key acls for resource type KEY with key name "*"
    final Set<IOzoneObj> rangerKeySet = objSet(key("my-bucket", "*"));

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(rangerKeySet, keyAcls));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testWildcardActionGroupDeleteStar() {
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

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedNative =
        new LinkedHashSet<>();

    // Expected for native: bucket DELETE acl
    final Set<IOzoneObj> bucketSet = objSet(bucket("my-bucket"));

    final Set<ACLType> bucketAcl = acls(DELETE);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcl));

    // Expected for native: DELETE acl on prefix "" under bucket
    final Set<IOzoneObj> keyPrefixSet = objSet(prefix("my-bucket", ""));

    final Set<ACLType> keyAcl = acls(DELETE);

    expectedResolvedNative.add(new AbstractMap.SimpleImmutableEntry<>(keyPrefixSet, keyAcl));

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedNative.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> expectedResolvedRanger =
        new LinkedHashSet<>();

    // Expected for Ranger: bucket DELETE acl
    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(bucketSet, bucketAcl));

    // Expected for Ranger: DELETE key acl for resource type KEY with key name "*"
    final Set<IOzoneObj> rangerKeySet = objSet(key("my-bucket", "*"));

    expectedResolvedRanger.add(new AbstractMap.SimpleImmutableEntry<>(rangerKeySet, keyAcl));

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    ).isEqualTo(expectedResolvedRanger.stream()
        .map(TestIamSessionPolicyResolver::stringifyEntry)
        .collect(Collectors.toSet())
    );
  }

  @Test
  public void testMismatchedActionAndResourceReturnsEmpty() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Effect\": \"Allow\",\n" +
        "    \"Action\": \"s3:GetObject\",\n" +             // object-level action
        "    \"Resource\": \"arn:aws:s3:::my-bucket\"\n" +  // bucket-level resource
        "  }]\n" +
        "}";

    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromNativeAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
    final Set<AbstractMap.SimpleImmutableEntry<Set<IOzoneObj>, Set<ACLType>>> resolvedFromRangerAuthorizer =
        IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);

    // Ensure what we got is what we expected

    // Native authorizer assertion
    assertThat(resolvedFromNativeAuthorizer.isEmpty());

    // Ranger authorizer assertion
    assertThat(resolvedFromRangerAuthorizer.isEmpty());
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

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid policy JSON - missing Statement");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid policy JSON - missing Statement");
    }
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

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid Resource Arn - arn:aws:s3:::");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid Resource Arn - arn:aws:s3:::");
    }
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

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid Effect in JSON policy " +
          "(must be a String) - [\"Allow\"]"
      );
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid Effect in JSON policy " +
          "(must be a String) - [\"Allow\"]"
      );
    }
  }

  @Test
  public void testMissingEffectInStatementThrows() {
    final String json = "{\n" +
        "  \"Statement\": [{\n" +
        "    \"Action\": \"s3:ListBucket\",\n" +
        "    \"Resource\": \"arn:aws:s3:::bucket1\"\n" +
        "  }]\n" +
        "}";

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Effect is missing from JSON policy");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Effect is missing from JSON policy");
    }
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

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (UnsupportedOperationException ex) {
      assertThat(ex.getMessage()).isEqualTo("Only one Condition is supported");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (UnsupportedOperationException ex) {
      assertThat(ex.getMessage()).isEqualTo("Only one Condition is supported");
    }
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

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid Condition (must have " +
          "operator StringEquals and attribute s3:prefix) - [\"RandomCondition\"]"
      );
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid Condition (must have " +
          "operator StringEquals and attribute s3:prefix) - [\"RandomCondition\"]"
      );
    }
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

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Missing Condition attribute - StringEquals");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Missing Condition attribute - StringEquals");
    }
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

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid Condition attribute structure - " +
          "[{\"s3:prefix\":\"folder/\"}]");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid Condition attribute structure - " +
          "[{\"s3:prefix\":\"folder/\"}]");
    }
  }

  @Test
  public void testInvalidJsonThrows() {
    final String invalidJson = "{[{{}]\"\"";

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(invalidJson, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid policy JSON (most likely JSON structure is incorrect)");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(invalidJson, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid policy JSON (most likely JSON structure is incorrect)");
    }
  }

  @Test
  public void testTooManyStatementsThrows() {
    final String tooManyStatements = buildTooManyStatementsString();

    final String json = "{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        tooManyStatements +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": \"s3:*\",\n" +
        "      \"Resource\": \"arn:aws:s3:::my-bucket/*\"\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid policy JSON - too many Statements. Max is: 100");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid policy JSON - too many Statements. Max is: 100");
    }
  }

  @Test
  public void testTooManyActionsThrows() {
    final String tooManyActions = buildTooManyActionsString();

    final String json = "{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Sid\": \"AllowListingOfDataLakeFolder\",\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
                tooManyActions +
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

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid policy JSON - too many Actions. Max is: 100");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid policy JSON - too many Actions. Max is: 100");
    }
  }

  @Test
  public void testTooManyResourcesThrows() {
    final String tooManyResources = buildTooManyResourcesString();

    final String json = "{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Action\": [\n" +
        "        \"s3:*\"\n" +
        "      ],\n" +
        "      \"Resource\": [\n" +
                tooManyResources +
        "        \"arn:aws:s3:::my-bucket/*\"\n" +
        "      ]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    // Native authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid policy JSON - too many Resources. Max is: 100");
    }

    // Ranger authorizer assertion
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.RANGER);
      throw new AssertionError("Expected exception not thrown");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage()).isEqualTo("Invalid policy JSON - too many Resources. Max is: 100");
    }
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

  /**
   * Converts the return type of resolve() method to a String
   * for easier comparison of expected output.
   */
  private static String stringifyEntry(Map.Entry<Set<IOzoneObj>, Set<ACLType>> e) {
    final List<String> resources = e.getKey().stream()
        .map(o -> (OzoneObj) o)
        .map(oz -> oz.getResourceType() + "|" + oz.getStoreType() + "|" + oz.getPath())
        .sorted()
        .collect(Collectors.toList());
    final List<String> perms = e.getValue().stream()
        .map(Enum::name)
        .sorted()
        .collect(Collectors.toList());
    return resources + "->" + perms;
  }

  /**
   * Ensure resources containing wildcards in buckets throw an Exception
   * when the OzoneNativeAuthorizer is used.
   */
  private static void expectBucketWildcardUnsupportedExceptionForNativeAuthorizer(String json) {
    try {
      IamSessionPolicyResolver.resolve(json, VOLUME, IamSessionPolicyResolver.AuthorizerType.NATIVE);
      throw new AssertionError("Expected exception not thrown");
    } catch (UnsupportedOperationException ex) {
      assertThat(ex.getMessage()).isEqualTo(
          "Wildcard bucket patterns are not supported for Ozone native authorizer"
      );
    }
  }

  /**
   * Builds String with too many Statement objects.
   */
  private static String buildTooManyStatementsString() {
    final StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < 120; i++) {
      stringBuilder.append("    {\n" +
          "      \"Effect\": \"Allow\",\n" +
          "      \"Action\": \"s3:*\",\n" +
          "      \"Resource\": \"arn:aws:s3:::my-bucket/*\"\n" +
          "    },\n"
      );
    }
    return stringBuilder.toString();
  }

  /**
   * Builds String with too many Action objects.
   */
  private static String buildTooManyActionsString() {
    final StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < 120; i++) {
      stringBuilder.append("\"s3:GetObject\",\n");
    }
    return stringBuilder.toString();
  }

  /**
   * Builds String with too many Resource objects.
   */
  private static String buildTooManyResourcesString() {
    final StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < 120; i++) {
      stringBuilder.append("\"arn:aws:s3:::my-bucket/*\",\n");
    }
    return stringBuilder.toString();
  }
}

