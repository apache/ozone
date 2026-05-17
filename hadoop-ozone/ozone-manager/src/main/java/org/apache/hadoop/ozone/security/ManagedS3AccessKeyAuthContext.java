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

package org.apache.hadoop.ozone.security;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;

/**
 * OM-local managed S3 access key identity for the current request.
 */
public final class ManagedS3AccessKeyAuthContext {

  public static final String CREDENTIAL_TYPE = "MANAGED_S3_ACCESS_KEY";

  private final String credentialAccessKeyId;
  private final String s3NamespaceAccessId;
  private final String effectiveUser;
  private final ImmutableList<String> groupsSnapshot;
  private final boolean policyDocumentPresent;

  public ManagedS3AccessKeyAuthContext(String credentialAccessKeyId,
      String s3NamespaceAccessId, String effectiveUser,
      Iterable<String> groupsSnapshot, boolean policyDocumentPresent) {
    this.credentialAccessKeyId =
        Objects.requireNonNull(credentialAccessKeyId);
    this.s3NamespaceAccessId = Objects.requireNonNull(s3NamespaceAccessId);
    this.effectiveUser = Objects.requireNonNull(effectiveUser);
    this.groupsSnapshot = ImmutableList.copyOf(
        Objects.requireNonNull(groupsSnapshot));
    this.policyDocumentPresent = policyDocumentPresent;
  }

  public String getCredentialAccessKeyId() {
    return credentialAccessKeyId;
  }

  public String getS3NamespaceAccessId() {
    return s3NamespaceAccessId;
  }

  public String getEffectiveUser() {
    return effectiveUser;
  }

  public List<String> getGroupsSnapshot() {
    return groupsSnapshot;
  }

  public boolean isPolicyDocumentPresent() {
    return policyDocumentPresent;
  }

  public String getCredentialType() {
    return CREDENTIAL_TYPE;
  }
}
