/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.util.Objects;

/**
 * This class is used for storing Ozone tenant info.
 */
public final class OmDBTenantInfo implements Comparable<OmDBTenantInfo> {
  /**
   * Name of the tenant.
   */
  private final String tenantId;
  /**
   * Name of the bucket namespace (volume name).
   */
  private final String bucketNamespaceName;
  /**
   * Name of the account namespace.
   */
  private final String accountNamespaceName;
  /**
   * Name of the user policy group.
   */
  private final String userPolicyGroupName;
  /**
   * Name of the bucket policy group.
   */
  private final String bucketPolicyGroupName;

  public OmDBTenantInfo(String tenantId,
      String bucketNamespaceName, String accountNamespaceName,
      String userPolicyGroupName, String bucketPolicyGroupName) {
    this.tenantId = tenantId;
    this.bucketNamespaceName = bucketNamespaceName;
    this.accountNamespaceName = accountNamespaceName;
    this.userPolicyGroupName = userPolicyGroupName;
    this.bucketPolicyGroupName = bucketPolicyGroupName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmDBTenantInfo that = (OmDBTenantInfo) o;
    return Objects.equals(tenantId, that.tenantId)
        && Objects.equals(bucketNamespaceName, that.bucketNamespaceName)
        && Objects.equals(accountNamespaceName, that.accountNamespaceName)
        && Objects.equals(userPolicyGroupName, that.userPolicyGroupName)
        && Objects.equals(bucketPolicyGroupName, that.bucketPolicyGroupName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, bucketNamespaceName, accountNamespaceName,
        userPolicyGroupName, bucketPolicyGroupName);
  }

  @Override
  public int compareTo(OmDBTenantInfo o) {
    return this.getTenantId().compareTo(o.getTenantId());
  }

  public String getTenantId() {
    return tenantId;
  }

  /**
   * Returns the bucket namespace name. a.k.a. volume name.
   *
   * Note: This returns an empty string ("") if the tenant is somehow not
   * associated with a volume. Should never return null.
   */
  public String getBucketNamespaceName() {
    return bucketNamespaceName;
  }

  public String getAccountNamespaceName() {
    return accountNamespaceName;
  }

  public String getUserPolicyGroupName() {
    return userPolicyGroupName;
  }

  public String getBucketPolicyGroupName() {
    return bucketPolicyGroupName;
  }

  /**
   * Convert OmDBTenantInfo to protobuf to be persisted to DB.
   */
  public OzoneManagerProtocolProtos.TenantInfo getProtobuf() {
    return OzoneManagerProtocolProtos.TenantInfo.newBuilder()
        .setTenantId(tenantId)
        .setBucketNamespaceName(bucketNamespaceName)
        .setAccountNamespaceName(accountNamespaceName)
        .setUserPolicyGroupName(userPolicyGroupName)
        .setBucketPolicyGroupName(bucketPolicyGroupName)
        .build();
  }

  /**
   * Convert protobuf to OmDBTenantInfo.
   */
  public static OmDBTenantInfo getFromProtobuf(
      OzoneManagerProtocolProtos.TenantInfo proto) {
    return new Builder()
        .setTenantId(proto.getTenantId())
        .setBucketNamespaceName(proto.getBucketNamespaceName())
        .setAccountNamespaceName(proto.getAccountNamespaceName())
        .setUserPolicyGroupName(proto.getUserPolicyGroupName())
        .setBucketPolicyGroupName(proto.getBucketPolicyGroupName())
        .build();
  }

  /**
   * Builder for OmDBTenantInfo.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private String tenantId;
    private String bucketNamespaceName;
    private String accountNamespaceName;
    private String userPolicyGroupName;
    private String bucketPolicyGroupName;

    private Builder() {
    }

    public Builder setTenantId(String tenantId) {
      this.tenantId = tenantId;
      return this;
    }

    public Builder setBucketNamespaceName(String bucketNamespaceName) {
      this.bucketNamespaceName = bucketNamespaceName;
      return this;
    }

    public Builder setAccountNamespaceName(String accountNamespaceName) {
      this.accountNamespaceName = accountNamespaceName;
      return this;
    }

    public Builder setUserPolicyGroupName(String userPolicyGroupName) {
      this.userPolicyGroupName = userPolicyGroupName;
      return this;
    }

    public Builder setBucketPolicyGroupName(String bucketPolicyGroupName) {
      this.bucketPolicyGroupName = bucketPolicyGroupName;
      return this;
    }

    public OmDBTenantInfo build() {
      return new OmDBTenantInfo(tenantId, bucketNamespaceName,
          accountNamespaceName, userPolicyGroupName, bucketPolicyGroupName);
    }
  }
}
