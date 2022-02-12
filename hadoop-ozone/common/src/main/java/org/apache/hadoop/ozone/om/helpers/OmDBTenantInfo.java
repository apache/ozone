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
import org.jetbrains.annotations.NotNull;

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
  // Implies above names should NOT contain the split key.
  public static final String TENANT_INFO_SPLIT_KEY = ";";

  public OmDBTenantInfo(String tenantId,
      String bucketNamespaceName, String accountNamespaceName,
      String userPolicyGroupName, String bucketPolicyGroupName) {
    this.tenantId = tenantId;
    this.bucketNamespaceName = bucketNamespaceName;
    this.accountNamespaceName = accountNamespaceName;
    this.userPolicyGroupName = userPolicyGroupName;
    this.bucketPolicyGroupName = bucketPolicyGroupName;
  }

  private OmDBTenantInfo(String tenantInfoString) {
    String[] tInfo = tenantInfoString.split(TENANT_INFO_SPLIT_KEY);
    Preconditions.checkState(tInfo.length == 5,
        "Incorrect tenantInfoString");

    tenantId = tInfo[0];
    bucketNamespaceName = tInfo[1];
    accountNamespaceName = tInfo[2];
    userPolicyGroupName = tInfo[3];
    bucketPolicyGroupName = tInfo[4];
  }

  @Override
  public int compareTo(OmDBTenantInfo o) {
    if (this == o) {
      return 0;
    }
    return this.tenantId.compareTo(o.tenantId);
  }

  public String getTenantId() {
    return tenantId;
  }

  private String generateTenantInfo() {
    StringBuilder sb = new StringBuilder();
    sb.append(tenantId).append(TENANT_INFO_SPLIT_KEY);
    sb.append(bucketNamespaceName).append(TENANT_INFO_SPLIT_KEY);
    sb.append(accountNamespaceName).append(TENANT_INFO_SPLIT_KEY);
    sb.append(userPolicyGroupName).append(TENANT_INFO_SPLIT_KEY);
    sb.append(bucketPolicyGroupName);
    return sb.toString();
  }

  /**
   * Convert OmDBTenantInfo to byteArray to be persisted to DB.
   * @return byte[]
   */
  public byte[] convertToByteArray() {
    return StringUtils.string2Bytes(generateTenantInfo());
  }

  /**
   * Convert byte array to OmDBTenantInfo.
   * @param bytes
   * @return OmDBTenantInfo
   */
  public static OmDBTenantInfo getFromByteArray(byte[] bytes) {
    String tInfo = StringUtils.bytes2String(bytes);
    return new OmDBTenantInfo(tInfo);
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
