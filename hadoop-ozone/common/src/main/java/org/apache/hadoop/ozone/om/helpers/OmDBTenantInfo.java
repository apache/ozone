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

public final class OmDBTenantInfo {
  /**
   * Name of the tenant.
   */
  private final String tenantName;
  /**
   * Name of the tenant's bucket namespace.
   */
  private final String bucketNamespaceName;
  /**
   * Name of the tenant's account namespace.
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

  public OmDBTenantInfo(String tenantName,
      String bucketNamespaceName, String accountNamespaceName,
      String userPolicyGroupName, String bucketPolicyGroupName) {
    this.tenantName = tenantName;
    this.bucketNamespaceName = bucketNamespaceName;
    this.accountNamespaceName = accountNamespaceName;
    this.userPolicyGroupName = userPolicyGroupName;
    this.bucketPolicyGroupName = bucketPolicyGroupName;
  }

  private OmDBTenantInfo(String tenantInfoString) {
    String[] tInfo = tenantInfoString.split(TENANT_INFO_SPLIT_KEY);
    Preconditions.checkState(tInfo.length == 5,
        "Incorrect tenantInfoString");

    tenantName = tInfo[0];
    bucketNamespaceName = tInfo[1];
    accountNamespaceName = tInfo[2];
    userPolicyGroupName = tInfo[3];
    bucketPolicyGroupName = tInfo[4];
  }

  public static class Builder {
    // TODO: Finish this if necessary later. ref: OmMultipartKeyInfo
  }

  public String getTenantName() {
    return tenantName;
  }

  private String generateTenantInfo() {
    StringBuilder sb = new StringBuilder();
    sb.append(tenantName).append(TENANT_INFO_SPLIT_KEY);
    sb.append(bucketNamespaceName).append(TENANT_INFO_SPLIT_KEY);
    sb.append(accountNamespaceName).append(TENANT_INFO_SPLIT_KEY);
    sb.append(userPolicyGroupName).append(TENANT_INFO_SPLIT_KEY);
    sb.append(bucketPolicyGroupName);
    return sb.toString();
  }

  /**
   * Convert OmTenantInfo to byteArray to be persisted to DB.
   * @return byte[]
   */
  public byte[] convertToByteArray() {
    return StringUtils.string2Bytes(generateTenantInfo());
  }

  /**
   * Convert byte array to OmTenantInfo.
   * @param bytes
   * @return OmTenantInfo
   */
  public static OmDBTenantInfo getFromByteArray(byte[] bytes) {
    String tInfo = StringUtils.bytes2String(bytes);
    return new OmDBTenantInfo(tInfo);
  }

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
}
