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

package org.apache.hadoop.ozone.om.helpers;

import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantState;

/**
 * This class is used for storing Ozone tenant state info.
 * <p>
 * This class is immutable.
 */
public final class OmDBTenantState implements Comparable<OmDBTenantState> {
  private static final Codec<OmDBTenantState> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(TenantState.getDefaultInstance()),
      OmDBTenantState::getFromProtobuf,
      OmDBTenantState::getProtobuf,
      OmDBTenantState.class,
      DelegatedCodec.CopyType.SHALLOW);

  /**
   * Name of the tenant.
   */
  private final String tenantId;
  /**
   * Name of the bucket namespace (volume).
   */
  private final String bucketNamespaceName;
  /**
   * Name of the user role of this tenant.
   */
  private final String userRoleName;
  /**
   * Name of the admin role of this tenant.
   */
  private final String adminRoleName;
  /**
   * Name of the volume access policy of this tenant.
   */
  private final String bucketNamespacePolicyName;
  /**
   * Name of the bucket access policy of this tenant.
   */
  private final String bucketPolicyName;

  public OmDBTenantState(String tenantId, String bucketNamespaceName,
      String userRoleName, String adminRoleName,
      String bucketNamespacePolicyName, String bucketPolicyName) {
    this.tenantId = tenantId;
    this.bucketNamespaceName = bucketNamespaceName;
    this.userRoleName = userRoleName;
    this.adminRoleName = adminRoleName;
    this.bucketNamespacePolicyName = bucketNamespacePolicyName;
    this.bucketPolicyName = bucketPolicyName;
  }

  public static Codec<OmDBTenantState> getCodec() {
    return CODEC;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmDBTenantState that = (OmDBTenantState) o;
    return Objects.equals(tenantId, that.tenantId)
        && Objects.equals(bucketNamespaceName, that.bucketNamespaceName)
        && Objects.equals(userRoleName, that.userRoleName)
        && Objects.equals(adminRoleName, that.adminRoleName)
        && Objects.equals(
            bucketNamespacePolicyName, that.bucketNamespacePolicyName)
        && Objects.equals(bucketPolicyName, that.bucketPolicyName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, bucketNamespaceName,
        userRoleName, adminRoleName,
        bucketNamespacePolicyName, bucketPolicyName);
  }

  @Override
  public int compareTo(OmDBTenantState o) {
    return this.getTenantId().compareTo(o.getTenantId());
  }

  public String getTenantId() {
    return tenantId;
  }

  /**
   * Returns the bucket namespace name. a.k.a. volume name.
   * <p>
   * Note: This returns an empty string ("") if the tenant is somehow not
   * associated with a volume. Should never return null.
   */
  public String getBucketNamespaceName() {
    return bucketNamespaceName;
  }

  public String getUserRoleName() {
    return userRoleName;
  }

  public String getAdminRoleName() {
    return adminRoleName;
  }

  public String getBucketNamespacePolicyName() {
    return bucketNamespacePolicyName;
  }

  public String getBucketPolicyName() {
    return bucketPolicyName;
  }

  /**
   * Convert OmDBTenantState to protobuf to be persisted to DB.
   */
  public TenantState getProtobuf() {
    return TenantState.newBuilder()
        .setTenantId(tenantId)
        .setBucketNamespaceName(bucketNamespaceName)
        .setUserRoleName(userRoleName)
        .setAdminRoleName(adminRoleName)
        .setBucketNamespacePolicyName(bucketNamespacePolicyName)
        .setBucketPolicyName(bucketPolicyName)
        .build();
  }

  /**
   * Convert protobuf to OmDBTenantState.
   */
  public static OmDBTenantState getFromProtobuf(TenantState proto) {
    return new Builder()
        .setTenantId(proto.getTenantId())
        .setBucketNamespaceName(proto.getBucketNamespaceName())
        .setUserRoleName(proto.getUserRoleName())
        .setAdminRoleName(proto.getAdminRoleName())
        .setBucketNamespacePolicyName(proto.getBucketNamespacePolicyName())
        .setBucketPolicyName(proto.getBucketPolicyName())
        .build();
  }

  /**
   * Builder for OmDBTenantState.
   */
  public static final class Builder {
    private String tenantId;
    private String bucketNamespaceName;
    private String userRoleName;
    private String adminRoleName;
    private String bucketNamespacePolicyName;
    private String bucketPolicyName;

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

    public Builder setUserRoleName(String userRoleName) {
      this.userRoleName = userRoleName;
      return this;
    }

    public Builder setAdminRoleName(String adminRoleName) {
      this.adminRoleName = adminRoleName;
      return this;
    }

    public Builder setBucketNamespacePolicyName(
        String bucketNamespacePolicyName) {
      this.bucketNamespacePolicyName = bucketNamespacePolicyName;
      return this;
    }

    public Builder setBucketPolicyName(String bucketPolicyName) {
      this.bucketPolicyName = bucketPolicyName;
      return this;
    }

    public OmDBTenantState build() {
      return new OmDBTenantState(tenantId, bucketNamespaceName,
          userRoleName, adminRoleName,
          bucketNamespacePolicyName, bucketPolicyName);
    }
  }
}
