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

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.util.List;
import java.util.Objects;

/**
 * This class is used for storing Ozone tenant state info.
 */
public final class OmDBTenantState implements Comparable<OmDBTenantState> {
  /**
   * Name of the tenant.
   */
  private final String tenantId;
  /**
   * Name of the bucket namespace (volume).
   */
  private final String bucketNamespaceName;
  /**
   * Bucket policy IDs stored as Strings.
   */
  private final List<String> policyIds;

  public OmDBTenantState(String tenantId,
      String bucketNamespaceName, List<String> policyIds) {
    this.tenantId = tenantId;
    this.bucketNamespaceName = bucketNamespaceName;
    this.policyIds = policyIds;
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
        && Objects.equals(policyIds, that.policyIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, bucketNamespaceName, policyIds);
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
   *
   * Note: This returns an empty string ("") if the tenant is somehow not
   * associated with a volume. Should never return null.
   */
  public String getBucketNamespaceName() {
    return bucketNamespaceName;
  }

  public List<String> getPolicyIds() {
    return policyIds;
  }

  /**
   * Convert OmDBTenantInfo to protobuf to be persisted to DB.
   */
  public OzoneManagerProtocolProtos.TenantInfo getProtobuf() {
    return OzoneManagerProtocolProtos.TenantInfo.newBuilder()
        .setTenantId(tenantId)
        .setBucketNamespaceName(bucketNamespaceName)
        .addAllPolicyIds(policyIds)
        .build();
  }

  /**
   * Convert protobuf to OmDBTenantInfo.
   */
  public static OmDBTenantState getFromProtobuf(
      OzoneManagerProtocolProtos.TenantInfo proto) {
    return new Builder()
        .setTenantId(proto.getTenantId())
        .setBucketNamespaceName(proto.getBucketNamespaceName())
        .setPolicyIdsList(proto.getPolicyIdsList())
        .build();
  }

  /**
   * Builder for OmDBTenantInfo.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private String tenantId;
    private String bucketNamespaceName;
    private List<String> policyIds;

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

    public Builder setPolicyIdsList(List<String> policyIds) {
      this.policyIds = policyIds;
      return this;
    }

    public OmDBTenantState build() {
      return new OmDBTenantState(tenantId, bucketNamespaceName, policyIds);
    }
  }
}
