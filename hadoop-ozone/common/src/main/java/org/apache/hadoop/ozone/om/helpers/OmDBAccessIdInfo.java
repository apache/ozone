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

import java.io.IOException;

/**
 * This class is used for storing Ozone tenant accessId info.
 */
public final class OmDBAccessIdInfo {
  /**
   * Name of the tenant.
   */
  private final String tenantId;
  /**
   * User principal this accessId belongs to.
   */
  private final String userPrincipal;
  /**
   * Whether this accessId is an administrator of the tenant.
   */
  private final boolean isAdmin;
  /**
   * Whether this accessId is a delegated admin of the tenant.
   * Only effective if isAdmin is true.
   */
  private final boolean isDelegatedAdmin;
  /**
   * Role name of the user (that this access ID is assigned to) in this tenant.
   * e.g. "user", "admin", "auditor"
   */
  private final String roleId;

  public OmDBAccessIdInfo(String tenantId, String userPrincipal,
      boolean isAdmin, boolean isDelegatedAdmin, String roleId) {
    this.tenantId = tenantId;
    this.userPrincipal = userPrincipal;
    this.isAdmin = isAdmin;
    this.isDelegatedAdmin = isDelegatedAdmin;
    this.roleId = roleId;
  }

  public String getTenantId() {
    return tenantId;
  }

  /**
   * Convert OmDBAccessIdInfo to protobuf to be persisted to DB.
   */
  public OzoneManagerProtocolProtos.OmDBAccessInfo getProtobuf() {
    return OzoneManagerProtocolProtos.OmDBAccessInfo.newBuilder()
        .setTenantId(tenantId)
        .setUserPrincipal(userPrincipal)
        .setIsAdmin(isAdmin)
        .setIsDelegatedAdmin(isDelegatedAdmin)
        .setRoleId(roleId)
        .build();
  }

  /**
   * Convert protobuf to OmDBAccessIdInfo.
   */
  public static OmDBAccessIdInfo getFromProtobuf(
      OzoneManagerProtocolProtos.OmDBAccessInfo infoProto) throws IOException {
    return new Builder()
        .setTenantId(infoProto.getTenantId())
        .setUserPrincipal(infoProto.getUserPrincipal())
        .setIsAdmin(infoProto.getIsAdmin())
        .setIsDelegatedAdmin(infoProto.getIsDelegatedAdmin())
        .setRoleId(infoProto.getRoleId())
        .build();
  }

  public String getUserPrincipal() {
    return userPrincipal;
  }

  public boolean getIsAdmin() {
    return isAdmin;
  }

  public boolean getIsDelegatedAdmin() {
    return isDelegatedAdmin;
  }

  /**
   * Builder for OmDBAccessIdInfo.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private String tenantId;
    private String userPrincipal;
    private String roleId;
    private boolean isAdmin;
    private boolean isDelegatedAdmin;

    public Builder setTenantId(String tenantId) {
      this.tenantId = tenantId;
      return this;
    }

    public Builder setUserPrincipal(String userPrincipal) {
      this.userPrincipal = userPrincipal;
      return this;
    }

    public Builder setIsAdmin(boolean isAdmin) {
      this.isAdmin = isAdmin;
      return this;
    }

    public Builder setIsDelegatedAdmin(boolean isDelegatedAdmin) {
      this.isDelegatedAdmin = isDelegatedAdmin;
      return this;
    }

    public Builder setRoleId(String roleId) {
      this.roleId = roleId;
      return this;
    }

    public OmDBAccessIdInfo build() {
      return new OmDBAccessIdInfo(
          tenantId, userPrincipal, isAdmin, isDelegatedAdmin, roleId);
    }
  }
}
