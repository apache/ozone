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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
   * Role names of the user (that this access ID is assigned to) in this tenant.
   * e.g. OzoneConsts.TENANT_ROLE_USER, OzoneConsts.TENANT_ROLE_ADMIN,
   *      or other custom role names.
   */
  private final Set<String> roleIds;

  private static final Logger LOG =
      LoggerFactory.getLogger(OmDBAccessIdInfo.class);

  public OmDBAccessIdInfo(String tenantId, String userPrincipal,
      boolean isAdmin, boolean isDelegatedAdmin, Set<String> roleId) {
    this.tenantId = tenantId;
    this.userPrincipal = userPrincipal;
    this.isAdmin = isAdmin;
    this.isDelegatedAdmin = isDelegatedAdmin;
    this.roleIds = roleId;
  }

  public String getTenantId() {
    return tenantId;
  }

  /**
   * Convert OmDBAccessIdInfo to protobuf to be persisted to DB.
   */
  public OzoneManagerProtocolProtos.ExtendedAccessIdInfo getProtobuf() {
    return OzoneManagerProtocolProtos.ExtendedAccessIdInfo.newBuilder()
        .setTenantId(tenantId)
        .setUserPrincipal(userPrincipal)
        .setIsAdmin(isAdmin)
        .setIsDelegatedAdmin(isDelegatedAdmin)
        .addAllRoleIds(roleIds)
        .build();
  }

  /**
   * Convert protobuf to OmDBAccessIdInfo.
   */
  public static OmDBAccessIdInfo getFromProtobuf(
      OzoneManagerProtocolProtos.ExtendedAccessIdInfo infoProto)
      throws IOException {
    return new Builder()
        .setTenantId(infoProto.getTenantId())
        .setUserPrincipal(infoProto.getUserPrincipal())
        .setIsAdmin(infoProto.getIsAdmin())
        .setIsDelegatedAdmin(infoProto.getIsDelegatedAdmin())
        .setRoleIds(infoProto.getRoleIdsList())
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

  public Set<String> getRoleIdsSet() {
    return roleIds;
  }

  public OmDBAccessIdInfo addRoleId(String roleId) {
    if (roleIds.contains(roleId)) {
      LOG.warn("Role ID '" + roleId + "' already exists. Ignored addRoleId");
    } else {
      roleIds.add(roleId);
    }
    return this;
  }

  public OmDBAccessIdInfo removeRoleId(String roleId) {
    if (roleIds.contains(roleId)) {
      roleIds.remove(roleId);
    } else {
      LOG.warn("Role ID '" + roleId + "' doesn't exist. Ignored removeRoleId");
    }
    return this;
  }

  /**
   * Builder for OmDBAccessIdInfo.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private String tenantId;
    private String userPrincipal;
    private Set<String> roleIds;
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

    public Builder setRoleIds(Set<String> roleIds) {
      this.roleIds = roleIds;
      return this;
    }

    public Builder setRoleIds(List<String> roleIds) {
      // Convert list to set
      this.roleIds = new HashSet<>(roleIds);
      return this;
    }

    public Builder addRoleId(String roleId) {
      if (roleIds == null) {
        roleIds = new HashSet<>();
      }
      this.roleIds.add(roleId);
      return this;
    }

    public OmDBAccessIdInfo build() {
      return new OmDBAccessIdInfo(
          tenantId, userPrincipal, isAdmin, isDelegatedAdmin, roleIds);
    }
  }
}
