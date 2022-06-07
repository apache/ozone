/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.multitenant;

import java.util.HashMap;

/**
 * A collection of things that we want to maintain about a tenant in memory.
 */
public class CachedTenantState {

  private final String tenantId;
  private final String tenantUserRoleName;
  private final String tenantAdminRoleName;
  // accessId -> userPrincipal and isAdmin flag
  private final HashMap<String, CachedAccessIdInfo> accessIdInfoMap;

  public String getTenantUserRoleName() {
    return tenantUserRoleName;
  }

  public String getTenantAdminRoleName() {
    return tenantAdminRoleName;
  }

  /**
   * Stores cached Access ID info.
   */
  public static class CachedAccessIdInfo {
    private final String userPrincipal;
    /**
     * Stores if the accessId is a tenant admin (either delegated or not).
     */
    private boolean isAdmin;

    public CachedAccessIdInfo(String userPrincipal, boolean isAdmin) {
      this.userPrincipal = userPrincipal;
      this.isAdmin = isAdmin;
    }

    public String getUserPrincipal() {
      return userPrincipal;
    }

    public void setIsAdmin(boolean isAdmin) {
      this.isAdmin = isAdmin;
    }

    public boolean getIsAdmin() {
      return isAdmin;
    }
  }

  public CachedTenantState(String tenantId,
      String tenantUserRoleName, String tenantAdminRoleName) {
    this.tenantId = tenantId;
    this.tenantUserRoleName = tenantUserRoleName;
    this.tenantAdminRoleName = tenantAdminRoleName;
    this.accessIdInfoMap = new HashMap<>();
  }

  public String getTenantId() {
    return tenantId;
  }

  public HashMap<String, CachedAccessIdInfo> getAccessIdInfoMap() {
    return accessIdInfoMap;
  }

  public boolean isTenantEmpty() {
    return accessIdInfoMap.isEmpty();
  }
}
