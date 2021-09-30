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
package org.apache.hadoop.ozone.om.multitenantImpl;

import org.apache.hadoop.ozone.om.multitenant.OzoneTenantGroupPrincipal;

/**
 * Implements OzoneMultiTenantPrincipal.
 */
public class OzoneTenantUserGroupPrincipal implements
    OzoneTenantGroupPrincipal {
  private final String tenantID;
  private static final String DEFAULT_TENANT_GROUP_ALL_USERS =
      "GroupTenantAllUsers";

  public OzoneTenantUserGroupPrincipal(String tenantID) {
    this.tenantID = tenantID;
  }

  @Override
  public String getTenantID() {
    return tenantID;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public String getName() {
    return tenantID + TENANT_ID_SEPARATOR + DEFAULT_TENANT_GROUP_ALL_USERS;
  }
}
