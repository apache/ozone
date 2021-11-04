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
package org.apache.hadoop.ozone.om.multitenant;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.ozone.om.multitenant.impl.AccountNameSpaceImpl;
import org.apache.hadoop.ozone.om.multitenant.impl.SingleVolumeTenantNamespace;

/**
 * Implements Tenant.
 */
public class DefaultOzoneS3Tenant implements Tenant {
  private final String tenantID;
  private List<String> tenantRoleIds;
  private List<AccessPolicy> accessPolicies;
  private final AccountNameSpace accountNameSpace;
  private final BucketNameSpace bucketNameSpace;

  public DefaultOzoneS3Tenant(String id) {
    tenantID = id;
    accessPolicies = new ArrayList<>();
    tenantRoleIds = new ArrayList<>();
    accountNameSpace = new AccountNameSpaceImpl(id);
    bucketNameSpace = new SingleVolumeTenantNamespace(id);
  }

  @Override
  public String getTenantId() {
    return tenantID;
  }

  @Override
  public AccountNameSpace getTenantAccountNameSpace() {
    return accountNameSpace;
  }

  @Override
  public BucketNameSpace getTenantBucketNameSpace() {
    return bucketNameSpace;
  }

  @Override
  public List<AccessPolicy> getTenantAccessPolicies() {
    return accessPolicies;
  }

  @Override
  public void addTenantAccessPolicy(AccessPolicy policy) {
    accessPolicies.add(policy);
  }

  @Override
  public void removeTenantAccessPolicy(AccessPolicy policy) {
    accessPolicies.remove(policy);
  }

  @Override
  public void addTenantAccessRole(String roleId) {
    tenantRoleIds.add(roleId);
  }

  @Override
  public void removeTenantAccessRole(String roleId) {
    tenantRoleIds.remove(roleId);
  }

  @Override
  public List<String> getTenantRoles() {
    return tenantRoleIds;
  }
}
