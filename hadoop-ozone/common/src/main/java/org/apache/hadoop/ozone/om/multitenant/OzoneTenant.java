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

package org.apache.hadoop.ozone.om.multitenant;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.ozone.om.multitenant.impl.AccountNameSpaceImpl;
import org.apache.hadoop.ozone.om.multitenant.impl.SingleVolumeTenantNamespace;

/**
 * In-memory tenant info. For DB state, see OmDBTenantState.
 */
public class OzoneTenant implements Tenant {
  private final String tenantId;
  private final List<String> tenantRoleNames;
  private final List<String> accessPolicies;
  private final AccountNameSpace accountNameSpace;
  private final BucketNameSpace bucketNameSpace;

  public OzoneTenant(String id) {
    tenantId = id;
    accessPolicies = new ArrayList<>();
    tenantRoleNames = new ArrayList<>();
    accountNameSpace = new AccountNameSpaceImpl(id);
    bucketNameSpace = new SingleVolumeTenantNamespace(id);
  }

  @Override
  public String getTenantName() {
    return tenantId;
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
  public List<String> getTenantAccessPolicies() {
    return accessPolicies;
  }

  @Override
  public void addTenantAccessPolicy(String policy) {
    accessPolicies.add(policy);
  }

  @Override
  public void removeTenantAccessPolicy(String policy) {
    accessPolicies.remove(policy);
  }

  @Override
  public void addTenantAccessRole(String roleName) {
    tenantRoleNames.add(roleName);
  }

  @Override
  public void removeTenantAccessRole(String roleName) {
    tenantRoleNames.remove(roleName);
  }

  @Override
  public List<String> getTenantRoles() {
    return tenantRoleNames;
  }

  @Override
  public String toString() {
    return "OzoneTenant{" + "tenantId='" + tenantId + '\''
        + ", tenantRoleNames=" + tenantRoleNames + ", accessPolicies="
        + accessPolicies + ", accountNameSpace=" + accountNameSpace
        + ", bucketNameSpace=" + bucketNameSpace + '}';
  }
}
