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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * In Memory version of MultiTenantAccessController.
 */
public class InMemoryMultiTenantAccessController
    implements MultiTenantAccessController {

  private final Map<String, Policy> policies;
  private final Map<String, Role> roles;
  private long nextRoleID;
  private long serviceVersion;

  public InMemoryMultiTenantAccessController() {
    nextRoleID = 0;
    policies = new HashMap<>();
    roles = new HashMap<>();
    serviceVersion = 0;
  }

  @Override
  public Policy createPolicy(Policy policy) throws IOException {
    if (policies.containsKey(policy.getName())) {
      throw new IOException("Policy already exists.");
    }
    // Multiple policies for the same resource should not be allowed.
    for (Policy existingPolicy: policies.values()) {
      if (existingPolicy.getVolumes().equals(policy.getVolumes()) &&
          existingPolicy.getBuckets().equals(policy.getBuckets()) &&
          existingPolicy.getKeys().equals(policy.getKeys())) {
        throw new IOException("Policy for the same resource already defined.");
      }
    }
    policies.put(policy.getName(), policy);
    // Ranger will create roles if specified with policy creation.
    for (String roleName: policy.getRoleAcls().keySet()) {
      if (!roles.containsKey(roleName)) {
        createRole(new Role.Builder().setName(roleName).build());
      }
    }

    serviceVersion++;

    return policy;
  }

  @Override
  public Policy getPolicy(String policyName) throws IOException {
    if (!policies.containsKey(policyName)) {
      throw new IOException("Policy does not exist.");
    }
    return policies.get(policyName);
  }

  @Override
  public List<Policy> getLabeledPolicies(String label) throws IOException {
    List<Policy> result = new ArrayList<>();
    for (Policy policy: policies.values()) {
      if (policy.getLabels().contains(label)) {
        result.add(policy);
      }
    }

    return result;
  }

  @Override
  public Policy updatePolicy(Policy policy) throws IOException {
    if (!policies.containsKey(policy.getName())) {
      throw new IOException("Policy does not exist.");
    }
    policies.put(policy.getName(), policy);
    serviceVersion++;

    return policy;
  }

  @Override
  public void deletePolicy(String policyName) throws IOException {
    if (!policies.containsKey(policyName)) {
      throw new IOException("Policy does not exist.");
    }
    policies.remove(policyName);
    serviceVersion++;
  }

  @Override
  public Role createRole(Role role) throws IOException {
    if (roles.containsKey(role.getName())) {
      throw new IOException("Role already exists.");
    }
    Role newRole = new Role.Builder(role)
        .setID(nextRoleID)
        .build();
    nextRoleID++;
    roles.put(newRole.getName(), newRole);
    serviceVersion++;

    return role;
  }

  @Override
  public Role getRole(String roleName) throws IOException {
    if (!roles.containsKey(roleName)) {
      throw new IOException("Role does not exist.");
    }
    return roles.get(roleName);
  }

  @Override
  public Role updateRole(long roleId, Role role) throws IOException {
    Optional<Role> originalRole = roles.values().stream()
        .filter(r -> r.getId().isPresent() && r.getId().get() == roleId)
        .findFirst();
    if (!originalRole.isPresent()) {
      throw new IOException("Role does not exist.");
    }
    // New role may have same ID but different name.
    roles.remove(originalRole.get().getName());
    roles.put(role.getName(), role);
    serviceVersion++;

    return role;
  }

  @Override
  public void deleteRole(String roleName) throws IOException {
    if (!roles.containsKey(roleName)) {
      throw new IOException("Role does not exist.");
    }
    roles.remove(roleName);
    serviceVersion++;
  }

  @Override
  public long getRangerServicePolicyVersion() {
    return serviceVersion;
  }
}
