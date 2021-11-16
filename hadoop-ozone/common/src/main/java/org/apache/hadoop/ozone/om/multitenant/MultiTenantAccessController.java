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

import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.http.auth.BasicUserPrincipal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

/**
 * Defines the operations needed for multi-tenant access control.
 * Each implemented method should be atomic. A failure partway through any
 * one method call should not leave any state behind.
 */
public interface MultiTenantAccessController {
  /**
   * This operation will fail if a policy with the same name already exists,
   * or a policy for the same set of resources already exists.
   *
   * Roles defined in this policy that do not already exist will be created.
   *
   * @return The unique ID to refer to this policy.
   */
  long createPolicy(Policy policy) throws Exception;

  void deletePolicy(long policyID) throws Exception;

  Policy getPolicy(long policyID) throws Exception;

  void updatePolicy(long policyID, Policy policy) throws Exception;

  Map<Long, Policy> getPolicies() throws Exception;

  /**
   * This operation will fail if a role with the same name already exists.
   *
   * @return The unique ID to refer to this role.
   */
  long createRole(Role role) throws Exception;

  void deleteRole(long roleID) throws Exception;

  Role getRole(long roleID) throws Exception;

  void updateRole(long roleID, Role role) throws Exception;

  Map<Long, Role> getRoles() throws Exception;

  /**
   * Define a role to be created.
   */
  class Role {
    private final String name;
    private final Collection<BasicUserPrincipal> users;
    private String description;

    public Role(String roleName) {
      this.name = roleName;
      this.users = new HashSet<>();
    }

    public String getName() {
      return name;
    }

    public void addUsers(BasicUserPrincipal... newUsers) {
      users.addAll(Arrays.asList(newUsers));
    }

    public boolean removeUsers(BasicUserPrincipal... newUsers) {
      return users.removeAll(Arrays.asList(newUsers));
    }

    public Collection<BasicUserPrincipal> getUsers() {
      return Collections.unmodifiableCollection(users);
    }

    public Optional<String> getDescription() {
      return Optional.ofNullable(description);
    }

    public void setDescription(String description) {
      this.description = description;
    }
  }

  /**
   * Define an acl.
   */
  class Acl {
    private final boolean isAllowed;
    private final IAccessAuthorizer.ACLType acl;

    private Acl(IAccessAuthorizer.ACLType acl, boolean isAllowed) {
      this.isAllowed = isAllowed;
      this.acl = acl;
    }

    public static Acl allow(IAccessAuthorizer.ACLType acl) {
      return new Acl(acl, true);
    }

    public static Acl deny(IAccessAuthorizer.ACLType acl) {
      return new Acl(acl, false);
    }

    public IAccessAuthorizer.ACLType getAclType() {
      return acl;
    }

    public boolean isAllowed() {
      return isAllowed;
    }
  }

  /**
   * Define a policy to be created.
   */
  class Policy {
    private final String name;
    private final Collection<String> volumes;
    private final Collection<String> buckets;
    private final Collection<String> keys;
    private String description;
    private final Map<String, Collection<Acl>> roleAcls;
    private boolean isEnabled;

    public Policy(String policyName, String... volumeName) {
      this.name = policyName;
      this.volumes = new ArrayList<>();
      this.volumes.addAll(Arrays.asList(volumeName));
      this.buckets = new ArrayList<>();
      this.keys = new ArrayList<>();
      this.roleAcls = new HashMap<>();
      this.isEnabled = true;
    }

    public void setEnabled(boolean enabled) {
      isEnabled = enabled;
    }

    public boolean isEnabled() {
      return isEnabled;
    }

    public Collection<String> getVolumes() {
      return Collections.unmodifiableCollection(volumes);
    }

    public Collection<String> getBuckets() {
      return Collections.unmodifiableCollection(buckets);
    }

    public Collection<String> getKeys() {
      return Collections.unmodifiableCollection(keys);
    }

    public void addVolumes(String... volumeNames) {
      volumes.addAll(Arrays.asList(volumeNames));
    }

    public void addBuckets(String... bucketNames) {
      buckets.addAll(Arrays.asList(bucketNames));
    }

    public void addKeys(String... keyNames) {
      keys.addAll(Arrays.asList(keyNames));
    }

    public String getName() {
      return name;
    }

    public Optional<String> getDescription() {
      return Optional.ofNullable(description);
    }

    public void addRoleAcls(String roleName, Acl... acl) {
      roleAcls.putIfAbsent(roleName, new ArrayList<>());
      roleAcls.get(roleName).addAll(Arrays.asList(acl));
    }

    public Map<String, Collection<Acl>> getRoleAcls() {
      return Collections.unmodifiableMap(roleAcls);
    }

    public void setDescription(String description) {
      this.description = description;
    }
  }
}
