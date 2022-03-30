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
<<<<<<< HEAD
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Defines the operations needed for multi-tenant access control.
=======
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
>>>>>>> HDDS-4944
 */
public interface MultiTenantAccessController {
  /**
   * This operation will fail if a policy with the same name already exists,
   * or a policy for the same set of resources already exists.
   *
   * Roles defined in this policy that do not already exist will be created.
<<<<<<< HEAD
   */
  void createPolicy(Policy policy) throws Exception;

  Policy getPolicy(String policyName) throws Exception;

  List<Policy> getLabeledPolicies(String label) throws Exception;

  void updatePolicy(Policy policy) throws Exception;

  void deletePolicy(String policyName) throws Exception;

  /**
   * This operation will fail if a role with the same name already exists.
   */
  void createRole(Role role) throws Exception;

  Role getRole(String roleName) throws Exception;

  /**
   * Replaces the role given by {@code roleID} with the contents of {@code
   * role}. If {@code roleID} does not correspond to a role, an exception is
   * thrown.
   *
   * The roleID of a given role can be retrieved from the {@code getRole}
   * method.
   */
  void updateRole(long roleID, Role role) throws Exception;

  void deleteRole(String roleName) throws Exception;

  static Map<IAccessAuthorizer.ACLType, String> getRangerAclStrings() {
    Map<IAccessAuthorizer.ACLType, String> rangerAclStrings =
        new EnumMap<>(IAccessAuthorizer.ACLType.class);
    rangerAclStrings.put(IAccessAuthorizer.ACLType.ALL, "All");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.LIST, "List");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.READ, "Read");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.WRITE, "Write");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.CREATE, "Create");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.DELETE, "Delete");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.READ_ACL, "Read_ACL");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.WRITE_ACL, "Write_ACL");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.NONE, "");

    return rangerAclStrings;
=======
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
>>>>>>> HDDS-4944
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
<<<<<<< HEAD

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      Acl otherAcl = (Acl) other;
      return isAllowed() == otherAcl.isAllowed() && acl == otherAcl.acl;
    }
  }

  /**
   * Define a role to be created.
   */
  class Role {
    private final String name;
    private final Set<BasicUserPrincipal> users;
    private final String description;
    private final Long roleID;

    private Role(Builder builder) {
      name = builder.name;
      users = builder.users;
      description = builder.description;
      roleID = builder.roleID;
    }

    public String getName() {
      return name;
    }

    public Set<BasicUserPrincipal> getUsers() {
      return users;
    }

    public Optional<String> getDescription() {
      return Optional.ofNullable(description);
    }

    public Optional<Long> getRoleID() {
      return Optional.ofNullable(roleID);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      Role role = (Role) other;
      // If one role does not have the ID set, still consider them equal.
      // Role ID may not be set if the policy is being sent to Ranger for
      // creation, but will be set if the same policy is retrieved from Ranger.
      boolean roleIDsMatch = true;
      if (getRoleID().isPresent() && role.getRoleID().isPresent()) {
        roleIDsMatch = getRoleID().equals(role.getRoleID());
      }
      return Objects.equals(getName(), role.getName()) &&
          Objects.equals(getUsers(), role.getUsers()) &&
          Objects.equals(getDescription(), role.getDescription()) &&
          roleIDsMatch;
    }

    public static final class Builder {
      private String name;
      private final Set<BasicUserPrincipal> users;
      private String description;
      private Long roleID;

      public Builder() {
        this.users = new HashSet<>();
      }

      public Builder setName(String name) {
        this.name = name;
        return this;
      }

      public Builder addUser(BasicUserPrincipal user) {
        this.users.add(user);
        return this;
      }

      public Builder addUsers(Collection<BasicUserPrincipal> users) {
        this.users.addAll(users);
        return this;
      }

      public Builder setDescription(String description) {
        this.description = description;
        return this;
      }

      public Builder setID(long roleID) {
        this.roleID = roleID;
        return this;
      }

      public Role build() {
        return new Role(this);
      }
    }
  }

  /**
   * Define a policy to be created.
   */
  class Policy {
    private final String name;
    private final Set<String> volumes;
    private final Set<String> buckets;
    private final Set<String> keys;
    private final String description;
    private final Map<String, Collection<Acl>> roleAcls;
    private final Set<String> labels;

    private Policy(Builder builder) {
      name = builder.name;
      volumes = builder.volumes;
      buckets = builder.buckets;
      keys = builder.keys;
      description = builder.description;
      roleAcls = builder.roleAcls;
      labels = builder.labels;
    }

    public Set<String> getVolumes() {
      return volumes;
    }

    public Set<String> getBuckets() {
      return buckets;
    }

    public Set<String> getKeys() {
      return keys;
=======
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
>>>>>>> HDDS-4944
    }

    public String getName() {
      return name;
    }

    public Optional<String> getDescription() {
      return Optional.ofNullable(description);
    }

<<<<<<< HEAD
    public Set<String> getLabels() {
      return (labels);
    }

    public Map<String, Collection<Acl>> getRoleAcls() {
      return roleAcls;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      Policy policy = (Policy) other;
      return Objects.equals(getName(), policy.getName()) &&
          Objects.equals(getVolumes(), policy.getVolumes()) &&
          Objects.equals(getBuckets(), policy.getBuckets()) &&
          Objects.equals(getKeys(), policy.getKeys()) &&
          Objects.equals(getDescription(), policy.getDescription()) &&
          Objects.equals(getRoleAcls(), policy.getRoleAcls()) &&
          Objects.equals(getLabels(), policy.getLabels());
    }

    public static final class Builder {
      private String name;
      private final Set<String> volumes;
      private final Set<String> buckets;
      private final Set<String> keys;
      private String description;
      private final Map<String, Collection<Acl>> roleAcls;
      private final Set<String> labels;

      public Builder() {
        this.volumes = new HashSet<>();
        this.buckets = new HashSet<>();
        this.keys = new HashSet<>();
        this.roleAcls = new HashMap<>();
        this.labels = new HashSet<>();
      }

      public Builder setName(String name) {
        this.name = name;
        return this;
      }

      public Builder addVolume(String volume) {
        this.volumes.add(volume);
        return this;
      }

      public Builder addBucket(String bucket) {
        this.buckets.add(bucket);
        return this;
      }

      public Builder addKey(String key) {
        this.keys.add(key);
        return this;
      }

      public Builder addVolumes(Collection<String> volumes) {
        this.volumes.addAll(volumes);
        return this;
      }

      public Builder addBuckets(Collection<String> buckets) {
        this.buckets.addAll(buckets);
        return this;
      }

      public Builder addKeys(Collection<String> keys) {
        this.keys.addAll(keys);
        return this;
      }

      public Builder setDescription(String description) {
        this.description = description;
        return this;
      }

      public Builder addRoleAcl(String roleName, Collection<Acl> acls) {
        this.roleAcls.put(roleName, new ArrayList<>(acls));
        return this;
      }

      public Builder addLabel(String label) {
        this.labels.add(label);
        return this;
      }

      public Builder addLabels(Collection<String> labels) {
        this.labels.addAll(labels);
        return this;
      }

      public Policy build() {
        if (name == null || name.isEmpty()) {
          throw new IllegalStateException("A policy must have a non-empty " +
              "name.");
        }
        return new Policy(this);
      }
=======
    public void addRoleAcls(String roleName, Acl... acl) {
      roleAcls.putIfAbsent(roleName, new ArrayList<>());
      roleAcls.get(roleName).addAll(Arrays.asList(acl));
    }

    public Map<String, Collection<Acl>> getRoleAcls() {
      return Collections.unmodifiableMap(roleAcls);
    }

    public void setDescription(String description) {
      this.description = description;
>>>>>>> HDDS-4944
    }
  }
}
