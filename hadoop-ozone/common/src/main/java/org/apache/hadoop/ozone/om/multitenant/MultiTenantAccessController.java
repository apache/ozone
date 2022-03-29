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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Defines the operations needed for multi-tenant access control.
 */
public interface MultiTenantAccessController {
  /**
   * This operation will fail if a policy with the same name already exists,
   * or a policy for the same set of resources already exists.
   *
   * Roles defined in this policy that do not already exist will be created.
   */
  void createPolicy(Policy policy) throws Exception;

  Policy getPolicy(String policyName) throws Exception;

  List<Policy> getLabelledPolicies(String... labels) throws Exception;

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
  }

  /**
   * Define a role to be created.
   */
  class Role {
    private final String name;
    private final Collection<BasicUserPrincipal> users;
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

    public Collection<BasicUserPrincipal> getUsers() {
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
      return Objects.equals(getName(), role.getName()) &&
          Objects.equals(getUsers(), role.getUsers()) &&
          Objects.equals(getDescription(), role.getDescription()) &&
          Objects.equals(getRoleID(), role.getRoleID());
    }

    public static final class Builder {
      private String name;
      private final Collection<BasicUserPrincipal> users;
      private String description;
      private Long roleID;

      public Builder() {
        this.users = new ArrayList<>();
      }

      public Builder(Role copy) {
        this.name = copy.getName();
        this.users = copy.getUsers();
        copy.getDescription().ifPresent(desc -> this.description = desc);
        copy.getRoleID().ifPresent(id -> this.roleID = id);
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
   * Define a policy to be created.
   */
  class Policy {
    private final String name;
    private final List<String> volumes;
    private final List<String> buckets;
    private final List<String> keys;
    private final String description;
    private final Map<String, Collection<Acl>> roleAcls;
    private final List<String> labels;

    private Policy(Builder builder) {
      name = builder.name;
      volumes = builder.volumes;
      buckets = builder.buckets;
      keys = builder.keys;
      description = builder.description;
      roleAcls = builder.roleAcls;
      labels = builder.labels;
    }

    public List<String> getVolumes() {
      return Collections.unmodifiableList(volumes);
    }

    public List<String> getBuckets() {
      return Collections.unmodifiableList(buckets);
    }

    public List<String> getKeys() {
      return Collections.unmodifiableList(keys);
    }

    public String getName() {
      return name;
    }

    public Optional<String> getDescription() {
      return Optional.ofNullable(description);
    }

    public List<String> getLabels() {
      return Collections.unmodifiableList(labels);
    }

    public Map<String, Collection<Acl>> getRoleAcls() {
      return Collections.unmodifiableMap(roleAcls);
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
      private final List<String> volumes;
      private final List<String> buckets;
      private final List<String> keys;
      private String description;
      private final Map<String, Collection<Acl>> roleAcls;
      private final List<String> labels;

      public Builder() {
        this.volumes = new ArrayList<>();
        this.buckets = new ArrayList<>();
        this.keys = new ArrayList<>();
        this.roleAcls = new HashMap<>();
        this.labels = new ArrayList<>();
      }

      public Builder(Policy copy) {
        this.name = copy.getName();
        this.volumes = copy.getVolumes();
        this.buckets = copy.getBuckets();
        this.keys = copy.getKeys();
        copy.getDescription().ifPresent(desc -> this.description = desc);
        this.roleAcls = copy.getRoleAcls();
        this.labels = copy.getLabels();
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
        // All policies must have at least a volume specified as a resource.
        if (volumes.isEmpty()) {
          throw new IllegalStateException("A policy must have at least one " +
              "volume as a resource.");
        }
        if (name == null || name.isEmpty()) {
          throw new IllegalStateException("A policy must have a non-empty " +
              "name.");
        }
        return new Policy(this);
      }
    }
  }
}
