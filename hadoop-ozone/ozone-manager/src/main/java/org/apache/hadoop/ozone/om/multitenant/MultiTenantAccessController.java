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

import static org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl.OZONE_OM_TENANT_DEV_SKIP_RANGER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.ratis.util.ReflectionUtils;

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
  Policy createPolicy(Policy policy) throws IOException;

  Policy getPolicy(String policyName) throws IOException;

  List<Policy> getLabeledPolicies(String label) throws IOException;

  Policy updatePolicy(Policy policy) throws IOException;

  void deletePolicy(String policyName) throws IOException;

  /**
   * This operation will fail if a role with the same name already exists.
   *
   * @return Role ID returned from remote server.
   */
  Role createRole(Role role) throws IOException;

  Role getRole(String roleName) throws IOException;

  /**
   * Replaces the role given by {@code roleId} with the contents of {@code
   * role}. If {@code roleId} does not correspond to a role, an exception is
   * thrown.
   *
   * The roleId of a given role can be retrieved from the {@code getRole}
   * method.
   */
  Role updateRole(long roleId, Role role) throws IOException;

  void deleteRole(String roleName) throws IOException;

  long getRangerServicePolicyVersion() throws IOException;

  static Map<IAccessAuthorizer.ACLType, String> getRangerAclStrings() {
    Map<IAccessAuthorizer.ACLType, String> rangerAclStrings =
        new EnumMap<>(IAccessAuthorizer.ACLType.class);
    rangerAclStrings.put(IAccessAuthorizer.ACLType.ALL, "all");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.LIST, "list");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.READ, "read");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.WRITE, "write");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.CREATE, "create");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.DELETE, "delete");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.READ_ACL, "read_acl");
    rangerAclStrings.put(IAccessAuthorizer.ACLType.WRITE_ACL, "write_acl");

    return rangerAclStrings;
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
    public int hashCode() {
      return Objects.hash(acl);
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

    @Override
    public String toString() {
      return "Acl{" +
          "isAllowed=" + isAllowed +
          ", acl=" + acl +
          '}';
    }
  }

  /**
   * Define a role to be created.
   */
  class Role {
    private final String name;
    private final Map<String, Boolean> usersMap;
    private final Map<String, Boolean> rolesMap;
    private final String description;
    private final Long id;
    private final String createdByUser;

    private Role(Builder builder) {
      this.name = builder.name;
      this.usersMap = builder.usersMap;
      this.rolesMap = builder.rolesMap;
      this.description = builder.description;
      this.id = builder.id;
      this.createdByUser = builder.createdByUser;
    }

    public String getName() {
      return name;
    }

    public Map<String, Boolean> getUsersMap() {
      return usersMap;
    }

    public Map<String, Boolean> getRolesMap() {
      return rolesMap;
    }

    public Optional<String> getDescription() {
      return Optional.ofNullable(description);
    }

    public Optional<Long> getId() {
      return Optional.ofNullable(id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
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
      boolean roleIdsMatch = true;
      if (getId().isPresent() && role.getId().isPresent()) {
        roleIdsMatch = getId().equals(role.getId());
      }
      return Objects.equals(getName(), role.getName()) &&
          Objects.equals(getUsersMap(), role.getUsersMap()) &&
          Objects.equals(getDescription(), role.getDescription()) &&
          roleIdsMatch;
    }

    @Override
    public String toString() {
      return "Role{" +
          "id=" + id +
          ", name='" + name + '\'' +
          ", usersMap=" + usersMap +
          ", rolesMap=" + rolesMap +
          ", description='" + description + '\'' +
          ", createdByUser='" + createdByUser + '\'' +
          '}';
    }

    public String getCreatedByUser() {
      return createdByUser;
    }

    /**
     * Builder class for a role.
     */
    public static final class Builder {
      private String name;
      // userName -> isRoleAdmin
      private final Map<String, Boolean> usersMap;
      // roleName -> isRoleAdmin
      private final Map<String, Boolean> rolesMap;
      private String description;
      private Long id;
      private String createdByUser;

      public Builder() {
        this.usersMap = new HashMap<>();
        this.rolesMap = new HashMap<>();
      }

      public Builder(Role other) {
        this.name = other.getName();
        this.usersMap = new HashMap<>(other.getUsersMap());
        this.rolesMap = new HashMap<>(other.getRolesMap());
        other.getDescription().ifPresent(desc -> this.description = desc);
        other.getId().ifPresent(roleId -> this.id = roleId);
        this.createdByUser = other.getCreatedByUser();
      }

      public Builder setName(String roleName) {
        this.name = roleName;
        return this;
      }

      /**
       * Add one user to this role.
       */
      public Builder addUser(String userName, boolean isRoleAdmin) {
        this.usersMap.put(userName, isRoleAdmin);
        return this;
      }

      /**
       * Add a list of users as role non-admins.
       */
      public Builder addUsers(Collection<String> userNamesList) {
        userNamesList.forEach(userName -> this.usersMap.put(userName, false));
        return this;
      }

      /**
       * Merge with another users map.
       */
      public Builder addUsersMap(Map<String, Boolean> userNamesList) {
        this.usersMap.putAll(userNamesList);
        return this;
      }

      public Builder removeUser(String userName) {
        this.usersMap.remove(userName);
        return this;
      }

      /**
       * Clear users map.
       */
      public Builder clearUsers() {
        this.usersMap.clear();
        return this;
      }

      /**
       * Add one other role to this role.
       */
      public Builder addRole(String roleName, boolean isRoleAdmin) {
        this.rolesMap.put(roleName, isRoleAdmin);
        return this;
      }

      /**
       * Add a list of other roles as role non-admins.
       */
      public Builder addRoles(Collection<String> roleNamesList) {
        roleNamesList.forEach(userName -> this.rolesMap.put(userName, false));
        return this;
      }

      public Builder setDescription(String roleDescription) {
        this.description = roleDescription;
        return this;
      }

      public Builder setID(long roleId) {
        this.id = roleId;
        return this;
      }

      public Builder setCreatedByUser(String createdByUser) {
        this.createdByUser = createdByUser;
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
    private final long id;
    private final String name;
    private final Set<String> volumes;
    private final Set<String> buckets;
    private final Set<String> keys;
    private final String description;
    private final Map<String, Collection<Acl>> userAcls, roleAcls;
    private final Set<String> labels;
    private final boolean isEnabled;

    private Policy(Builder builder) {
      this.id = builder.id;
      this.name = builder.name;
      this.volumes = builder.volumes;
      this.buckets = builder.buckets;
      this.keys = builder.keys;
      this.description = builder.description;
      this.userAcls = builder.userAcls;
      this.roleAcls = builder.roleAcls;
      this.labels = builder.labels;
      this.isEnabled = builder.isEnabled;
    }

    public Set<String> getVolumes() {
      return volumes;
    }

    public Set<String> getBuckets() {
      return buckets;
    }

    public Set<String> getKeys() {
      return keys;
    }

    public long getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public Optional<String> getDescription() {
      return Optional.ofNullable(description);
    }

    public Set<String> getLabels() {
      return (labels);
    }

    public Map<String, Collection<Acl>> getUserAcls() {
      return userAcls;
    }

    public Map<String, Collection<Acl>> getRoleAcls() {
      return roleAcls;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
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
          Objects.equals(getUserAcls(), policy.getUserAcls()) &&
          Objects.equals(getRoleAcls(), policy.getRoleAcls()) &&
          Objects.equals(getLabels(), policy.getLabels());
    }

    @Override
    public String toString() {
      return "Policy{" +
          "id=" + id +
          ", name='" + name + '\'' +
          ", volumes=" + volumes +
          ", buckets=" + buckets +
          ", keys=" + keys +
          ", description='" + description + '\'' +
          ", userAcls=" + userAcls +
          ", roleAcls=" + roleAcls +
          ", labels=" + labels +
          ", isEnabled=" + isEnabled +
          '}';
    }

    public boolean isEnabled() {
      return isEnabled;
    }

    /**
     * Builder class for a policy.
     */
    public static final class Builder {
      private long id;
      private String name;
      private final Set<String> volumes;
      private final Set<String> buckets;
      private final Set<String> keys;
      private String description;
      private final Map<String, Collection<Acl>> userAcls, roleAcls;
      private final Set<String> labels;
      private boolean isEnabled;

      public Builder() {
        this.volumes = new HashSet<>();
        this.buckets = new HashSet<>();
        this.keys = new HashSet<>();
        this.userAcls = new HashMap<>();
        this.roleAcls = new HashMap<>();
        this.labels = new HashSet<>();
      }

      public Builder setId(Long policyId) {
        this.id = policyId;
        return this;
      }

      public Builder setName(String policyName) {
        this.name = policyName;
        return this;
      }

      public Builder setEnabled(boolean enabled) {
        this.isEnabled = enabled;
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

      public Builder addVolumes(Collection<String> volumeList) {
        this.volumes.addAll(volumeList);
        return this;
      }

      public Builder addBuckets(Collection<String> bucketList) {
        this.buckets.addAll(bucketList);
        return this;
      }

      public Builder addKeys(Collection<String> keyList) {
        this.keys.addAll(keyList);
        return this;
      }

      public Builder setDescription(String policyDescription) {
        this.description = policyDescription;
        return this;
      }

      public Builder addUserAcl(String userName, Collection<Acl> acls) {
        this.userAcls.put(userName, new ArrayList<>(acls));
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

      public Builder addLabels(Collection<String> labelsList) {
        this.labels.addAll(labelsList);
        return this;
      }

      public Policy build() {
        if (name == null || name.isEmpty()) {
          throw new IllegalStateException("A policy must have a non-empty " +
              "name.");
        }
        return new Policy(this);
      }
    }
  }

  /** Create {@code MultiTenantAccessController} implementation. */
  static MultiTenantAccessController create(ConfigurationSource conf) {
    if (conf.getBoolean(OZONE_OM_TENANT_DEV_SKIP_RANGER, false)) {
      return new InMemoryMultiTenantAccessController();
    }

    final String className = "org.apache.hadoop.ozone.om.multitenant.RangerClientMultiTenantAccessController";
    return ReflectionUtils.newInstance(
        ReflectionUtils.getClass(className, MultiTenantAccessController.class),
        new Class<?>[] {ConfigurationSource.class},
        conf
    );
  }
}
