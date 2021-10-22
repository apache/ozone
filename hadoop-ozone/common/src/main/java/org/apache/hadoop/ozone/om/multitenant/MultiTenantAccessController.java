package org.apache.hadoop.ozone.om.multitenant;

import org.apache.http.auth.BasicUserPrincipal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

/**
 * Defines the operations needed for multi-tenant access control.
 */
public interface MultiTenantAccessController {
  long createPolicy(Policy policy) throws Exception;
  void deletePolicy(long policyID) throws Exception;
  long createRole(Role role) throws Exception;
  void addUsersToRole(long roleID, Collection<BasicUserPrincipal> newUsers)
      throws Exception;
  void removeUsersFromRole(long roleID, Collection<BasicUserPrincipal> users)
      throws Exception;
  void deleteRole(long roleID) throws Exception;
  void enablePolicy(String policyName) throws Exception;
  void disablePolicy(String policyName) throws Exception;

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

    public Collection<BasicUserPrincipal> getUsers() {
      return users;
    }

    public Optional<String> getDescription() {
      return Optional.of(description);
    }

    public void setDescription(String description) {
      this.description = description;
    }
  }

  /**
   * Define a policy to be created.
   */
  class Policy {
    private final String name;
    private final String volume;
    private final Collection<Role> roles;
    private String bucket;
    private String key;
    private String description;

    public Policy(String policyName, String volumeName) {
      this.name = policyName;
      this.volume = volumeName;
      this.roles = new ArrayList<>();
    }

    public String getVolume() {
      return volume;
    }

    public String getName() {
      return name;
    }

    public Optional<String> getBucket() {
      return Optional.ofNullable(bucket);
    }

    public Optional<String> getKey() {
      return Optional.ofNullable(key);
    }

    public Optional<String> getDescription() {
      return Optional.ofNullable(description);
    }

    public Collection<Role> getRoles() {
      return roles;
    }

    public void setBucket(String bucket) {
      this.bucket = bucket;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public void setDescription(String description) {
      this.description = description;
    }
  }
}
