package org.apache.hadoop.ozone.om.multitenant;

import org.apache.commons.collections.Unmodifiable;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.http.auth.BasicUserPrincipal;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
  void enablePolicy(long policyID) throws Exception;
  void disablePolicy(long policyID) throws Exception;

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

    public Policy(String policyName, String volumeName) {
      this.name = policyName;
      this.volumes = new ArrayList<>();
      this.volumes.add(volumeName);
      this.buckets = new ArrayList<>();
      this.keys = new ArrayList<>();
      this.roleAcls = new HashMap<>();
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

    public void addVolume(String volumeName) {
      volumes.add(volumeName);
    }

    public void addBucket(String bucketName) {
      buckets.add(bucketName);
    }

    public void addKey(String keyName) {
      keys.add(keyName);
    }

    public String getName() {
      return name;
    }

    public Optional<String> getDescription() {
      return Optional.ofNullable(description);
    }

    public void addRoleAcl(String roleName, Acl acl) {
      roleAcls.putIfAbsent(roleName, new ArrayList<>());
      roleAcls.get(roleName).add(acl);
    }

    public Map<String, Collection<Acl>> getRoleAcls() {
      return Collections.unmodifiableMap(roleAcls);
    }

    public void setDescription(String description) {
      this.description = description;
    }
  }
}
