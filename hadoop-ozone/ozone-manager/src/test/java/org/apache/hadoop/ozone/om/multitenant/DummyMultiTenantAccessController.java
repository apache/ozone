package org.apache.hadoop.ozone.om.multitenant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Optional;

public class DummyMultiTenantAccessController
    implements MultiTenantAccessController {

  private final Map<String, Policy>  policies;
  private final Map<String, Role>  roles;
  private long nextRoleID;

  public DummyMultiTenantAccessController() {
    nextRoleID = 0;
    policies = new HashMap<>();
    roles = new HashMap<>();
  }

  @Override
  public void createPolicy(Policy policy) throws Exception {
    if (policies.containsKey(policy.getName())) {
      throw new Exception("Policy already exists.");
    }
    // Multiple policies for the sare resource should not be allowed.
    for (Policy existingPolicy: policies.values()) {
      if (existingPolicy.getVolumes().equals(policy.getVolumes()) &&
          existingPolicy.getBuckets().equals(policy.getBuckets()) &&
      existingPolicy.getKeys().equals(policy.getKeys())) {
        throw new Exception("Policy for the same resource already defined.");
      }
    }
    policies.put(policy.getName(), policy);
    // Ranger will create roles if specified with poicy creation.
    for (String roleName: policy.getRoleAcls().keySet()) {
      if (!roles.containsKey(roleName)) {
        createRole(new Role.Builder().setName(roleName).build());
      }
    }
  }

  @Override
  public Policy getPolicy(String policyName) throws Exception {
    if (!policies.containsKey(policyName)) {
      throw new Exception("Policy does not exist.");
    }
    return policies.get(policyName);
  }

  @Override
  public List<Policy> getLabeledPolicies(String label) throws Exception {
    List<Policy> result = new ArrayList<>();
    for (Policy policy: policies.values()) {
      if (policy.getLabels().contains(label)) {
        result.add(policy);
      }
    }

    return result;
  }

  @Override
  public void updatePolicy(Policy policy) throws Exception {
    if (!policies.containsKey(policy.getName())) {
      throw new Exception("Policy does not exist.");
    }
    policies.put(policy.getName(), policy);
  }

  @Override
  public void deletePolicy(String policyName) throws Exception {
    if (!policies.containsKey(policyName)) {
      throw new Exception("Policy does not exist.");
    }
    policies.remove(policyName);
  }

  @Override
  public void createRole(Role role) throws Exception {
    if (roles.containsKey(role.getName())) {
      throw new Exception("Role already exists.");
    }
    Role newRole = new Role.Builder(role)
        .setID(nextRoleID)
        .build();
    nextRoleID++;
    roles.put(newRole.getName(), newRole);
  }

  @Override
  public Role getRole(String roleName) throws Exception {
    if (!roles.containsKey(roleName)) {
      throw new Exception("Role does not exist.");
    }
    return roles.get(roleName);
  }

  @Override
  public void updateRole(long roleID, Role role) throws Exception {
    Optional<Role> originalRole = roles.values().stream()
        .filter(r -> r.getRoleID().isPresent() && r.getRoleID().get() == roleID)
        .findFirst();
    if (!originalRole.isPresent()) {
      throw new Exception("Role does not exist.");
    }
    // New role may have same ID but different name.
    roles.remove(originalRole.get().getName());
    roles.put(role.getName(), role);
  }

  @Override
  public void deleteRole(String roleName) throws Exception {
    if (!roles.containsKey(roleName)) {
      throw new Exception("Role does not exist.");
    }
    roles.remove(roleName);
  }
}
