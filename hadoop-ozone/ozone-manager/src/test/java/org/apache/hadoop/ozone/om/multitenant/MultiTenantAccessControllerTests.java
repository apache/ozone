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

import static org.apache.hadoop.ozone.om.OMMultiTenantManager.OZONE_TENANT_RANGER_ROLE_DESCRIPTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThatCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Acl;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Policy;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Role;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * To test MultiTenantAccessController with Ranger Client.
 */
public abstract class MultiTenantAccessControllerTests {
  private MultiTenantAccessController controller;
  private List<String> users;

  protected abstract MultiTenantAccessController createSubject();

  @BeforeEach
  public void setupUsers() {
    // If testing against a real cluster, users must already be added to Ranger.
    users = new ArrayList<>();
    users.add("om");
    users.add("hdfs");
  }

  @BeforeEach
  public void setupUnitTest() {
    controller = createSubject();
    assumeThatCode(() -> controller.getRangerServicePolicyVersion()).doesNotThrowAnyException();
  }

  @Test
  public void testCreateGetDeletePolicies() throws Exception {
    // load a policy with everything possible except roles.
    final String policyName = "test-policy";

    MultiTenantAccessController.Policy originalPolicy =
        new MultiTenantAccessController.Policy.Builder()
            .setName(policyName)
            .addVolume("vol1")
            .addVolume("vol2")
            .addBucket("vol1/bucket1")
            .addBucket("vol2/bucket2")
            .addKey("vol1/bucket1/key1")
            .addKey("vol2/bucket2/key2")
            .setDescription("description")
            .addLabel("label1")
            .addLabel("label2")
            .build();

    // Get the starting service version
    long prevPolicyVersion = controller.getRangerServicePolicyVersion();

    // create in ranger.
    controller.createPolicy(originalPolicy);
    // get to check it's there with all attributes.
    MultiTenantAccessController.Policy retrievedPolicy =
        controller.getPolicy(policyName);
    assertEquals(originalPolicy, retrievedPolicy);

    // Service policy version should have been bumped by 1 at this point
    long currPolicyVersion = controller.getRangerServicePolicyVersion();
    assertEquals(prevPolicyVersion + 1L, currPolicyVersion);

    // delete policy.
    controller.deletePolicy(policyName);

    // Service policy version should have been bumped again
    currPolicyVersion = controller.getRangerServicePolicyVersion();
    assertEquals(prevPolicyVersion + 2L, currPolicyVersion);

    // get to check it is deleted.
    assertThrows(Exception.class, () -> controller.getPolicy(policyName));
  }

  @Test
  public void testCreateDuplicatePolicy() throws Exception {
    final String policyName = "test-policy";
    final String volumeName = "vol1";
    MultiTenantAccessController.Policy originalPolicy =
        new MultiTenantAccessController.Policy.Builder()
            .setName(policyName)
            .addVolume(volumeName)
            .build();
    // create in ranger.
    controller.createPolicy(originalPolicy);
    assertEquals(originalPolicy, controller.getPolicy(policyName));

    // Create a policy with the same name but different resource.
    // Check for error.
    MultiTenantAccessController.Policy sameNamePolicy =
        new MultiTenantAccessController.Policy.Builder()
            .setName(policyName)
            .addVolume(volumeName + "2")
            .build();
    assertThrows(Exception.class, () -> controller.createPolicy(sameNamePolicy));

    // Create a policy with different name but same resource.
    // Check for error.
    MultiTenantAccessController.Policy sameResourcePolicy =
        new MultiTenantAccessController.Policy.Builder()
            .setName(policyName + "2")
            .addVolume(volumeName)
            .build();
    assertThrows(Exception.class, () -> controller.createPolicy(sameResourcePolicy));

    // delete policy.
    controller.deletePolicy(policyName);
  }

  @Test
  public void testGetLabeledPolicies() throws Exception  {
    final String label = "label";
    Policy labeledPolicy1 = new Policy.Builder()
        .setName("policy1")
        .addVolume(UUID.randomUUID().toString())
        .addLabel(label)
        .build();
    Policy labeledPolicy2 = new Policy.Builder()
        .setName("policy2")
        .addVolume(UUID.randomUUID().toString())
        .addLabel(label)
        .build();

    List<Policy> labeledPolicies = new ArrayList<>();
    labeledPolicies.add(labeledPolicy1);
    labeledPolicies.add(labeledPolicy2);
    Policy unlabeledPolicy = new Policy.Builder()
        .setName("policy3")
        .addVolume(UUID.randomUUID().toString())
        .build();

    for (Policy policy: labeledPolicies) {
      controller.createPolicy(policy);
    }
    controller.createPolicy(unlabeledPolicy);

    // Get should only return policies with the specified label.
    List<Policy> retrievedLabeledPolicies =
        controller.getLabeledPolicies(label);
    assertEquals(labeledPolicies.size(), retrievedLabeledPolicies.size());
    assertThat(retrievedLabeledPolicies).containsAll(labeledPolicies);

    // Get of a specific policy should also succeed.
    Policy retrievedPolicy = controller.getPolicy(unlabeledPolicy.getName());
    assertEquals(unlabeledPolicy, retrievedPolicy);

    // Get of policies with nonexistent label should give an empty list.
    assertThat(controller.getLabeledPolicies(label + "1")).isEmpty();

    // Cleanup
    for (Policy policy: labeledPolicies) {
      controller.deletePolicy(policy.getName());
    }
    controller.deletePolicy(unlabeledPolicy.getName());
  }

  @Test
  public void testUpdatePolicy() throws Exception {
    String policyName = "policy";
    // Since the roles will not exist when the policy is created, Ranger
    // should create them.
    Policy originalPolicy = new Policy.Builder()
        .setName(policyName)
        .addVolume("vol1")
        .addLabel("label1")
        .addRoleAcl("role1",
            Collections.singletonList(Acl.allow(ACLType.READ_ACL)))
        .build();
    controller.createPolicy(originalPolicy);
    assertEquals(originalPolicy, controller.getPolicy(policyName));

    Policy updatedPolicy = new Policy.Builder()
        .setName(policyName)
        .addVolume("vol2")
        .addLabel("label2")
        .addRoleAcl("role1",
            Collections.singletonList(Acl.allow(ACLType.WRITE_ACL)))
        .addRoleAcl("role2",
            Collections.singletonList(Acl.allow(ACLType.READ_ACL)))
        .build();
    controller.updatePolicy(updatedPolicy);
    assertEquals(updatedPolicy, controller.getPolicy(policyName));

    // Cleanup
    controller.deletePolicy(policyName);
  }

  @Test
  public void testCreatePolicyWithRoles() throws Exception {
    // Create a policy with role acls.
    final String roleName = "role1";
    Policy policy = new Policy.Builder()
        .setName("policy1")
        .addVolume("volume")
        .addRoleAcl(roleName,
            Collections.singletonList(Acl.allow(ACLType.ALL)))
        .build();
    // This should create the role as well.
    controller.createPolicy(policy);

    // Test the acls set on the role for the policy.
    Policy retrievedPolicy = controller.getPolicy(policy.getName());
    Map<String, Collection<Acl>> retrievedRoleAcls =
        retrievedPolicy.getRoleAcls();
    assertEquals(1, retrievedRoleAcls.size());
    List<Acl> roleAcls = new ArrayList<>(retrievedRoleAcls.get(roleName));
    assertEquals(1, roleAcls.size());
    assertEquals(ACLType.ALL, roleAcls.get(0).getAclType());
    assertTrue(roleAcls.get(0).isAllowed());

    // get one of the roles to check it is there but empty.
    Role retrievedRole = controller.getRole(roleName);
    assertFalse(retrievedRole.getDescription().isPresent());
    assertThat(retrievedRole.getUsersMap()).isEmpty();
    assertTrue(retrievedRole.getId().isPresent());

    // Add a user to the role.
    retrievedRole.getUsersMap().put(users.get(0), false);
    controller.updateRole(retrievedRole.getId().get(), retrievedRole);

    // Create a new policy containing the role. This should not overwrite the
    // role.
    Policy policy2 = new Policy.Builder()
        .setName("policy2")
        .addVolume("volume2")
        .addRoleAcl(roleName,
            Collections.singletonList(Acl.allow(ACLType.READ)))
        .build();
    controller.createPolicy(policy2);
    assertEquals(controller.getRole(roleName), retrievedRole);

    controller.deletePolicy("policy1");
    controller.deletePolicy("policy2");
    controller.deleteRole(roleName);
  }

  @Test
  public void testCreateGetDeleteRoles() throws Exception {
    // load a role with everything possible.
    final String roleName = "test-role";

    Role originalRole =
        new Role.Builder()
            .setName(roleName)
            .addUsers(users)
            .setDescription(OZONE_TENANT_RANGER_ROLE_DESCRIPTION)
            .build();

    // create in ranger.
    controller.createRole(originalRole);
    // get to check it's there with all attributes.
    Role retrievedRole = controller.getRole(roleName);
    // Role ID should have been added by Ranger.
    assertTrue(retrievedRole.getId().isPresent());
    assertEquals(originalRole, retrievedRole);

    // delete role.
    controller.deleteRole(roleName);
    // get to check it is deleted.
    assertThrows(Exception.class, () -> controller.getRole(roleName));
  }

  @Test
  public void testCreateDuplicateRole() throws Exception {
    final String roleName = "test-role";
    Role originalRole = new Role.Builder()
            .setName(roleName)
            .setDescription(OZONE_TENANT_RANGER_ROLE_DESCRIPTION)
            .build();
    // create in Ranger.
    controller.createRole(originalRole);
    assertEquals(originalRole, controller.getRole(roleName));

    // Create a role with the same name and check for error.
    Role sameNameRole = new Role.Builder()
            .setName(roleName)
            .setDescription(OZONE_TENANT_RANGER_ROLE_DESCRIPTION)
            .build();
    assertThrows(Exception.class, () -> controller.createRole(sameNameRole));
    // delete role.
    controller.deleteRole(roleName);
  }

  @Test
  public void testUpdateRole() throws Exception {
    final String roleName = "test-role";
    Role originalRole = new Role.Builder()
        .setName(roleName)
        .addUsers(users)
        .setDescription(OZONE_TENANT_RANGER_ROLE_DESCRIPTION)
        .build();
    // create in Ranger.
    controller.createRole(originalRole);

    Role retrievedRole = controller.getRole(roleName);
    assertEquals(originalRole, retrievedRole);
    assertTrue(retrievedRole.getId().isPresent());
    long roleId = retrievedRole.getId().get();

    // Remove a user from the role and update it.
    retrievedRole.getUsersMap().remove(users.get(0));
    assertEquals(originalRole.getUsersMap().size() - 1,
        retrievedRole.getUsersMap().size());
    controller.updateRole(roleId, retrievedRole);
    Role retrievedUpdatedRole = controller.getRole(roleName);
    assertEquals(retrievedRole, retrievedUpdatedRole);
    assertEquals(originalRole.getUsersMap().size() - 1,
        retrievedUpdatedRole.getUsersMap().size());

    // Cleanup.
    controller.deleteRole(roleName);
  }

  /**
   * Test that Acl types are correctly converted to strings that Ranger can
   * understand. An exception will be thrown if Ranger does not recognize the
   * Acl.
   */
  @Test
  public void testRangerAclStrings() throws Exception {
    // Create a policy that uses all possible acl types.
    List<Acl> acls = Arrays.stream(ACLType.values())
        .map(Acl::allow)
        .collect(Collectors.toList());
    // Ranger does not support the NONE acl type.
    Acl noneRemove = Acl.allow(ACLType.NONE);
    acls.remove(noneRemove);
    Policy policy = new Policy.Builder()
        .setName("policy")
        .addVolume("volume")
        .addRoleAcl("role", acls)
        .build();
    // Converting from ACLType to Ranger strings should not produce an error.
    controller.createPolicy(policy);
    // Converting from Ranger strings to ACLType should not produce an error.
    controller.getPolicy(policy.getName());
    // cleanup.
    controller.deletePolicy(policy.getName());
  }
}
