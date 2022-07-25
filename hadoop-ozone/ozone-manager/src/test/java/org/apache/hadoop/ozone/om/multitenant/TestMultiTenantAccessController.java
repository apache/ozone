/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.multitenant;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Acl;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Policy;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Role;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ranger.RangerClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_SERVICE;
import static org.apache.hadoop.ozone.om.OMMultiTenantManager.OZONE_TENANT_RANGER_ROLE_DESCRIPTION;

/**
 * To test MultiTenantAccessController with Ranger Client.
 */
public class TestMultiTenantAccessController {
  private MultiTenantAccessController controller;
  private List<String> users;

  @Before
  public void setupUsers() {
    // If testing against a real cluster, users must already be added to Ranger.
    users = new ArrayList<>();
    users.add("om");
    users.add("hdfs");
  }

  /**
   * Use this setup to test against a simulated Ranger instance.
   */
  @Before
  public void setupUnitTest() {
    controller = new InMemoryMultiTenantAccessController();
  }

  /**
   * Use this setup to test against a live Ranger instance.
   */
//  @Before
  public void setupClusterTest() throws Exception {

    // Set up truststore
    System.setProperty("javax.net.ssl.trustStore",
        "/path/to/cm-auto-global_truststore.jks");

    // Specify Kerberos client config (krb5.conf) path
    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");

    // Enable Kerberos debugging
    System.setProperty("sun.security.krb5.debug", "true");

    // DEFAULT rule uses the default realm configured in krb5.conf
    KerberosName.setRules("DEFAULT");

    final OzoneConfiguration conf = new OzoneConfiguration();

    // These config keys must be properly set when the test is run:
    //
    // OZONE_RANGER_HTTPS_ADDRESS_KEY
    // OZONE_RANGER_SERVICE
    // OZONE_OM_KERBEROS_PRINCIPAL_KEY
    // OZONE_OM_KERBEROS_KEYTAB_FILE_KEY

    // Same as OM ranger-ozone-security.xml ranger.plugin.ozone.policy.rest.url
    conf.set(OZONE_RANGER_HTTPS_ADDRESS_KEY,
        "https://RANGER_HOST:6182/");

    // Same as OM ranger-ozone-security.xml ranger.plugin.ozone.service.name
    conf.set(OZONE_RANGER_SERVICE, "cm_ozone");

    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY,
        "om/instance@REALM");

    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
        "/path/to/ozone.keytab");

    // TODO: Test with clear text username and password as well.
//    conf.set(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER, "rangeruser");
//    conf.set(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD, "passwd");

    // (Optional) Enable RangerClient debug log
    GenericTestUtils.setLogLevel(
        LoggerFactory.getLogger(RangerClient.class), Level.DEBUG);

    controller = new RangerClientMultiTenantAccessController(conf);
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
    Assert.assertEquals(originalPolicy, retrievedPolicy);

    // Service policy version should have been bumped by 1 at this point
    long currPolicyVersion = controller.getRangerServicePolicyVersion();
    Assert.assertEquals(prevPolicyVersion + 1L, currPolicyVersion);

    // delete policy.
    controller.deletePolicy(policyName);

    // Service policy version should have been bumped again
    currPolicyVersion = controller.getRangerServicePolicyVersion();
    Assert.assertEquals(prevPolicyVersion + 2L, currPolicyVersion);

    // get to check it is deleted.
    try {
      controller.getPolicy(policyName);
      Assert.fail("Expected exception for missing policy.");
    } catch (Exception ex) {
       // Expected since policy is not there.
    }
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
    Assert.assertEquals(originalPolicy, controller.getPolicy(policyName));

    // Create a policy with the same name but different resource.
    // Check for error.
    MultiTenantAccessController.Policy sameNamePolicy =
        new MultiTenantAccessController.Policy.Builder()
            .setName(policyName)
            .addVolume(volumeName + "2")
            .build();
    try {
      controller.createPolicy(sameNamePolicy);
      Assert.fail("Expected exception for duplicate policy.");
    } catch (Exception ex) {
      // Expected since a policy with the same name should not be allowed.
    }

    // Create a policy with different name but same resource.
    // Check for error.
    MultiTenantAccessController.Policy sameResourcePolicy =
        new MultiTenantAccessController.Policy.Builder()
            .setName(policyName + "2")
            .addVolume(volumeName)
            .build();
    try {
      controller.createPolicy(sameResourcePolicy);
      Assert.fail("Expected exception for duplicate policy.");
    } catch (Exception ex) {
      // Expected since a policy with the same resource should not be allowed.
    }

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
    Assert.assertEquals(labeledPolicies.size(),
        retrievedLabeledPolicies.size());
    Assert.assertTrue(retrievedLabeledPolicies.containsAll(labeledPolicies));

    // Get of a specific policy should also succeed.
    Policy retrievedPolicy = controller.getPolicy(unlabeledPolicy.getName());
    Assert.assertEquals(unlabeledPolicy, retrievedPolicy);

    // Get of policies with nonexistent label should give an empty list.
    Assert.assertTrue(controller.getLabeledPolicies(label + "1").isEmpty());

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
    Assert.assertEquals(originalPolicy,
        controller.getPolicy(policyName));

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
    Assert.assertEquals(updatedPolicy,
        controller.getPolicy(policyName));

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
    Assert.assertEquals(1, retrievedRoleAcls.size());
    List<Acl> roleAcls = new ArrayList<>(retrievedRoleAcls.get(roleName));
    Assert.assertEquals(1, roleAcls.size());
    Assert.assertEquals(ACLType.ALL, roleAcls.get(0).getAclType());
    Assert.assertTrue(roleAcls.get(0).isAllowed());

    // get one of the roles to check it is there but empty.
    Role retrievedRole = controller.getRole(roleName);
    Assert.assertFalse(retrievedRole.getDescription().isPresent());
    Assert.assertTrue(retrievedRole.getUsersMap().isEmpty());
    Assert.assertTrue(retrievedRole.getId().isPresent());

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
    Assert.assertEquals(controller.getRole(roleName), retrievedRole);

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
    Assert.assertTrue(retrievedRole.getId().isPresent());
    Assert.assertEquals(originalRole, retrievedRole);

    // delete role.
    controller.deleteRole(roleName);
    // get to check it is deleted.
    try {
      controller.getRole(roleName);
      Assert.fail("Expected exception for missing role.");
    } catch (Exception ex) {
      // Expected since policy is not there.
    }
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
    Assert.assertEquals(originalRole, controller.getRole(roleName));

    // Create a role with the same name and check for error.
    Role sameNameRole = new Role.Builder()
            .setName(roleName)
            .setDescription(OZONE_TENANT_RANGER_ROLE_DESCRIPTION)
            .build();
    try {
      controller.createRole(sameNameRole);
      Assert.fail("Expected exception for duplicate role.");
    } catch (Exception ex) {
      // Expected since a policy with the same name should not be allowed.
    }

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
    Assert.assertEquals(originalRole, retrievedRole);
    Assert.assertTrue(retrievedRole.getId().isPresent());
    long roleId = retrievedRole.getId().get();

    // Remove a user from the role and update it.
    retrievedRole.getUsersMap().remove(users.get(0));
    Assert.assertEquals(originalRole.getUsersMap().size() - 1,
        retrievedRole.getUsersMap().size());
    controller.updateRole(roleId, retrievedRole);
    Role retrievedUpdatedRole = controller.getRole(roleName);
    Assert.assertEquals(retrievedRole, retrievedUpdatedRole);
    Assert.assertEquals(originalRole.getUsersMap().size() - 1,
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