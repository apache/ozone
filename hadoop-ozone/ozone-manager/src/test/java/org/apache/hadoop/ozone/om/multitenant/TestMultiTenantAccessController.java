package org.apache.hadoop.ozone.om.multitenant;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Acl;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Policy;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Role;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.http.auth.BasicUserPrincipal;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class TestMultiTenantAccessController {
  private MultiTenantAccessController controller;
  private List<BasicUserPrincipal> users;

  @Before
  public void setupUsers() {
    // If testing against a real cluster, users must already be added to Ranger.
    users = new ArrayList<>();
    users.add(new BasicUserPrincipal("om"));
    users.add(new BasicUserPrincipal("hdfs"));
  }

  /**
   * Use this setup to test against a mock Ranger instance.
   */
   @Before
   public void setupUnitTest() {
     controller = new InMemoryMultiTenantAccessController();
   }

  /**
   * Use this setup to test against a live Ranger instance.
   */
  //  @Before
  public void setupClusterTest() {
    // These config keys must be set when the test is run:
    // OZONE_RANGER_HTTPS_ADDRESS_KEY
    // OZONE_RANGER_SERVICE
    // These config keys must be set in a secure cluster.
    // OZONE_OM_KERBEROS_PRINCIPAL_KEY
    // OZONE_OM_KERBEROS_KEYTAB_FILE_KEY
    OzoneConfiguration conf = new OzoneConfiguration();
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
              .addBucket( "vol2/bucket2")
              .addKey("vol1/bucket1/key1")
              .addKey("vol2/bucket2/key2")
              .setDescription("description")
              .addLabel("label1")
              .addLabel("label2")
              .build();

     // create in ranger.
     controller.createPolicy(originalPolicy);
     // get to check it's there with all attributes.
     MultiTenantAccessController.Policy retrievedPolicy =
         controller.getPolicy(policyName);
     Assert.assertEquals(originalPolicy, retrievedPolicy);

     // delete policy.
     controller.deletePolicy(policyName);
     // get to check it is deleted.
     try {
       controller.getPolicy(policyName);
       Assert.fail("Expected exception for missing policy.");
     } catch(Exception ex) {
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
      } catch(Exception ex) {
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
      } catch(Exception ex) {
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
    Assert.assertTrue(retrievedRole.getUsers().isEmpty());
    Assert.assertTrue(retrievedRole.getRoleID().isPresent());

    // Add a user to the role.
    retrievedRole.getUsers().add(users.get(0));
    controller.updateRole(retrievedRole.getRoleID().get(), retrievedRole);

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
            .build();

    // create in ranger.
    controller.createRole(originalRole);
    // get to check it's there with all attributes.
    Role retrievedRole = controller.getRole(roleName);
    // Role ID should have been added by Ranger.
    Assert.assertTrue(retrievedRole.getRoleID().isPresent());
    Assert.assertEquals(originalRole, retrievedRole);

    // delete role.
    controller.deleteRole(roleName);
    // get to check it is deleted.
    try {
      controller.getPolicy(roleName);
      Assert.fail("Expected exception for missing policy.");
    } catch(Exception ex) {
      // Expected since policy is not there.
    }
  }

  @Test
  public void testCreateDuplicateRole() throws Exception {
    final String roleName = "test-role";
    Role originalRole = new Role.Builder()
            .setName(roleName)
            .build();
    // create in Ranger.
    controller.createRole(originalRole);
    Assert.assertEquals(originalRole, controller.getRole(roleName));

    // Create a role with the same name and check for error.
    Role sameNameRole = new Role.Builder()
            .setName(roleName)
            .build();
    try {
      controller.createRole(sameNameRole);
      Assert.fail("Expected exception for duplicate role.");
    } catch(Exception ex) {
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
        .build();
    // create in Ranger.
    controller.createRole(originalRole);

    Role retrievedRole = controller.getRole(roleName);
    Assert.assertEquals(originalRole, retrievedRole);
    Assert.assertTrue(retrievedRole.getRoleID().isPresent());
    long roleID = retrievedRole.getRoleID().get();

    // Remove a user from the role and update it.
    retrievedRole.getUsers().remove(users.get(0));
    Assert.assertEquals(originalRole.getUsers().size() - 1,
        retrievedRole.getUsers().size());
    controller.updateRole(roleID, retrievedRole);
    Role retrievedUpdatedRole = controller.getRole(roleName);
    Assert.assertEquals(retrievedRole, retrievedUpdatedRole);
    Assert.assertEquals(originalRole.getUsers().size() - 1,
        retrievedUpdatedRole.getUsers().size());

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