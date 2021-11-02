package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessAuthorizerRangerPlugin;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Role;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Policy;
import org.apache.hadoop.ozone.om.multitenant.OzoneTenantGroupPrincipal;
import org.apache.hadoop.ozone.om.multitenant.RangerClientMultiTenantAccessController;
//import org.apache.hadoop.ozone.om.multitenant.RangerRestMultiTenantAccessController;
import org.apache.http.auth.BasicUserPrincipal;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;

public class TestRangerClientAccessController {
  // Placed before names of ranger items.
  private static final String prefix = "TEST-";
  private MultiTenantAccessController controller;
  private MultiTenantAccessAuthorizerRangerPlugin plugin;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    // TODO: hardcode these.
    conf.set(OZONE_RANGER_HTTPS_ADDRESS_KEY, "https://erose-1.erose.root.hwx" +
        ".site:6182");

    // Ranger client impl uses these.
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, "admin");
    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY, "admin123");
    // Rest client impl uses these.
    conf.set(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER, "admin");
    conf.set(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD, "admin123");

    // Currently getting ssl error here.
//    controller = new RangerClientMultiTenantAccessController(conf);
    // Currently getting 400 bad request jere.
//    controller = new RangerRestMultiTenantAccessController(conf);
    // This works.
    plugin = new MultiTenantAccessAuthorizerRangerPlugin();
    plugin.init(conf);
  }

  @Test
  public void testOldRangerAccess() throws Exception {
    plugin.createGroup(OzoneTenantGroupPrincipal.newAdminGroup("bar"));
  }

  @Test
  public void testNewRangerAccess() throws Exception {
    Role role = new Role(prefix + "role1");
    role.getUsers().add(new BasicUserPrincipal("hive"));
    role.setDescription(prefix + "description1");

    long roleID = controller.createRole(role);
    // Use existing users in the cluster.
    controller.addUsersToRole(roleID,
        Collections.singletonList(new BasicUserPrincipal("solr")));
    controller.addUsersToRole(roleID,
        Collections.singletonList(new BasicUserPrincipal("hdfs")));

    controller.removeUsersFromRole(roleID,
        Collections.singletonList(new BasicUserPrincipal("hdfs")));

    // Should have created a role called TEST-role1 with users hive and solr.

    Policy policy1 = new Policy(prefix + "policy1", prefix + "volume1");
    policy1.setBucket(prefix + "bucket1");
    policy1.setKey("*");
    policy1.setDescription(prefix + "policy1 description");
    policy1.getRoles().add(role.getName());
    controller.createPolicy(policy1);

    Policy policy2 = new Policy(prefix + "policy2", prefix + "volume1");
    // No buckets or keys for this policy.
    policy2.setDescription(prefix + "policy2 description");
    policy2.getRoles().add(role.getName());
    long policy2ID = controller.createPolicy(policy2);

    controller.disablePolicy(policy2ID);

    Policy policy3 = new Policy(prefix + "policy3", prefix + "volume1");
    // No buckets or keys for this policy.
    policy3.setDescription(prefix + "policy3 description");
    policy3.getRoles().add(role.getName());
    long policy3ID = controller.createPolicy(policy3);
    controller.deletePolicy(policy3ID);

    // policy 1 and 2 should have the role under them.
    // policy1 should have vol bucket and key.
    // policy2 should have volume only.
    // policy3 should not be present.
  }
}
