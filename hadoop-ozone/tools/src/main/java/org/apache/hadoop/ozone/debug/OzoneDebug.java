/*
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

package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController;
import org.apache.hadoop.ozone.om.multitenant.RangerClientMultiTenantAccessController;
import org.apache.http.auth.BasicUserPrincipal;
import picocli.CommandLine;

import java.util.Collections;

/**
 * Ozone Debug Command line tool.
 */
@CommandLine.Command(name = "ozone debug",
        description = "Developer tools for Ozone Debug operations",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true)
public class OzoneDebug extends GenericCli {

  private OzoneConfiguration ozoneConf;

  public OzoneDebug() {
    super(OzoneDebug.class);
  }

  public OzoneConfiguration getOzoneConf() {
    if (ozoneConf == null) {
      ozoneConf = createOzoneConfiguration();
    }
    return ozoneConf;
  }

  /**
     * Main for the Ozone Debug shell Command handling.
     *
     * @param argv - System Args Strings[]
     * @throws Exception
     */
  public static void main(String[] argv) throws Exception {
    OzoneDebug debug = new OzoneDebug();
    debug.testRangerClient();
//    debug.run(argv);
  }

  public void testRangerClient() throws Exception {
    // Pass a config with these values as the -conf argument.
//        "ozone.om.kerberos.keytab.file";
//        "ozone.om.kerberos.principal";
//        "ozone.om.ranger.https-address";
//        "ozone.om.ranger.client.ssl.file";
//        "ozone.om.ranger.service";

    // SETUP
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.om.ranger.https-address", "https://erose-1.erose.root.hwx.site:6182");
    conf.set("ozone.om.ranger.client.ssl.file", "/var/run/cloudera-scm-agent/process/29-ozone-OZONE_MANAGER/ozone-conf/ssl-client.xml");
    conf.set("ozone.om.ranger.service", "cm_ozone");

    String prefix = "TEST";
    MultiTenantAccessController controller =
        new RangerClientMultiTenantAccessController(conf);

    // TEST
    MultiTenantAccessController.Role role = new MultiTenantAccessController.Role(prefix + "role1");
    role.getUsers().add(new BasicUserPrincipal("hive"));
    role.setDescription(prefix + "description1");

    System.out.println("creating role1");
    long roleID = controller.createRole(role);
    // Use existing users in the cluster.
    System.out.println("adding users to role1");
    controller.addUsersToRole(roleID,
        Collections.singletonList(new BasicUserPrincipal("solr")));
    controller.addUsersToRole(roleID,
        Collections.singletonList(new BasicUserPrincipal("hdfs")));

    System.out.println("removing user from role1");
    controller.removeUsersFromRole(roleID,
        Collections.singletonList(new BasicUserPrincipal("hdfs")));

    // Should have created a role called TEST-role1 with users hive and solr.

    MultiTenantAccessController.Policy policy1 = new MultiTenantAccessController.Policy(prefix + "policy1", prefix + "volume1");
    policy1.setBucket(prefix + "bucket1");
    policy1.setKey("*");
    policy1.setDescription(prefix + "policy1 description");
    policy1.getRoles().add(role.getName());
    System.out.println("creating policy1");
    controller.createPolicy(policy1);

    MultiTenantAccessController.Policy policy2 = new MultiTenantAccessController.Policy(prefix + "policy2", prefix + "volume1");
    // No buckets or keys for this policy.
    policy2.setDescription(prefix + "policy2 description");
    policy2.getRoles().add(role.getName());
    System.out.println("creating policy2");
    long policy2ID = controller.createPolicy(policy2);

    System.out.println("disabling policy2 with ID " + policy2ID);
    controller.disablePolicy(policy2ID);

    MultiTenantAccessController.Policy policy3 = new MultiTenantAccessController.Policy(prefix + "policy3", prefix + "volume1");
    // No buckets or keys for this policy.
    policy3.setDescription(prefix + "policy3 description");
    policy3.getRoles().add(role.getName());
    System.out.println("creating policy3");
    long policy3ID = controller.createPolicy(policy3);
    System.out.println("deleting policy3 with ID " + policy3ID);
    controller.deletePolicy(policy3ID);

    // policy 1 and 2 should have the role under them.
    // policy1 should have vol bucket and key.
    // policy2 should have volume only.
    // policy3 should not be present.
  }
}
