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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Acl;
import org.apache.hadoop.ozone.om.multitenant.RangerClientMultiTenantAccessController;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.http.auth.BasicUserPrincipal;
import picocli.CommandLine;

/**
 * Temporary command for testing Ranger client in live cluster.
 */
@CommandLine.Command(name = "ozone rangertest",
        description = "Temporary testing for Ranger client",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true)
public class RangerTestCommand extends GenericCli {

  @CommandLine.Option(
      names = {"-testconf"},
      arity = "1..*",
      description = "Pass multiple configurations to use. These will be " +
          "passed to the Ranger Client as well."
  )
  private String[] confPaths;

  public RangerTestCommand() {
    super(RangerTestCommand.class);
  }

  public static void main(String[] argv) throws Exception {
    new RangerTestCommand().run(argv);
  }

  @Override
  public Void call() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    if (confPaths != null) {
      for (String path: confPaths) {
        System.out.println("loading conf: " + path);
        conf.addResource(new Path(path));
      }
    }

    // SETUP
    String prefix = "TEST";
    MultiTenantAccessController controller =
        new RangerClientMultiTenantAccessController(conf);

    // TEST
    MultiTenantAccessController.Role role = new MultiTenantAccessController.Role(prefix + "role1");
    role.addUser(new BasicUserPrincipal("hive"));
    role.setDescription(prefix + "description1");

    System.out.println("creating role1");
    long roleID = controller.createRole(role);
    // Use existing users in the cluster.
    System.out.println("adding users to role1");
    controller.addUsersToRole(roleID,
        new BasicUserPrincipal("solr"));
    controller.addUsersToRole(roleID, new BasicUserPrincipal("hdfs"));

    System.out.println("removing user from role1");
    controller.removeUsersFromRole(roleID, new BasicUserPrincipal("hdfs"));

    // Should have created a role called TEST-role1 with users hive and solr.

    MultiTenantAccessController.Policy policy1 = new MultiTenantAccessController.Policy(prefix + "policy1", prefix + "volume1");
    policy1.addBuckets(prefix + "bucket1");
    policy1.addKeys("*");
    policy1.setDescription(prefix + "policy1 description");
    policy1.addRoleAcls(role.getName(), Acl.allow(IAccessAuthorizer.ACLType.READ_ACL));
    System.out.println("creating policy1");
    controller.createPolicy(policy1);

    MultiTenantAccessController.Policy policy2 = new MultiTenantAccessController.Policy(prefix + "policy2", prefix + "volume1");
    // No buckets or keys for this policy.
    policy2.setDescription(prefix + "policy2 description");
    policy1.addRoleAcls(role.getName(), Acl.allow(IAccessAuthorizer.ACLType.READ_ACL));
    System.out.println("creating policy2");
    long policy2ID = controller.createPolicy(policy2);

    System.out.println("disabling policy2 with ID " + policy2ID);
    controller.disablePolicy(policy2ID);

    MultiTenantAccessController.Policy policy3 = new MultiTenantAccessController.Policy(prefix + "policy3", prefix + "volume1");
    // No buckets or keys for this policy.
    policy3.setDescription(prefix + "policy3 description");
    policy1.addRoleAcls(role.getName(), Acl.allow(IAccessAuthorizer.ACLType.READ_ACL));
    System.out.println("creating policy3");
    long policy3ID = controller.createPolicy(policy3);
    System.out.println("deleting policy3 with ID " + policy3ID);
    controller.deletePolicy(policy3ID);

    // policy 1 and 2 should have the role under them.
    // policy1 should have vol bucket and key.
    // policy2 should have volume only.
    // policy3 should not be present.

    return null;
  }
}
