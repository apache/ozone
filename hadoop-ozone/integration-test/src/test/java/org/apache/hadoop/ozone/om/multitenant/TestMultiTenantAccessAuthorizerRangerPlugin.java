/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.multitenant;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.multitenant.AccessPolicy.AccessGrantType.ALLOW;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ_ACL;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.http.auth.BasicUserPrincipal;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests TestMultiTenantGateKeeperImplWithRanger.
 * Marking it as Ignore because it needs Ranger access point.
 */
public class TestMultiTenantAccessAuthorizerRangerPlugin {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestMultiTenantAccessAuthorizerRangerPlugin.class);

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(300000);

  // The following values need to be set before this test can be enabled.
  private static final String RANGER_ENDPOINT = "";
  private static final String RANGER_ENDPOINT_USER = "";
  private static final String RANGER_ENDPOINT_USER_PASSWD = "";

  private List<String> usersIdsCreated = new ArrayList<String>();
  private List<String> groupIdsCreated = new ArrayList<String>();
  private List<String> policyIdsCreated = new ArrayList<String>();

  private static OzoneConfiguration conf;

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    simulateOzoneSiteXmlConfig();
  }

  @AfterClass
  public static void shutdown() {
  }

  private static void simulateOzoneSiteXmlConfig() {
    conf.setStrings(OZONE_RANGER_HTTPS_ADDRESS_KEY, RANGER_ENDPOINT);
    conf.setStrings(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER, RANGER_ENDPOINT_USER);
    conf.setStrings(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD,
        RANGER_ENDPOINT_USER_PASSWD);
  }

  @Test
  @Ignore("TODO:Requires (mocked) Ranger endpoint")
  public void testMultiTenantAccessAuthorizerRangerPlugin() throws Exception {
    simulateOzoneSiteXmlConfig();
    final MultiTenantAccessAuthorizer omm =
        new MultiTenantAccessAuthorizerRangerPlugin();
    omm.init(conf);

    try {
      OzoneTenantRolePrincipal group1Principal =
          OzoneTenantRolePrincipal.getAdminRole("tenant1");
      OzoneTenantRolePrincipal group2Principal =
          OzoneTenantRolePrincipal.getUserRole("tenant1");
      groupIdsCreated.add(omm.createRole(group1Principal, null));
      groupIdsCreated.add(omm.createRole(group2Principal,
          group1Principal.getName()));

      BasicUserPrincipal userPrincipal =
          new BasicUserPrincipal("user1Test");
//      usersIdsCreated.add(omm.assignUser(userPrincipal, groupIdsCreated));

      AccessPolicy tenant1VolumeAccessPolicy = createVolumeAccessPolicy(
          "vol1", "tenant1");
      policyIdsCreated.add(omm.createAccessPolicy(tenant1VolumeAccessPolicy));

      AccessPolicy tenant1BucketCreatePolicy = allowCreateBucketPolicy(
          "vol1", "tenant1");
      policyIdsCreated.add(omm.createAccessPolicy(tenant1BucketCreatePolicy));

      AccessPolicy tenant1BucketAccessPolicy = allowAccessBucketPolicy(
          "vol1", "bucket1", "tenant1");
      policyIdsCreated.add(omm.createAccessPolicy(tenant1BucketAccessPolicy));

      AccessPolicy tenant1KeyAccessPolicy = allowAccessKeyPolicy(
          "vol1", "bucket1", "tenant1");
      policyIdsCreated.add(omm.createAccessPolicy(tenant1KeyAccessPolicy));

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      for (String id : policyIdsCreated) {
        omm.deletePolicybyId(id);
      }
      for (String id : usersIdsCreated) {
        omm.deleteUser(id);
      }
      for (String id : groupIdsCreated) {
        omm.deleteRole(id);
      }
    }
  }

  @Test
  @Ignore("TODO:Requires (mocked) Ranger endpoint")
  public void testMultiTenantAccessAuthorizerRangerPluginWithoutIds()
      throws Exception {
    BasicUserPrincipal userPrincipal = null;
    simulateOzoneSiteXmlConfig();
    final MultiTenantAccessAuthorizer omm =
        new MultiTenantAccessAuthorizerRangerPlugin();
    omm.init(conf);

    try {
      Assert.assertTrue(policyIdsCreated.size() == 0);
      OzoneTenantRolePrincipal group1Principal =
          OzoneTenantRolePrincipal.getAdminRole("tenant1");
      OzoneTenantRolePrincipal group2Principal =
          OzoneTenantRolePrincipal.getUserRole("tenant1");
      omm.createRole(group1Principal, null);
      groupIdsCreated.add(omm.getRole(group1Principal));
      omm.createRole(group2Principal, group1Principal.getName());
      groupIdsCreated.add(omm.getRole(group2Principal));

      userPrincipal = new BasicUserPrincipal("user1Test");
//      omm.assignUser(userPrincipal, groupIdsCreated);

      AccessPolicy tenant1VolumeAccessPolicy = createVolumeAccessPolicy(
          "vol1", "tenant1");
      omm.createAccessPolicy(tenant1VolumeAccessPolicy);
      policyIdsCreated.add(tenant1VolumeAccessPolicy.getPolicyName());

      AccessPolicy tenant1BucketCreatePolicy = allowCreateBucketPolicy(
          "vol1", "tenant1");
      omm.createAccessPolicy(tenant1BucketCreatePolicy);
      policyIdsCreated.add(tenant1BucketCreatePolicy.getPolicyName());

      AccessPolicy tenant1BucketAccessPolicy = allowAccessBucketPolicy(
          "vol1", "bucket1", "tenant1");
      omm.createAccessPolicy(tenant1BucketAccessPolicy);
      policyIdsCreated.add(tenant1BucketAccessPolicy.getPolicyName());

      AccessPolicy tenant1KeyAccessPolicy = allowAccessKeyPolicy(
          "vol1", "bucket1", "tenant1");
      omm.createAccessPolicy(tenant1KeyAccessPolicy);
      policyIdsCreated.add(tenant1KeyAccessPolicy.getPolicyName());

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      for (String name : policyIdsCreated) {
        omm.deletePolicybyName(name);
      }
      String userId = omm.getUserId(userPrincipal);
      omm.deleteUser(userId);
      for (String id : groupIdsCreated) {
        omm.deleteRole(id);
      }
    }
  }

  private AccessPolicy createVolumeAccessPolicy(String vol, String tenant)
      throws IOException {
    OzoneTenantRolePrincipal principal =
        OzoneTenantRolePrincipal.getUserRole(tenant);
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        // principal already contains volume name
        principal.getName() + "VolumeAccess");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(VOLUME).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName("").setKeyName("").build();
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, READ, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, LIST, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal,
        READ_ACL, ALLOW);
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowCreateBucketPolicy(String vol, String tenant)
      throws IOException {
    OzoneTenantRolePrincipal principal =
        OzoneTenantRolePrincipal.getUserRole(tenant);
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        // principal already contains volume name
        principal.getName() + "BucketAccess");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(BUCKET).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName("*").setKeyName("").build();
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, CREATE, ALLOW);
    return tenantVolumeAccessPolicy;
  }

  // TODO: REMOVE THIS?
  private AccessPolicy allowAccessBucketPolicy(String vol, String bucketName,
      String tenant) throws IOException {
    OzoneTenantRolePrincipal principal =
        OzoneTenantRolePrincipal.getUserRole(tenant);
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        principal.getName() + "AllowBucketAccess" + vol + bucketName +
            "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(BUCKET).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName(bucketName).setKeyName("*").build();
    for (ACLType acl :ACLType.values()) {
      if (acl != NONE) {
        tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, acl,
            ALLOW);
      }
    }
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowAccessKeyPolicy(String vol, String bucketName,
      String tenant) throws IOException {
    OzoneTenantRolePrincipal principal =
        OzoneTenantRolePrincipal.getUserRole(tenant);
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        principal.getName() + "AllowBucketKeyAccess" + vol + bucketName +
            "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(KEY).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName(bucketName).setKeyName("*").build();
    for (ACLType acl :ACLType.values()) {
      if (acl != NONE) {
        tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, acl,
            ALLOW);
      }
    }
    return tenantVolumeAccessPolicy;
  }
}

