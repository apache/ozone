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
import static org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal.OzonePrincipalType.GROUP_PRINCIPAL;
import static org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal.OzonePrincipalType.USER_PRINCIPAL;
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
import org.apache.hadoop.ozone.om.multitenantImpl.OzoneMultiTenantPrincipalImpl;
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
      OzoneMultiTenantPrincipal group1Principal = getTestPrincipal("tenant1",
          "groupTestAdmin", GROUP_PRINCIPAL);
      OzoneMultiTenantPrincipal group2Principal = getTestPrincipal("tenant1",
          "groupTestUsers", GROUP_PRINCIPAL);
      groupIdsCreated.add(omm.createGroup(group1Principal));
      groupIdsCreated.add(omm.createGroup(group2Principal));

      OzoneMultiTenantPrincipal userPrincipal =
          getTestPrincipal("tenant1", "user1Test", USER_PRINCIPAL);
      usersIdsCreated.add(omm.createUser(userPrincipal, groupIdsCreated));

      AccessPolicy tenant1VolumeAccessPolicy = createVolumeAccessPolicy(
          "vol1", "tenant1", "Users");
      policyIdsCreated.add(omm.createAccessPolicy(tenant1VolumeAccessPolicy));

      AccessPolicy tenant1BucketCreatePolicy = allowCreateBucketPolicy(
          "vol1", "tenant1", "Users");
      policyIdsCreated.add(omm.createAccessPolicy(tenant1BucketCreatePolicy));

      AccessPolicy tenant1BucketAccessPolicy = allowAccessBucketPolicy(
          "vol1", "tenant1", "Users", "bucket1");
      policyIdsCreated.add(omm.createAccessPolicy(tenant1BucketAccessPolicy));

      AccessPolicy tenant1KeyAccessPolicy = allowAccessKeyPolicy(
          "vol1", "tenant1", "Users", "bucket1");
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
        omm.deleteGroup(id);
      }
    }
  }

  @Test
  @Ignore("TODO:Requires (mocked) Ranger endpoint")
  public void testMultiTenantAccessAuthorizerRangerPluginWithoutIds()
      throws Exception {
    OzoneMultiTenantPrincipal userPrincipal = null;
    simulateOzoneSiteXmlConfig();
    final MultiTenantAccessAuthorizer omm =
        new MultiTenantAccessAuthorizerRangerPlugin();
    omm.init(conf);

    try {
      Assert.assertTrue(policyIdsCreated.size() == 0);
      OzoneMultiTenantPrincipal group1Principal = getTestPrincipal("tenant1",
          "groupTestAdmin", GROUP_PRINCIPAL);
      OzoneMultiTenantPrincipal group2Principal = getTestPrincipal("tenant1",
          "groupTestUsers", GROUP_PRINCIPAL);
      omm.createGroup(group1Principal);
      groupIdsCreated.add(omm.getGroupId(group1Principal));
      omm.createGroup(group2Principal);
      groupIdsCreated.add(omm.getGroupId(group2Principal));

      userPrincipal =
          getTestPrincipal("tenant1", "user1Test", USER_PRINCIPAL);
      omm.createUser(userPrincipal, groupIdsCreated);

      AccessPolicy tenant1VolumeAccessPolicy = createVolumeAccessPolicy(
          "vol1", "tenant1", "Users");
      omm.createAccessPolicy(tenant1VolumeAccessPolicy);
      policyIdsCreated.add(tenant1VolumeAccessPolicy.getPolicyName());

      AccessPolicy tenant1BucketCreatePolicy = allowCreateBucketPolicy(
          "vol1", "tenant1", "Users");
      omm.createAccessPolicy(tenant1BucketCreatePolicy);
      policyIdsCreated.add(tenant1BucketCreatePolicy.getPolicyName());

      AccessPolicy tenant1BucketAccessPolicy = allowAccessBucketPolicy(
          "vol1", "tenant1", "Users", "bucket1");
      omm.createAccessPolicy(tenant1BucketAccessPolicy);
      policyIdsCreated.add(tenant1BucketAccessPolicy.getPolicyName());

      AccessPolicy tenant1KeyAccessPolicy = allowAccessKeyPolicy(
          "vol1", "tenant1", "Users", "bucket1");
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
        omm.deleteGroup(id);
      }
    }
  }

  private OzoneMultiTenantPrincipal getTestPrincipal(String tenant, String id,
      OzoneMultiTenantPrincipal.OzonePrincipalType type) {
    OzoneMultiTenantPrincipal principal = new OzoneMultiTenantPrincipalImpl(
        new BasicUserPrincipal(tenant),
        new BasicUserPrincipal(id), type);
    return principal;
  }

  private AccessPolicy createVolumeAccessPolicy(String vol, String tenant,
                                        String group) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        tenant + group + "VolumeAccess" + vol + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(VOLUME).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName("").setKeyName("").build();
    OzoneMultiTenantPrincipal principal = getTestPrincipal(tenant, group,
        GROUP_PRINCIPAL);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, READ, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, LIST, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal,
        READ_ACL, ALLOW);
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowCreateBucketPolicy(String vol, String tenant,
                                       String group) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        tenant + group + "AllowBucketCreate" + vol + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(BUCKET).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName("*").setKeyName("").build();
    OzoneMultiTenantPrincipal principal = getTestPrincipal(tenant, group,
        GROUP_PRINCIPAL);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, CREATE, ALLOW);
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowAccessBucketPolicy(String vol, String tenant,
                                       String group, String bucketName)
      throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        tenant + group + "AllowBucketAccess" + vol + bucketName + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(BUCKET).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName(bucketName).setKeyName("*").build();
    OzoneMultiTenantPrincipal principal = getTestPrincipal(tenant, group,
        GROUP_PRINCIPAL);
    for (ACLType acl :ACLType.values()) {
      if (acl != NONE) {
        tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, acl,
            ALLOW);
      }
    }
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowAccessKeyPolicy(String vol, String tenant,
                                    String group, String bucketName)
      throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        tenant + group + "AllowBucketKeyAccess" + vol + bucketName + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(KEY).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName(bucketName).setKeyName("*").build();
    OzoneMultiTenantPrincipal principal = getTestPrincipal(tenant, group,
        GROUP_PRINCIPAL);
    for (ACLType acl :ACLType.values()) {
      if (acl != NONE) {
        tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, acl,
            ALLOW);
      }
    }
    return tenantVolumeAccessPolicy;
  }
}

