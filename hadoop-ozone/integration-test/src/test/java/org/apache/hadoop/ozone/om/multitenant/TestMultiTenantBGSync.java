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

import static java.lang.Thread.sleep;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.multitenant.AccessPolicy.AccessGrantType.ALLOW;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ_ACL;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.apache.hadoop.security.authentication.util.KerberosName.DEFAULT_MECHANISM;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.http.auth.BasicUserPrincipal;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Tests Ozone Manager Multitenant feature Background Sync with Apache Ranger.
 * Marking it as Ignore because it needs Ranger access point.
 */
@Ignore("TODO:Requires (mocked) Ranger endpoint")
public class TestMultiTenantBGSync {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestMultiTenantBGSync.class);

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(600000);

  private static final long TEST_SYNC_INTERVAL_SEC = 10L;

  private TemporaryFolder folder = new TemporaryFolder();

  // The following values need to be set before this test can be enabled.
  private static final String RANGER_ENDPOINT = "";
  private static final String RANGER_ENDPOINT_USER = "";
  private static final String RANGER_ENDPOINT_USER_PASSWD = "";

  private MultiTenantAccessAuthorizer omm;
  private OMRangerBGSyncService bgSync;

  private List<String> usersIdsCreated = new ArrayList<>();
  private List<String> policyNamesCreated = new ArrayList<>();
  private List<String> roleIdsCreated = new ArrayList<>();
  private List<String> policyIdsCreated = new ArrayList<>();
  private List<BasicUserPrincipal> usersCreated = new ArrayList<>();

  private static OzoneConfiguration conf;
  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private AuditLogger auditLogger;

  // UGI-related vars
  private static final String USER_ALICE = "alice@EXAMPLE.COM";

  private UserGroupInformation ugiAlice;
  private OMMultiTenantManager omMultiTenantManager;
  private Tenant tenant;


  @BeforeClass
  public static void init() {
    conf = new OzoneConfiguration();
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

  public void setUpHelper() throws IOException {
    KerberosName.setRuleMechanism(DEFAULT_MECHANISM);
    KerberosName.setRules(
        "RULE:[2:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" +
        "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" +
        "DEFAULT");
    ugiAlice = UserGroupInformation.createRemoteUser(USER_ALICE);
    Assert.assertEquals("alice", ugiAlice.getShortUserName());

    ozoneManager = mock(OzoneManager.class);

    Server.Call call = spy(new Server.Call(1, 1, null, null,
        RPC.RpcKind.RPC_BUILTIN, new byte[] {1, 2, 3}));
    // Run as alice, so that Server.getRemoteUser() won't return null.
    when(call.getRemoteUser()).thenReturn(ugiAlice);
    Server.getCurCall().set(call);

    omMetrics = OMMetrics.create();
    folder = new TemporaryFolder(new File("/tmp"));
    folder.create();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    // No need to conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS, ...) here
    //  as we did the trick earlier with mockito.
    omMetadataManager = new OmMetadataManagerImpl(conf);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.isRatisEnabled()).thenReturn(true);
    auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));

    // Multi-tenant related initializations
    omMultiTenantManager = mock(OMMultiTenantManager.class);
    tenant = mock(Tenant.class);
    when(ozoneManager.getMultiTenantManager()).thenReturn(omMultiTenantManager);
    when(ozoneManager.getConfiguration()).thenReturn(conf);
    when(ozoneManager.isLeaderReady()).thenReturn(true);

    when(tenant.getTenantAccessPolicies()).thenReturn(new ArrayList<>());

    omm = new MultiTenantAccessAuthorizerRangerPlugin();
    omm.init(conf);
  }

  public void tearDownHelper() {
    omMetrics.unRegister();
    framework().clearInlineMocks();
  }

  private AccessPolicy createVolumeAccessPolicy(String vol, String tenantId)
      throws IOException {
    OzoneTenantRolePrincipal principal = new OzoneTenantRolePrincipal(
        OMMultiTenantManager.getDefaultUserRoleName(tenantId));
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        OMMultiTenantManager.getDefaultBucketNamespacePolicyName(tenantId));
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(VOLUME).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName("").setKeyName("").build();
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, READ, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, LIST, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal,
        READ_ACL, ALLOW);
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowCreateBucketPolicy(String vol, String tenantId)
      throws IOException {
    OzoneTenantRolePrincipal principal = new OzoneTenantRolePrincipal(
        OMMultiTenantManager.getDefaultUserRoleName(tenantId));
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        OMMultiTenantManager.getDefaultBucketPolicyName(tenantId));
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(BUCKET).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName("*").setKeyName("").build();
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, CREATE, ALLOW);
    return tenantVolumeAccessPolicy;
  }

  public void createRolesAndPoliciesInRanger() {
    BasicUserPrincipal userPrincipal = new BasicUserPrincipal("user1Test");
    BasicUserPrincipal userPrincipal2 = new BasicUserPrincipal("user2Test");
    simulateOzoneSiteXmlConfig();

    policyNamesCreated.clear();
    usersIdsCreated.clear();
    roleIdsCreated.clear();

    Assert.assertTrue(policyNamesCreated.size() == 0);
    OzoneTenantRolePrincipal role2Principal = new OzoneTenantRolePrincipal(
        OMMultiTenantManager.getDefaultUserRoleName("tenant1"));
    try {
      omm.createRole(role2Principal.getName(), null);
      String role2 = omm.getRole(role2Principal);
      roleIdsCreated.add(role2);
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }

    try {
      omm.createUser("user1Test", "user1Test1234");
      usersCreated.add(userPrincipal);
      omm.assignUser(userPrincipal, omm.getRole(role2Principal), false);
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }

    try {
      omm.createUser("user2Test", "user1Test1234");
      usersCreated.add(userPrincipal2);
      omm.assignUser(userPrincipal2, omm.getRole(role2Principal), false);
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }


    try {
      AccessPolicy tenant1VolumeAccessPolicy = createVolumeAccessPolicy("vol1",
          "tenant1");
      policyIdsCreated.add(omm.createAccessPolicy(tenant1VolumeAccessPolicy));
      policyNamesCreated.add(tenant1VolumeAccessPolicy.getPolicyName());
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }

    try {
      AccessPolicy tenant1BucketCreatePolicy = allowCreateBucketPolicy("vol1",
          "tenant1");
      policyIdsCreated.add(omm.createAccessPolicy(tenant1BucketCreatePolicy));
      policyNamesCreated.add(tenant1BucketCreatePolicy.getPolicyName());
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }
  }

  public void cleanupPolicies() {
    for (String name : policyNamesCreated) {
      try {
        omm.deletePolicybyName(name);
      } catch (Exception e) {
        LOG.info(e.getMessage());
      }
    }
  }

  public void cleanupRoles() {
    for (String role : roleIdsCreated) {
      try {
        omm.deleteRole(new JsonParser().parse(role).getAsJsonObject().get(
            "id").getAsString());
      } catch (Exception e) {
        LOG.info(e.getMessage());
      }
    }
  }

  public void cleanupUsers() {
    for (BasicUserPrincipal user : usersCreated) {
      try {
        String userId = omm.getUserId(user);
        omm.deleteUser(userId);
      } catch (Exception e) {
        LOG.info(e.getMessage());
      }
    }
  }

  long bgSyncSetup() throws IOException {
    bgSync = new OMRangerBGSyncService(ozoneManager, omm,
        TEST_SYNC_INTERVAL_SEC, TimeUnit.SECONDS, TEST_SYNC_INTERVAL_SEC / 2);
    OzoneClient ozoneClient = Mockito.mock(OzoneClient.class);
    ObjectStore objectStore = Mockito.mock(ObjectStore.class);
    when(ozoneClient.getObjectStore()).thenReturn(objectStore);
    long ozoneVersion =
        bgSync.retrieveRangerServiceVersion();
//    Mockito.doAnswer(invocationOnMock -> {
//      long v = invocationOnMock.getArgument(0);
//      ozoneManager.getMetadataManager().getOmRangerStateTable()
//          .put(OmMetadataManagerImpl.RANGER_OZONE_SERVICE_VERSION_KEY, v);
//      return null;
//    }).when(objectStore).rangerServiceVersionSync(ozoneVersion);
    return ozoneVersion;
  }

  @Test
  public void testRangerBGSyncRemoveTablesFromRanger() throws IOException {
    simulateOzoneSiteXmlConfig();
    setUpHelper();
    long ozoneVersion = bgSyncSetup();

    try {
      // This will create roles and policies in ranger that are
      // backed up by OzoneManger Multi-Tenant tables.
      createRolesAndPoliciesInRanger();

      bgSync.start();
      // Wait for background sync to go through few cycles.
      while (bgSync.getRangerSyncRunCount() <= 4) {
        // TODO: Trigger the sync rather than busy waiting?
        sleep(TEST_SYNC_INTERVAL_SEC * 1000);
      }
      Assert.assertEquals(ozoneVersion, bgSync.getOMDBRangerServiceVersion());

      // Now lets make sure that the ranger policies and roles not backed up
      // by OzoneManager multitenant tables are cleaned up.
      for (String policy : policyNamesCreated) {
        AccessPolicy verifier = omm.getAccessPolicyByName(policy);

        cleanupPoliciesRolesUsers();
        Assert.fail("Policy Exists: " + verifier);
      }

      for (String role : roleIdsCreated) {
        String verifier =
            omm.getRole(new JsonParser().parse(role).getAsJsonObject().get(
            "name").getAsString());
        cleanupPoliciesRolesUsers();
        Assert.fail("Role Exists: " + verifier);
      }

      cleanupPoliciesRolesUsers();
    } catch (Exception e) {
      cleanupPoliciesRolesUsers();
      Assert.fail(e.getMessage());
    } finally {
      bgSync.shutdown();
      tearDownHelper();
    }
  }

  public void cleanupPoliciesRolesUsers() {
    cleanupPolicies();
    cleanupRoles();
    cleanupUsers();
  }

  @Test
  public void testRangerBGSyncsRangerPoliciesBackedByOMDB() throws IOException {
    simulateOzoneSiteXmlConfig();
    setUpHelper();
    long ozoneVersion = bgSyncSetup();

    try {
      createRolesAndPoliciesInRanger();

      bgSync.start();
      while (bgSync.getRangerSyncRunCount() <= 4) {
        // TODO: Trigger the sync rather than busy waiting?
        sleep(TEST_SYNC_INTERVAL_SEC * 1000);
      }
      Assert.assertTrue(bgSync.getOMDBRangerServiceVersion() == ozoneVersion);

      for (String policy : policyNamesCreated) {
        try {
          AccessPolicy verifier =
              omm.getAccessPolicyByName(policy);

          Assert.assertTrue(verifier.getPolicyName().equals(policy));
        } catch (Exception e) {
        }
      }

      for (String role : roleIdsCreated) {
        try {
          String rolename = new JsonParser().parse(role).getAsJsonObject().get(
              "name").getAsString();
          String verifier =
              new JsonParser().parse(omm.getRole(rolename)).getAsJsonObject()
              .get("name").getAsString();
          Assert.assertTrue(verifier.equals(rolename));
        } catch (Exception e) {
        }
      }

      cleanupPoliciesRolesUsers();
    } catch (Exception e) {
      cleanupPoliciesRolesUsers();
      Assert.fail(e.getMessage());
    } finally {
      bgSync.shutdown();
      tearDownHelper();
    }
  }

  @Test
  public void testRangerBGSyncsDeletedRolesRecreated() throws IOException {
    simulateOzoneSiteXmlConfig();
    setUpHelper();
    long ozoneVersion = bgSyncSetup();

    try {
      createRolesAndPoliciesInRanger();
      // now lets delete the role and make sure it gets recreated
      String rolename =
          new JsonParser().parse(roleIdsCreated.get(0)).getAsJsonObject().get(
          "name").getAsString();

      omm.revokeUserFromRole(new BasicUserPrincipal("user2Test"),
          omm.getRole(rolename));

      HashSet<String> userSet = new HashSet<>();
      userSet.add("user1Test");
      userSet.add("user2Test");
      for (String userPrincipal : userSet) {
        String userAccessId =
            OMMultiTenantManager.getDefaultAccessId("tenant1", userPrincipal);

        OmDBAccessIdInfo omDBAccessIdInfo = new OmDBAccessIdInfo.Builder()
            .setTenantId("tenant1")
            .setUserPrincipal(userPrincipal)
            .build();
        ozoneManager.getMetadataManager().getTenantAccessIdTable()
            .put(userAccessId, omDBAccessIdInfo);
      }

      long baseVersion = bgSync.getRangerSyncRunCount();
      bgSync.start();
      while (bgSync.getRangerSyncRunCount() <= baseVersion + 1) {
        // TODO: Trigger the sync rather than busy waiting?
        sleep(TEST_SYNC_INTERVAL_SEC * 1000);
      }
      Assert.assertTrue(bgSync.getOMDBRangerServiceVersion() >= ozoneVersion);

      for (String policy : policyNamesCreated) {
        AccessPolicy verifier =
            omm.getAccessPolicyByName(policy);

        Assert.assertTrue(verifier.getPolicyName().equals(policy));
      }

      for (String role : roleIdsCreated) {
        rolename = new JsonParser().parse(role).getAsJsonObject().get(
            "name").getAsString();
        JsonObject newrole =
            new JsonParser().parse(omm.getRole(rolename)).getAsJsonObject();
        JsonArray verifier =
                newrole.get("users").getAsJsonArray();
        Assert.assertTrue(verifier.size() == 2);
        for (int i = 0; i < verifier.size();  ++i) {
          String user = verifier.get(i).getAsJsonObject().get("name")
              .getAsString();
          Assert.assertTrue(userSet.contains(user));
          userSet.remove(user);
        }
      }

      cleanupPoliciesRolesUsers();
    } catch (IOException | InterruptedException e) {
      cleanupPoliciesRolesUsers();
      Assert.fail(e.getMessage());
    } finally {
      bgSync.shutdown();
      tearDownHelper();
    }
  }
}
