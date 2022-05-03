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
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ_ACL;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.apache.hadoop.security.authentication.util.KerberosName.DEFAULT_MECHANISM;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
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
import org.slf4j.event.Level;

/**
 * Tests Ozone Manager Multitenant feature Background Sync with Apache Ranger.
 * Marking it as Ignore because it needs Ranger access point.
 */
@Ignore("TODO: Requires (mocked) Ranger endpoint")
public class TestMultiTenantBGSync {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestMultiTenantBGSync.class);

  /**
   * Timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(120, TimeUnit.SECONDS);

  private static final long TEST_SYNC_INTERVAL_SEC = 3L;
  private static final long TEST_SYNC_TIMEOUT_SEC = 3L;

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
    GenericTestUtils.setLogLevel(OMRangerBGSyncService.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(
        MultiTenantAccessAuthorizerRangerPlugin.LOG, Level.INFO);
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

    String omID = UUID.randomUUID().toString();
    final String path = GenericTestUtils.getTempPath(omID);
    Path metaDirPath = Paths.get(path, "om-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());

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

    // Raft client request handling
//    int ratisPort = 9873;
//    InetSocketAddress rpcAddress = new InetSocketAddress(
//        InetAddress.getLocalHost(), 0);
//    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
//        .setRpcAddress(rpcAddress)
//        .setRatisPort(ratisPort)
//        .setOMNodeId(UUID.randomUUID().toString())
//        .setOMServiceId("omSIdCustom123")
//        .build();
//    SecurityConfig secConfig = new SecurityConfig(conf);
//    OMCertificateClient certClient = new OMCertificateClient(secConfig);
//    OzoneManagerRatisServer omRatisServer = OzoneManagerRatisServer
//        .newOMRatisServer(conf, ozoneManager, nodeDetails,
//            Collections.emptyMap(), secConfig, certClient, false);
//    omRatisServer.start();

    OzoneManagerRatisServer omRatisServer = mock(OzoneManagerRatisServer.class);
    when(omRatisServer.getRaftPeerId())
        .thenReturn(RaftPeerId.valueOf("peerId"));
    when(omRatisServer.getRaftGroupId())
        .thenReturn(RaftGroupId.randomId());

    when(ozoneManager.getOmRatisServer()).thenReturn(omRatisServer);
    try {
      doAnswer(invocation -> {
        OMRequest request = invocation.getArgument(0);
        long v = request.getRangerServiceVersionSyncRequest()
            .getRangerServiceVersion();
        LOG.info("Writing Ranger Ozone Service Version: {}", v);
        ozoneManager.getMetadataManager().getOmRangerStateTable()
            .put(OmMetadataManagerImpl.RANGER_OZONE_SERVICE_VERSION_KEY, v);
        return null;
      }).when(omRatisServer).submitRequest(Mockito.any(), Mockito.any());
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    when(tenant.getTenantAccessPolicies()).thenReturn(new ArrayList<>());

    omm = new MultiTenantAccessAuthorizerRangerPlugin();
    omm.init(conf);
  }

  public void tearDownHelper() {
    bgSync.shutdown();
    cleanupPoliciesRolesUsers();
    omMetrics.unRegister();
    framework().clearInlineMocks();
  }

  private AccessPolicy createVolumeAccessPolicy(String vol, String tenantId)
      throws IOException {
    OzoneTenantRolePrincipal principal = new OzoneTenantRolePrincipal(
        OMMultiTenantManager.getDefaultUserRoleName(tenantId));
    OzoneTenantRolePrincipal adminRole = new OzoneTenantRolePrincipal(
        OMMultiTenantManager.getDefaultAdminRoleName(tenantId));
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        OMMultiTenantManager.getDefaultBucketNamespacePolicyName(tenantId));
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(VOLUME).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName("").setKeyName("").build();
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, READ, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, LIST, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal,
        READ_ACL, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, adminRole, ALL, ALLOW);
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

  public void createRolesAndPoliciesInRanger(boolean createInOMDB) {
    simulateOzoneSiteXmlConfig();

    policyNamesCreated.clear();
    usersIdsCreated.clear();
    roleIdsCreated.clear();

    BasicUserPrincipal userAlice = new BasicUserPrincipal("alice");
    BasicUserPrincipal userBob = new BasicUserPrincipal("bob");
    // Tenant name to be used for this test
    final String tenantId = "tenant1";
    // volume name = bucket namespace name
    final String volumeName = tenantId;

    final OzoneTenantRolePrincipal adminRole = new OzoneTenantRolePrincipal(
        OMMultiTenantManager.getDefaultAdminRoleName(tenantId));
    final OzoneTenantRolePrincipal userRole = new OzoneTenantRolePrincipal(
        OMMultiTenantManager.getDefaultUserRoleName(tenantId));
    final String bucketNamespacePolicyName =
        OMMultiTenantManager.getDefaultBucketNamespacePolicyName(tenantId);
    final String bucketPolicyName =
        OMMultiTenantManager.getDefaultBucketPolicyName(tenantId);

    // Add tenant entry in OM DB
    if (createInOMDB) {
      try {
        // Tenant State entry
        omMetadataManager.getTenantStateTable().put(tenantId,
            new OmDBTenantState(
                tenantId, volumeName, userRole.getName(), adminRole.getName(),
                bucketNamespacePolicyName, bucketPolicyName));
        // Access ID entry for alice
        final String aliceAccessId = OMMultiTenantManager.getDefaultAccessId(
            tenantId, userAlice.getName());
        omMetadataManager.getTenantAccessIdTable().put(aliceAccessId,
            new OmDBAccessIdInfo.Builder()
                .setTenantId(tenantId)
                .setUserPrincipal(userAlice.getName())
                .setIsAdmin(false)
                .setIsDelegatedAdmin(false)
                .build());
        // Access ID entry for bob
        final String bobAccessId = OMMultiTenantManager.getDefaultAccessId(
            tenantId, userBob.getName());
        omMetadataManager.getTenantAccessIdTable().put(bobAccessId,
            new OmDBAccessIdInfo.Builder()
                .setTenantId(tenantId)
                .setUserPrincipal(userBob.getName())
                .setIsAdmin(false)
                .setIsDelegatedAdmin(false)
                .build());
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
      // Tenant accessId entry

    }

    try {
      omm.createRole(adminRole.getName(), null);
      String role1 = omm.getRole(adminRole);
      roleIdsCreated.add(0, role1);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }

    try {
      omm.createRole(userRole.getName(), adminRole.getName());
      String role2 = omm.getRole(userRole);
      // Prepend user role (order matters when deleting due to dependency)
      roleIdsCreated.add(0, role2);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      // TODO: Append anyway?
    }

    try {
      omm.createUser("alice", "password1");
      usersCreated.add(userAlice);
      omm.assignUserToRole(userAlice, omm.getRole(userRole), false);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }

    try {
      omm.createUser("bob", "password2");
      usersCreated.add(userBob);
      omm.assignUserToRole(userBob, omm.getRole(userRole), false);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }

    try {
      AccessPolicy tenant1VolumeAccessPolicy = createVolumeAccessPolicy(
          volumeName, tenantId);
      policyIdsCreated.add(omm.createAccessPolicy(tenant1VolumeAccessPolicy));
      policyNamesCreated.add(tenant1VolumeAccessPolicy.getPolicyName());
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }

    try {
      AccessPolicy tenant1BucketCreatePolicy = allowCreateBucketPolicy(
          volumeName, tenantId);
      policyIdsCreated.add(omm.createAccessPolicy(tenant1BucketCreatePolicy));
      policyNamesCreated.add(tenant1BucketCreatePolicy.getPolicyName());
    } catch (Exception e) {
      LOG.error(e.getMessage());
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
    for (String roleObj : roleIdsCreated) {
      try {
        final String roleId = new JsonParser().parse(roleObj)
            .getAsJsonObject().get("id").getAsString();
        omm.deleteRole(roleId);
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
        TEST_SYNC_INTERVAL_SEC, TimeUnit.SECONDS, TEST_SYNC_TIMEOUT_SEC);
//    OzoneClient ozoneClient = Mockito.mock(OzoneClient.class);
//    ObjectStore objectStore = Mockito.mock(ObjectStore.class);
//    when(ozoneClient.getObjectStore()).thenReturn(objectStore);
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
  public void testRangerBGSyncRemoveFromRanger() throws IOException {
    simulateOzoneSiteXmlConfig();
    setUpHelper();
    long startingOzoneVersion = bgSyncSetup();

    try {
      // Create roles and policies in ranger that are NOT
      // backed up by OzoneManger Multi-Tenant tables
      createRolesAndPoliciesInRanger(false);
      long ozoneVersionAfterCreation = bgSync.retrieveRangerServiceVersion();
      Assert.assertTrue(ozoneVersionAfterCreation >= startingOzoneVersion);

      bgSync.start();
      // Wait for background sync to go through few cycles.
      while (bgSync.getRangerSyncRunCount() <= 4) {
        LOG.info("Waiting for sync");
        // TODO: Trigger the sync rather than busy waiting?
        sleep(TEST_SYNC_INTERVAL_SEC * 1000);
      }
      bgSync.shutdown();
      long ozoneVersionAfterSync = bgSync.getOMDBRangerServiceVersion();
      Assert.assertTrue(ozoneVersionAfterSync >= ozoneVersionAfterCreation);

      // Verify that the Ranger policies and roles not backed up
      // by OzoneManager Multi-Tenancy tables are cleaned up by sync thread

      for (String policy : policyNamesCreated) {
        final AccessPolicy policyRead = omm.getAccessPolicyByName(policy);
        Assert.assertNull("This policy should have been deleted from Ranger: " +
            policy, policyRead);
//        cleanupPoliciesRolesUsers();
//        Assert.fail("Policy Exists: " + verifier);
      }

      for (String roleObj : roleIdsCreated) {
        final String roleName = new JsonParser().parse(roleObj)
            .getAsJsonObject().get("name").getAsString();
        final String roleObjRead = omm.getRole(roleName);
        Assert.assertNull("This policy should have been deleted from Ranger: " +
            roleName, roleObjRead);
//        cleanupPoliciesRolesUsers();
//        Assert.fail("Role Exists: " + verifier);
      }

//      cleanupPoliciesRolesUsers();
    } catch (IOException e) {
      throw e;
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
    } finally {
      tearDownHelper();
    }
  }

  public void cleanupOMDB() {
    try {
      omMetadataManager.getTenantStateTable().delete("tenant1");
      omMetadataManager.getTenantAccessIdTable().delete(
          OMMultiTenantManager.getDefaultAccessId("tenant1", "alice"));
      omMetadataManager.getTenantAccessIdTable().delete(
          OMMultiTenantManager.getDefaultAccessId("tenant1", "bob"));
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  public void cleanupPoliciesRolesUsers() {
    cleanupPolicies();
    cleanupRoles();
    cleanupUsers();

    cleanupOMDB();
  }

  @Test
  public void testRangerBGSyncBackedByOMDB() throws IOException {
    simulateOzoneSiteXmlConfig();
    setUpHelper();
    long startingOzoneVersion = bgSyncSetup();

    try {
      // Create roles and policies in ranger that are
      // backed up by OzoneManger Multi-Tenant tables
      createRolesAndPoliciesInRanger(true);
      long ozoneVersionAfterCreation = bgSync.retrieveRangerServiceVersion();
      Assert.assertTrue(ozoneVersionAfterCreation >= startingOzoneVersion);

      bgSync.start();
      while (bgSync.getRangerSyncRunCount() <= 4) {
        LOG.info("Waiting for sync");
        // TODO: Trigger the sync rather than busy waiting?
        sleep(TEST_SYNC_INTERVAL_SEC * 1000);
      }
      bgSync.shutdown();
      long ozoneVersionAfterSync = bgSync.getOMDBRangerServiceVersion();
      Assert.assertTrue(ozoneVersionAfterSync >= ozoneVersionAfterCreation);

      for (String policy : policyNamesCreated) {
        try {
          final AccessPolicy policyRead = omm.getAccessPolicyByName(policy);

          Assert.assertEquals(policy, policyRead.getPolicyName());
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      for (String roleObj : roleIdsCreated) {
        try {
          final String roleName = new JsonParser().parse(roleObj)
              .getAsJsonObject().get("name").getAsString();
          String roleObjRead = omm.getRole(roleName);
          final String roleNameReadBack = new JsonParser().parse(roleObjRead)
              .getAsJsonObject().get("name").getAsString();
          Assert.assertEquals(roleName, roleNameReadBack);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

    } catch (IOException e) {
      throw e;
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
    } finally {
//      bgSync.shutdown();
//      cleanupPoliciesRolesUsers();
      tearDownHelper();
    }
  }

  @Test
  public void testRangerBGSyncUpdateRole() throws IOException {
    simulateOzoneSiteXmlConfig();
    setUpHelper();
    long ozoneVersion = bgSyncSetup();

    try {
      createRolesAndPoliciesInRanger(true);

      // Delete a user from user role, expect Ranger sync thread to update it
      String userRoleName = new JsonParser().parse(roleIdsCreated.get(0))
          .getAsJsonObject().get("name").getAsString();
      Assert.assertEquals(
          OMMultiTenantManager.getDefaultUserRoleName("tenant1"), userRoleName);

      omm.revokeUserFromRole(new BasicUserPrincipal("bob"),
          omm.getRole(userRoleName));

      HashSet<String> userSet = new HashSet<>();
      userSet.add("alice");
      userSet.add("bob");
//      for (String userPrincipal : userSet) {
//        String userAccessId =
//            OMMultiTenantManager.getDefaultAccessId("tenant1", userPrincipal);
//
//        OmDBAccessIdInfo omDBAccessIdInfo = new OmDBAccessIdInfo.Builder()
//            .setTenantId("tenant1")
//            .setUserPrincipal(userPrincipal)
//            .build();
//        ozoneManager.getMetadataManager().getTenantAccessIdTable()
//            .put(userAccessId, omDBAccessIdInfo);
//      }

      long currRunCount = bgSync.getRangerSyncRunCount();
      bgSync.start();
      while (bgSync.getRangerSyncRunCount() <= currRunCount + 1) {
        LOG.info("Waiting for sync");
        // TODO: Trigger the sync rather than busy waiting?
        sleep(TEST_SYNC_INTERVAL_SEC * 1000);
      }
      bgSync.shutdown();
      Assert.assertTrue(bgSync.getOMDBRangerServiceVersion() >= ozoneVersion);

      for (String policy : policyNamesCreated) {
        final AccessPolicy verifier = omm.getAccessPolicyByName(policy);
        Assert.assertNotNull("Policy should exist in Ranger: " + policy,
            verifier);
        Assert.assertEquals(policy, verifier.getPolicyName());
      }

      for (String role : roleIdsCreated) {
        final String roleName = new JsonParser().parse(role).getAsJsonObject()
            .get("name").getAsString();
        if (!roleName.equals(userRoleName)) {
          continue;
        }
        final String roleObjRead = omm.getRole(roleName);
        final JsonObject jsonObj = new JsonParser().parse(roleObjRead)
            .getAsJsonObject();
        final JsonArray verifier = jsonObj.get("users").getAsJsonArray();
        Assert.assertEquals(2, verifier.size());
        // Verify that users are in the role
        for (int i = 0; i < verifier.size();  ++i) {
          String user = verifier.get(i).getAsJsonObject().get("name")
              .getAsString();
          Assert.assertTrue(userSet.contains(user));
          userSet.remove(user);
        }
        break;
      }

    } catch (IOException e) {
      throw e;
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
    } finally {
//      bgSync.shutdown();
//      cleanupPoliciesRolesUsers();
      tearDownHelper();
    }
  }
}
