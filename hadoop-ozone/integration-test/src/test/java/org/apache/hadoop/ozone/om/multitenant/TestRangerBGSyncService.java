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
import org.apache.hadoop.ozone.OzoneConsts;
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
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Policy;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
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
 * Tests Ozone Manager Multi-Tenancy feature Background Sync with Apache Ranger.
 * Marking it as Ignore because it needs Ranger access point.
 */
@Ignore("TODO: Requires a Ranger endpoint")
public class TestRangerBGSyncService {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRangerBGSyncService.class);

  /**
   * Timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(180, TimeUnit.SECONDS);

  private static final long TEST_SYNC_INTERVAL_SEC = 1L;
  private static final long TEST_SYNC_TIMEOUT_SEC = 3L;

  private static final int CHECK_SYNC_MILLIS = 1000;
  private static final int WAIT_SYNC_TIMEOUT_MILLIS = 60000;

  private TemporaryFolder folder = new TemporaryFolder();

  // TODO: Remove auth
  private MultiTenantAccessAuthorizer auth;
  private MultiTenantAccessController accessController;
  private OMRangerBGSyncService bgSync;

  // List of policy names created in Ranger
  private final List<String> policiesCreated = new ArrayList<>();
  // List of role ID created in Ranger
  private final List<String> rolesCreated = new ArrayList<>();
  // List of users created in Ranger
  private final List<String> usersCreated = new ArrayList<>();

  private static OzoneConfiguration conf;
  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private OMMultiTenantManager omMultiTenantManager;
  private AuditLogger auditLogger;

  private Tenant tenant;
  private static final String TENANT_ID = "tenant1";

  // UGI-related vars
  private static final String USER_ALICE = "alice@EXAMPLE.COM";
  private static final String USER_ALICE_SHORT = "alice";
  private UserGroupInformation ugiAlice;
  private static final String USER_BOB_SHORT = "bob";

  private static void simulateOzoneSiteXmlConfig() {
    // The following configs need to be set before the test can be enabled.
    // Pass them in as JVM properties. e.g.:
    //
    // -Dozone.om.ranger.https-address=http://ranger:6080
    // -Dozone.om.ranger.https.admin.api.user=admin
    // -Dozone.om.ranger.https.admin.api.passwd=passwd

    conf.setStrings(OZONE_RANGER_HTTPS_ADDRESS_KEY,
        System.getProperty(OZONE_RANGER_HTTPS_ADDRESS_KEY));
    conf.setStrings(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER,
        System.getProperty(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER));
    conf.setStrings(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD,
        System.getProperty(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD));
  }

  @BeforeClass
  public static void init() {
    conf = new OzoneConfiguration();
    simulateOzoneSiteXmlConfig();

    GenericTestUtils.setLogLevel(OMRangerBGSyncService.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(
        MultiTenantAccessAuthorizerRangerPlugin.LOG, Level.INFO);
  }

  @AfterClass
  public static void shutdown() {
  }

  @Before
  public void setUp() throws IOException {

    KerberosName.setRuleMechanism(DEFAULT_MECHANISM);
    KerberosName.setRules(
        "RULE:[2:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" +
            "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" +
            "DEFAULT");
    ugiAlice = UserGroupInformation.createRemoteUser(USER_ALICE);
    Assert.assertEquals(USER_ALICE_SHORT, ugiAlice.getShortUserName());

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

    when(omMultiTenantManager.getTenantVolumeName(TENANT_ID))
        .thenReturn(TENANT_ID);
    when(omMultiTenantManager.getTenantUserRoleName(TENANT_ID))
        .thenReturn(OMMultiTenantManager.getDefaultUserRoleName(TENANT_ID));
    when(omMultiTenantManager.getTenantAdminRoleName(TENANT_ID))
        .thenReturn(OMMultiTenantManager.getDefaultAdminRoleName(TENANT_ID));

    when(omMultiTenantManager.getAuthorizerLock())
        .thenReturn(new AuthorizerLockImpl());

    // Raft client request handling
    OzoneManagerRatisServer omRatisServer = mock(OzoneManagerRatisServer.class);
    when(omRatisServer.getRaftPeerId())
        .thenReturn(RaftPeerId.valueOf("peerId"));
    when(omRatisServer.getRaftGroupId())
        .thenReturn(RaftGroupId.randomId());

    when(ozoneManager.getOmRatisServer()).thenReturn(omRatisServer);

    try {
      doAnswer(invocation -> {
        OMRequest request = invocation.getArgument(0);
        long v = request.getSetRangerServiceVersionRequest()
            .getRangerServiceVersion();
        LOG.info("Writing Ranger Ozone Service Version to DB: {}", v);
        ozoneManager.getMetadataManager().getMetaTable().put(
            OzoneConsts.RANGER_OZONE_SERVICE_VERSION_KEY, String.valueOf(v));
        return null;
      }).when(omRatisServer).submitRequest(Mockito.any(), Mockito.any());
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    when(tenant.getTenantAccessPolicies()).thenReturn(new ArrayList<>());

    auth = new MultiTenantAccessAuthorizerRangerPlugin();
    accessController = new InMemoryMultiTenantAccessController();
    auth.init(conf);
  }

  @After
  public void tearDown() {
    bgSync.shutdown();
    cleanupPoliciesRolesUsers();
    omMetrics.unRegister();
    framework().clearInlineMocks();
  }

  long initBGSync() throws IOException {
    bgSync = new OMRangerBGSyncService(ozoneManager,
        ozoneManager.getMultiTenantManager(), accessController,
        TEST_SYNC_INTERVAL_SEC, TimeUnit.SECONDS, TEST_SYNC_TIMEOUT_SEC);
    return bgSync.getRangerOzoneServicePolicyVersion();
  }

  public void createRolesAndPoliciesInRanger(boolean populateDB) {

    policiesCreated.clear();
    rolesCreated.clear();

    // Tenant name to be used for this test
    final String tenantId = TENANT_ID;
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
    if (populateDB) {
      LOG.info("Creating OM DB tenant entries");
      try {
        // Tenant State entry
        omMetadataManager.getTenantStateTable().put(tenantId,
            new OmDBTenantState(
                tenantId, volumeName, userRole.getName(), adminRole.getName(),
                bucketNamespacePolicyName, bucketPolicyName));
        // Access ID entry for alice
        final String aliceAccessId = OMMultiTenantManager.getDefaultAccessId(
            tenantId, USER_ALICE_SHORT);
        omMetadataManager.getTenantAccessIdTable().put(aliceAccessId,
            new OmDBAccessIdInfo.Builder()
                .setTenantId(tenantId)
                .setUserPrincipal(USER_ALICE_SHORT)
                .setIsAdmin(false)
                .setIsDelegatedAdmin(false)
                .build());
        // Access ID entry for bob
        final String bobAccessId = OMMultiTenantManager.getDefaultAccessId(
            tenantId, USER_BOB_SHORT);
        omMetadataManager.getTenantAccessIdTable().put(bobAccessId,
            new OmDBAccessIdInfo.Builder()
                .setTenantId(tenantId)
                .setUserPrincipal(USER_BOB_SHORT)
                .setIsAdmin(false)
                .setIsDelegatedAdmin(false)
                .build());
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      }
    }

    try {
      LOG.info("Creating admin role in Ranger: {}", adminRole.getName());
      auth.createRole(adminRole.getName(), null);
      String role1 = auth.getRole(adminRole);
      rolesCreated.add(0, role1);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    try {
      LOG.info("Creating user role in Ranger: {}", userRole.getName());
      auth.createRole(userRole.getName(), adminRole.getName());
      String role2 = auth.getRole(userRole);
      // Prepend user role (order matters when deleting due to dependency)
      rolesCreated.add(0, role2);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    try {
      LOG.info("Creating user in Ranger: {}", USER_ALICE_SHORT);
      auth.createUser(USER_ALICE_SHORT, "password1");
      usersCreated.add(USER_ALICE_SHORT);
      auth.assignUserToRole(USER_ALICE_SHORT, auth.getRole(userRole), false);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    try {
      LOG.info("Creating user in Ranger: {}", USER_BOB_SHORT);
      auth.createUser(USER_BOB_SHORT, "password2");
      usersCreated.add(USER_BOB_SHORT);
      auth.assignUserToRole(USER_BOB_SHORT, auth.getRole(userRole), false);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    final String userRoleName =
        OMMultiTenantManager.getDefaultUserRoleName(tenantId);
    final String adminRoleName =
        OMMultiTenantManager.getDefaultAdminRoleName(tenantId);

    try {
      Policy tenant1VolumeAccessPolicy =
          OMMultiTenantManager.getDefaultVolumeAccessPolicy(
              tenantId, volumeName, userRoleName, adminRoleName);
      LOG.info("Creating VolumeAccess policy in Ranger: {}",
          tenant1VolumeAccessPolicy.getName());
      accessController.createPolicy(tenant1VolumeAccessPolicy);
      policiesCreated.add(tenant1VolumeAccessPolicy.getName());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    try {
      Policy tenant1BucketCreatePolicy =
          OMMultiTenantManager.getDefaultBucketAccessPolicy(
              tenantId, volumeName, userRoleName);
      LOG.info("Creating BucketAccess policy in Ranger: {}",
          tenant1BucketCreatePolicy.getName());
      accessController.createPolicy(tenant1BucketCreatePolicy);
      policiesCreated.add(tenant1BucketCreatePolicy.getName());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  public void cleanupPolicies() {
    for (String name : policiesCreated) {
      try {
        LOG.info("Deleting policy: {}", name);
        auth.deletePolicyByName(name);
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }
    }
  }

  public void cleanupRoles() {
    for (String roleObj : rolesCreated) {
      final JsonObject jObj = new JsonParser().parse(roleObj).getAsJsonObject();
      final String roleId = jObj.get("id").getAsString();
      final String roleName = jObj.get("name").getAsString();
      try {
        LOG.info("Deleting role: {}", roleName);
        auth.deleteRoleById(roleId);
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }
    }
  }

  public void cleanupUsers() {
    for (String user : usersCreated) {
      try {
        LOG.info("Deleting user: {}", user);
        String userId = auth.getUserId(user);
        auth.deleteUser(userId);
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }
    }
  }

  public void cleanupOMDB() {
    try {
      omMetadataManager.getTenantStateTable().delete(TENANT_ID);
      omMetadataManager.getTenantAccessIdTable().delete(
          OMMultiTenantManager.getDefaultAccessId(TENANT_ID, USER_ALICE_SHORT));
      omMetadataManager.getTenantAccessIdTable().delete(
          OMMultiTenantManager.getDefaultAccessId(TENANT_ID, USER_BOB_SHORT));
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

  /**
   * OM DB does not have the tenant state.
   * Expect sync service to clean up all the leftover multi-tenancy
   * policies and roles in Ranger.
   */
  @Test
  public void testRemovePolicyAndRole() throws Exception {
    long startingRangerVersion = initBGSync();

    // Create roles and policies in ranger that are NOT
    // backed up by OzoneManger Multi-Tenant tables
    createRolesAndPoliciesInRanger(false);

    final long rangerSvcVersionBefore =
        bgSync.getRangerOzoneServicePolicyVersion();
    Assert.assertTrue(rangerSvcVersionBefore >= startingRangerVersion);

    // Note: DB Service Version will be -1 if the test starts with an empty DB
    final long dbSvcVersionBefore = bgSync.getOMDBRangerServiceVersion();
    bgSync.start();
    // Wait for sync to finish once.
    // The counter is incremented at the beginning of the run, hence the ">"
    GenericTestUtils.waitFor(() -> bgSync.getRangerSyncRunCount() > 1L,
        CHECK_SYNC_MILLIS, WAIT_SYNC_TIMEOUT_MILLIS);
    bgSync.shutdown();
    final long dbSvcVersionAfter = bgSync.getOMDBRangerServiceVersion();
    final long rangerSvcVersionAfter =
        bgSync.getRangerOzoneServicePolicyVersion();
    Assert.assertEquals(rangerSvcVersionAfter, dbSvcVersionAfter);
    Assert.assertTrue(dbSvcVersionAfter > dbSvcVersionBefore);
    Assert.assertTrue(rangerSvcVersionAfter > rangerSvcVersionBefore);

    // Verify that the Ranger policies and roles not backed up
    // by OzoneManager Multi-Tenancy tables are cleaned up by sync thread

    for (String policy : policiesCreated) {
      final AccessPolicy policyRead = auth.getAccessPolicyByName(policy);
      Assert.assertNull("This policy should have been deleted from Ranger: " +
          policy, policyRead);
    }

    for (String roleObj : rolesCreated) {
      final String roleName = new JsonParser().parse(roleObj)
          .getAsJsonObject().get("name").getAsString();
      final String roleObjRead = auth.getRole(roleName);
      Assert.assertNull("This role should have been deleted from Ranger: " +
          roleName, roleObjRead);
    }
  }

  /**
   * OM DB has the tenant state.
   * Ranger has the consistent role status, and the policies are in-place.
   * Expect sync service to check Ranger state but write nothing to Ranger.
   */
  @Test
  public void testConsistentState() throws Exception {
    long startingRangerVersion = initBGSync();

    // Create roles and policies in ranger that are
    // backed up by OzoneManger Multi-Tenant tables
    createRolesAndPoliciesInRanger(true);

    long rangerSvcVersionBefore = bgSync.getRangerOzoneServicePolicyVersion();
    Assert.assertTrue(rangerSvcVersionBefore >= startingRangerVersion);

    // Note: DB Service Version will be -1 if the test starts with an empty DB
    final long dbSvcVersionBefore = bgSync.getOMDBRangerServiceVersion();
    bgSync.start();
    // Wait for sync to finish once.
    // The counter is incremented at the beginning of the run, hence the ">"
    GenericTestUtils.waitFor(() -> bgSync.getRangerSyncRunCount() > 1L,
        CHECK_SYNC_MILLIS, WAIT_SYNC_TIMEOUT_MILLIS);
    bgSync.shutdown();
    final long dbSvcVersionAfter = bgSync.getOMDBRangerServiceVersion();
    final long rangerSvcVersionAfter =
        bgSync.getRangerOzoneServicePolicyVersion();
    Assert.assertEquals(rangerSvcVersionAfter, dbSvcVersionAfter);
    Assert.assertEquals(rangerSvcVersionAfter, rangerSvcVersionBefore);
    if (dbSvcVersionBefore != -1L) {
      Assert.assertEquals(dbSvcVersionBefore, dbSvcVersionAfter);
    }

    for (String policy : policiesCreated) {
      try {
        final AccessPolicy policyRead = auth.getAccessPolicyByName(policy);

        Assert.assertEquals(policy, policyRead.getPolicyName());
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
    }

    for (String roleObj : rolesCreated) {
      try {
        final String roleName = new JsonParser().parse(roleObj)
            .getAsJsonObject().get("name").getAsString();
        String roleObjRead = auth.getRole(roleName);
        final String roleNameReadBack = new JsonParser().parse(roleObjRead)
            .getAsJsonObject().get("name").getAsString();
        Assert.assertEquals(roleName, roleNameReadBack);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
    }
  }

  /**
   * OM DB has the tenant state.
   * But the user list in a Ranger role is tampered with.
   * Expect sync service to restore that Ranger role to the desired state.
   */
  @Test
  public void testRecoverRangerRole() throws Exception {
    long startingRangerVersion = initBGSync();

    createRolesAndPoliciesInRanger(true);

    long rangerVersionAfterCreation =
        bgSync.getRangerOzoneServicePolicyVersion();
    Assert.assertTrue(rangerVersionAfterCreation >= startingRangerVersion);

    // Delete a user from user role, expect Ranger sync thread to update it
    String userRoleName = new JsonParser().parse(rolesCreated.get(0))
        .getAsJsonObject().get("name").getAsString();
    Assert.assertEquals(
        OMMultiTenantManager.getDefaultUserRoleName(TENANT_ID), userRoleName);

    auth.revokeUserFromRole(USER_BOB_SHORT, auth.getRole(userRoleName));

    HashSet<String> userSet = new HashSet<>();
    userSet.add(USER_ALICE_SHORT);
    userSet.add(USER_BOB_SHORT);

    // Note: DB Service Version will be -1 if the test starts with an empty DB
    final long dbSvcVersionBefore = bgSync.getOMDBRangerServiceVersion();
    final long rangerSvcVersionBefore =
        bgSync.getRangerOzoneServicePolicyVersion();
    final long currRunCount = bgSync.getRangerSyncRunCount();
    bgSync.start();
    // Wait for sync to finish once.
    // The counter is incremented at the beginning of the run, hence the ">"
    GenericTestUtils.waitFor(
        () -> bgSync.getRangerSyncRunCount() > currRunCount + 1L,
        CHECK_SYNC_MILLIS, WAIT_SYNC_TIMEOUT_MILLIS);
    bgSync.shutdown();
    final long dbSvcVersionAfter = bgSync.getOMDBRangerServiceVersion();
    final long rangerSvcVersionAfter =
        bgSync.getRangerOzoneServicePolicyVersion();
    Assert.assertEquals(rangerSvcVersionAfter, dbSvcVersionAfter);
    Assert.assertTrue(dbSvcVersionAfter > dbSvcVersionBefore);
    Assert.assertTrue(rangerSvcVersionAfter > rangerSvcVersionBefore);

    for (String policy : policiesCreated) {
      final AccessPolicy verifier = auth.getAccessPolicyByName(policy);
      Assert.assertNotNull("Policy should exist in Ranger: " + policy,
          verifier);
      Assert.assertEquals(policy, verifier.getPolicyName());
    }

    for (String role : rolesCreated) {
      final String roleName = new JsonParser().parse(role).getAsJsonObject()
          .get("name").getAsString();
      if (!roleName.equals(userRoleName)) {
        continue;
      }
      final String roleObjRead = auth.getRole(roleName);
      final JsonObject jsonObj = new JsonParser().parse(roleObjRead)
          .getAsJsonObject();
      final JsonArray verifier = jsonObj.get("users").getAsJsonArray();
      Assert.assertEquals(2, verifier.size());
      // Verify that users are in the role
      for (int i = 0; i < verifier.size();  ++i) {
        String user = verifier.get(i).getAsJsonObject()
            .get("name").getAsString();
        Assert.assertTrue(userSet.contains(user));
        userSet.remove(user);
      }
      Assert.assertTrue(userSet.isEmpty());
      break;
    }
  }

  /**
   * OM DB has the tenant state. But tenant policies are deleted from Ranger.
   * Expect sync service to recover both policies to their default states.
   */
  @Test
  public void testRecreateDeletedRangerPolicy() throws Exception {
    long startingRangerVersion = initBGSync();

    // Create roles and policies in ranger that are
    // backed up by OzoneManger Multi-Tenant tables
    createRolesAndPoliciesInRanger(true);

    long rangerVersionAfterCreation =
        bgSync.getRangerOzoneServicePolicyVersion();
    Assert.assertTrue(rangerVersionAfterCreation >= startingRangerVersion);

    // Delete both policies, expect Ranger sync thread to recover both
    auth.deletePolicyByName(
        OMMultiTenantManager.getDefaultBucketNamespacePolicyName(TENANT_ID));
    auth.deletePolicyByName(
        OMMultiTenantManager.getDefaultBucketPolicyName(TENANT_ID));

    final long rangerSvcVersionBefore =
        bgSync.getRangerOzoneServicePolicyVersion();
    // Note: DB Service Version will be -1 if the test starts with an empty DB
    final long dbSvcVersionBefore = bgSync.getOMDBRangerServiceVersion();
    bgSync.start();
    // Wait for sync to finish once.
    // The counter is incremented at the beginning of the run, hence the ">"
    GenericTestUtils.waitFor(() -> bgSync.getRangerSyncRunCount() > 1L,
        CHECK_SYNC_MILLIS, WAIT_SYNC_TIMEOUT_MILLIS);
    bgSync.shutdown();
    long dbSvcVersionAfter = bgSync.getOMDBRangerServiceVersion();
    final long rangerSvcVersionAfter =
        bgSync.getRangerOzoneServicePolicyVersion();
    Assert.assertEquals(rangerSvcVersionAfter, dbSvcVersionAfter);
    Assert.assertTrue(dbSvcVersionAfter > dbSvcVersionBefore);
    Assert.assertTrue(rangerSvcVersionAfter > rangerSvcVersionBefore);

    for (String policy : policiesCreated) {
      try {
        final AccessPolicy policyRead = auth.getAccessPolicyByName(policy);

        Assert.assertEquals(policy, policyRead.getPolicyName());
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
    }

    for (String roleObj : rolesCreated) {
      try {
        final String roleName = new JsonParser().parse(roleObj)
            .getAsJsonObject().get("name").getAsString();
        String roleObjRead = auth.getRole(roleName);
        final String roleNameReadBack = new JsonParser().parse(roleObjRead)
            .getAsJsonObject().get("name").getAsString();
        Assert.assertEquals(roleName, roleNameReadBack);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
    }
  }

}
