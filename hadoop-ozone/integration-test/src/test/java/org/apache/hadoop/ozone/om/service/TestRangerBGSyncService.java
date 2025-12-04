/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.hdds.scm.HddsTestUtils.mockRemoteUser;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_SERVICE;
import static org.apache.hadoop.ozone.om.OMMultiTenantManager.OZONE_TENANT_RANGER_ROLE_DESCRIPTION;
import static org.apache.hadoop.security.authentication.util.KerberosName.DEFAULT_MECHANISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
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
import org.apache.hadoop.ozone.om.multitenant.AuthorizerLockImpl;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Policy;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Role;
import org.apache.hadoop.ozone.om.multitenant.RangerClientMultiTenantAccessController;
import org.apache.hadoop.ozone.om.multitenant.RangerUserRequest;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.apache.ranger.RangerServiceException;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Tests Ozone Manager Multi-Tenancy feature Background Sync with Apache Ranger.
 * Marking it as Ignore because it needs Ranger access point.
 */
@Unhealthy("Requires a Ranger endpoint")
public class TestRangerBGSyncService {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRangerBGSyncService.class);

  private static final long TEST_SYNC_INTERVAL_SEC = 1L;
  private static final long TEST_SYNC_TIMEOUT_SEC = 3L;

  private static final int CHECK_SYNC_MILLIS = 1000;
  private static final int WAIT_SYNC_TIMEOUT_MILLIS = 60000;

  @TempDir
  private Path folder;
  @TempDir
  private String path;

  private MultiTenantAccessController accessController;
  private OMRangerBGSyncService bgSync;

  // List of policy names created in Ranger for the test
  private final List<String> policiesCreated = new ArrayList<>();
  // List of role names created in Ranger for the test
  private final List<String> rolesCreated = new ArrayList<>();
  // List of usernames created in Ranger for the test
  private final List<String> usersCreated = new ArrayList<>();

  private static OzoneConfiguration conf;
  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;

  private static final String TENANT_ID = "tenant1";

  // UGI-related vars
  private static final String USER_ALICE = "alice@EXAMPLE.COM";
  private static final String USER_ALICE_SHORT = "alice";
  private static final String USER_BOB_SHORT = "bob";
  private RangerUserRequest rangerUserRequest;

  private static void simulateOzoneSiteXmlConfig() {
    // The following configs need to be set before the test can be enabled.
    // Pass them in as JVM properties. e.g.:
    //
    // -Dozone.om.ranger.https-address=http://ranger:6080
    // -Dozone.om.ranger.https.admin.api.user=admin
    // -Dozone.om.ranger.https.admin.api.passwd=passwd

    conf.set(OZONE_RANGER_HTTPS_ADDRESS_KEY,
        System.getProperty(OZONE_RANGER_HTTPS_ADDRESS_KEY));
    conf.set(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER,
        System.getProperty(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER));
    conf.set(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD,
        System.getProperty(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD));
  }

  @BeforeAll
  public static void init() {
    conf = new OzoneConfiguration();
    simulateOzoneSiteXmlConfig();

    GenericTestUtils.setLogLevel(OMRangerBGSyncService.class, Level.DEBUG);
    GenericTestUtils.setLogLevel(RangerClientMultiTenantAccessController.class, Level.INFO);
  }

  @AfterAll
  public static void shutdown() {
  }

  @BeforeEach
  public void setUp() throws IOException {

    KerberosName.setRuleMechanism(DEFAULT_MECHANISM);
    KerberosName.setRules(
        "RULE:[2:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" +
            "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" +
            "DEFAULT");
    UserGroupInformation ugiAlice = UserGroupInformation.createRemoteUser(USER_ALICE);
    assertEquals(USER_ALICE_SHORT, ugiAlice.getShortUserName());

    ozoneManager = mock(OzoneManager.class);

    // Run as alice, so that Server.getRemoteUser() won't return null.
    mockRemoteUser(ugiAlice);

    Path metaDirPath = Paths.get(path, "om-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());

    omMetrics = OMMetrics.create();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.resolve("om").toAbsolutePath().toString());
    // No need to conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS, ...) here
    //  as we did the trick earlier with mockito.
    omMetadataManager = new OmMetadataManagerImpl(conf, ozoneManager);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    AuditLogger auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));

    // Multi-tenant related initializations
    OMMultiTenantManager omMultiTenantManager = mock(OMMultiTenantManager.class);
    Tenant tenant = mock(Tenant.class);
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
      }).when(omRatisServer).submitRequest(any(), any(), anyLong());
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    when(tenant.getTenantAccessPolicies()).thenReturn(new ArrayList<>());

    System.setProperty("javax.net.ssl.trustStore",
        "/path/to/cm-auto-global_truststore.jks");

    conf.set(OZONE_RANGER_SERVICE, "cm_ozone");

    // Helper to create and delete users from Ranger for the test.
    // RangerClient hasn't implemented these yet so we had to roll our own.
    rangerUserRequest = new RangerUserRequest(
        conf.get(OZONE_RANGER_HTTPS_ADDRESS_KEY),
        conf.get(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER),
        conf.get(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD));

    accessController = new RangerClientMultiTenantAccessController(conf);
  }

  @AfterEach
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

  private void createRoleHelper(Role role) throws IOException {
    Role roleCreated = accessController.createRole(role);
    // Confirm role creation
    assertEquals(role.getName(), roleCreated.getName());
    // Add to created roles list
    rolesCreated.add(0, role.getName());
  }

  private void createRolesAndPoliciesInRanger(boolean populateDB) throws IOException {

    policiesCreated.clear();
    rolesCreated.clear();

    // Tenant name to be used for this test
    final String tenantId = TENANT_ID;
    // volume name = bucket namespace name
    final String volumeName = tenantId;

    final String adminRoleName =
        OMMultiTenantManager.getDefaultAdminRoleName(tenantId);
    final String userRoleName =
        OMMultiTenantManager.getDefaultUserRoleName(tenantId);
    final String bucketNamespacePolicyName =
        OMMultiTenantManager.getDefaultBucketNamespacePolicyName(tenantId);
    final String bucketPolicyName =
        OMMultiTenantManager.getDefaultBucketPolicyName(tenantId);

    // Add tenant entry in OM DB
    if (populateDB) {
      LOG.info("Creating OM DB tenant entries");
      // Tenant State entry
      omMetadataManager.getTenantStateTable().put(tenantId,
          new OmDBTenantState(
              tenantId, volumeName, userRoleName, adminRoleName,
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
    }


    LOG.info("Creating user in Ranger: {}", USER_ALICE_SHORT);
    rangerUserRequest.createUser(USER_ALICE_SHORT, "Password12");
    usersCreated.add(USER_ALICE_SHORT);

    LOG.info("Creating user in Ranger: {}", USER_BOB_SHORT);
    rangerUserRequest.createUser(USER_BOB_SHORT, "Password12");
    usersCreated.add(USER_BOB_SHORT);

    LOG.info("Creating admin role in Ranger: {}", adminRoleName);
    // Create empty admin role first
    Role adminRole = new Role.Builder()
        .setName(adminRoleName)
        .setDescription(OZONE_TENANT_RANGER_ROLE_DESCRIPTION)
        .build();
    createRoleHelper(adminRole);

    LOG.info("Creating user role in Ranger: {}", userRoleName);
    Role userRole = new Role.Builder()
        .setName(userRoleName)
        .setDescription(OZONE_TENANT_RANGER_ROLE_DESCRIPTION)
        .addRole(adminRoleName, true)
        // Add alice and bob to the user role
        .addUsers(Arrays.asList(USER_ALICE_SHORT, USER_BOB_SHORT))
        .build();
    createRoleHelper(userRole);

    Policy tenant1VolumeAccessPolicy =
        OMMultiTenantManager.getDefaultVolumeAccessPolicy(
            tenantId, volumeName, userRoleName, adminRoleName);
    LOG.info("Creating VolumeAccess policy in Ranger: {}",
        tenant1VolumeAccessPolicy.getName());
    accessController.createPolicy(tenant1VolumeAccessPolicy);
    policiesCreated.add(tenant1VolumeAccessPolicy.getName());

    Policy tenant1BucketCreatePolicy =
        OMMultiTenantManager.getDefaultBucketAccessPolicy(
            tenantId, volumeName, userRoleName);
    LOG.info("Creating BucketAccess policy in Ranger: {}",
        tenant1BucketCreatePolicy.getName());
    accessController.createPolicy(tenant1BucketCreatePolicy);
    policiesCreated.add(tenant1BucketCreatePolicy.getName());
  }

  public void cleanupPolicies() {
    for (String policyName : policiesCreated) {
      try {
        LOG.info("Deleting policy: {}", policyName);
        accessController.deletePolicy(policyName);
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }
    }
  }

  public void cleanupRoles() {
    for (String roleName : rolesCreated) {
      try {
        LOG.info("Deleting role: {}", roleName);
        accessController.deleteRole(roleName);
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }
    }
  }

  public void cleanupUsers() {
    for (String user : usersCreated) {
      try {
        LOG.info("Deleting user: {}", user);
        String userId = rangerUserRequest.getUserId(user);
        rangerUserRequest.deleteUser(userId);
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
    assertThat(rangerSvcVersionBefore).isGreaterThanOrEqualTo(startingRangerVersion);

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
    assertEquals(rangerSvcVersionAfter, dbSvcVersionAfter);
    assertThat(dbSvcVersionAfter).isGreaterThan(dbSvcVersionBefore);
    assertThat(rangerSvcVersionAfter).isGreaterThan(rangerSvcVersionBefore);

    // Verify that the Ranger policies and roles not backed up
    // by OzoneManager Multi-Tenancy tables are cleaned up by sync thread

    for (String policy : policiesCreated) {
      IOException ex = assertThrows(IOException.class, () -> accessController.getPolicy(policy));
      RangerServiceException rse = assertInstanceOf(RangerServiceException.class, ex.getCause());
      assertEquals(404, rse.getStatus().getStatusCode());
    }

    for (String roleName : rolesCreated) {
      IOException ex = assertThrows(IOException.class, () -> accessController.getRole(roleName));
      RangerServiceException rse = assertInstanceOf(RangerServiceException.class, ex.getCause());
      assertEquals(400, rse.getStatus().getStatusCode());
    }
  }

  /**
   * OM DB has the tenant state.
   * Ranger has the consistent role status, and the policies are in-place.
   * Expect sync service to check Ranger state but write nothing to Ranger.
   */
  @Test
  void testConsistentState() throws Exception {
    long startingRangerVersion = initBGSync();

    // Create roles and policies in ranger that are
    // backed up by OzoneManger Multi-Tenant tables
    createRolesAndPoliciesInRanger(true);

    long rangerSvcVersionBefore = bgSync.getRangerOzoneServicePolicyVersion();
    assertThat(rangerSvcVersionBefore).isGreaterThanOrEqualTo(startingRangerVersion);

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
    assertEquals(rangerSvcVersionAfter, dbSvcVersionAfter);
    assertEquals(rangerSvcVersionAfter, rangerSvcVersionBefore);
    if (dbSvcVersionBefore != -1L) {
      assertEquals(dbSvcVersionBefore, dbSvcVersionAfter);
    }

    for (String policyName : policiesCreated) {
      final Policy policyRead = accessController.getPolicy(policyName);
      assertEquals(policyName, policyRead.getName());
    }

    for (String roleName : rolesCreated) {
      final Role roleResponse = accessController.getRole(roleName);
      assertEquals(roleName, roleResponse.getName());
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
    assertThat(rangerVersionAfterCreation).isGreaterThanOrEqualTo(startingRangerVersion);

    // Delete user bob from user role, expect Ranger sync thread to update it
    String userRoleName = rolesCreated.get(0);
    assertEquals(
        OMMultiTenantManager.getDefaultUserRoleName(TENANT_ID), userRoleName);

    Role userRole = accessController.getRole(userRoleName);
    // Remove user from role
    Role updatedRole = new Role.Builder(userRole)
        .removeUser(USER_BOB_SHORT)
        .build();
    accessController.updateRole(userRole.getId().get(), updatedRole);

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
    assertEquals(rangerSvcVersionAfter, dbSvcVersionAfter);
    assertThat(dbSvcVersionAfter).isGreaterThan(dbSvcVersionBefore);
    assertThat(rangerSvcVersionAfter).isGreaterThan(rangerSvcVersionBefore);

    for (String policyName : policiesCreated) {
      final Policy policy = accessController.getPolicy(policyName);
      assertNotNull(policy, "Policy should exist in Ranger: " + policyName);
      assertEquals(policyName, policy.getName());
    }

    for (String roleName : rolesCreated) {
      if (!roleName.equals(userRoleName)) {
        continue;
      }
      final Role roleRead = accessController.getRole(roleName);
      final Set<String> usersGot = roleRead.getUsersMap().keySet();
      assertEquals(userSet, usersGot);
      break;
    }
  }

  /**
   * OM DB has the tenant state. But tenant policies are deleted from Ranger.
   * Expect sync service to recover both policies to their default states.
   */
  @Test
  void testRecreateDeletedRangerPolicy() throws Exception {
    long startingRangerVersion = initBGSync();

    // Create roles and policies in ranger that are
    // backed up by OzoneManger Multi-Tenant tables
    createRolesAndPoliciesInRanger(true);

    long rangerVersionAfterCreation =
        bgSync.getRangerOzoneServicePolicyVersion();
    assertThat(rangerVersionAfterCreation).isGreaterThanOrEqualTo(startingRangerVersion);

    // Delete both policies, expect Ranger sync thread to recover both
    accessController.deletePolicy(
        OMMultiTenantManager.getDefaultBucketNamespacePolicyName(TENANT_ID));
    accessController.deletePolicy(
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
    assertEquals(rangerSvcVersionAfter, dbSvcVersionAfter);
    assertThat(dbSvcVersionAfter).isGreaterThan(dbSvcVersionBefore);
    assertThat(rangerSvcVersionAfter).isGreaterThan(rangerSvcVersionBefore);

    for (String policyName : policiesCreated) {
      final Policy policyRead = accessController.getPolicy(policyName);
      assertEquals(policyName, policyRead.getName());
    }

    for (String roleName : rolesCreated) {
      final Role roleRead = accessController.getRole(roleName);
      assertEquals(roleName, roleRead.getName());
    }
  }

}
