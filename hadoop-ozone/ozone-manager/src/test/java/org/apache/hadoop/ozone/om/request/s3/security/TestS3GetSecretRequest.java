/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.request.s3.security;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.Server.Call;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMAssignUserToTenantRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMRangerServiceVersionSyncRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantCreateRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3GetSecretResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMRangerServiceVersionSyncResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantAssignUserAccessIdResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantCreateResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Secret;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignUserAccessIdRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.TENANT_NAME_USER_NAME_DELIMITER;
import static org.apache.hadoop.security.authentication.util.KerberosName.DEFAULT_MECHANISM;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Test S3GetSecretRequest.
 */
public class TestS3GetSecretRequest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private AuditLogger auditLogger;
  // Set ozoneManagerDoubleBuffer to do nothing.
  private final OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper =
      ((response, transactionIndex) -> null);

  // Multi-tenant related vars
  private static final String USER_ALICE = "alice@EXAMPLE.COM";
  private static final String TENANT_NAME = "finance";
  private static final String USER_BOB = "bob@EXAMPLE.COM";
  private static final String ACCESS_ID_BOB =
      TENANT_NAME + TENANT_NAME_USER_NAME_DELIMITER + USER_BOB;

  private UserGroupInformation ugiAlice;

  private OMMultiTenantManager omMultiTenantManager;
  private Tenant tenant;

  @Before
  public void setUp() throws Exception {
    KerberosName.setRuleMechanism(DEFAULT_MECHANISM);
    KerberosName.setRules(
        "RULE:[2:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" +
        "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" +
        "DEFAULT");
    ugiAlice = UserGroupInformation.createRemoteUser(USER_ALICE);
    Assert.assertEquals("alice", ugiAlice.getShortUserName());

    ozoneManager = mock(OzoneManager.class);

    Call call = spy(new Call(1, 1, null, null,
        RPC.RpcKind.RPC_BUILTIN, new byte[] {1, 2, 3}));
    // Run as alice, so that Server.getRemoteUser() won't return null.
    when(call.getRemoteUser()).thenReturn(ugiAlice);
    Server.getCurCall().set(call);

    omMetrics = OMMetrics.create();
    OzoneConfiguration conf = new OzoneConfiguration();
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

    when(tenant.getTenantAccessPolicies()).thenReturn(new ArrayList<>());
    when(omMultiTenantManager.createTenantAccessInAuthorizer(TENANT_NAME))
        .thenReturn(tenant);
  }

  @After
  public void tearDown() throws Exception {
    omMetrics.unRegister();
    framework().clearInlineMocks();
  }

  private OMRequest createRangerSyncRequest(long version) {

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.RangerServiceVersionSync)
        .setRangerServiceVersionSyncRequest(
            OzoneManagerProtocolProtos.RangerServiceVersionSyncRequest
                .newBuilder()
                .setRangerServiceVersion(version)
                .build()
        ).build();
  }

  private OMRequest createTenantRequest(String tenantNameStr) {

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.CreateTenant)
        .setCreateTenantRequest(
            CreateTenantRequest.newBuilder()
                .setTenantName(tenantNameStr)
                .setVolumeName(tenantNameStr)
                .build()
        ).build();
  }

  private OMRequest assignUserToTenantRequest(
      String tenantNameStr, String userPrincipalStr, String accessIdStr) {

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.TenantAssignUserAccessId)
        .setTenantAssignUserAccessIdRequest(
            TenantAssignUserAccessIdRequest.newBuilder()
                .setTenantName(tenantNameStr)
                .setTenantUsername(userPrincipalStr)
                .setAccessId(accessIdStr)
                .build()
        ).build();
  }

  private OMRequest s3GetSecretRequest(String userPrincipalStr) {

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.GetS3Secret)
        .setGetS3SecretRequest(
            GetS3SecretRequest.newBuilder()
                .setKerberosID(userPrincipalStr)
                .build()
        ).build();
  }

  @Test
  public void testGetSecretOfAnotherUserAsAdmin() throws IOException {

    // This effectively makes alice an admin.
    when(ozoneManager.isAdmin(ugiAlice)).thenReturn(true);

    // 1. Get secret of "bob@EXAMPLE.COM" (as an admin).
    long txLogIndex = 1;

    // Run preExecute
    S3GetSecretRequest s3GetSecretRequest =
        new S3GetSecretRequest(
            new S3GetSecretRequest(
                s3GetSecretRequest(ACCESS_ID_BOB)
            ).preExecute(ozoneManager)
        );

    // Run validateAndUpdateCache
    OMClientResponse omClientResponse =
        s3GetSecretRequest.validateAndUpdateCache(ozoneManager,
            txLogIndex, ozoneManagerDoubleBufferHelper);

    // Check response type and cast
    Assert.assertTrue(omClientResponse instanceof S3GetSecretResponse);
    final S3GetSecretResponse s3GetSecretResponse =
        (S3GetSecretResponse) omClientResponse;

    // Check response
    final S3SecretValue s3SecretValue = s3GetSecretResponse.getS3SecretValue();
    Assert.assertEquals(ACCESS_ID_BOB, s3SecretValue.getKerberosID());
    final String awsSecret = s3SecretValue.getAwsSecret();
    Assert.assertNotNull(awsSecret);

    final GetS3SecretResponse getS3SecretResponse =
        s3GetSecretResponse.getOMResponse().getGetS3SecretResponse();
    // The secret inside should be the same.
    final S3Secret s3Secret = getS3SecretResponse.getS3Secret();
    Assert.assertEquals(ACCESS_ID_BOB, s3Secret.getKerberosID());
    Assert.assertEquals(awsSecret, s3Secret.getAwsSecret());
  }

  @Test
  public void testGetOwnSecretAsNonAdmin() throws IOException {

    // This effectively makes alice a regular user.
    when(ozoneManager.isAdmin(ugiAlice)).thenReturn(false);

    // 1. Get secret of "alice" (as herself).
    long txLogIndex = 1;

    // Run preExecute
    S3GetSecretRequest s3GetSecretRequest1 =
        new S3GetSecretRequest(
            new S3GetSecretRequest(
                s3GetSecretRequest(USER_ALICE)
            ).preExecute(ozoneManager)
        );

    // Run validateAndUpdateCache
    OMClientResponse omClientResponse =
        s3GetSecretRequest1.validateAndUpdateCache(ozoneManager,
            txLogIndex, ozoneManagerDoubleBufferHelper);

    // Check response type and cast
    Assert.assertTrue(omClientResponse instanceof S3GetSecretResponse);
    final S3GetSecretResponse s3GetSecretResponse =
        (S3GetSecretResponse) omClientResponse;

    // Check response
    final S3SecretValue s3SecretValue = s3GetSecretResponse.getS3SecretValue();
    Assert.assertEquals(USER_ALICE, s3SecretValue.getKerberosID());
    final String awsSecret1 = s3SecretValue.getAwsSecret();
    Assert.assertNotNull(awsSecret1);

    final GetS3SecretResponse getS3SecretResponse =
        s3GetSecretResponse.getOMResponse().getGetS3SecretResponse();
    // The secret inside should be the same.
    final S3Secret s3Secret1 = getS3SecretResponse.getS3Secret();
    Assert.assertEquals(USER_ALICE, s3Secret1.getKerberosID());
    Assert.assertEquals(awsSecret1, s3Secret1.getAwsSecret());


    // 2. Get secret of "alice" (as herself) again.
    ++txLogIndex;

    // Run preExecute
    S3GetSecretRequest s3GetSecretRequest2 =
        new S3GetSecretRequest(
            new S3GetSecretRequest(
                s3GetSecretRequest(USER_ALICE)
            ).preExecute(ozoneManager)
        );

    // Run validateAndUpdateCache
    OMClientResponse omClientResponse2 =
        s3GetSecretRequest2.validateAndUpdateCache(ozoneManager,
            txLogIndex, ozoneManagerDoubleBufferHelper);

    // Check response type and cast
    Assert.assertTrue(omClientResponse2 instanceof S3GetSecretResponse);
    final S3GetSecretResponse s3GetSecretResponse2 =
        (S3GetSecretResponse) omClientResponse2;

    // Check response
    Assert.assertNull(s3GetSecretResponse2.getS3SecretValue());

    final GetS3SecretResponse getS3SecretResponse2 =
        s3GetSecretResponse2.getOMResponse().getGetS3SecretResponse();
    // The secret inside should be the same.
    final S3Secret s3Secret2 = getS3SecretResponse2.getS3Secret();
    Assert.assertEquals(USER_ALICE, s3Secret2.getKerberosID());

    // Should get the same secret as the first request's.
    Assert.assertEquals(awsSecret1, s3Secret2.getAwsSecret());
  }

  @Test
  public void testGetSecretOfAnotherUserAsNonAdmin() throws IOException {

    // This effectively makes alice a regular user.
    when(ozoneManager.isAdmin(ugiAlice)).thenReturn(false);

    // Get secret of "bob@EXAMPLE.COM" (as another regular user).
    // Run preExecute, expect USER_MISMATCH
    try {
      new S3GetSecretRequest(
          s3GetSecretRequest(ACCESS_ID_BOB)
      ).preExecute(ozoneManager);
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.USER_MISMATCH, omEx.getResult());
      return;
    }

    Assert.fail("Should have thrown OMException because alice should not have "
        + "the permission to get bob's secret in this test case!");
  }

  @Test
  public void testGetSecretWithTenant() throws IOException {

    // This effectively makes alice an admin.
    when(ozoneManager.isAdmin(ugiAlice)).thenReturn(true);
    // Init LayoutVersionManager to prevent NPE in checkLayoutFeature
    final OMLayoutVersionManager lvm =
        new OMLayoutVersionManager(OMLayoutVersionManager.maxLayoutVersion());
    when(ozoneManager.getVersionManager()).thenReturn(lvm);

    // 1. CreateTenantRequest: Create tenant "finance".
    long txLogIndex = 1;
    // Run preExecute
    OMTenantCreateRequest omTenantCreateRequest =
        new OMTenantCreateRequest(
            new OMTenantCreateRequest(
                createTenantRequest(TENANT_NAME)
            ).preExecute(ozoneManager)
        );
    // Run validateAndUpdateCache
    OMClientResponse omClientResponse =
        omTenantCreateRequest.validateAndUpdateCache(ozoneManager,
            txLogIndex, ozoneManagerDoubleBufferHelper);
    // Check response type and cast
    Assert.assertTrue(omClientResponse instanceof OMTenantCreateResponse);
    final OMTenantCreateResponse omTenantCreateResponse =
        (OMTenantCreateResponse) omClientResponse;
    // Check response
    Assert.assertTrue(omTenantCreateResponse.getOMResponse().getSuccess());
    Assert.assertEquals(TENANT_NAME,
        omTenantCreateResponse.getOmDBTenantInfo().getTenantId());


    // 2. AssignUserToTenantRequest: Assign "bob@EXAMPLE.COM" to "finance".
    ++txLogIndex;
    // Run preExecute
    OMAssignUserToTenantRequest omAssignUserToTenantRequest =
        new OMAssignUserToTenantRequest(
            new OMAssignUserToTenantRequest(
                assignUserToTenantRequest(TENANT_NAME, USER_BOB, ACCESS_ID_BOB)
            ).preExecute(ozoneManager)
        );

    // Run validateAndUpdateCache
    omClientResponse =
        omAssignUserToTenantRequest.validateAndUpdateCache(ozoneManager,
            txLogIndex, ozoneManagerDoubleBufferHelper);

    // Check response type and cast
    Assert.assertTrue(
        omClientResponse instanceof OMTenantAssignUserAccessIdResponse);
    final OMTenantAssignUserAccessIdResponse
        omTenantAssignUserAccessIdResponse =
        (OMTenantAssignUserAccessIdResponse) omClientResponse;

    // Check response
    Assert.assertTrue(omTenantAssignUserAccessIdResponse.getOMResponse()
        .getSuccess());
    Assert.assertTrue(omTenantAssignUserAccessIdResponse.getOMResponse()
        .getTenantAssignUserAccessIdResponse().getSuccess());
    final OmDBAccessIdInfo omDBAccessIdInfo =
        omTenantAssignUserAccessIdResponse.getOmDBAccessIdInfo();
    Assert.assertNotNull(omDBAccessIdInfo);
    final S3SecretValue originalS3Secret =
        omTenantAssignUserAccessIdResponse.getS3Secret();
    Assert.assertNotNull(originalS3Secret);


    // 3. S3GetSecretRequest: Get secret of "bob@EXAMPLE.COM" (as an admin).
    ++txLogIndex;

    // Run preExecute
    S3GetSecretRequest s3GetSecretRequest =
        new S3GetSecretRequest(
            new S3GetSecretRequest(
                s3GetSecretRequest(ACCESS_ID_BOB)
            ).preExecute(ozoneManager)
        );

    // Run validateAndUpdateCache
    omClientResponse =
        s3GetSecretRequest.validateAndUpdateCache(ozoneManager,
            txLogIndex, ozoneManagerDoubleBufferHelper);

    // Check response type and cast
    Assert.assertTrue(omClientResponse instanceof S3GetSecretResponse);
    final S3GetSecretResponse s3GetSecretResponse =
        (S3GetSecretResponse) omClientResponse;

    // Check response
    Assert.assertTrue(s3GetSecretResponse.getOMResponse().getSuccess());
    /*
       getS3SecretValue() should be null in this case because
       the entry is already inserted to DB in the previous request.
       The entry will get overwritten if it isn't null.
       See {@link S3GetSecretResponse#addToDBBatch}.
     */
    Assert.assertNull(s3GetSecretResponse.getS3SecretValue());
    // The secret retrieved should be the same as previous response's.
    final GetS3SecretResponse getS3SecretResponse =
        s3GetSecretResponse.getOMResponse().getGetS3SecretResponse();
    final S3Secret s3Secret = getS3SecretResponse.getS3Secret();
    Assert.assertEquals(ACCESS_ID_BOB, s3Secret.getKerberosID());
    Assert.assertEquals(originalS3Secret.getAwsSecret(),
        s3Secret.getAwsSecret());
    Assert.assertEquals(originalS3Secret.getKerberosID(),
        s3Secret.getKerberosID());
  }

  @Test
  public void testRangerSyncRequest() throws IOException {

    // This effectively makes alice an admin.
    when(ozoneManager.isAdmin(ugiAlice)).thenReturn(true);
    when(ozoneManager.getMultiTenantManager().tryAcquireInProgressMtOp(30))
        .thenReturn(true);

    // 1. CreateTenantRequest: Create tenant "finance".
    long txLogIndex = 1;
    // Run preExecute
    OMRangerServiceVersionSyncRequest omRangerServiceVersionSyncRequest =
        new OMRangerServiceVersionSyncRequest(
            new OMRangerServiceVersionSyncRequest(
                createRangerSyncRequest(10)
            ).preExecute(ozoneManager)
        );
    // Run validateAndUpdateCache
    OMClientResponse omClientResponse =
        omRangerServiceVersionSyncRequest.validateAndUpdateCache(ozoneManager,
            txLogIndex, ozoneManagerDoubleBufferHelper);
    // Check response type and cast
    Assert.assertTrue(omClientResponse
        instanceof OMRangerServiceVersionSyncResponse);
    final OMRangerServiceVersionSyncResponse
        omRangerServiceVersionSyncResponse =
        (OMRangerServiceVersionSyncResponse) omClientResponse;
    // Check response
    Assert.assertTrue(omRangerServiceVersionSyncResponse.getNewServiceVersion()
        == 10);
  }
}
