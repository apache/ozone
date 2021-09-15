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
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMAssignUserToTenantRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantCreateRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3GetSecretResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMAssignUserToTenantResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantCreateResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssignUserToTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Test case to verify S3GetSecretRequest.
 */
public class TestS3GetSecretRequest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private AuditLogger auditLogger;
  // Just setting ozoneManagerDoubleBuffer which does nothing.
  private OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper =
      ((response, transactionIndex) -> null);

  private OMMultiTenantManager omMultiTenantManager;
  private Tenant tenant;

  @Before
  public void setUp() throws Exception {
    ozoneManager = mock(OzoneManager.class);

    Server.Call call = spy(new Server.Call(1, 1, null, null,
        RPC.RpcKind.RPC_BUILTIN, new byte[] {1, 2, 3}));
    final UserGroupInformation ugiAdmin =
        UserGroupInformation.createRemoteUser("alice");
    // So that ProtobufRpcEngine.Server.getRemoteUser() won't return empty
    when(call.getRemoteUser()).thenReturn(ugiAdmin);
    // alice is an admin
    when(ozoneManager.isAdmin(ugiAdmin)).thenReturn(true);
    Server.getCurCall().set(call);

    omMetrics = OMMetrics.create();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    // No need to set OzoneConfigKeys.OZONE_ADMINISTRATORS in conf
    //  as we did the trick earlier with mockito
    omMetadataManager = new OmMetadataManagerImpl(conf);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getMaxUserVolumeCount()).thenReturn(10L);
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(0);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    when(ozoneManager.isRatisEnabled()).thenReturn(true);
    auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    // Multi-tenant stuff
    omMultiTenantManager = mock(OMMultiTenantManager.class);
    tenant = mock(Tenant.class);
    when(omMultiTenantManager.getTenantInfo("finance")).thenReturn(tenant);
    when(ozoneManager.getMultiTenantManager()).thenReturn(omMultiTenantManager);
  }

  @After
  public void tearDown() throws Exception {
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
  }

  static OMRequest newCreateTenantRequest(String tenantname) {

    final CreateTenantRequest createTenantRequest =
        CreateTenantRequest.newBuilder()
            .setTenantName(tenantname)
            .build();

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateTenant)
        .setCreateTenantRequest(createTenantRequest)
        .build();
  }

  static OMRequest newAssignUserToTenantRequest(
      String tenantname, String username, String accessId) {

    final AssignUserToTenantRequest assignUserToTenantRequest =
        AssignUserToTenantRequest.newBuilder()
            .setTenantName(tenantname)
            .setTenantUsername(username)
            .setAccessId(accessId)
            .build();

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.AssignUserToTenant)
        .setAssignUserToTenantRequest(assignUserToTenantRequest)
        .build();
  }

  static OMRequest newS3GetSecretRequest(String username) {

    final GetS3SecretRequest getS3SecretRequest =
        GetS3SecretRequest.newBuilder()
            .setKerberosID(username)
            .build();

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.GetS3Secret)
        .setGetS3SecretRequest(getS3SecretRequest)
        .build();
  }

  @Test
  public void testS3GetSecretRequest() throws IOException {
    when(ozoneManager.getMaxUserVolumeCount()).thenReturn(0L);
    long txLogIndex = 1;
//    long expectedObjId = ozoneManager.getObjectIdFromTxId(txLogIndex);
    final String tenantname = "finance";
    final String username = "bob@EXAMPLE.COM";
    final String accessId =
        tenantname + OzoneConsts.TENANT_NAME_USER_NAME_DELIMITER + username;

    // CreateTenantRequest: Create tenant "finance"
    OMRequest request = newCreateTenantRequest(tenantname);
    OMTenantCreateRequest omTenantCreateRequest =
        new OMTenantCreateRequest(request);

    request = omTenantCreateRequest.preExecute(ozoneManager);
    // Just in case preExecute returns an updated OMRequest
    omTenantCreateRequest = new OMTenantCreateRequest(request);

    try {
      final OMClientResponse omClientResponse =
          omTenantCreateRequest.validateAndUpdateCache(ozoneManager,
              txLogIndex, ozoneManagerDoubleBufferHelper);
      Assert.assertTrue(omClientResponse instanceof OMTenantCreateResponse);

      final OMTenantCreateResponse response =
          (OMTenantCreateResponse) omClientResponse;
      Assert.assertEquals(tenantname,
          response.getOmDBTenantInfo().getTenantName());
    } catch (IllegalArgumentException ex){
      GenericTestUtils.assertExceptionContains(
          "should be greater than zero", ex);
    }
    // Prep for next transaction
    ++txLogIndex;

    // AssignUserToTenantRequest: Assign "bob@EXAMPLE.COM" to "finance"
    request = newAssignUserToTenantRequest(tenantname, username, accessId);
    OMAssignUserToTenantRequest omAssignUserToTenantRequest =
        new OMAssignUserToTenantRequest(request);

    request = omAssignUserToTenantRequest.preExecute(ozoneManager);
    // Just in case preExecute returns an updated OMRequest
    omAssignUserToTenantRequest = new OMAssignUserToTenantRequest(request);

    OmDBAccessIdInfo omDBAccessIdInfo = null;

    try {
      final OMClientResponse omClientResponse =
          omAssignUserToTenantRequest.validateAndUpdateCache(ozoneManager,
              txLogIndex, ozoneManagerDoubleBufferHelper);
      Assert.assertTrue(omClientResponse
          instanceof OMAssignUserToTenantResponse);

      final OMAssignUserToTenantResponse response =
          (OMAssignUserToTenantResponse) omClientResponse;
      omDBAccessIdInfo = response.getOmDBAccessIdInfo();
    } catch (IllegalArgumentException ex){
      GenericTestUtils.assertExceptionContains(
          "should be greater than zero", ex);
    }
    Assert.assertNotNull(omDBAccessIdInfo);
    // Prep for next transaction
    ++txLogIndex;

    // S3GetSecretRequest: Get secret of "bob@EXAMPLE.COM"
    request = newS3GetSecretRequest(accessId);
    S3GetSecretRequest s3GetSecretRequest = new S3GetSecretRequest(request);

    request = s3GetSecretRequest.preExecute(ozoneManager);
    // Just in case preExecute returns an updated OMRequest
    s3GetSecretRequest = new S3GetSecretRequest(request);

    try {
      final OMClientResponse omClientResponse =
          s3GetSecretRequest.validateAndUpdateCache(ozoneManager,
              txLogIndex, ozoneManagerDoubleBufferHelper);
      Assert.assertTrue(omClientResponse instanceof S3GetSecretResponse);

      final S3GetSecretResponse response =
          (S3GetSecretResponse) omClientResponse;
      // response.getS3SecretValue() should be null because the entry already
      //  exists in DB, we are not overwriting the value.
      Assert.assertNull(response.getS3SecretValue());
      // Verify the secret retrieved from the DB.
      Assert.assertEquals(omDBAccessIdInfo.getSharedSecret(),
          response.getOMResponse()
              .getGetS3SecretResponse().getS3Secret().getAwsSecret());
    } catch (IllegalArgumentException ex){
      GenericTestUtils.assertExceptionContains(
          "should be greater than zero", ex);
    }

  }
}
