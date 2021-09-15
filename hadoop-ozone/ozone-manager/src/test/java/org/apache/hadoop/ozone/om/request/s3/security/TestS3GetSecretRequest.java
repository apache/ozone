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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssignUserToTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.UUID;

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
  private final String adminName = "alice";
  private final UserGroupInformation ugiAdmin =
      UserGroupInformation.createRemoteUser(adminName);
  private final String tenantName = "finance";
  private final String userPrincipal = "bob@EXAMPLE.COM";
  private final String accessId =
      tenantName + OzoneConsts.TENANT_NAME_USER_NAME_DELIMITER + userPrincipal;

  private OMMultiTenantManager omMultiTenantManager;
  private Tenant tenant;

  @Before
  public void setUp() throws Exception {
    ozoneManager = mock(OzoneManager.class);

    Server.Call call = spy(new Server.Call(1, 1, null, null,
        RPC.RpcKind.RPC_BUILTIN, new byte[] {1, 2, 3}));
    // So that ProtobufRpcEngine.Server.getRemoteUser() won't be null.
    when(call.getRemoteUser()).thenReturn(ugiAdmin);
    // This effectively makes alice an admin.
    when(ozoneManager.isAdmin(ugiAdmin)).thenReturn(true);
    Server.getCurCall().set(call);

    omMetrics = OMMetrics.create();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    // No need to set OzoneConfigKeys.OZONE_ADMINISTRATORS in conf here
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
    when(omMultiTenantManager.getTenantInfo(tenantName)).thenReturn(tenant);
    when(ozoneManager.getMultiTenantManager()).thenReturn(omMultiTenantManager);
  }

  @After
  public void tearDown() throws Exception {
    omMetrics.unRegister();
    framework().clearInlineMocks();
  }

  private OMRequest createTenantRequest(String tenantNameStr) {

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.CreateTenant)
        .setCreateTenantRequest(
            CreateTenantRequest.newBuilder()
                .setTenantName(tenantNameStr)
                .build()
        ).build();
  }

  private OMRequest assignUserToTenantRequest(
      String tenantNameStr, String userPrincipalStr, String accessIdStr) {

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(Type.AssignUserToTenant)
        .setAssignUserToTenantRequest(
            AssignUserToTenantRequest.newBuilder()
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
  public void testS3GetSecretRequest() throws IOException {
    // 1. CreateTenantRequest: Create tenant "finance".
    long txLogIndex = 1;
    OMTenantCreateRequest omTenantCreateRequest =
        new OMTenantCreateRequest(
            new OMTenantCreateRequest(
                createTenantRequest(tenantName)
            ).preExecute(ozoneManager)
        );

    OMClientResponse omClientResponse =
        omTenantCreateRequest.validateAndUpdateCache(ozoneManager,
            txLogIndex, ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse instanceof OMTenantCreateResponse);
    final OMTenantCreateResponse omTenantCreateResponse =
        (OMTenantCreateResponse) omClientResponse;

    Assert.assertEquals(
        tenantName, omTenantCreateResponse.getOmDBTenantInfo().getTenantName());

    // 2. AssignUserToTenantRequest: Assign "bob@EXAMPLE.COM" to "finance".
    ++txLogIndex;
    OMAssignUserToTenantRequest omAssignUserToTenantRequest =
        new OMAssignUserToTenantRequest(
            new OMAssignUserToTenantRequest(
                assignUserToTenantRequest(tenantName, userPrincipal, accessId)
            ).preExecute(ozoneManager)
        );

    omClientResponse =
        omAssignUserToTenantRequest.validateAndUpdateCache(ozoneManager,
            txLogIndex, ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse instanceof OMAssignUserToTenantResponse);
    final OMAssignUserToTenantResponse omAssignUserToTenantResponse =
        (OMAssignUserToTenantResponse) omClientResponse;

    final OmDBAccessIdInfo omDBAccessIdInfo =
        omAssignUserToTenantResponse.getOmDBAccessIdInfo();
    Assert.assertNotNull(omDBAccessIdInfo);

    // 3. S3GetSecretRequest: Get secret of "bob@EXAMPLE.COM" (as an admin).
    ++txLogIndex;
    S3GetSecretRequest s3GetSecretRequest =
        new S3GetSecretRequest(
            new S3GetSecretRequest(
                s3GetSecretRequest(accessId)
            ).preExecute(ozoneManager)
        );

    omClientResponse =
        s3GetSecretRequest.validateAndUpdateCache(ozoneManager,
            txLogIndex, ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse instanceof S3GetSecretResponse);
    final S3GetSecretResponse s3GetSecretResponse =
        (S3GetSecretResponse) omClientResponse;

    /*
       response.getS3SecretValue() should be null because the entry is
       already inserted to DB in the previous request.
       The entry would be overwritten if it isn't null.
       See {@link S3GetSecretResponse#addToDBBatch}.
     */
    Assert.assertNull(s3GetSecretResponse.getS3SecretValue());
    // The secret retrieved should be the same as previous response's.
    final GetS3SecretResponse getS3SecretResponse =
        s3GetSecretResponse.getOMResponse().getGetS3SecretResponse();
    Assert.assertEquals(
        omDBAccessIdInfo.getSharedSecret(),
        getS3SecretResponse.getS3Secret().getAwsSecret());
  }
}
