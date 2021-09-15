package org.apache.hadoop.ozone.om.request.s3.security;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
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
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test case to verify S3GetSecretRequest.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest(ProtobufRpcEngine.Server.class)
public class TestS3GetSecretRequest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  protected OzoneManager ozoneManager;
  protected OMMetrics omMetrics;
  protected OMMetadataManager omMetadataManager;
  protected AuditLogger auditLogger;
  // Just setting ozoneManagerDoubleBuffer which does nothing.
  protected OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper =
      ((response, transactionIndex) -> null);

  protected OMMultiTenantManager omMultiTenantManager;
  protected Tenant tenant;

  protected UserGroupInformation ugi;

  @Before
  public void setUp() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
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

    omMultiTenantManager = mock(OMMultiTenantManager.class);
    tenant = mock(Tenant.class);
    when(omMultiTenantManager.getTenantInfo("finance")).thenReturn(tenant);
    when(ozoneManager.getMultiTenantManager()).thenReturn(omMultiTenantManager);

    // Extras for S3GetSecretRequest
    ugi = UserGroupInformation.createRemoteUser("bob");
    PowerMockito.mockStatic(ProtobufRpcEngine.Server.class);
    PowerMockito.when(ProtobufRpcEngine.Server.getRemoteUser()).thenReturn(ugi);
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
    long expectedObjId = ozoneManager.getObjectIdFromTxId(txLogIndex);
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
      Assert.assertEquals(omDBAccessIdInfo.getSharedSecret(),
          response.getS3SecretValue().getAwsSecret());
    } catch (IllegalArgumentException ex){
      GenericTestUtils.assertExceptionContains(
          "should be greater than zero", ex);
    }

  }
}
