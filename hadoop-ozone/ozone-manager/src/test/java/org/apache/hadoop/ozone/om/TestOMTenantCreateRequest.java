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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_ALREADY_EXISTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.multitenant.AuthorizerLock;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantCreateRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests create tenant request.
 */
public class TestOMTenantCreateRequest {
  @TempDir
  private Path folder;
  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getMaxUserVolumeCount()).thenReturn(10L);
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(0);
    when(lvm.isAllowed(anyString())).thenReturn(true);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    AuditLogger auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));

    OMMultiTenantManager multiTenantManager = mock(OMMultiTenantManager.class);
    doNothing().when(multiTenantManager).checkAdmin();
    AuthorizerLock authorizerLock = mock(AuthorizerLock.class);
    doNothing().when(authorizerLock).tryWriteLockInOMRequest();
    when(multiTenantManager.getAuthorizerLock()).thenReturn(authorizerLock);
    TenantOp tenantOp = mock(TenantOp.class);
    doNothing().when(tenantOp).createTenant(any(), any(), any());
    when(multiTenantManager.getAuthorizerOp()).thenReturn(tenantOp);
    when(multiTenantManager.getCacheOp()).thenReturn(tenantOp);

    when(ozoneManager.getMultiTenantManager()).thenReturn(multiTenantManager);
  }

  @AfterEach
  public void stop() {
    omMetrics.unRegister();
    framework().clearInlineMocks();
  }

  @Test
  public void testValidateAndUpdateCache() throws IOException {
    // Happy path

    final String tenantId = UUID.randomUUID().toString();

    OMRequest originalRequest =
        OMRequestTestUtils.createTenantRequest(tenantId, false);
    OMTenantCreateRequest omTenantCreateRequest =
        spy(new OMTenantCreateRequest(originalRequest));
    doReturn("username").when(omTenantCreateRequest).getUserName();

    // First creation should be successful
    OMRequest modifiedRequest = omTenantCreateRequest.preExecute(ozoneManager);
    omTenantCreateRequest = new OMTenantCreateRequest(modifiedRequest);

    long txLogIndex = 1L;
    OMClientResponse omClientResponse =
        omTenantCreateRequest.validateAndUpdateCache(ozoneManager, txLogIndex);
    OMResponse omResponse = omClientResponse.getOMResponse();

    assertNotNull(omResponse.getCreateTenantResponse());
    assertEquals(Status.OK, omResponse.getStatus());
    assertNotNull(omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(tenantId)));
  }

  @Test
  public void testValidateAndUpdateCacheWhenVolumeExists() throws Exception {
    // Check that forceCreationWhenVolumeExists flag behaves as expected

    final String tenantId = UUID.randomUUID().toString();
    final String ownerName = "username";

    // Deliberately put volume entry in VolumeTable, to simulate the case where
    //  the volume already exists.
    OMRequestTestUtils.addVolumeToDB(tenantId, ownerName, omMetadataManager);

    // First with forceCreationWhenVolumeExists = false
    OMRequest originalRequest =
        OMRequestTestUtils.createTenantRequest(tenantId, false);
    OMTenantCreateRequest omTenantCreateRequest1 =
        spy(new OMTenantCreateRequest(originalRequest));
    doReturn(ownerName).when(omTenantCreateRequest1).getUserName();

    // Should throw in preExecute
    OMException omException = assertThrows(OMException.class,
        () -> omTenantCreateRequest1.preExecute(ozoneManager));
    assertEquals(VOLUME_ALREADY_EXISTS, omException.getResult());

    // Now with forceCreationWhenVolumeExists = true
    originalRequest =
        OMRequestTestUtils.createTenantRequest(tenantId, true);
    OMTenantCreateRequest omTenantCreateRequest2 =
        spy(new OMTenantCreateRequest(originalRequest));
    doReturn(ownerName).when(omTenantCreateRequest2).getUserName();

    // Should not throw now that forceCreationWhenVolumeExists = true
    OMRequest modifiedRequest = omTenantCreateRequest2.preExecute(ozoneManager);
    omTenantCreateRequest2 = new OMTenantCreateRequest(modifiedRequest);

    // Craft a request that sets forceCreationWhenVolumeExists to false to test
    //  validateAndUpdateCache.
    CreateTenantRequest reqPostPreExecute =
        omTenantCreateRequest2.getOmRequest().getCreateTenantRequest();
    OMRequest modReqPostPreExecute =
        omTenantCreateRequest2.getOmRequest().toBuilder()
            .setCreateTenantRequest(
                CreateTenantRequest.newBuilder()
                    .setTenantId(tenantId)
                    .setVolumeName(reqPostPreExecute.getVolumeName())
                    .setUserRoleName(reqPostPreExecute.getUserRoleName())
                    .setAdminRoleName(reqPostPreExecute.getAdminRoleName())
                    .setForceCreationWhenVolumeExists(false)).build();
    OMTenantCreateRequest modTenantCreateRequest =
        new OMTenantCreateRequest(modReqPostPreExecute);
    // OMResponse should have status VOLUME_ALREADY_EXISTS in this crafted case
    OMClientResponse modOMClientResponse =
        modTenantCreateRequest.validateAndUpdateCache(ozoneManager, 2L);
    assertEquals(Status.VOLUME_ALREADY_EXISTS,
        modOMClientResponse.getOMResponse().getStatus());
    assertEquals("Volume already exists",
        modOMClientResponse.getOMResponse().getMessage());

    // validateAndUpdateCache with forceCreationWhenVolumeExists = true
    OMClientResponse omClientResponse =
        omTenantCreateRequest2.validateAndUpdateCache(ozoneManager, 2L);
    OMResponse omResponse = omClientResponse.getOMResponse();

    assertNotNull(omResponse.getCreateTenantResponse());
    assertEquals(Status.OK, omResponse.getStatus());
    assertNotNull(omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(tenantId)));
  }

  @Test
  public void
      testAcceptS3CompliantTenantIdCreationRegardlessOfStrictS3Setting()
      throws Exception {
    boolean[] omStrictS3Configs = {true, false};
    for (boolean isStrictS3 : omStrictS3Configs) {
      when(ozoneManager.isStrictS3()).thenReturn(isStrictS3);
      String tenantId = UUID.randomUUID().toString();
      acceptTenantIdCreationHelper(tenantId);
    }
  }

  @Test
  public void testRejectNonS3CompliantTenantIdCreationWithStrictS3True() {
    String[] nonS3CompliantTenantId =
        {"tenantid_underscore", "_tenantid___multi_underscore_", "tenantid_"};
    when(ozoneManager.isStrictS3()).thenReturn(true);
    for (String tenantId : nonS3CompliantTenantId) {

      OMException omException = assertThrows(OMException.class,
          () -> doPreExecute(tenantId));
      assertEquals(
          "volume name has an unsupported character : _",
          omException.getMessage()
      );
    }
  }

  @Test
  public void testAcceptNonS3CompliantTenantIdCreationWithStrictS3False()
      throws Exception {
    String[] nonS3CompliantTenantId =
        {"tenantid_underscore", "_tenantid___multi_underscore_", "tenantid_"};
    when(ozoneManager.isStrictS3()).thenReturn(false);
    for (String tenantId : nonS3CompliantTenantId) {
      acceptTenantIdCreationHelper(tenantId);
    }
  }

  private void acceptTenantIdCreationHelper(String tenantId)
      throws Exception {
    OMRequest originalRequest =
        OMRequestTestUtils.createTenantRequest(tenantId, false);
    OMTenantCreateRequest omTenantCreateRequest =
        spy(new OMTenantCreateRequest(originalRequest));
    doReturn("username").when(omTenantCreateRequest).getUserName();

    OMRequest modifiedRequest = omTenantCreateRequest.preExecute(ozoneManager);
    omTenantCreateRequest = new OMTenantCreateRequest(modifiedRequest);
    long txLogIndex = 1;
    OMClientResponse omClientResponse =
        omTenantCreateRequest.validateAndUpdateCache(ozoneManager, txLogIndex);
    OMResponse omResponse = omClientResponse.getOMResponse();

    assertNotNull(omResponse.getCreateTenantResponse());
    assertEquals(Status.OK, omResponse.getStatus());
    assertNotNull(omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(tenantId)));
  }

  private void doPreExecute(String tenantId) throws Exception {
    OMRequest originalRequest =
        OMRequestTestUtils.createTenantRequest(tenantId, false);

    OMTenantCreateRequest omTenantCreateRequest =
        spy(new OMTenantCreateRequest(originalRequest));
    doReturn("username").when(omTenantCreateRequest).getUserName();

    omTenantCreateRequest.preExecute(ozoneManager);
  }

}
