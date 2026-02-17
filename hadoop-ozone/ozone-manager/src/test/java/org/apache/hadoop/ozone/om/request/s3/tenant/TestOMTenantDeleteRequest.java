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

package org.apache.hadoop.ozone.om.request.s3.tenant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.TenantOp;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.multitenant.AuthorizerLock;
import org.apache.hadoop.ozone.om.multitenant.OzoneTenant;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests delete tenant request.
 */
public class TestOMTenantDeleteRequest {

  @TempDir
  private Path folder;
  private OzoneManager ozoneManager;
  private OMMetadataManager omMetadataManager;
  private OMMetrics omMetrics;
  private OMMultiTenantManager multiTenantManager;

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

    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(0);
    when(lvm.isAllowed(anyString())).thenReturn(true);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);

    AuditLogger auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));

    multiTenantManager = mock(OMMultiTenantManager.class);
    doNothing().when(multiTenantManager).checkAdmin();
    when(multiTenantManager.isTenantEmpty(anyString())).thenReturn(true);
    when(multiTenantManager.getTenantFromDBById(anyString())).thenAnswer(
        invocation -> new OzoneTenant(invocation.getArgument(0, String.class)));
    AuthorizerLock authorizerLock = mock(AuthorizerLock.class);
    doNothing().when(authorizerLock).tryWriteLockInOMRequest();
    doNothing().when(authorizerLock).unlockWriteInOMRequest();
    when(multiTenantManager.getAuthorizerLock()).thenReturn(authorizerLock);

    TenantOp tenantOp = mock(TenantOp.class);
    doNothing().when(tenantOp).deleteTenant(any(Tenant.class));
    when(multiTenantManager.getAuthorizerOp()).thenReturn(tenantOp);
    when(multiTenantManager.getCacheOp()).thenReturn(tenantOp);

    when(ozoneManager.getMultiTenantManager()).thenReturn(multiTenantManager);
  }

  @AfterEach
  public void tearDown() {
    omMetrics.unRegister();
    framework().clearInlineMocks();
  }

  @Test
  public void preExecutePermissionDeniedWhenAclEnabled() throws Exception {
    when(ozoneManager.getAclsEnabled()).thenReturn(true);

    final String tenantId = UUID.randomUUID().toString();
    final String volumeName = tenantId;
    when(multiTenantManager.isTenantEmpty(tenantId)).thenReturn(true);
    when(multiTenantManager.getTenantFromDBById(tenantId)).thenReturn(
        new OzoneTenant(tenantId));

    OmDBTenantState tenantState =
        new OmDBTenantState(tenantId, volumeName, "userRole", "adminRole",
            "bucketNamespacePolicy", "bucketPolicy");
    omMetadataManager.getTenantStateTable().put(tenantId, tenantState);

    OMRequest deleteRequest = OMRequestTestUtils.deleteTenantRequest(tenantId);

    OMTenantDeleteRequest omTenantDeleteRequest =
        new OMTenantDeleteRequest(deleteRequest) {
          @Override
          public void checkAcls(OzoneManager om,
              OzoneObj.ResourceType resType, OzoneObj.StoreType storeType,
              IAccessAuthorizer.ACLType aclType, String volume, String bucket,
              String key) throws IOException {
            throw new OMException("denied",
                OMException.ResultCodes.PERMISSION_DENIED);
          }
        };

    OMException exception = assertThrows(OMException.class,
        () -> omTenantDeleteRequest.preExecute(ozoneManager));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED,
        exception.getResult());
  }
}

