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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.S3SecretFunction;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.TenantOp;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.multitenant.AuthorizerLock;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.upgrade.OMVersionManager;
import org.apache.hadoop.ozone.om.upgrade.OMVersionManagerTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests tenant assign user accessId request.
 */
public class TestOMTenantAssignUserAccessIdRequest {
  @TempDir
  private Path folder;
  private OzoneManager ozoneManager;
  private OMMetadataManager omMetadataManager;
  private OMMetrics omMetrics;
  private OMMultiTenantManager multiTenantManager;
  private S3SecretManager s3SecretManager;

  @BeforeEach
  public void setup() throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, folder.toAbsolutePath().toString());
    omMetrics = OMMetrics.create(conf);
    ozoneManager = mock(OzoneManager.class);
    omMetadataManager = new OmMetadataManagerImpl(conf, ozoneManager);
    final OMVersionManager versionManager = OMVersionManagerTestUtils.mockFinalizedOmVersionManager();
    multiTenantManager = mock(OMMultiTenantManager.class);
    final TenantOp tenantOp = mock(TenantOp.class);
    s3SecretManager = mock(S3SecretManager.class);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getVersionManager()).thenReturn(versionManager);
    when(ozoneManager.getMultiTenantManager()).thenReturn(multiTenantManager);
    when(ozoneManager.getS3SecretManager()).thenReturn(s3SecretManager);
    when(ozoneManager.getAuditLogger()).thenReturn(mock(AuditLogger.class));
    when(multiTenantManager.getAuthorizerLock()).thenReturn(mock(AuthorizerLock.class));
    when(multiTenantManager.getAuthorizerOp()).thenReturn(tenantOp);
    when(multiTenantManager.getCacheOp()).thenReturn(tenantOp);
    when(s3SecretManager.doUnderLock(any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(1, S3SecretFunction.class).accept(s3SecretManager));
  }

  @AfterEach
  public void tearDown() throws Exception {
    omMetadataManager.stop();
    omMetrics.unRegister();
    framework().clearInlineMocks();
  }

  @Test
  public void testFailedAssignLeavesNoTableCacheEntries() throws Exception {
    final String user = "bob";
    final String failingTenant = "tenant1";
    final String otherTenant = "tenant2";
    final String failingAccessId = OMMultiTenantManager.getDefaultAccessId(failingTenant, user);
    final String otherAccessId = OMMultiTenantManager.getDefaultAccessId(otherTenant, user);
    for (String tenantId : new String[] {failingTenant, otherTenant}) {
      omMetadataManager.getTenantStateTable().put(tenantId, new OmDBTenantState(tenantId, tenantId,
          OMMultiTenantManager.getDefaultUserRoleName(tenantId),
          OMMultiTenantManager.getDefaultAdminRoleName(tenantId),
          OMMultiTenantManager.getDefaultBucketNamespacePolicyName(tenantId),
          OMMultiTenantManager.getDefaultBucketPolicyName(tenantId)));
      when(multiTenantManager.getTenantVolumeName(tenantId)).thenReturn(tenantId);
    }
    // The S3SecretTable check fails for this accessId.
    when(s3SecretManager.hasS3Secret(failingAccessId)).thenReturn(true);

    OMResponse omResponse = assignUser(user, failingTenant, failingAccessId, 1L);
    assertThat(omResponse.getStatus()).isEqualTo(Status.TENANT_USER_ACCESS_ID_ALREADY_EXISTS);
    assertThat(omMetadataManager.getTenantAccessIdTable().get(failingAccessId)).isNull();
    assertThat(omMetadataManager.getPrincipalToAccessIdsTable().get(user)).isNull();

    // The next write of the same principal row must not carry the accessId of the failed request.
    omResponse = assignUser(user, otherTenant, otherAccessId, 2L);
    assertThat(omResponse.getStatus()).isEqualTo(Status.OK);
    assertThat(omMetadataManager.getTenantAccessIdTable().get(otherAccessId)).isNotNull();
    assertThat(omMetadataManager.getPrincipalToAccessIdsTable().get(user).getAccessIds())
        .containsExactly(otherAccessId);
  }

  // Runs preExecute and validateAndUpdateCache for one assign request
  private OMResponse assignUser(String user, String tenantId, String accessId, long txLogIndex) throws IOException {
    final OMTenantAssignUserAccessIdRequest request = new OMTenantAssignUserAccessIdRequest(
        OMRequestTestUtils.tenantAssignUserAccessIdRequest(user, tenantId, accessId));
    return new OMTenantAssignUserAccessIdRequest(request.preExecute(ozoneManager))
        .validateAndUpdateCache(ozoneManager, txLogIndex).getOMResponse();
  }
}
