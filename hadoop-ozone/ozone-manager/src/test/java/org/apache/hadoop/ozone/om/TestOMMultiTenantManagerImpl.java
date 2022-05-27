/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl.OZONE_OM_TENANT_DEV_SKIP_RANGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserAccessIdInfo;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Tests for Multi Tenant Manager APIs.
 */
public class TestOMMultiTenantManagerImpl {

  private OMMultiTenantManagerImpl tenantManager;
  private static final String TENANT_ID = "tenant1";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    conf.set(OZONE_OM_TENANT_DEV_SKIP_RANGER, "true");
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(conf);

    final String bucketNamespaceName = TENANT_ID;
    final String bucketNamespacePolicyName =
        OMMultiTenantManager.getDefaultBucketNamespacePolicyName(TENANT_ID);
    final String bucketPolicyName =
        OMMultiTenantManager.getDefaultBucketPolicyName(TENANT_ID);
    final String userRoleName =
        OMMultiTenantManager.getDefaultUserRoleName(TENANT_ID);
    final String adminRoleName =
        OMMultiTenantManager.getDefaultAdminRoleName(TENANT_ID);
    final OmDBTenantState omDBTenantState = new OmDBTenantState(TENANT_ID,
        bucketNamespaceName, userRoleName, adminRoleName,
        bucketNamespacePolicyName, bucketPolicyName);

    omMetadataManager.getTenantStateTable().put(TENANT_ID, omDBTenantState);

    omMetadataManager.getTenantAccessIdTable().put("seed-accessId1",
        new OmDBAccessIdInfo(TENANT_ID, "seed-user1", false, false));

    OzoneManager ozoneManager = Mockito.mock(OzoneManager.class);
    Mockito.when(ozoneManager.getMetadataManager())
        .thenReturn(omMetadataManager);

    OzoneConfiguration ozoneConfiguration =
        Mockito.mock(OzoneConfiguration.class);
    Mockito.when(ozoneConfiguration.getTimeDuration(
        OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL,
        OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL_DEFAULT.getDuration(),
        OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL_DEFAULT.getUnit(),
        TimeUnit.SECONDS))
        .thenReturn(10L);
    Mockito.when(ozoneManager.getConfiguration())
        .thenReturn(ozoneConfiguration);

    tenantManager = new OMMultiTenantManagerImpl(ozoneManager, conf);
    assertEquals(1, tenantManager.getTenantCache().size());
    assertEquals(1, tenantManager.getTenantCache().get(TENANT_ID)
        .getAccessIdInfoMap().size());
  }

  @Test
  public void testListUsersInTenant() throws Exception {
    tenantManager.getCacheOp()
        .assignUserToTenant("user1", TENANT_ID, "accessId1");

    TenantUserList tenantUserList =
        tenantManager.listUsersInTenant(TENANT_ID, "");
    List<UserAccessIdInfo> userAccessIds = tenantUserList.getUserAccessIds();
    assertEquals(2, userAccessIds.size());

    for (final UserAccessIdInfo userAccessId : userAccessIds) {
      String user = userAccessId.getUserPrincipal();
      if (user.equals("user1")) {
        assertEquals("accessId1", userAccessId.getAccessId());
      } else if (user.equals("seed-user1")) {
        assertEquals("seed-accessId1", userAccessId.getAccessId());
      } else {
        Assert.fail();
      }
    }

    LambdaTestUtils.intercept(IOException.class,
        "Tenant 'tenant2' not found", () -> {
          tenantManager.listUsersInTenant("tenant2", null);
        });

    assertTrue(tenantManager.listUsersInTenant(TENANT_ID, "abc")
        .getUserAccessIds().isEmpty());
  }

  @Test
  public void testRevokeUserAccessId() throws Exception {

    LambdaTestUtils.intercept(OMException.class, () ->
        tenantManager.getCacheOp()
            .revokeUserAccessId("unknown-AccessId1", TENANT_ID));
    assertEquals(1, tenantManager.getTenantCache().size());

    tenantManager.getCacheOp()
        .revokeUserAccessId("seed-accessId1", TENANT_ID);
    assertTrue(tenantManager.getTenantCache().get(TENANT_ID)
        .getAccessIdInfoMap().isEmpty());
    assertTrue(tenantManager.listUsersInTenant(TENANT_ID, null)
        .getUserAccessIds().isEmpty());
  }

  @Test
  public void testGetTenantForAccessId() throws Exception {
    Optional<String> optionalTenant = tenantManager.getTenantForAccessID(
        "seed-accessId1");
    assertTrue(optionalTenant.isPresent());
    assertEquals(TENANT_ID, optionalTenant.get());
  }
}