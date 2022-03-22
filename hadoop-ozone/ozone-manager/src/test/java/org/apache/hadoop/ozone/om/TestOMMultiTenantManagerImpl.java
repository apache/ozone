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
import static org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl.OZONE_OM_TENANT_DEV_SKIP_RANGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import com.google.common.base.Optional;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantInfo;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantUserAccessId;
import org.apache.http.auth.BasicUserPrincipal;
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
  private static String tenantName = "tenant1";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    conf.set(OZONE_OM_TENANT_DEV_SKIP_RANGER, "true");
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(conf);

    final String bucketNamespaceName = tenantName;

    // Note: policyIds is initialized with an empty list here.
    //  Expand if needed.
    final OmDBTenantInfo omDBTenantInfo = new OmDBTenantInfo(
        tenantName, bucketNamespaceName, new ArrayList<>());

    omMetadataManager.getTenantStateTable().put(tenantName, omDBTenantInfo);

    omMetadataManager.getTenantAccessIdTable().put("seed-accessId1",
        new OmDBAccessIdInfo(tenantName, "seed-user1", false, false,
            new HashSet<String>() {{ add(OzoneConsts.TENANT_ROLE_USER); }}));

    OzoneManager ozoneManager = Mockito.mock(OzoneManager.class);
    Mockito.when(ozoneManager.getMetadataManager())
        .thenReturn(omMetadataManager);

    tenantManager = new OMMultiTenantManagerImpl(ozoneManager, conf);
    assertEquals(1, tenantManager.getTenantCache().size());
    assertEquals(1,
        tenantManager.getTenantCache().get(tenantName).getTenantUsers().size());

  }

  @Test
  public void testListUsersInTenant() throws Exception {
    tenantManager.assignUserToTenant(new BasicUserPrincipal("user1"),
        tenantName, "accessId1");

    TenantUserList tenantUserList =
        tenantManager.listUsersInTenant(tenantName, "");
    List<TenantUserAccessId> userAccessIds = tenantUserList.getUserAccessIds();
    assertEquals(2, userAccessIds.size());

    for (TenantUserAccessId userAccessId : userAccessIds) {
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

    assertTrue(tenantManager.listUsersInTenant(tenantName, "abc")
        .getUserAccessIds().isEmpty());
  }

  @Test
  public void testRevokeUserAccessId() throws Exception {

    LambdaTestUtils.intercept(OMException.class, () ->
        tenantManager.revokeUserAccessId("accessId1"));
    assertEquals(1, tenantManager.getTenantCache().size());

    tenantManager.revokeUserAccessId("seed-accessId1");
    assertTrue(tenantManager.getTenantCache()
        .get(tenantName).getTenantUsers().isEmpty());
    assertTrue(tenantManager.listUsersInTenant(tenantName, null)
        .getUserAccessIds().isEmpty());
  }

  @Test
  public void testGetTenantForAccessID() throws Exception {
    Optional<String> optionalTenant = tenantManager.getTenantForAccessID(
        "seed-accessId1");
    assertTrue(optionalTenant.isPresent());
    assertEquals(tenantName, optionalTenant.get());
  }
}