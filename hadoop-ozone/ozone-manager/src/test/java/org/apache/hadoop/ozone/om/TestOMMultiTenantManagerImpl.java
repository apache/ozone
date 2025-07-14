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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl.OZONE_OM_TENANT_DEV_SKIP_RANGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.multitenant.CachedTenantState;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserAccessIdInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for Multi Tenant Manager APIs.
 */
public class TestOMMultiTenantManagerImpl {

  private OMMultiTenantManagerImpl tenantManager;
  private static final String TENANT_ID = "tenant1";
  private OMMetadataManager omMetadataManager;
  private OzoneConfiguration conf;
  private OzoneManager ozoneManager;

  @TempDir
  private Path folder;

  @BeforeEach
  public void setUp() throws IOException {
    conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS, folder.toAbsolutePath().toString());
    conf.set(OZONE_OM_TENANT_DEV_SKIP_RANGER, "true");
    omMetadataManager = new OmMetadataManagerImpl(conf, ozoneManager);

    createTenantInDB(TENANT_ID);
    assignUserToTenantInDB(TENANT_ID, "seed-accessId1", "seed-user1", false,
        false);

    ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);

    OzoneConfiguration ozoneConfiguration = mock(OzoneConfiguration.class);
    when(ozoneConfiguration.getTimeDuration(
        OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL,
        OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL_DEFAULT.getDuration(),
        OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL_DEFAULT.getUnit(),
        TimeUnit.SECONDS))
        .thenReturn(10L);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);

    tenantManager = new OMMultiTenantManagerImpl(ozoneManager, conf);
  }

  /**
   * Tests rebuilding the tenant cache on restart.
   */
  @Test
  public void testReloadCache() throws IOException {
    // Create a tenant with multiple users.
    CachedTenantState expectedTenant2State = createTenant("tenant2");
    assignUserToTenant(expectedTenant2State, "access2", "user2", false, false);
    assignUserToTenant(expectedTenant2State, "access3", "user2", true, false);
    assignUserToTenant(expectedTenant2State, "access4", "user2", true, true);

    // Create a tenant with no users.
    CachedTenantState expectedTenant3State = createTenant("tenant3");

    // Reload the cache as part of new object creation.
    OMMultiTenantManagerImpl tenantManager2 =
        new OMMultiTenantManagerImpl(ozoneManager, conf);
    // Check that the cache was restored correctly.
    // Setup created a tenant in addition to the ones created for this test.
    assertEquals(3, tenantManager2.getTenantCache().size());
    // Check tenant2
    assertEquals(expectedTenant2State, tenantManager.getTenantCache()
        .get("tenant2"));
    // Check tenant3
    assertEquals(expectedTenant3State, tenantManager.getTenantCache()
        .get("tenant3"));

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
        fail();
      }
    }

    IOException ioException = assertThrows(IOException.class,
        () -> tenantManager.listUsersInTenant("tenant2", null));
    assertEquals("Tenant 'tenant2' not found!", ioException.getMessage());

    assertThat(tenantManager.listUsersInTenant(TENANT_ID, "abc")
        .getUserAccessIds()).isEmpty();
  }

  @Test
  public void testRevokeUserAccessId() throws Exception {

    assertThrows(OMException.class, () ->
        tenantManager.getCacheOp()
            .revokeUserAccessId("unknown-AccessId1", TENANT_ID));
    assertEquals(1, tenantManager.getTenantCache().size());

    tenantManager.getCacheOp()
        .revokeUserAccessId("seed-accessId1", TENANT_ID);
    assertThat(tenantManager.getTenantCache().get(TENANT_ID)
        .getAccessIdInfoMap()).isEmpty();
    assertThat(tenantManager.listUsersInTenant(TENANT_ID, null)
        .getUserAccessIds()).isEmpty();
  }

  @Test
  public void testGetTenantForAccessId() throws Exception {
    Optional<String> optionalTenant = tenantManager.getTenantForAccessID(
        "seed-accessId1");
    assertTrue(optionalTenant.isPresent());
    assertEquals(TENANT_ID, optionalTenant.get());
  }

  /**
   * @return A new {@link CachedTenantState} object expected to match the one
   * created by the cache.
   */
  private CachedTenantState createTenant(String tenantId) throws IOException {
    final String userRoleName =
        OMMultiTenantManager.getDefaultUserRoleName(tenantId);
    final String adminRoleName =
        OMMultiTenantManager.getDefaultAdminRoleName(tenantId);
    createTenantInDB(tenantId, userRoleName, adminRoleName);
    tenantManager.getCacheOp().createTenant(tenantId, userRoleName,
        adminRoleName);

    return new CachedTenantState(tenantId, userRoleName, adminRoleName);
  }

  private void createTenantInDB(String tenantId) throws IOException {
    final String userRoleName =
        OMMultiTenantManager.getDefaultUserRoleName(tenantId);
    final String adminRoleName =
        OMMultiTenantManager.getDefaultAdminRoleName(tenantId);
    createTenantInDB(tenantId, userRoleName, adminRoleName);
  }

  private void createTenantInDB(String tenantId, String userRoleName,
      String adminRoleName) throws IOException {
    final String bucketNamespaceName = tenantId;
    final String bucketNamespacePolicyName =
        OMMultiTenantManager.getDefaultBucketNamespacePolicyName(tenantId);
    final String bucketPolicyName =
        OMMultiTenantManager.getDefaultBucketPolicyName(tenantId);
    final OmDBTenantState omDBTenantState = new OmDBTenantState(tenantId,
        bucketNamespaceName, userRoleName, adminRoleName,
        bucketNamespacePolicyName, bucketPolicyName);

    omMetadataManager.getTenantStateTable().put(tenantId, omDBTenantState);
  }

  /**
   * The {@link CachedTenantState} parameter will be updated to match the
   * expected update performed by the cache.
   */
  private void assignUserToTenant(
      CachedTenantState tenantState, String accessId, String user,
      boolean isAdmin, boolean isDelegatedAdmin) throws IOException {
    assignUserToTenantInDB(tenantState.getTenantId(), accessId, user, isAdmin,
        isDelegatedAdmin);
    tenantManager.getCacheOp().assignUserToTenant(user,
        tenantState.getTenantId(), accessId);
    if (isAdmin) {
      tenantManager.getCacheOp().assignTenantAdmin(accessId, isDelegatedAdmin);
    }

    tenantState.getAccessIdInfoMap().put(accessId,
        new CachedTenantState.CachedAccessIdInfo(user, isAdmin));
  }

  private void assignUserToTenantInDB(String tenantId, String accessId,
      String user, boolean isAdmin, boolean isDelegatedAdmin)
      throws IOException {
    omMetadataManager.getTenantAccessIdTable().put(accessId,
        new OmDBAccessIdInfo(tenantId, user, isAdmin, isDelegatedAdmin));
  }
}
