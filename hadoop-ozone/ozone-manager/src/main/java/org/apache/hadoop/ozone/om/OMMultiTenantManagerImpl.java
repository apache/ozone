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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_RANGER_SYNC_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_RANGER_SYNC_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_ACCESS_ID;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_AUTHORIZER_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_NOT_FOUND;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.multitenant.AuthorizerLock;
import org.apache.hadoop.ozone.om.multitenant.AuthorizerLockImpl;
import org.apache.hadoop.ozone.om.multitenant.BucketNameSpace;
import org.apache.hadoop.ozone.om.multitenant.CachedTenantState;
import org.apache.hadoop.ozone.om.multitenant.CachedTenantState.CachedAccessIdInfo;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Policy;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Role;
import org.apache.hadoop.ozone.om.multitenant.OzoneTenant;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.service.OMRangerBGSyncService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserAccessIdInfo;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements OMMultiTenantManager.
 */
public class OMMultiTenantManagerImpl implements OMMultiTenantManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMMultiTenantManagerImpl.class);

  // Internal flag to skip Ranger communication,
  // and to skip Ozone config validation for S3 multi-tenancy
  public static final String OZONE_OM_TENANT_DEV_SKIP_RANGER =
      "ozone.om.tenant.dev.skip.ranger";

  private final OzoneManager ozoneManager;
  private final OMMetadataManager omMetadataManager;
  private final OzoneConfiguration conf;
  // tenantCache: tenantId -> CachedTenantState
  private final Map<String, CachedTenantState> tenantCache;
  private final ReentrantReadWriteLock tenantCacheLock;
  private final OMRangerBGSyncService omRangerBGSyncService;
  private final MultiTenantAccessController accessController;
  private final AuthorizerLock authorizerLock;
  /**
   * Authorizer operations. Meant to be called in tenant preExecute.
   */
  private final TenantOp authorizerOp;
  /**
   * Cache operations. Meant to be called in tenant validateAndUpdateCache.
   */
  private final TenantOp cacheOp;

  public OMMultiTenantManagerImpl(OzoneManager ozoneManager,
                                  OzoneConfiguration conf)
      throws IOException {
    this.conf = conf;
    this.ozoneManager = ozoneManager;
    this.omMetadataManager = ozoneManager.getMetadataManager();
    this.tenantCache = new ConcurrentHashMap<>();
    this.tenantCacheLock = new ReentrantReadWriteLock();
    this.authorizerLock = new AuthorizerLockImpl();

    loadTenantCacheFromDB();

    accessController = MultiTenantAccessController.create(conf);

    cacheOp = new CacheOp(tenantCache, tenantCacheLock);
    authorizerOp = new AuthorizerOp(accessController,
        tenantCache, tenantCacheLock);

    // Define the internal time unit for the config
    final TimeUnit internalTimeUnit = TimeUnit.SECONDS;
    // Get the interval in internal time unit
    long rangerSyncInterval = ozoneManager.getConfiguration().getTimeDuration(
        OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL,
        OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL_DEFAULT.getDuration(),
        OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL_DEFAULT.getUnit(),
        internalTimeUnit);
    // Get the timeout in internal time unit
    long rangerSyncTimeout = ozoneManager.getConfiguration().getTimeDuration(
        OZONE_OM_MULTITENANCY_RANGER_SYNC_TIMEOUT,
        OZONE_OM_MULTITENANCY_RANGER_SYNC_TIMEOUT_DEFAULT.getDuration(),
        OZONE_OM_MULTITENANCY_RANGER_SYNC_TIMEOUT_DEFAULT.getUnit(),
        internalTimeUnit);
    // Initialize the Ranger Sync Thread
    omRangerBGSyncService = new OMRangerBGSyncService(ozoneManager, this,
        accessController, rangerSyncInterval, internalTimeUnit,
        rangerSyncTimeout);

    // Start the Ranger Sync Thread
    this.start();
  }

  @Override
  public OMRangerBGSyncService getOMRangerBGSyncService() {
    return omRangerBGSyncService;
  }

  /**
   * Start the Ranger policy and role sync thread.
   */
  @Override
  public void start() throws IOException {
    omRangerBGSyncService.start();
  }

  /**
   * Stop the Ranger policy and role sync thread.
   */
  @Override
  public void stop() throws IOException {
    omRangerBGSyncService.shutdown();
  }

  @Override
  public OMMetadataManager getOmMetadataManager() {
    return omMetadataManager;
  }

  @Override
  public TenantOp getAuthorizerOp() {
    return authorizerOp;
  }

  @Override
  public TenantOp getCacheOp() {
    return cacheOp;
  }

  /**
   * Implements tenant authorizer operations.
   */
  public class AuthorizerOp implements TenantOp {

    private final MultiTenantAccessController accessController;
    private final Map<String, CachedTenantState> tenantCache;
    private final ReentrantReadWriteLock tenantCacheLock;

    AuthorizerOp(MultiTenantAccessController accessController,
        Map<String, CachedTenantState> tenantCache,
        ReentrantReadWriteLock tenantCacheLock) {
      this.accessController = accessController;
      this.tenantCache = tenantCache;
      this.tenantCacheLock = tenantCacheLock;
    }

    /**
     * Throws if authorizer write lock hasn't been acquired.
     */
    private void checkAcquiredAuthorizerWriteLock() throws OMException {

      // Check if lock is acquired by the current thread
      if (!authorizerLock.isWriteLockHeldByCurrentThread()) {
        throw new OMException("Authorizer write lock must have been held "
            + "before calling this", INTERNAL_ERROR);
      }
    }

    /**
     *  Algorithm
     *  OM State :
     *    - Validation (Part of Ratis Request)
     *    - create volume {Part of RATIS request}
     *    - Persistence to OM DB {Part of RATIS request}
     *  Authorizer-plugin(Ranger) State :
     *    - For every tenant create two user groups
     *        # GroupTenantAllUsers
     *        # GroupTenantAllAdmins
     *
     *    - For every tenant create two default policies
     *    - Note: plugin states are made idempotent. Onus of returning EEXIST is
     *      part of validation in Ratis-Request. if the groups/policies exist
     *      with the same name (Due to an earlier failed/success request), in
     *      plugin, we just update in-memory-map here and return success.
     *    - The job of cleanup of any half-done authorizer-plugin state is done
     *      by a background thread.
     *  Finally :
     *    - Update all Maps maintained by Multi-Tenant-Manager
     *  In case of failure :
     *    - Undo all Ranger State
     *    - remove updates to the Map
     *  Locking :
     *    - Create/Manage Tenant/User operations are control path operations.
     *      We can do all of this as part of holding a coarse lock and
     *      synchronize these control path operations.
     *
     * @param tenantId tenant name
     * @param userRoleName user role name
     * @param adminRoleName admin role name
     * @throws IOException
     */
    @Override
    public void createTenant(
        String tenantId, String userRoleName, String adminRoleName)
        throws IOException {

      checkAcquiredAuthorizerWriteLock();

      Tenant tenant = new OzoneTenant(tenantId);

      try {
        // Create empty admin role first
        Role adminRole = new Role.Builder()
            .setName(adminRoleName)
            .setDescription(OZONE_TENANT_RANGER_ROLE_DESCRIPTION)
            .build();
        adminRole = accessController.createRole(adminRole);
        // Sanity check. Response role name should be equal to the one we sent.
        Preconditions.checkState(adminRole.getName().equals(adminRoleName));
        tenant.addTenantAccessRole(adminRoleName);

        // Then create user role with the admin role as its delegated admin
        Role userRole = new Role.Builder()
            .setName(userRoleName)
            .addRole(adminRoleName, true)
            .setDescription(OZONE_TENANT_RANGER_ROLE_DESCRIPTION)
            .build();
        userRole = accessController.createRole(userRole);
        // Sanity check. Response role name should be equal to the one we sent.
        Preconditions.checkState(userRole.getName().equals(userRoleName));
        tenant.addTenantAccessRole(userRoleName);

        BucketNameSpace bucketNameSpace = tenant.getTenantBucketNameSpace();
        // Bucket namespace is volume.
        // Note at the moment we only support one volume for each tenant.
        for (OzoneObj volume : bucketNameSpace.getBucketNameSpaceObjects()) {
          String volumeName = volume.getVolumeName();

          // Allow Volume List access
          Policy volumePolicy =
              OMMultiTenantManager.getDefaultVolumeAccessPolicy(
                  tenantId, volumeName, userRoleName, adminRoleName);
          volumePolicy = accessController.createPolicy(volumePolicy);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Created volume policy: {}", volumePolicy);
          }
          // TODO: Review if this tenant object is useful.
          tenant.addTenantAccessPolicy(volumePolicy.getName());

          // Allow Bucket Create within Volume
          Policy bucketPolicy =
              OMMultiTenantManager.getDefaultBucketAccessPolicy(
                  tenantId, volumeName, userRoleName);
          bucketPolicy = accessController.createPolicy(bucketPolicy);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Created bucket policy: {}", bucketPolicy);
          }
          // TODO: Review if this tenant object is useful.
          tenant.addTenantAccessPolicy(bucketPolicy.getName());
        }

      } catch (IOException e) {
        // Expect the sync thread to restore the admin role later if op succeeds
        throw new OMException(e, TENANT_AUTHORIZER_ERROR);
      }
    }

    @Override
    public void deleteTenant(Tenant tenant) throws IOException {

      checkAcquiredAuthorizerWriteLock();

      LOG.info("Deleting tenant policies and roles from Ranger: {}", tenant);

      try {
        for (String policyName : tenant.getTenantAccessPolicies()) {
          accessController.deletePolicy(policyName);
        }

        for (String roleName : tenant.getTenantRoles()) {
          accessController.deleteRole(roleName);
        }
      } catch (IOException e) {
        // Expect the sync thread to restore the admin role later if op succeeds
        throw new OMException(e, TENANT_AUTHORIZER_ERROR);
      }
    }

    /**
     * Helper method to check roleId presence in a Role.
     */
    private void checkRoleIdExistence(Role role) throws IOException {
      if (!role.getId().isPresent()) {
        final String errMsg = String.format("Received no role ID in: %s", role);
        LOG.error(errMsg);
        throw new IOException(errMsg);
      }
    }

    /**
     *  Algorithm
     *  Authorizer-plugin(Ranger) State :
     *    - create User in Ranger DB
     *    - For every user created
     *        Add them to # GroupTenantAllUsers
     *  In case of failure :
     *    - Undo all Ranger State
     *    - remove updates to the Map
     *  Locking :
     *    - Create/Manage Tenant/User operations are control path operations.
     *      We can do all of this as part of holding a coarse lock and
     *      synchronize these control path operations.
     *
     * @param userPrincipal
     * @param tenantId
     * @param accessId
     * @throws IOException
     */
    @Override
    public void assignUserToTenant(String userPrincipal,
        String tenantId, String accessId) throws IOException {

      checkAcquiredAuthorizerWriteLock();

      tenantCacheLock.readLock().lock();
      try {
        final CachedTenantState cachedTenantState = tenantCache.get(tenantId);
        Objects.requireNonNull(cachedTenantState,
            "Cache entry for tenant '" + tenantId + "' does not exist");

        final String tenantUserRoleName =
            tenantCache.get(tenantId).getTenantUserRoleName();
        // Get tenant user role from Ranger
        Role userRole = accessController.getRole(tenantUserRoleName);
        checkRoleIdExistence(userRole);
        final long roleId = userRole.getId().get();

        // Sanity check user existence in tenant, but won't throw
        if (userRole.getUsersMap().containsKey(userPrincipal)) {
          LOG.warn("User '{}' is already assigned to tenant '{}'",
              userPrincipal, tenantId);
        }

        // Add user (not accessId) to the role
        userRole = new Role.Builder(userRole)
            .addUser(userPrincipal, false)
            .build();
        // Push updated role to Ranger
        userRole = accessController.updateRole(roleId, userRole);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Updated user role: {}", userRole);
        }

      } catch (IOException e) {
        // If the user name doesn't exist in Ranger, it throws 400 Bad Request
        // with message: user with name: USERNAME does not exist

        // Expect the sync thread to restore the user role later if op succeeds
        throw new OMException(e, TENANT_AUTHORIZER_ERROR);
      } finally {
        tenantCacheLock.readLock().unlock();
      }
    }

    @Override
    public void revokeUserAccessId(String accessId, String tenantId)
        throws IOException {

      checkAcquiredAuthorizerWriteLock();

      tenantCacheLock.readLock().lock();
      try {
        final OmDBAccessIdInfo omDBAccessIdInfo =
            omMetadataManager.getTenantAccessIdTable().get(accessId);
        if (omDBAccessIdInfo == null) {
          throw new OMException(INVALID_ACCESS_ID);
        }
        final String tenantIdGot = omDBAccessIdInfo.getTenantId();
        Preconditions.checkArgument(tenantIdGot.equals(tenantId));

        final String userPrincipal = omDBAccessIdInfo.getUserPrincipal();

        final String tenantUserRoleName =
            tenantCache.get(tenantId).getTenantUserRoleName();
        // Get tenant user role from Ranger
        Role userRole = accessController.getRole(tenantUserRoleName);
        checkRoleIdExistence(userRole);
        final long roleId = userRole.getId().get();

        // Sanity check user existence in tenant, but won't throw
        if (!userRole.getUsersMap().containsKey(userPrincipal)) {
          LOG.warn("User '{}' is not assigned to tenant '{}'",
              userPrincipal, tenantId);
        }

        // Remove user from role
        userRole = new Role.Builder(userRole)
            .removeUser(userPrincipal)
            .build();
        // Push updated role to ranger
        userRole = accessController.updateRole(roleId, userRole);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Updated user role: {}", userRole);
        }

        // Does NOT update tenant cache here
      } catch (IOException e) {
        // Expect the sync thread to restore the user role later if op succeeds
        throw new OMException(e, TENANT_AUTHORIZER_ERROR);
      } finally {
        tenantCacheLock.readLock().unlock();
      }
    }

    @Override
    public void assignTenantAdmin(String accessId, boolean delegated)
        throws IOException {

      checkAcquiredAuthorizerWriteLock();

      tenantCacheLock.readLock().lock();
      try {
        // tenant name is needed to retrieve role name
        final String tenantId = getTenantForAccessIDThrowIfNotFound(accessId);
        final CachedTenantState cachedTenantState = tenantCache.get(tenantId);
        final String tenantAdminRoleName =
            cachedTenantState.getTenantAdminRoleName();
        final String userPrincipal = getUserNameGivenAccessId(accessId);

        // Get tenant admin role from Ranger
        Role adminRole = accessController.getRole(tenantAdminRoleName);
        checkRoleIdExistence(adminRole);

        // Sanity check user existence in tenant, but won't throw
        // TODO: Or throw if user is already in admin role?
        if (adminRole.getUsersMap().containsKey(userPrincipal)) {
          LOG.warn("User '{}' is already admin in tenant '{}'",
              userPrincipal, tenantId);
        }

        final long roleId = adminRole.getId().get();
        // Add user principal (not accessId!) to admin role
        adminRole = new Role.Builder(adminRole)
            .addUser(userPrincipal, delegated)
            .build();
        // Push updated role to Ranger
        adminRole = accessController.updateRole(roleId, adminRole);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Updated admin role: {}", adminRole);
        }

      } catch (IOException e) {
        // Expect the sync thread to restore the admin role later if op succeeds
        throw new OMException(e, TENANT_AUTHORIZER_ERROR);
      } finally {
        tenantCacheLock.readLock().unlock();
      }
    }

    @Override
    public void revokeTenantAdmin(String accessId)
        throws IOException {

      checkAcquiredAuthorizerWriteLock();

      tenantCacheLock.readLock().lock();
      try {
        // tenant name is needed to retrieve role name
        final String tenantId = getTenantForAccessIDThrowIfNotFound(accessId);
        final CachedTenantState cachedTenantState = tenantCache.get(tenantId);
        final String tenantAdminRoleName =
            cachedTenantState.getTenantAdminRoleName();
        final String userPrincipal = getUserNameGivenAccessId(accessId);

        // Get tenant admin role from Ranger
        Role adminRole = accessController.getRole(tenantAdminRoleName);
        checkRoleIdExistence(adminRole);
        final long roleId = adminRole.getId().get();

        // Sanity check user existence in tenant, but won't throw
        // TODO: Or throw if user is not in admin role?
        if (!adminRole.getUsersMap().containsKey(userPrincipal)) {
          LOG.warn("User '{}' is not admin in tenant '{}'",
              userPrincipal, tenantId);
        }

        // Add user principal (not accessId!) to admin role
        adminRole = new Role.Builder(adminRole)
            .removeUser(userPrincipal)
            .build();
        // Push updated role to Ranger
        adminRole = accessController.updateRole(roleId, adminRole);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Updated admin role: {}", adminRole);
        }

      } catch (IOException e) {
        // Expect the sync thread to restore the admin role later if op succeeds
        throw new OMException(e, TENANT_AUTHORIZER_ERROR);
      } finally {
        tenantCacheLock.readLock().unlock();
      }
    }

  }

  /**
   * Implements tenant cache operations.
   */
  public class CacheOp implements TenantOp {

    private final Map<String, CachedTenantState> tenantCache;
    private final ReentrantReadWriteLock tenantCacheLock;

    CacheOp(Map<String, CachedTenantState> tenantCache,
        ReentrantReadWriteLock tenantCacheLock) {
      this.tenantCache = tenantCache;
      this.tenantCacheLock = tenantCacheLock;
    }

    @Override
    public void createTenant(
        String tenantId, String userRoleName, String adminRoleName) {

      tenantCacheLock.writeLock().lock();
      try {
        if (tenantCache.containsKey(tenantId)) {
          LOG.warn("Cache entry for tenant '{}' already exists, "
              + "will be overwritten", tenantId);
        }

        // New entry in tenant cache
        tenantCache.put(tenantId, new CachedTenantState(
            tenantId, userRoleName, adminRoleName));
      } finally {
        tenantCacheLock.writeLock().unlock();
      }
    }

    @Override
    public void deleteTenant(Tenant tenant) throws IOException {

      final String tenantId = tenant.getTenantName();

      tenantCacheLock.writeLock().lock();
      try {
        if (tenantCache.containsKey(tenantId)) {
          LOG.info("Removing tenant from in-memory cache: {}", tenantId);
          tenantCache.remove(tenantId);
        } else {
          throw new OMException("Tenant does not exist in cache: " + tenantId,
              INTERNAL_ERROR);
        }
      } finally {
        tenantCacheLock.writeLock().unlock();
      }
    }

    @Override
    public void assignUserToTenant(String userPrincipal,
        String tenantId, String accessId) {

      final CachedAccessIdInfo cacheEntry =
          new CachedAccessIdInfo(userPrincipal, false);

      tenantCacheLock.writeLock().lock();
      try {
        final CachedTenantState cachedTenantState = tenantCache.get(tenantId);
        Objects.requireNonNull(cachedTenantState,
            "Cache entry for tenant '" + tenantId + "' does not exist");

        LOG.info("Adding to cache: user '{}' accessId '{}' in tenant '{}'",
            userPrincipal, accessId, tenantId);
        cachedTenantState.getAccessIdInfoMap().put(accessId, cacheEntry);
      } finally {
        tenantCacheLock.writeLock().unlock();
      }
    }

    @Override
    public void revokeUserAccessId(String accessId, String tenantId)
        throws IOException {

      tenantCacheLock.writeLock().lock();
      try {
        LOG.info("Removing from cache: accessId '{}' in tenant '{}'",
            accessId, tenantId);
        if (!tenantCache.get(tenantId).getAccessIdInfoMap()
            .containsKey(accessId)) {
          throw new OMException("accessId '" + accessId + "' doesn't exist "
              + "in tenant cache!", INTERNAL_ERROR);
        }
        tenantCache.get(tenantId).getAccessIdInfoMap().remove(accessId);
      } finally {
        tenantCacheLock.writeLock().unlock();
      }
    }

    /**
     * This should be called in validateAndUpdateCache after
     * the InAuthorizer variant (called in preExecute).
     */
    @Override
    public void assignTenantAdmin(String accessId, boolean delegated)
        throws IOException {

      tenantCacheLock.writeLock().lock();
      try {
        // tenant name is needed to retrieve role name
        final String tenantId = getTenantForAccessIDThrowIfNotFound(accessId);
        final CachedTenantState cachedTenantState = tenantCache.get(tenantId);

        LOG.info("Updating cache: accessId '{}' isAdmin '{}' isDelegated '{}'",
            accessId, true, delegated);
        // Update cache. Note: tenant cache does not store delegated flag yet.
        cachedTenantState.getAccessIdInfoMap().get(accessId).setIsAdmin(true);
      } finally {
        tenantCacheLock.writeLock().unlock();
      }
    }

    @Override
    public void revokeTenantAdmin(String accessId) throws IOException {

      tenantCacheLock.writeLock().lock();
      try {
        // tenant name is needed to retrieve role name
        final String tenantId = getTenantForAccessIDThrowIfNotFound(accessId);

        final CachedTenantState cachedTenantState = tenantCache.get(tenantId);

        LOG.info("Updating cache: accessId '{}' isAdmin '{}' isDelegated '{}'",
            accessId, false, false);
        // Update cache
        cachedTenantState.getAccessIdInfoMap().get(accessId).setIsAdmin(false);

      } finally {
        tenantCacheLock.writeLock().unlock();
      }
    }

  }

  @Override
  public String getUserNameGivenAccessId(String accessId) {

    Objects.requireNonNull(accessId, "accessId == null");

    tenantCacheLock.readLock().lock();
    try {
      OmDBAccessIdInfo omDBAccessIdInfo =
          omMetadataManager.getTenantAccessIdTable().get(accessId);
      if (omDBAccessIdInfo != null) {
        String userName = omDBAccessIdInfo.getUserPrincipal();
        LOG.debug("Username for accessId {} = {}", accessId, userName);
        return userName;
      }
    } catch (IOException ioEx) {
      LOG.error("Unexpected error while obtaining DB Access Info for {}",
          accessId, ioEx);
    } finally {
      tenantCacheLock.readLock().unlock();
    }
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isTenantAdmin(UserGroupInformation callerUgi,
      String tenantId, boolean delegated) {
    if (callerUgi == null) {
      return false;
    } else {
      return isTenantAdmin(callerUgi.getShortUserName(), tenantId, delegated)
          || isTenantAdmin(callerUgi.getUserName(), tenantId, delegated)
          || ozoneManager.isAdmin(callerUgi);
    }
  }

  /**
   * Internal isTenantAdmin method that takes a username String instead of UGI.
   */
  private boolean isTenantAdmin(String username, String tenantId,
      boolean delegated) {
    if (StringUtils.isEmpty(username) || StringUtils.isEmpty(tenantId)) {
      return false;
    }

    try {
      final OmDBUserPrincipalInfo principalInfo =
          omMetadataManager.getPrincipalToAccessIdsTable().get(username);

      if (principalInfo == null) {
        // The user is not assigned to any tenant
        return false;
      }

      // Find accessId assigned to the specified tenant
      for (final String accessId : principalInfo.getAccessIds()) {
        final OmDBAccessIdInfo accessIdInfo =
            omMetadataManager.getTenantAccessIdTable().get(accessId);
        // accessIdInfo could be null since we may not have a lock on the tenant
        if (accessIdInfo == null) {
          return false;
        }
        if (tenantId.equals(accessIdInfo.getTenantId())) {
          if (!delegated) {
            return accessIdInfo.getIsAdmin();
          } else {
            return accessIdInfo.getIsAdmin()
                && accessIdInfo.getIsDelegatedAdmin();
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Error while retrieving value for key '" + username
          + "' in PrincipalToAccessIdsTable");
    }

    return false;
  }

  @Override
  public TenantUserList listUsersInTenant(String tenantID, String prefix)
      throws IOException {

    List<UserAccessIdInfo> userAccessIds = new ArrayList<>();

    tenantCacheLock.readLock().lock();

    try {
      if (!omMetadataManager.getTenantStateTable().isExist(tenantID)) {
        throw new IOException("Tenant '" + tenantID + "' not found!");
      }

      CachedTenantState cachedTenantState = tenantCache.get(tenantID);

      if (cachedTenantState == null) {
        throw new IOException("Inconsistent in memory Tenant cache '" + tenantID
            + "' not found in cache, but present in OM DB!");
      }

      cachedTenantState.getAccessIdInfoMap().entrySet().stream()
          .filter(
              // Include if user principal matches the prefix
              k -> StringUtils.isEmpty(prefix) ||
                  k.getValue().getUserPrincipal().startsWith(prefix))
          .forEach(k -> {
            final String accessId = k.getKey();
            final CachedAccessIdInfo cacheEntry = k.getValue();
            userAccessIds.add(
                UserAccessIdInfo.newBuilder()
                    .setUserPrincipal(cacheEntry.getUserPrincipal())
                    .setAccessId(accessId)
                    .build());
          });

    } finally {
      tenantCacheLock.readLock().unlock();
    }

    return new TenantUserList(userAccessIds);
  }

  @Override
  public Optional<String> getTenantForAccessID(String accessID)
      throws IOException {
    OmDBAccessIdInfo omDBAccessIdInfo =
        omMetadataManager.getTenantAccessIdTable().get(accessID);
    if (omDBAccessIdInfo == null) {
      return Optional.empty();
    }
    return Optional.of(omDBAccessIdInfo.getTenantId());
  }

  /**
   * Internal helper method that gets tenant name from accessId.
   * Throws if not found.
   */
  private String getTenantForAccessIDThrowIfNotFound(String accessId)
      throws IOException {

    final Optional<String> optionalTenant = getTenantForAccessID(accessId);
    if (!optionalTenant.isPresent()) {
      throw new OMException("No tenant found for access ID: " + accessId,
          INVALID_ACCESS_ID);
    }
    return optionalTenant.get();
  }

  public OzoneConfiguration getConf() {
    return conf;
  }

  private void loadTenantCacheFromDB() {
    // First load each tenant as a key into the cache.
    final Table<String, OmDBTenantState> tenantStateTable =
        omMetadataManager.getTenantStateTable();
    try (TableIterator<String, ? extends KeyValue<String, OmDBTenantState>>
        tenantStateTableIter = tenantStateTable.iterator()) {
      while (tenantStateTableIter.hasNext()) {
        final KeyValue<String, OmDBTenantState> next =
            tenantStateTableIter.next();

        final String tenantId = next.getKey();
        final OmDBTenantState tenantState = next.getValue();

        tenantCache.put(tenantId, new CachedTenantState(tenantId,
            tenantState.getUserRoleName(), tenantState.getAdminRoleName()));
      }
    } catch (IOException ex) {
      // Do not allow an inconsistent OM to start up.
      throw new RuntimeException(
          "Error while building tenant state cache from DB.", ex);
    }

    // Next use the access ID table to fill in membership info for each tenant.
    int userCount = 0;
    final Table<String, OmDBAccessIdInfo> tenantAccessIdTable =
        omMetadataManager.getTenantAccessIdTable();
    try (TableIterator<String, ? extends KeyValue<String, OmDBAccessIdInfo>>
          accessIdTableIter = tenantAccessIdTable.iterator()) {
      while (accessIdTableIter.hasNext()) {
        final KeyValue<String, OmDBAccessIdInfo> next =
            accessIdTableIter.next();

        final String accessId = next.getKey();
        final OmDBAccessIdInfo value = next.getValue();

        final String tenantId = value.getTenantId();
        final String userPrincipal = value.getUserPrincipal();
        final boolean isAdmin = value.getIsAdmin();

        // If the TenantState doesn't exist, it means the accessId entry is
        //  orphaned or incorrect, likely metadata inconsistency
        CachedTenantState cachedTenantState = tenantCache.get(tenantId);
        Objects.requireNonNull(cachedTenantState,
            "OmDBTenantState should have existed for " + tenantId);

        cachedTenantState.getAccessIdInfoMap().put(accessId,
            new CachedAccessIdInfo(userPrincipal, isAdmin));
        userCount++;
      }
      LOG.info("Loaded {} tenants and {} tenant users from the database",
          tenantCache.size(), userCount);
    } catch (IOException ex) {
      // Do not allow an inconsistent OM to start up.
      throw new RuntimeException(
          "Error while building tenant user cache from DB.", ex);
    }
  }

  @Override
  public void checkAdmin() throws OMException {

    final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    if (!ozoneManager.isAdmin(ugi)) {
      throw new OMException("User '" + ugi.getShortUserName() +
          "' is not an Ozone admin", OMException.ResultCodes.PERMISSION_DENIED);
    }
  }

  @Override
  public void checkTenantAdmin(String tenantId, boolean delegated)
      throws OMException {

    final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    if (!isTenantAdmin(ugi, tenantId, delegated)) {
      throw new OMException("User '" + ugi.getUserName() +
          "' is neither an Ozone admin nor a delegated admin of tenant '" +
          tenantId + "'.", OMException.ResultCodes.PERMISSION_DENIED);
    }
  }

  @Override
  public void checkTenantExistence(String tenantId) throws OMException {

    try {
      if (!omMetadataManager.getTenantStateTable().isExist(tenantId)) {
        throw new OMException("Tenant '" + tenantId + "' doesn't exist.",
            OMException.ResultCodes.TENANT_NOT_FOUND);
      }
    } catch (IOException ex) {
      if (ex instanceof OMException) {
        final OMException omEx = (OMException) ex;
        if (omEx.getResult().equals(OMException.ResultCodes.TENANT_NOT_FOUND)) {
          throw omEx;
        }
      }
      throw new OMException("Error while retrieving OmDBTenantInfo for tenant "
          + "'" + tenantId + "': " + ex.getMessage(),
          OMException.ResultCodes.METADATA_ERROR);
    }
  }

  @Override
  public String getTenantVolumeName(String tenantId) throws IOException {

    // TODO: lock here?
    // tenantCacheLock.readLock().lock();

    final OmDBTenantState tenantState =
        omMetadataManager.getTenantStateTable().get(tenantId);

    if (tenantState == null) {
      throw new OMException("Tenant '" + tenantId + "' does not exist",
          OMException.ResultCodes.TENANT_NOT_FOUND);
    }

    final String volumeName = tenantState.getBucketNamespaceName();

    if (volumeName == null) {
      throw new OMException("Volume for tenant '" + tenantId +
          "' is not set!", OMException.ResultCodes.VOLUME_NOT_FOUND);
    }

    return volumeName;
  }

  @Override
  public String getTenantUserRoleName(String tenantId) throws IOException {

    tenantCacheLock.readLock().lock();

    try {
      final CachedTenantState cachedTenantState = tenantCache.get(tenantId);

      if (cachedTenantState == null) {
        throw new OMException("Tenant not found in cache: " + tenantId,
            TENANT_NOT_FOUND);
      }

      return cachedTenantState.getTenantUserRoleName();
    } finally {
      tenantCacheLock.readLock().unlock();
    }
  }

  @Override
  public String getTenantAdminRoleName(String tenantId) throws IOException {

    tenantCacheLock.readLock().lock();

    try {
      final CachedTenantState cachedTenantState = tenantCache.get(tenantId);

      if (cachedTenantState == null) {
        throw new OMException("Tenant not found in cache: " + tenantId,
            TENANT_NOT_FOUND);
      }

      return cachedTenantState.getTenantAdminRoleName();
    } finally {
      tenantCacheLock.readLock().unlock();
    }
  }

  @Override
  public Tenant getTenantFromDBById(String tenantId) throws IOException {

    // Policy names (not cached at the moment) have to retrieved from OM DB.
    // TODO: Store policy names in cache as well if needed.

    final OmDBTenantState tenantState =
        omMetadataManager.getTenantStateTable().get(tenantId);

    if (tenantState == null) {
      throw new OMException("Tenant '" + tenantId + "' does not exist",
          OMException.ResultCodes.TENANT_NOT_FOUND);
    }

    final Tenant tenantObj = new OzoneTenant(tenantState.getTenantId());

    tenantObj.addTenantAccessPolicy(tenantState.getBucketNamespacePolicyName());
    tenantObj.addTenantAccessPolicy(tenantState.getBucketPolicyName());

    tenantObj.addTenantAccessRole(tenantState.getUserRoleName());
    tenantObj.addTenantAccessRole(tenantState.getAdminRoleName());

    return tenantObj;
  }

  @Override
  public boolean isUserAccessIdPrincipalOrTenantAdmin(String accessId,
      UserGroupInformation ugi) throws IOException {

    final OmDBAccessIdInfo accessIdInfo =
        omMetadataManager.getTenantAccessIdTable().get(accessId);

    if (accessIdInfo == null) {
      // Doesn't have the accessId entry in TenantAccessIdTable.
      // Probably came from `ozone s3 getsecret` with older OM.
      return false;
    }

    final String tenantId = accessIdInfo.getTenantId();
    // Sanity check
    if (tenantId == null) {
      throw new OMException("Unexpected error: OmDBAccessIdInfo " +
          "tenantId field should not have been null",
          OMException.ResultCodes.METADATA_ERROR);
    }

    final String accessIdPrincipal = accessIdInfo.getUserPrincipal();
    // Sanity check
    if (accessIdPrincipal == null) {
      throw new OMException("Unexpected error: OmDBAccessIdInfo " +
          "kerberosPrincipal field should not have been null",
          OMException.ResultCodes.METADATA_ERROR);
    }

    // Check if ugi matches the holder of the accessId
    if (ugi.getShortUserName().equals(accessIdPrincipal)) {
      return true;
    }

    // Check if ugi is a tenant admin (or an Ozone cluster admin)
    if (isTenantAdmin(ugi, tenantId, false)) {
      return true;
    }


    return false;
  }

  @Override
  public boolean isTenantEmpty(String tenantId) throws IOException {

    if (!tenantCache.containsKey(tenantId)) {
      throw new OMException("Tenant does not exist for tenantId: " + tenantId,
          TENANT_NOT_FOUND);
    }

    return tenantCache.get(tenantId).isTenantEmpty();
  }

  @VisibleForTesting
  public Map<String, CachedTenantState> getTenantCache() {
    return tenantCache;
  }

  /**
   * Generate and return a mapping from roles to a set of user principals from
   * tenantCache.
   */
  public HashMap<String, HashSet<String>> getAllRolesFromCache() {
    final HashMap<String, HashSet<String>> mtRoles = new HashMap<>();

    tenantCacheLock.readLock().lock();

    try {
      // tenantCache: tenantId -> CachedTenantState
      for (Map.Entry<String, CachedTenantState> e1 : tenantCache.entrySet()) {
        final CachedTenantState cachedTenantState = e1.getValue();

        final String userRoleName = cachedTenantState.getTenantUserRoleName();
        mtRoles.computeIfAbsent(userRoleName, any -> new HashSet<>());
        final String adminRoleName = cachedTenantState.getTenantAdminRoleName();
        mtRoles.computeIfAbsent(adminRoleName, any -> new HashSet<>());

        final Map<String, CachedAccessIdInfo> accessIdInfoMap =
            cachedTenantState.getAccessIdInfoMap();

        // accessIdInfoMap: accessId -> CachedAccessIdInfo
        for (Map.Entry<String, CachedAccessIdInfo> e2 :
            accessIdInfoMap.entrySet()) {
          final CachedAccessIdInfo cachedAccessIdInfo = e2.getValue();

          final String userPrincipal = cachedAccessIdInfo.getUserPrincipal();
          final boolean isAdmin = cachedAccessIdInfo.getIsAdmin();

          addUserToMtRoles(mtRoles, userRoleName, userPrincipal);

          if (isAdmin) {
            addUserToMtRoles(mtRoles, adminRoleName, userPrincipal);
          }
        }
      }
    } finally {
      tenantCacheLock.readLock().unlock();
    }

    return mtRoles;
  }

  /**
   * Helper function to add user principal to a role in mtRoles.
   */
  private void addUserToMtRoles(HashMap<String, HashSet<String>> mtRoles,
      String roleName, String userPrincipal) {
    if (!mtRoles.containsKey(roleName)) {
      mtRoles.put(roleName, new HashSet<>(
          Collections.singletonList(userPrincipal)));
    } else {
      final HashSet<String> usersInTheRole = mtRoles.get(roleName);
      usersInTheRole.add(userPrincipal);
    }
  }

  @Override
  public AuthorizerLock getAuthorizerLock() {
    return authorizerLock;
  }
}
