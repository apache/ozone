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

import static org.apache.hadoop.ozone.OzoneConsts.TENANT_ID_USERNAME_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_ACCESS_ID;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_AUTHORIZER_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_NOT_FOUND;
import static org.apache.hadoop.ozone.om.multitenant.AccessPolicy.AccessGrantType.ALLOW;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ_ACL;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.multitenant.AccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.BucketNameSpace;
import org.apache.hadoop.ozone.om.multitenant.CachedTenantState;
import org.apache.hadoop.ozone.om.multitenant.OzoneTenant;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessAuthorizer;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessAuthorizerDummyPlugin;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessAuthorizerRangerPlugin;
import org.apache.hadoop.ozone.om.multitenant.OzoneOwnerPrincipal;
import org.apache.hadoop.ozone.om.multitenant.OzoneTenantRolePrincipal;
import org.apache.hadoop.ozone.om.multitenant.RangerAccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserAccessIdInfo;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.auth.BasicUserPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Implements OMMultiTenantManager.
 */
public class OMMultiTenantManagerImpl implements OMMultiTenantManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMMultiTenantManagerImpl.class);

  // TODO: Remove when proper testing infra is deployed.
  // Internal dev flag to skip Ranger communication.
  public static final String OZONE_OM_TENANT_DEV_SKIP_RANGER =
      "ozone.om.tenant.dev.skip.ranger";

  private MultiTenantAccessAuthorizer authorizer;
  private final OzoneManager ozoneManager;
  private final OMMetadataManager omMetadataManager;
  private final OzoneConfiguration conf;
  private final ReentrantReadWriteLock controlPathLock;
  private final Map<String, CachedTenantState> tenantCache;

  OMMultiTenantManagerImpl(OzoneManager ozoneManager, OzoneConfiguration conf)
      throws IOException {
    this.conf = conf;
    this.controlPathLock = new ReentrantReadWriteLock();
    this.ozoneManager = ozoneManager;
    this.omMetadataManager = ozoneManager.getMetadataManager();
    this.tenantCache = new ConcurrentHashMap<>();
    boolean devSkipRanger = conf.getBoolean(OZONE_OM_TENANT_DEV_SKIP_RANGER,
        false);
    if (devSkipRanger) {
      this.authorizer = new MultiTenantAccessAuthorizerDummyPlugin();
    } else {
      this.authorizer = new MultiTenantAccessAuthorizerRangerPlugin();
    }
    this.authorizer.init(conf);
    loadUsersFromDB();
  }


// start() and stop() lifeycle methods can be added when there is a background
// work going on.
//  @Override
//  public void start() throws IOException {
//  }
//
//  @Override
//  public void stop() throws Exception {
//
//  }

  @Override
  public OMMetadataManager getOmMetadataManager() {
    return omMetadataManager;
  }

  // TODO: Cleanup up this Java doc.
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
   *      We can do all of this as part of holding a coarse lock and synchronize
   *      these control path operations.
   *
   * @param tenantID
   * @return Tenant
   * @throws IOException
   */
  @Override
  public Tenant createTenantAccessInAuthorizer(String tenantID)
      throws IOException {

    Tenant tenant = new OzoneTenant(tenantID);
    try {
      controlPathLock.writeLock().lock();

      // Create admin role first
      final OzoneTenantRolePrincipal adminRole =
          OzoneTenantRolePrincipal.getAdminRole(tenantID);
      String adminRoleId = authorizer.createRole(adminRole, null);
      tenant.addTenantAccessRole(adminRoleId);

      // Then create user role, and add admin role as its delegated admin
      final OzoneTenantRolePrincipal userRole =
          OzoneTenantRolePrincipal.getUserRole(tenantID);
      String userRoleId = authorizer.createRole(userRole, adminRole.getName());
      tenant.addTenantAccessRole(userRoleId);

      BucketNameSpace bucketNameSpace = tenant.getTenantBucketNameSpace();
      // bucket namespace is volume name ??
      for (OzoneObj volume : bucketNameSpace.getBucketNameSpaceObjects()) {
        String volumeName = volume.getVolumeName();

        // Allow Volume List access
        AccessPolicy tenantVolumeAccessPolicy = newDefaultVolumeAccessPolicy(
            volumeName, userRole, adminRole);
        tenantVolumeAccessPolicy.setPolicyID(
            authorizer.createAccessPolicy(tenantVolumeAccessPolicy));
        tenant.addTenantAccessPolicy(tenantVolumeAccessPolicy);

        // Allow Bucket Create within Volume
        AccessPolicy tenantBucketCreatePolicy =
            newDefaultBucketAccessPolicy(volumeName, userRole);
        tenantBucketCreatePolicy.setPolicyID(
            authorizer.createAccessPolicy(tenantBucketCreatePolicy));
        tenant.addTenantAccessPolicy(tenantBucketCreatePolicy);
      }

      tenantCache.put(tenantID, new CachedTenantState(tenantID));
    } catch (Exception e) {
      try {
        removeTenantAccessFromAuthorizer(tenant);
      } catch (Exception exception) {
        // Best effort cleanup.
      }
      throw new IOException(e.getMessage());
    } finally {
      controlPathLock.writeLock().unlock();
    }
    return tenant;
  }

  @Override
  public void removeTenantAccessFromAuthorizer(Tenant tenant) throws Exception {
    try {
      controlPathLock.writeLock().lock();
      for (AccessPolicy policy : tenant.getTenantAccessPolicies()) {
        authorizer.deletePolicybyId(policy.getPolicyID());
      }
      for (String roleId : tenant.getTenantRoles()) {
        authorizer.deleteRole(roleId);
      }
      if (tenantCache.containsKey(tenant.getTenantId())) {
        LOG.info("Removing tenant {} from in memory cached state",
            tenant.getTenantId());
        tenantCache.remove(tenant.getTenantId());
      }
    }  finally {
      controlPathLock.writeLock().unlock();
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
   *      We can do all of this as part of holding a coarse lock and synchronize
   *      these control path operations.
   *
   * @param principal
   * @param tenantId
   * @param accessId
   * @return Tenant, or null on error
   * @throws IOException
   */
  @Override
  public String assignUserToTenant(BasicUserPrincipal principal,
                                 String tenantId,
                                 String accessId) throws IOException {
    ImmutablePair<String, String> userAccessIdPair =
        new ImmutablePair<>(principal.getName(), accessId);
    try {
      controlPathLock.writeLock().lock();

      LOG.info("Adding user '{}' to tenant '{}' in-memory state.",
          principal.getName(), tenantId);
      CachedTenantState cachedTenantState =
          tenantCache.getOrDefault(tenantId,
              new CachedTenantState(tenantId));
      cachedTenantState.getTenantUsers().add(userAccessIdPair);

      final OzoneTenantRolePrincipal roleTenantAllUsers =
          OzoneTenantRolePrincipal.getUserRole(tenantId);
      String roleJsonStr = authorizer.getRole(roleTenantAllUsers);
      String roleId = authorizer.assignUser(principal, roleJsonStr, false);
      return roleId;
    } catch (Exception e) {
      revokeUserAccessId(accessId);
      tenantCache.get(tenantId).getTenantUsers().remove(userAccessIdPair);
      throw new OMException(e.getMessage(), TENANT_AUTHORIZER_ERROR);
    } finally {
      controlPathLock.writeLock().unlock();
    }
  }

  @Override
  public void revokeUserAccessId(String accessID) throws IOException {
    try {
      controlPathLock.writeLock().lock();
      OmDBAccessIdInfo omDBAccessIdInfo =
          omMetadataManager.getTenantAccessIdTable().get(accessID);
      if (omDBAccessIdInfo == null) {
        throw new OMException(INVALID_ACCESS_ID);
      }
      String tenantId = omDBAccessIdInfo.getTenantId();
      if (tenantId == null) {
        LOG.error("Tenant doesn't exist");
        return;
      }
      tenantCache.get(tenantId).getTenantUsers()
          .remove(new ImmutablePair<>(omDBAccessIdInfo.getUserPrincipal(),
              accessID));
      // TODO: Determine how to replace this code.
//      final String userID = authorizer.getUserId(userPrincipal);
//      authorizer.deleteUser(userID);

    } finally {
      controlPathLock.writeLock().unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  public void removeUserAccessIdFromCache(String accessId, String userPrincipal,
                                          String tenantId) {
    try {
      tenantCache.get(tenantId).getTenantUsers().remove(
          new ImmutablePair<>(userPrincipal, accessId));
    } catch (NullPointerException e) {
      // tenantCache is somehow empty. Ignore for now.
      // But how?
    }
  }

  @Override
  public String getUserNameGivenAccessId(String accessId) {
    Preconditions.checkNotNull(accessId);
    try {
      controlPathLock.readLock().lock();
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
      controlPathLock.readLock().unlock();
    }
    return null;
  }

  public String getDefaultAccessId(String tenantId, String userPrincipal) {
    return tenantId + TENANT_ID_USERNAME_DELIMITER + userPrincipal;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isTenantAdmin(UserGroupInformation callerUgi,
      String tenantId, boolean delegated) {
    if (callerUgi == null) {
      return false;
    } else {
      return isTenantAdmin(
              callerUgi.getShortUserName(), tenantId, delegated)
          || isTenantAdmin(
              callerUgi.getUserName(), tenantId, delegated)
          || ozoneManager.isAdmin(callerUgi.getShortUserName())
          || ozoneManager.isAdmin(callerUgi.getUserName());
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

    if (!omMetadataManager.getTenantStateTable().isExist(tenantID)) {
      throw new IOException("Tenant '" + tenantID + "' not found!");
    }

    List<UserAccessIdInfo> userAccessIds = new ArrayList<>();
    CachedTenantState cachedTenantState = tenantCache.get(tenantID);
    if (cachedTenantState == null) {
      throw new IOException("Inconsistent in memory Tenant cache '" + tenantID
          + "' not found in cache, but present in OM DB!");
    }

    cachedTenantState.getTenantUsers().stream()
        .filter(
            k -> StringUtils.isEmpty(prefix) || k.getKey().startsWith(prefix))
        .forEach(
            k -> userAccessIds.add(
                UserAccessIdInfo.newBuilder()
                    .setUserPrincipal(k.getKey())
                    .setAccessId(k.getValue())
                    .build()));

    return new TenantUserList(userAccessIds);
  }

  @Override
  public Optional<String> getTenantForAccessID(String accessID)
      throws IOException {
    OmDBAccessIdInfo omDBAccessIdInfo =
        omMetadataManager.getTenantAccessIdTable().get(accessID);
    if (omDBAccessIdInfo == null) {
      return Optional.absent();
    }
    return Optional.of(omDBAccessIdInfo.getTenantId());
  }

  @Override
  public void assignTenantAdmin(String accessID, boolean delegated)
      throws IOException {
    try {
      controlPathLock.writeLock().lock();
      // tenantId (tenant name) is necessary to retrieve role name
      Optional<String> optionalTenant = getTenantForAccessID(accessID);
      if (!optionalTenant.isPresent()) {
        throw new OMException("No tenant found for access ID " + accessID,
            INVALID_ACCESS_ID);
      }
      final String tenantId = optionalTenant.get();

      final OzoneTenantRolePrincipal existingAdminRole =
          OzoneTenantRolePrincipal.getAdminRole(tenantId);
      final String roleJsonStr = authorizer.getRole(existingAdminRole);
      final String userPrincipal = getUserNameGivenAccessId(accessID);
      // Add user principal (not accessId!) to the role
      final String roleId = authorizer.assignUser(
          new BasicUserPrincipal(userPrincipal), roleJsonStr, delegated);
      assert (roleId != null);

      // TODO: update some in-memory mappings?

    } catch (IOException e) {
      revokeTenantAdmin(accessID);
      throw e;
    } finally {
      controlPathLock.writeLock().unlock();
    }
  }

  @Override
  public void revokeTenantAdmin(String accessID) throws IOException {

  }

  private AccessPolicy newDefaultVolumeAccessPolicy(String tenantId,
      OzoneTenantRolePrincipal userRole, OzoneTenantRolePrincipal adminRole)
      throws IOException {

    final String volumeAccessPolicyName =
        OMMultiTenantManager.getDefaultBucketNamespacePolicyName(tenantId);
    AccessPolicy policy = new RangerAccessPolicy(volumeAccessPolicyName);
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(VOLUME).setStoreType(OZONE).setVolumeName(tenantId)
        .setBucketName("").setKeyName("").build();
    // Tenant users have READ, LIST and READ_ACL access on the volume
    policy.addAccessPolicyElem(obj, userRole, READ, ALLOW);
    policy.addAccessPolicyElem(obj, userRole, LIST, ALLOW);
    policy.addAccessPolicyElem(obj, userRole, READ_ACL, ALLOW);
    // Tenant admins have ALL access on the volume
    policy.addAccessPolicyElem(obj, adminRole, ALL, ALLOW);
    return policy;
  }

  private AccessPolicy newDefaultBucketAccessPolicy(String tenantId,
      OzoneTenantRolePrincipal userRole) throws IOException {

    final String bucketAccessPolicyName =
        OMMultiTenantManager.getDefaultBucketPolicyName(tenantId);
    AccessPolicy policy = new RangerAccessPolicy(bucketAccessPolicyName);
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(BUCKET).setStoreType(OZONE).setVolumeName(tenantId)
        .setBucketName("*").setKeyName("").build();
    // Tenant users have permission to CREATE buckets
    policy.addAccessPolicyElem(obj, userRole, CREATE, ALLOW);
    // Bucket owner have ALL access on their own buckets
    policy.addAccessPolicyElem(obj, new OzoneOwnerPrincipal(), ALL, ALLOW);
    return policy;
  }

  // TODO: This policy doesn't seem necessary as the bucket-level policy has
  //  already granted the key-level access.
  //  Not sure if that is the intended behavior in Ranger though.
  //  Still, could add this KeyAccess policy as well in Ranger, doesn't hurt.
  private AccessPolicy newDefaultKeyAccessPolicy(String volumeName,
      String bucketName) throws IOException {
    AccessPolicy policy = new RangerAccessPolicy(
        // principal already contains volume name
        volumeName + " - KeyAccess");
    // TODO: Double check the policy
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(KEY).setStoreType(OZONE).setVolumeName(volumeName)
        .setBucketName("*").setKeyName("*").build();
    // Bucket owners should have ALL permission on their keys
    policy.addAccessPolicyElem(obj, new OzoneOwnerPrincipal(), ALL, ALLOW);
    return policy;
  }

  public OzoneConfiguration getConf() {
    return conf;
  }

  public void loadUsersFromDB() {
    Table<String, OmDBAccessIdInfo> tenantAccessIdTable =
        omMetadataManager.getTenantAccessIdTable();
    TableIterator<String, ? extends KeyValue<String, OmDBAccessIdInfo>>
        iterator = tenantAccessIdTable.iterator();
    int userCount = 0;

    try {
      while (iterator.hasNext()) {
        KeyValue<String, OmDBAccessIdInfo> next = iterator.next();
        String accessId = next.getKey();
        OmDBAccessIdInfo value = next.getValue();
        String tenantId = value.getTenantId();
        String user = value.getUserPrincipal();

        CachedTenantState cachedTenantState = tenantCache
            .computeIfAbsent(tenantId, k -> new CachedTenantState(tenantId));
        cachedTenantState.getTenantUsers().add(
            new ImmutablePair<>(user, accessId));
        userCount++;
      }
      LOG.info("Loaded {} tenants and {} tenant-users from the database.",
          tenantCache.size(), userCount);
    } catch (Exception ex) {
      LOG.error("Error while loading user list. ", ex);
    }
  }


  @Override
  public void checkAdmin() throws OMException {

    final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    if (!ozoneManager.isAdmin(ugi)) {
      throw new OMException("User '" + ugi.getUserName() +
          "' is not an Ozone admin.",
          OMException.ResultCodes.PERMISSION_DENIED);
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

    final OmDBTenantState tenantState =
        omMetadataManager.getTenantStateTable().get(tenantId);

    if (tenantState == null) {
      throw new OMException("Potential DB error or race condition. "
          + "OmDBTenantState entry is missing for tenant '" + tenantId + "'.",
          OMException.ResultCodes.TENANT_NOT_FOUND);
    }

    final String volumeName = tenantState.getBucketNamespaceName();

    if (volumeName == null) {
      throw new OMException("Potential DB error. volumeName "
          + "field is null for tenantId '" + tenantId + "'.",
          OMException.ResultCodes.VOLUME_NOT_FOUND);
    }

    return volumeName;
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
  Map<String, CachedTenantState> getTenantCache() {
    return tenantCache;
  }
}
