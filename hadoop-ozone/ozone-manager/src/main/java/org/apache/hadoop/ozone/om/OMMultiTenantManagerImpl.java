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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_TENANT_ACCESSID;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TENANT_AUTHORIZER_ERROR;
import static org.apache.hadoop.ozone.om.multitenant.AccessPolicy.AccessGrantType.ALLOW;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.multitenant.AccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.AccountNameSpace;
import org.apache.hadoop.ozone.om.multitenant.BucketNameSpace;
import org.apache.hadoop.ozone.om.multitenant.CachedTenantInfo;
import org.apache.hadoop.ozone.om.multitenant.CephCompatibleTenantImpl;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessAuthorizer;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessAuthorizerDummyPlugin;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessAuthorizerRangerPlugin;
import org.apache.hadoop.ozone.om.multitenant.OzoneTenantGroupPrincipal;
import org.apache.hadoop.ozone.om.multitenant.RangerAccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantUserAccessId;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.http.auth.BasicUserPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final OMMetadataManager omMetadataManager;
  private final OzoneConfiguration conf;
  private final ReentrantReadWriteLock controlPathLock;
  private final Map<String, CachedTenantInfo> tenantCache;

  OMMultiTenantManagerImpl(OMMetadataManager mgr, OzoneConfiguration conf)
      throws IOException {
    this.conf = conf;
    controlPathLock = new ReentrantReadWriteLock();
    omMetadataManager = mgr;
    tenantCache = new ConcurrentHashMap<>();
    boolean devSkipRanger = conf.getBoolean(OZONE_OM_TENANT_DEV_SKIP_RANGER,
        false);
    if (devSkipRanger) {
      authorizer = new MultiTenantAccessAuthorizerDummyPlugin();
    } else {
      authorizer = new MultiTenantAccessAuthorizerRangerPlugin();
    }
    authorizer.init(conf);
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

    Tenant tenant = new CephCompatibleTenantImpl(tenantID);
    try {
      controlPathLock.writeLock().lock();

      OzoneTenantGroupPrincipal allTenantUsers =
          OzoneTenantGroupPrincipal.newUserGroup(tenantID);
      String allTenantUsersGroupID = authorizer.createGroup(allTenantUsers);
      tenant.addTenantAccessGroup(allTenantUsersGroupID);

      OzoneTenantGroupPrincipal allTenantAdmins =
          OzoneTenantGroupPrincipal.newAdminGroup(tenantID);
      String allTenantAdminsGroupID = authorizer.createGroup(allTenantAdmins);
      tenant.addTenantAccessGroup(allTenantAdminsGroupID);

      BucketNameSpace bucketNameSpace = tenant.getTenantBucketNameSpace();
      for (OzoneObj volume : bucketNameSpace.getBucketNameSpaceObjects()) {
        String volumeName = volume.getVolumeName();
        // Allow Volume List access
        AccessPolicy tenantVolumeAccessPolicy = createVolumeAccessPolicy(
            volumeName, allTenantUsers);
        tenantVolumeAccessPolicy.setPolicyID(
            authorizer.createAccessPolicy(tenantVolumeAccessPolicy));
        tenant.addTenantAccessPolicy(tenantVolumeAccessPolicy);

        // Allow Bucket Create within Volume
        AccessPolicy tenantBucketCreatePolicy = allowCreateBucketPolicy(
            volumeName, allTenantUsers);
        tenantBucketCreatePolicy.setPolicyID(
            authorizer.createAccessPolicy(tenantBucketCreatePolicy));
        tenant.addTenantAccessPolicy(tenantBucketCreatePolicy);
      }

      tenantCache.put(tenantID, new CachedTenantInfo(tenantID));
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
  public Tenant getTenantInfo(String tenantID) throws IOException {
    // Todo : fix this.
    return null;
  }

  @Override
  public void deactivateTenant(String tenantID) throws IOException {

  }

  @Override
  public void removeTenantAccessFromAuthorizer(Tenant tenant) throws Exception {
    try {
      controlPathLock.writeLock().lock();
      for (AccessPolicy policy : tenant.getTenantAccessPolicies()) {
        authorizer.deletePolicybyId(policy.getPolicyID());
      }
      for (String groupID : tenant.getTenantGroups()) {
        authorizer.deleteGroup(groupID);
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
   * @param tenantName
   * @param accessID
   * @return Tenant, or null on error
   */
  @Override
  public void assignUserToTenant(BasicUserPrincipal principal,
                                 String tenantName,
                                 String accessID) throws OMException {
    try {
      controlPathLock.writeLock().lock();
      //TODO : Add user to role in Authorizer.
      CachedTenantInfo cachedTenantInfo =
          tenantCache.getOrDefault(tenantName,
              new CachedTenantInfo(tenantName));
      cachedTenantInfo.getTenantUsers().add(new ImmutablePair<>(
          principal.getName(), accessID));
      LOG.info("Adding user '{}' to tenant '{}' in-memory state.",
          principal.getName(), tenantName);
    } catch (Exception e) {
      throw new OMException(e.getMessage(), TENANT_AUTHORIZER_ERROR);
    } finally {
      controlPathLock.writeLock().unlock();
    }
  }

  @Override
  public void destroyUser(BasicUserPrincipal principal, String accessID) {
    try {
      controlPathLock.writeLock().lock();
      //TODO : Remove user from group in Authorizer.
      String tenantName = getTenantForAccessID(accessID);
      if (tenantName == null) {
        LOG.error("Tenant doesn't exist");
        return;
      }
      tenantCache.get(tenantName).getTenantUsers()
          .remove(new ImmutablePair<>(principal.getName(), accessID));
      // TODO: Determine how to replace this code.
//      final String userID = authorizer.getUserId(userPrincipal);
//      authorizer.deleteUser(userID);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    } finally {
      controlPathLock.writeLock().unlock();
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
        String userName = omDBAccessIdInfo.getKerberosPrincipal();
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

  @Override
  public String getUserSecret(String accessID)
      throws IOException {
    return "";
  }

  @Override
  public void modifyUser(String accessID,
                         List<String> groupsAdded,
                         List<String> groupsRemoved) throws IOException {

  }

  @Override
  public void deactivateUser(String accessID)
      throws IOException {

  }

  @Override
  public boolean isTenantAdmin(String user, String tenantName) {
    return true;
  }

  @Override
  public TenantUserList listUsersInTenant(String tenantID, String prefix)
      throws IOException {

    if (!omMetadataManager.getTenantStateTable().isExist(tenantID)) {
      throw new IOException("Tenant '" + tenantID + "' not found!");
    }

    List<TenantUserAccessId> userAccessIds = new ArrayList<>();
    CachedTenantInfo cachedTenantInfo = tenantCache.get(tenantID);
    if (cachedTenantInfo == null) {
      throw new IOException("Inconsistent in memory Tenant cache '" + tenantID
          + "' not found in cache, but present in OM DB!");
    }

    cachedTenantInfo.getTenantUsers().stream()
        .filter(
            k -> StringUtils.isEmpty(prefix) || k.getKey().startsWith(prefix))
        .forEach(
            k -> userAccessIds.add(
                TenantUserAccessId.newBuilder()
                    .setUser(k.getKey())
                    .setAccessId(k.getValue())
                    .build()));
    return new TenantUserList(tenantID, userAccessIds);
  }

  @Override
  public String getTenantForAccessID(String accessID) throws IOException {
    OmDBAccessIdInfo omDBAccessIdInfo =
        omMetadataManager.getTenantAccessIdTable().get(accessID);
    if (omDBAccessIdInfo == null) {
      throw new OMException(INVALID_TENANT_ACCESSID);
    }
    return omDBAccessIdInfo.getTenantId();
  }

  public List<String> listAllAccessIDs(String tenantID)
      throws IOException {
    return null;
  }


  @Override
  public void assignTenantAdminRole(String accessID)
      throws IOException {

  }

  @Override
  public void revokeTenantAdmin(String accessID)
      throws IOException {

  }

  @Override
  public List<String> listAllTenantAdmin(String tenantID)
      throws IOException {
    return null;
  }

  @Override
  public void grantAccess(String accessID,
      BucketNameSpace bucketNameSpace) throws IOException {

  }

  @Override
  public void grantBucketAccess(String accessID,
      BucketNameSpace bucketNameSpace, String bucketName) throws IOException {

  }

  @Override
  public void revokeAccess(String accessID,
      BucketNameSpace bucketNameSpace) throws IOException {

  }

  @Override
  public void grantAccess(String accessID,
      AccountNameSpace accountNameSpace) throws IOException {

  }

  @Override
  public void revokeAccess(String accessID,
      AccountNameSpace accountNameSpace) throws IOException {

  }

  @Override
  public String createTenantDefaultPolicy(Tenant tenant,
      AccessPolicy policy) throws IOException {
    return null;
  }

  @Override
  public List<Pair<String, AccessPolicy>> listDefaultTenantPolicies(
      Tenant tenant) throws IOException {
    return null;
  }

  @Override
  public List<Pair<String, AccessPolicy>> listAllTenantPolicies(
      Tenant tenant) throws IOException {
    return null;
  }

  @Override
  public void updateTenantPolicy(Tenant tenant, String policyID,
      AccessPolicy policy) throws IOException {

  }

  private AccessPolicy createVolumeAccessPolicy(String vol,
      OzoneTenantGroupPrincipal principal) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        principal.getName() + "VolumeAccess" + vol + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(VOLUME).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName("").setKeyName("").build();
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, READ, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, LIST, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal,
        READ_ACL, ALLOW);
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowCreateBucketPolicy(String vol,
      OzoneTenantGroupPrincipal principal) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        principal.getName() + "AllowBucketCreate" + vol + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(BUCKET).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName("*").setKeyName("").build();
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, CREATE, ALLOW);
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowAccessBucketPolicy(String vol, String bucketName,
      OzoneTenantGroupPrincipal principal) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        principal.getName() + "AllowBucketAccess" + vol + bucketName +
            "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(BUCKET).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName(bucketName).setKeyName("*").build();
    for (ACLType acl : ACLType.values()) {
      if (acl != NONE) {
        tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, acl,
            ALLOW);
      }
    }
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowAccessKeyPolicy(String vol, String bucketName,
      OzoneTenantGroupPrincipal principal) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        principal.getName() + "AllowBucketKeyAccess" + vol + bucketName +
            "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(KEY).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName(bucketName).setKeyName("*").build();
    for (ACLType acl :ACLType.values()) {
      if (acl != NONE) {
        tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, acl,
            ALLOW);
      }
    }
    return tenantVolumeAccessPolicy;
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
        String user = value.getKerberosPrincipal();

        CachedTenantInfo cachedTenantInfo = tenantCache
            .computeIfAbsent(tenantId, k -> new CachedTenantInfo(tenantId));
        cachedTenantInfo.getTenantUsers().add(
            new ImmutablePair<>(user, accessId));
        userCount++;
      }
      LOG.info("Loaded {} tenants and {} tenant-users from the database.",
          tenantCache.size(), userCount);
    } catch (Exception ex) {
      LOG.error("Error while loading user list. ", ex);
    }
  }

}
