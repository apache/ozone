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

import static org.apache.hadoop.ozone.OzoneConsts.DEFAULT_TENANT_GROUP_ALL_ADMINS;
import static org.apache.hadoop.ozone.OzoneConsts.DEFAULT_TENANT_GROUP_ALL_USERS;
import static org.apache.hadoop.ozone.om.multitenant.AccessPolicy.AccessGrantType.ALLOW;
import static org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal.OzonePrincipalType.GROUP_PRINCIPAL;
import static org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal.OzonePrincipalType.USER_PRINCIPAL;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.multitenant.AccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.AccountNameSpace;
import org.apache.hadoop.ozone.om.multitenant.BucketNameSpace;
import org.apache.hadoop.ozone.om.multitenant.CephCompatibleTenantImpl;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessAuthorizer;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessAuthorizerRangerPlugin;
import org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal;
import org.apache.hadoop.ozone.om.multitenant.RangerAccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.multitenantImpl.OzoneMultiTenantPrincipalImpl;
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

  private MultiTenantAccessAuthorizer authorizer;
  private OMMetadataManager omMetadataManager;
  private OzoneConfiguration conf;
  private ReentrantReadWriteLock controlPathLock;

  // The following Mappings maintain all of the multi-tenancy states.
  // These mappings needs to have their persistent counterpart in OM tables.
  // Long term, we can bring those tables here as part of multi-tenant-Manager
  // and not mixing up things with the rest of the OM. And thus giving this
  // module a clean separation from the rest of the OM.

  // key : tenantName, value : TenantInfo
  private Map<String, Tenant> inMemoryTenantNameToTenantInfoMap;

  // This Mapping maintains all policies for all tenants
  //   key = tenantName
  //   value = list of all PolicyNames for this tenant in authorizor-plugin
  // Typical Usage : find out all the bucket/user policies for a tenant.
  private Map<String, List<String>> inMemoryTenantToPolicyNameListMap;

  // This Mapping maintains all groups for all tenants
  //   key = tenantName
  //   value = list of all GroupNames that belong to this tenant
  // There are at least two default groups created for every tenant.
  //    Tenant_XYZ$GroupTenantAllUsers
  //    Tenant_XYZ$GroupTenantAdmins
  // There are also predefined global groups like (TODO)
  //    - AllAuthenticateUsers (TODO)
  //    - AllUsers (TODO)
  // Typical usage : Put together all the users that have access to some
  // resource in the same group. E.g.
  //      1) users in Tenant_XYZ$GroupTenantAllUsers would be able to
  //      access the volume created for Tenant_XYZ.
  //      2) If user creates an access policy for a bucket, all the users
  //      that would have same access to the bucket can go in the same group.
  private Map<String, List<String>> inMemoryTenantToTenantGroups;

  // Mapping for user-access-id to TenantName
  // Typical usage: given a user-access-id find out which tenant
  private Map<String, String> inMemoryAccessIDToTenantNameMap;

  // Mapping from user-access-id to all the groups that they belong to.
  // Typical usage: Adding a user or modify user, provide a list of groups
  //          that they would belong to. Note that groupIDs are opaque to OM.
  //          This may make sense just to the authorizer-plugin.
  private Map<String, List<String>> inMemoryAccessIDToListOfGroupsMap;

  // Used for testing (where there's no ranger instance) to inject a mock
  // authorizer.
  private static Supplier<MultiTenantAccessAuthorizer> authorizerSupplier =
      MultiTenantAccessAuthorizerRangerPlugin::new;


  OMMultiTenantManagerImpl(OMMetadataManager mgr, OzoneConfiguration conf)
      throws IOException {
    this.conf = conf;
    inMemoryTenantNameToTenantInfoMap = new ConcurrentHashMap<>();
    inMemoryTenantToPolicyNameListMap = new ConcurrentHashMap<>();
    inMemoryTenantToTenantGroups = new ConcurrentHashMap<>();
    inMemoryAccessIDToTenantNameMap = new ConcurrentHashMap<>();
    inMemoryAccessIDToListOfGroupsMap = new ConcurrentHashMap<>();

    controlPathLock = new ReentrantReadWriteLock();
    omMetadataManager = mgr;
    start(conf);
  }

  @VisibleForTesting
  public static void setAuthorizerSupplier(
      Supplier<MultiTenantAccessAuthorizer> authSupplier) {
    authorizerSupplier = authSupplier;
  }

  @Override
  public void start(OzoneConfiguration configuration) throws IOException {
    authorizer = authorizerSupplier.get();
    authorizer.init(configuration);
  }

  @Override
  public void stop() throws Exception {

  }

  @Override
  public OMMetadataManager getOmMetadataManager() {
    return omMetadataManager;
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
   *      We can do all of this as part of holding a coarse lock and synchronize
   *      these control path operations.
   *
   * @param tenantID
   * @return Tenant
   * @throws IOException
   */
  @Override
  public Tenant createTenant(String tenantID) throws IOException {

    Tenant tenant = new CephCompatibleTenantImpl(tenantID);
    try {
      controlPathLock.writeLock().lock();
      inMemoryTenantNameToTenantInfoMap.put(tenantID, tenant);

      // TODO : for now just create state in the Ranger. OM state is already
      //  created in ValidateAndUpdateCache for the ratis transaction.

      // TODO : Make it an idempotent operation. If any ranger state creation
      //  fails because it already exists, Ignore it.

      OzoneMultiTenantPrincipal groupTenantAllUsers = getOzonePrincipal(
          tenantID, DEFAULT_TENANT_GROUP_ALL_USERS, GROUP_PRINCIPAL);
      String groupTenantAllUsersID =
          authorizer.createGroup(groupTenantAllUsers);
      tenant.addTenantAccessGroup(groupTenantAllUsersID);

      OzoneMultiTenantPrincipal groupTenantAllAdmins = getOzonePrincipal(
          tenantID, DEFAULT_TENANT_GROUP_ALL_ADMINS, GROUP_PRINCIPAL);
      String groupTenantAllAdminsID =
          authorizer.createGroup(groupTenantAllAdmins);
      tenant.addTenantAccessGroup(groupTenantAllAdminsID);

      List<String> allTenantGroups = new ArrayList<>();
      allTenantGroups.add(groupTenantAllAdmins.getFullMultiTenantPrincipalID());
      allTenantGroups.add(groupTenantAllUsers.getFullMultiTenantPrincipalID());
      inMemoryTenantToTenantGroups.put(tenantID, allTenantGroups);

      BucketNameSpace bucketNameSpace = tenant.getTenantBucketNameSpace();
      for (OzoneObj volume : bucketNameSpace.getBucketNameSpaceObjects()) {
        String volumeName = volume.getVolumeName();
        // Allow Volume List access
        AccessPolicy tenantVolumeAccessPolicy = createVolumeAccessPolicy(
            volumeName, tenantID, groupTenantAllUsers.getUserID());
        tenantVolumeAccessPolicy.setPolicyID(
            authorizer.createAccessPolicy(tenantVolumeAccessPolicy));
        tenant.addTenantAccessPolicy(tenantVolumeAccessPolicy);

        // Allow Bucket Create within Volume
        AccessPolicy tenantBucketCreatePolicy = allowCreateBucketPolicy(
            volumeName, tenantID, groupTenantAllUsers.getUserID());
        tenantBucketCreatePolicy.setPolicyID(
            authorizer.createAccessPolicy(tenantBucketCreatePolicy));
        tenant.addTenantAccessPolicy(tenantBucketCreatePolicy);
      }

      inMemoryTenantToPolicyNameListMap.put(tenantID,
          tenant.getTenantAccessPolicies().stream().map(e->e.getPolicyName())
              .collect(Collectors.toList()));
    } catch (Exception e) {
      try {
        destroyTenant(tenant);
      } catch (Exception exception) {
        // Best effort cleanup.
      }
      controlPathLock.writeLock().unlock();
      throw new IOException(e.getMessage());
    }
    controlPathLock.writeLock().unlock();
    return tenant;
  }

  @Override
  public Tenant getTenantInfo(String tenantID) throws IOException {
    if (!inMemoryTenantNameToTenantInfoMap.containsKey(tenantID)) {
      return null;
    }
    for (Map.Entry<String, Tenant> entry :
        inMemoryTenantNameToTenantInfoMap.entrySet()) {
      if (entry.getKey().equals(tenantID)) {
        return entry.getValue();
      }
    }
    throw new IOException("All Tenants Map is corrupt");
  }

  @Override
  public void deactivateTenant(String tenantID) throws IOException {

  }

  @Override
  public void destroyTenant(Tenant tenant) throws Exception {
    try {
      controlPathLock.writeLock().lock();
      for (AccessPolicy policy : tenant.getTenantAccessPolicies()) {
        authorizer.deletePolicybyId(policy.getPolicyID());
      }
      for (String groupID : tenant.getTenantGroups()) {
        authorizer.deleteGroup(groupID);
      }

      inMemoryTenantNameToTenantInfoMap.remove(tenant.getTenantId());
      inMemoryTenantToPolicyNameListMap.remove(tenant.getTenantId());
      inMemoryTenantToTenantGroups.remove(tenant.getTenantId());
    } catch (Exception e) {
      controlPathLock.writeLock().unlock();
      throw e;
    }
    controlPathLock.writeLock().unlock();
  }

  /**
   *  Algorithm
   *  OM State :
   *    - Validation (Part of Ratis Request)
   *    - create user in OMDB {Part of RATIS request}
   *    - Persistence to OM DB {Part of RATIS request}
   *  Authorizer-plugin(Ranger) State :
   *    - create User in Ranger DB
   *    - For every user created
   *        Add them to # GroupTenantAllUsers
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
   * @param tenantName
   * @param userName
   * @return Tenant, or null on error
   */
  @Override
  public String assignUserToTenant(String tenantName, String userName) {
    try {
      controlPathLock.writeLock().lock();
      Tenant tenant = getTenantInfo(tenantName);
      if (tenant == null) {
        LOG.error("Tenant doesn't exist");
        return null;
      }
      final OzoneMultiTenantPrincipal userPrincipal =
          new OzoneMultiTenantPrincipalImpl(new BasicUserPrincipal(tenantName),
              new BasicUserPrincipal(userName), USER_PRINCIPAL);

      final OzoneMultiTenantPrincipal groupTenantAllUsers = getOzonePrincipal(
          tenantName, DEFAULT_TENANT_GROUP_ALL_USERS, GROUP_PRINCIPAL);
      String idGroupTenantAllUsers = authorizer.getGroupId(groupTenantAllUsers);
      List<String> userGroupIDs = new ArrayList<>();
      userGroupIDs.add(idGroupTenantAllUsers);

      String userID = authorizer.createUser(userPrincipal, userGroupIDs);

      inMemoryAccessIDToTenantNameMap.put(
          userPrincipal.getFullMultiTenantPrincipalID(), tenantName);
      inMemoryAccessIDToListOfGroupsMap.put(
          userPrincipal.getFullMultiTenantPrincipalID(), userGroupIDs);

      return userID;
    } catch (Exception e) {
      destroyUser(tenantName, userName);
      LOG.error(e.getMessage());
      return null;
    } finally {
      controlPathLock.writeLock().unlock();
    }
  }

  @Override
  public void destroyUser(String tenantName, String userName) {
    try {
      controlPathLock.writeLock().lock();
      final Tenant tenant = getTenantInfo(tenantName);
      if (tenant == null) {
        LOG.error("Tenant doesn't exist");
        return;
      }
      final OzoneMultiTenantPrincipal userPrincipal =
          new OzoneMultiTenantPrincipalImpl(new BasicUserPrincipal(tenantName),
              new BasicUserPrincipal(userName), USER_PRINCIPAL);
      final String userID = authorizer.getUserId(userPrincipal);
      authorizer.deleteUser(userID);

      inMemoryAccessIDToTenantNameMap.remove(
          userPrincipal.getFullMultiTenantPrincipalID());
      inMemoryAccessIDToListOfGroupsMap.remove(
          userPrincipal.getFullMultiTenantPrincipalID());
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
  public String getUserSecret(OzoneMultiTenantPrincipal user)
      throws IOException {
    return "";
  }

  @Override
  public void modifyUser(OzoneMultiTenantPrincipal user,
                         List<String> groupsAdded,
                         List<String> groupsRemoved) throws IOException {

  }

  @Override
  public void deactivateUser(OzoneMultiTenantPrincipal user)
      throws IOException {

  }

  @Override
  public List<OzoneMultiTenantPrincipal> listAllUsers(String tenantID)
      throws IOException {
    return null;
  }

  @Override
  public Tenant getTenantInfoForAccessID(String accessID) {
    LOG.info("--- looking up tenant for access ID {}", accessID);
//    if (inMemoryTenantNameToTenantInfoMap.containsKey(accessID)) {
//      return inMemoryTenantNameToTenantInfoMap.get(accessID);
//    } else {
//      return null;
//    }
    return inMemoryTenantNameToTenantInfoMap.getOrDefault(accessID, null);
  }

  @Override
  public void assignTenantAdminRole(OzoneMultiTenantPrincipal user)
      throws IOException {

  }

  @Override
  public void revokeTenantAdmin(OzoneMultiTenantPrincipal user)
      throws IOException {

  }

  @Override
  public List<OzoneMultiTenantPrincipal> listAllTenantAdmin(String tenantID)
      throws IOException {
    return null;
  }

  @Override
  public void grantAccess(OzoneMultiTenantPrincipal user,
      BucketNameSpace bucketNameSpace) throws IOException {

  }

  @Override
  public void grantBucketAccess(OzoneMultiTenantPrincipal user,
      BucketNameSpace bucketNameSpace, String bucketName) throws IOException {

  }

  @Override
  public void revokeAccess(OzoneMultiTenantPrincipal user,
      BucketNameSpace bucketNameSpace) throws IOException {

  }

  @Override
  public void grantAccess(OzoneMultiTenantPrincipal user,
      AccountNameSpace accountNameSpace) throws IOException {

  }

  @Override
  public void revokeAccess(OzoneMultiTenantPrincipal user,
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

  private OzoneMultiTenantPrincipal getOzonePrincipal(String tenant, String id,
      OzoneMultiTenantPrincipal.OzonePrincipalType type) {
    OzoneMultiTenantPrincipal principal = new OzoneMultiTenantPrincipalImpl(
        new BasicUserPrincipal(tenant),
        new BasicUserPrincipal(id), type);
    return principal;
  }

  private AccessPolicy createVolumeAccessPolicy(String vol, String tenant,
      String group) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        tenant + group + "VolumeAccess" + vol + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(VOLUME).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName("").setKeyName("").build();
    OzoneMultiTenantPrincipal principal = getOzonePrincipal(tenant, group,
        GROUP_PRINCIPAL);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, READ, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, LIST, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal,
        READ_ACL, ALLOW);
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowCreateBucketPolicy(String vol, String tenant,
      String group) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        tenant + group + "AllowBucketCreate" + vol + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(BUCKET).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName("*").setKeyName("").build();
    OzoneMultiTenantPrincipal principal = getOzonePrincipal(tenant, group,
        GROUP_PRINCIPAL);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, CREATE, ALLOW);
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowAccessBucketPolicy(String vol, String tenant,
      String group, String bucketName) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        tenant + group + "AllowBucketAccess" + vol + bucketName + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(BUCKET).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName(bucketName).setKeyName("*").build();
    OzoneMultiTenantPrincipal principal = getOzonePrincipal(tenant, group,
        GROUP_PRINCIPAL);
    for (ACLType acl : ACLType.values()) {
      if (acl != NONE) {
        tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, acl,
            ALLOW);
      }
    }
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowAccessKeyPolicy(String vol, String tenant,
      String group, String bucketName) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        tenant + group + "AllowBucketKeyAccess" + vol + bucketName + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(KEY).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName(bucketName).setKeyName("*").build();
    OzoneMultiTenantPrincipal principal = getOzonePrincipal(tenant, group,
        GROUP_PRINCIPAL);
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
}
