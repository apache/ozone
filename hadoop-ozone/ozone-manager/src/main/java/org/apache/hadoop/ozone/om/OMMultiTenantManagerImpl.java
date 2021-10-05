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
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessAuthorizerDummyPlugin;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessAuthorizerRangerPlugin;
import org.apache.hadoop.ozone.om.multitenant.OzoneTenantGroupPrincipal;
import org.apache.hadoop.ozone.om.multitenant.RangerAccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.multitenantImpl.OzoneTenantAdminGroupPrincipal;
import org.apache.hadoop.ozone.om.multitenantImpl.OzoneTenantUserGroupPrincipal;
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
  private final boolean devSkipRanger;

  private MultiTenantAccessAuthorizer authorizer;
  private final OMMetadataManager omMetadataManager;
  private final OzoneConfiguration conf;
  private final ReentrantReadWriteLock controlPathLock;

  // The following Mappings maintain all of the multi-tenancy states.
  // These mappings needs to have their persistent counterpart in OM tables.
  // Long term, we can bring those tables here as part of multi-tenant-Manager
  // and not mixing up things with the rest of the OM. And thus giving this
  // module a clean separation from the rest of the OM.

  // key : tenantName, value : TenantInfo
  private final Map<String, Tenant> inMemoryTenantNameToTenantInfoMap;

  // This Mapping maintains all policies for all tenants
  //   key = tenantName
  //   value = list of all PolicyNames for this tenant in authorizor-plugin
  // Typical Usage : find out all the bucket/user policies for a tenant.
  private final Map<String, List<String>> inMemoryTenantToPolicyNameListMap;

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
  private final Map<String, List<String>> inMemoryTenantToTenantGroups;

  // Mapping for user-access-id to TenantName
  // Typical usage: given a user-access-id find out which tenant
  private final Map<String, String> inMemoryAccessIDToTenantNameMap;

  // Mapping from user-access-id to all the groups that they belong to.
  // Typical usage: Adding a user or modify user, provide a list of groups
  //          that they would belong to. Note that groupIDs are opaque to OM.
  //          This may make sense just to the authorizer-plugin.
  private final Map<String, List<String>> inMemoryAccessIDToListOfGroupsMap;

  // Used for testing (where there's no ranger instance) to inject a mock
  // authorizer. Use the normal Ranger plugin by default.
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

    devSkipRanger = conf.getBoolean(OZONE_OM_TENANT_DEV_SKIP_RANGER, false);
    start(conf);
  }

  @VisibleForTesting
  public static void setAuthorizerSupplier(
      Supplier<MultiTenantAccessAuthorizer> authSupplier) {
    authorizerSupplier = authSupplier;
  }

  @Override
  public void start(OzoneConfiguration configuration) throws IOException {
    if (devSkipRanger) {
      authorizer = new MultiTenantAccessAuthorizerDummyPlugin();
    } else {
      authorizer = new MultiTenantAccessAuthorizerRangerPlugin();
    }
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

      OzoneTenantUserGroupPrincipal allTenantUsers =
          new OzoneTenantUserGroupPrincipal(tenantID);
      String allTenantUsersGroupID = authorizer.createGroup(allTenantUsers);
      tenant.addTenantAccessGroup(allTenantUsersGroupID);

      OzoneTenantAdminGroupPrincipal allTenantAdmins =
          new OzoneTenantAdminGroupPrincipal(tenantID);
      String allTenantAdminsGroupID = authorizer.createGroup(allTenantAdmins);
      tenant.addTenantAccessGroup(allTenantAdminsGroupID);

      List<String> allTenantGroups = new ArrayList<>();
      allTenantGroups.add(allTenantUsers.toString());
      allTenantGroups.add(allTenantAdmins.toString());
      inMemoryTenantToTenantGroups.put(tenantID, allTenantGroups);

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
    // TODO：Should read from DB. Ditch the in-memory maps.
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
    // TODO: Make sure this is idempotent. This can be called by ALL 3 OMs
    //  in the case of a createTenant checkAcl failure for instance.
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
   * @param principal
   * @param tenantName
   * @param accessID
   * @return Tenant, or null on error
   */
  @Override
  public String assignUserToTenant(BasicUserPrincipal principal,
      String tenantName, String accessID) {
    try {
      controlPathLock.writeLock().lock();
      Tenant tenant = getTenantInfo(tenantName);
      if (tenant == null) {
        LOG.error("Cannot assign user to tenant {} that doesn't exist",
            tenantName);
        return null;
      }
      final OzoneTenantGroupPrincipal groupTenantAllUsers =
          new OzoneTenantUserGroupPrincipal(tenantName);
      String idGroupTenantAllUsers = authorizer.getGroupId(groupTenantAllUsers);
      List<String> userGroupIDs = new ArrayList<>();
      userGroupIDs.add(idGroupTenantAllUsers);

      String userID = authorizer.createUser(principal, userGroupIDs);

      inMemoryAccessIDToTenantNameMap.put(accessID, tenantName);
      inMemoryAccessIDToListOfGroupsMap.put(accessID, userGroupIDs);

      return userID;
    } catch (Exception e) {
      destroyUser(accessID);
      LOG.error(e.getMessage());
      return null;
    } finally {
      controlPathLock.writeLock().unlock();
    }
  }

  @Override
  public void destroyUser(String accessID) {
    try {
      controlPathLock.writeLock().lock();
      String tenantName = getTenantForAccessID(accessID);
      if (tenantName == null) {
        LOG.error("Tenant doesn't exist");
        return;
      }
      // TODO: Determine how to replace this code.
//      final String userID = authorizer.getUserId(userPrincipal);
//      authorizer.deleteUser(userID);

      inMemoryAccessIDToTenantNameMap.remove(accessID);
      inMemoryAccessIDToListOfGroupsMap.remove(accessID);
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
  public List<String> listAllAccessIDs(String tenantID)
      throws IOException {
    return null;
  }

  @Override
  public String getTenantForAccessID(String accessID) {
    return inMemoryAccessIDToTenantNameMap.getOrDefault(accessID, null);
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
}
