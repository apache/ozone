/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.multitenant.AccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.AccountNameSpace;
import org.apache.hadoop.ozone.om.multitenant.BucketNameSpace;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.http.auth.BasicUserPrincipal;

/**
 * OM MultiTenant manager interface.
 */
public interface OMMultiTenantManager {
  /*
   * Init multi-tenant manager. Performs initialization e.g.
   *  - Initialize Multi-Tenant-Gatekeeper-Plugin
   *  - Validate Multi-Tenant Bucket-NameSpaces
   *  - Validate Multi-Tenant Account-NameSpaces
   *  - Validating various OM (Multi-Tenant state)tables and corresponding
   *    state in IMultiTenantGateKeeperPlugin (Ranger/Native/AnyOtherPlugIn).
   *  - Setup SuperUsers for Multi-Tenant environment from Ozone-Conf
   *  - Periodic BackGround thread to keep MultiTenant-State consistent e.g.
   *       . superusers  <-in-sync-> OzoneConf,
   *       . OM-DB state <-in-sync-> IMultiTenantGateKeeperPluginState
   *       . OM DB state is always the source of truth.
   *
   * @throws IOException
   */
//  void start() throws IOException;
//
//  /**
//   * Stop multi-tenant manager.
//   */
//  void stop() throws Exception;

  /**
   * Returns the corresponding OzoneManager instance.
   *
   * @return OMMetadataManager
   */
  OMMetadataManager getOmMetadataManager();

  /**
   * Given a TenantID String, Create and return Tenant Interface.
   *
   * @param tenantID
   * @return Tenant interface.
   */
  Tenant createTenantAccessInAuthorizer(String tenantID) throws IOException;


  /**
   * Given a TenantID String, Return Tenant Interface. If the Tenant doesn't
   * exist in the system already, throw Exception.
   *
   * @param tenantID
   * @return Tenant interface.
   * @throws IOException
   */
  Tenant getTenantInfo(String tenantID) throws IOException;

  /**
   * Given a TenantID String, deactivate the Tenant. If the Tenant has active
   * users and volumes, tenant gets dectivated. This means
   * * No new write/modify operations allowed under that tenant.
   * * No new users can be added.
   * * All the users of that tenant will not be able to create new
   *    bucket/keys and carry out any new type of write/update opertations in
   *    Tenant bucketNamespace or accountNamespace.
   * * If Tenant has users they will be able to do read and delete operations.
   * * If the Tenant doesn't have any user or buckets, Tenant will be removed
   *    from the system.
   *
   * * If the Tenant doesn't exist in the system already, throw Exception.
   *
   * @param tenantID
   * @return Tenant interface.
   * @throws IOException
   */
  void deactivateTenant(String tenantID) throws IOException;

  /**
   * Given a TenantID, destroys all state associated with that tenant.
   * This is different from deactivateTenant() above.
   * @param tenant
   * @return
   * @throws IOException
   */
  void removeTenantAccessFromAuthorizer(Tenant tenant) throws Exception;


  /**
   * Creates a new user that exists for S3 API access to Ozone.
   * @param principal
   * @param tenantName
   * @param accessID
   * @return Unique UserID.
   * @throws IOException if there is any error condition detected.
   */
  String assignUserToTenant(BasicUserPrincipal principal, String tenantName,
                            String accessID) throws IOException;

  /**
   * Revoke user accessId.
   * @param accessID
   * @throws IOException
   */
  void revokeUserAccessId(String accessID) throws IOException;

  /**
   * A placeholder method to remove a failed-to-assign accessId from
   * tenantCache.
   * Triggered in OMAssignUserToTenantRequest#handleRequestFailure.
   * Most likely becomes unnecessary if we move OMMTM call to the end of the
   * request (current it runs in preExecute).
   * TODO: Remove this if unneeded when Ranger thread patch lands.
   */
  void removeUserAccessIdFromCache(String accessID, String userPrincipal,
                                   String tenantName);

  /**
   * Given an accessId, return kerberos user name for the tenant user.
   */
  String getUserNameGivenAccessId(String accessId);

  /**
   * Given a user, return their S3-Secret Key.
   * @param accessID
   * @return S3 secret Key
   */
  String getUserSecret(String accessID) throws IOException;

  /**
   * Modify the groups that a user belongs to.
   * @param accessID
   * @param groupsAdded
   * @param groupsRemoved
   * @throws IOException
   */
  void modifyUser(String accessID, List<String> groupsAdded,
                  List<String> groupsRemoved) throws IOException;

  /**
   * Given a user, deactivate them. We will need a recon command/job to cleanup
   * any data owned by this user (ReconMultiTenantManager).
   * @param accessID
   */
  void deactivateUser(String accessID) throws IOException;

  /**
   * Check if a user is a tenant Admin.
   * @param user user name.
   * @param tenantName tenant name.
   * @return
   */
  boolean isTenantAdmin(String user, String tenantName);

  /**
   * List all the user & accessIDs of all users that belong to this Tenant.
   * Note this read is unprotected. See OzoneManager#listUserInTenant
   * @param tenantID
   * @return List of users
   */
  TenantUserList listUsersInTenant(String tenantID, String prefix)
      throws IOException;

  /**
   * List all the access IDs of all users that belong to this Tenant.
   * @param tenantID
   * @return List of users
   */
  List<String> listAllAccessIDs(String tenantID)
      throws IOException;

  /**
   * Given an access ID return its corresponding tenant.
   * @param accessID
   * @return String tenant name
   */
  Optional<String> getTenantForAccessID(String accessID) throws IOException;

  /**
   * Given a user, make him an admin of the corresponding Tenant.
   * @param accessID
   * @param delegated
   */
  void assignTenantAdmin(String accessID, boolean delegated) throws IOException;

  /**
   * Given a user, remove him as admin of the corresponding Tenant.
   */
  void revokeTenantAdmin(String accessID) throws IOException;

  /**
   * List all the Admin users that belong to this Tenant.
   * @param tenantID
   * @return List of users
   */
  List<String> listAllTenantAdmin(String tenantID)
      throws IOException;

  /**
   * grant given user access to the given BucketNameSpace.
   * @param accessID
   * @param bucketNameSpace
   */
  void grantAccess(String accessID,
                   BucketNameSpace bucketNameSpace) throws IOException;

  /**
   * grant given user access to the given Bucket.
   * @param accessID
   * @param bucketNameSpace
   */
  void grantBucketAccess(String accessID,
                   BucketNameSpace bucketNameSpace, String bucketName)
      throws IOException;

  /**
   * revoke user access from the given BucketNameSpace.
   * @param accessID
   * @param bucketNameSpace
   */
  void revokeAccess(String accessID,
                    BucketNameSpace bucketNameSpace) throws IOException;

  /**
   * grant given user access to the given AccountNameSpace.
   * @param accessID
   * @param accountNameSpace
   */
  void grantAccess(String accessID,
                   AccountNameSpace accountNameSpace) throws IOException;

  /**
   * revoke user access from the given AccountNameSpace.
   * @param accessID
   * @param accountNameSpace
   */
  void revokeAccess(String accessID,
                    AccountNameSpace accountNameSpace) throws IOException;

  /**
   * Create given policy for the tenant.
   * @param tenant
   * @param policy
   * @return ID of the policy
   */
  String createTenantDefaultPolicy(Tenant tenant, AccessPolicy policy)
      throws IOException;

  /**
   * Returns default Access policies for a Tenant. Default access policies
   * are system defined and can not be changed by anyone.
   * @param tenant
   * @return list of Default Access policies for a Tenant
   */
  List<Pair<String, AccessPolicy>> listDefaultTenantPolicies(Tenant tenant)
      throws IOException;

  /**
   * Returns All Access policies for a Tenant. In future we may support
   * bucket-policies/user-policies to provide cross-tenant accesses.
   * @param tenant
   * @return list of Default Access policies for a Tenant
   */
  List<Pair<String, AccessPolicy>> listAllTenantPolicies(Tenant tenant)
      throws IOException;

  /**
   * Update given policy identified by policyID for the tenant.
   * @param tenant
   * @param policyID
   * @param policy
   * @return ID of the policy
   */
  void updateTenantPolicy(Tenant tenant, String policyID,
                          AccessPolicy policy) throws IOException;

}
