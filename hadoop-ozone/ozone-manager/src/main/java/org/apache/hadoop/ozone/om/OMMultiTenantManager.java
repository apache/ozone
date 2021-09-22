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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.multitenant.AccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.AccountNameSpace;
import org.apache.hadoop.ozone.om.multitenant.BucketNameSpace;
import org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal;
import org.apache.hadoop.ozone.om.multitenant.Tenant;

/**
 * OM MultiTenant manager interface.
 */
public interface OMMultiTenantManager {
  /**
   * Start multi-tenant manager. Performs initialization e.g.
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
   * @param configuration
   * @throws IOException
   */
  void start(OzoneConfiguration configuration) throws IOException;

  /**
   * Stop multi-tenant manager.
   */
  void stop() throws Exception;

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
  Tenant createTenant(String tenantID) throws IOException;

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
  void destroyTenant(Tenant tenant) throws Exception;


  /**
   * Creates a new user that exists for S3 API access to Ozone.
   * TODO: FIX the description.
   * @param tenantName
   * @param userName
   * @return Unique UserID.
   * @throws IOException if there is any error condition detected.
   */
  String assignUserToTenant(String tenantName, String userName);

  /**
   * Given a user, destroys all state associated with that user.
   * This is different from deactivateUser().
   * @param tenantName
   @ @param userName
   * @return
   * @throws IOException
   */
  void destroyUser(String tenantName, String userName);


  /**
   * Given an accessId, return kerberos user name for the tenant user.
   */
  String getUserNameGivenAccessId(String accessId);

  /**
   * Given a user, return their S3-Secret Key.
   * @param user
   * @return S3 secret Key
   */
  String getUserSecret(OzoneMultiTenantPrincipal user) throws IOException;

  /**
   * Generates a new S3 secret key for the user. They can obtain the newly
   * generated secret with getUserSecret();
   * @param user
   */
  void modifyUser(OzoneMultiTenantPrincipal user, List<String> groupsAdded,
                  List<String> groupsRemoved) throws IOException;

  /**
   * Given a user, deactivate them. We will need a recon command/job to cleanup
   * any data owned by this user (ReconMultiTenantManager).
   * @param user
   */
  void deactivateUser(OzoneMultiTenantPrincipal user) throws IOException;

  /**
   * List all the users that belong to this Tenant.
   * @param tenantID
   * @return List of users
   */
  List<OzoneMultiTenantPrincipal> listAllUsers(String tenantID)
      throws IOException;

  /**
   * Given an access ID return its corresponding tenant.
   * @param accessID
   * @return Tenant
   */
  Tenant getTenantInfoForAccessID(String accessID) throws IOException;

  /**
   * Given a user, make him an admin of the corresponding Tenant.
   * @param user
   */
  void assignTenantAdminRole(OzoneMultiTenantPrincipal user) throws IOException;

  /**
   * Given a user, make him an admin of the corresponding Tenant.
   */
  void revokeTenantAdmin(OzoneMultiTenantPrincipal user) throws IOException;

  /**
   * List all the Admin users that belong to this Tenant.
   * @param tenantID
   * @return List of users
   */
  List<OzoneMultiTenantPrincipal> listAllTenantAdmin(String tenantID)
      throws IOException;

  /**
   * grant given user access to the given BucketNameSpace.
   * @param user
   * @param bucketNameSpace
   */
  void grantAccess(OzoneMultiTenantPrincipal user,
                   BucketNameSpace bucketNameSpace) throws IOException;

  /**
   * grant given user access to the given BucketNameSpace.
   * @param user
   * @param bucketNameSpace
   */
  void grantBucketAccess(OzoneMultiTenantPrincipal user,
                   BucketNameSpace bucketNameSpace, String bucketName)
      throws IOException;

  /**
   * revoke user access from the given BucketNameSpace.
   * @param user
   * @param bucketNameSpace
   */
  void revokeAccess(OzoneMultiTenantPrincipal user,
                    BucketNameSpace bucketNameSpace) throws IOException;

  /**
   * grant given user access to the given AccountNameSpace.
   * @param user
   * @param accountNameSpace
   */
  void grantAccess(OzoneMultiTenantPrincipal user,
                   AccountNameSpace accountNameSpace) throws IOException;

  /**
   * revoke user access from the given AccountNameSpace.
   * @param user
   * @param accountNameSpace
   */
  void revokeAccess(OzoneMultiTenantPrincipal user,
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
