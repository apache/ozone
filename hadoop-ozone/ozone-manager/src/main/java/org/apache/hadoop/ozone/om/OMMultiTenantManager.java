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

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
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
   * @param tenantId
   * @param accessId
   * @return Unique UserID.
   * @throws IOException if there is any error condition detected.
   */
  String assignUserToTenant(BasicUserPrincipal principal, String tenantId,
                            String accessId) throws IOException;

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
  void removeUserAccessIdFromCache(String accessId, String userPrincipal,
                                   String tenantId);

  /**
   * Given an accessId, return kerberos user name for the tenant user.
   */
  String getUserNameGivenAccessId(String accessId);

  /**
   * Get the default Access ID string given tenant name and user name.
   * @param tenantId tenant name
   * @param userPrincipal user name
   * @return access ID in the form of tenantName$username
   */
  String getDefaultAccessId(String tenantId, String userPrincipal);

  /**
   * Check if a user is a tenant Admin.
   * @param user user name.
   * @param tenantId tenant name.
   * @return
   */
  boolean isTenantAdmin(String user, String tenantId);

  /**
   * List all the user & accessIDs of all users that belong to this Tenant.
   * Note this read is unprotected. See OzoneManager#listUserInTenant
   * @param tenantID
   * @return List of users
   */
  TenantUserList listUsersInTenant(String tenantID, String prefix)
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
}
