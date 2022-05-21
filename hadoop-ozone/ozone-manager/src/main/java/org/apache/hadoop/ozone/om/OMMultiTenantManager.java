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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.multitenant.AccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.OMRangerBGSyncService;
import org.apache.hadoop.ozone.om.multitenant.OzoneTenantRolePrincipal;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.http.auth.BasicUserPrincipal;
import org.slf4j.Logger;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl.OZONE_OM_TENANT_DEV_SKIP_RANGER;

/**
 * OM MultiTenant manager interface.
 */
public interface OMMultiTenantManager {
  /* TODO: Outdated
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
   */

  /**
   * Start background thread(s) in the multi-tenant manager.
   */
  void start() throws IOException;

  /**
   * Stop background thread(s) in the multi-tenant manager.
   */
  void stop() throws IOException;

  /**
   * Returns the instance of OMRangerBGSyncService.
   */
  @VisibleForTesting
  OMRangerBGSyncService getOMRangerBGSyncService();

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
   * @param userRoleName
   * @param adminRoleName
   * @return Tenant interface.
   */
  Tenant createTenantAccessInAuthorizer(String tenantID, String userRoleName,
      String adminRoleName) throws IOException;

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
   * @param accessId
   * @throws IOException
   */
  void revokeUserAccessId(String accessId) throws IOException;

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
  static String getDefaultAccessId(String tenantId, String userPrincipal) {
    return tenantId + OzoneConsts.TENANT_ID_USERNAME_DELIMITER + userPrincipal;
  }

  /**
   * Returns true if user is the tenant's admin or Ozone admin, false otherwise.
   * @param callerUgi caller's UserGroupInformation
   * @param tenantId tenant name
   * @param delegated if set to true, checks if the user is a delegated tenant
   *                  admin; if set to false, checks if the user is a tenant
   *                  admin, delegated or not
   */
  boolean isTenantAdmin(UserGroupInformation callerUgi, String tenantId,
      boolean delegated);

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
   * Get default user role name given tenant name.
   * @param tenantId tenant name
   * @return user role name. e.g. tenant1-UserRole
   */
  static String getDefaultUserRoleName(String tenantId) {
    return tenantId + OzoneConsts.DEFAULT_TENANT_ROLE_USER_SUFFIX;
  }

  /**
   * Get default admin role name given tenant name.
   * @param tenantId tenant name
   * @return admin role name. e.g. tenant1-AdminRole
   */
  static String getDefaultAdminRoleName(String tenantId) {
    return tenantId + OzoneConsts.DEFAULT_TENANT_ROLE_ADMIN_SUFFIX;
  }

  /**
   * Get default bucket namespace (volume) policy name given tenant name.
   * @param tenantId tenant name
   * @return bucket namespace (volume) policy name. e.g. tenant1-VolumeAccess
   */
  static String getDefaultBucketNamespacePolicyName(String tenantId) {
    return tenantId + OzoneConsts.DEFAULT_TENANT_BUCKET_NAMESPACE_POLICY_SUFFIX;
  }

  /**
   * Get default bucket policy name given tenant name.
   * @param tenantId tenant name
   * @return bucket policy name. e.g. tenant1-BucketAccess
   */
  static String getDefaultBucketPolicyName(String tenantId) {
    return tenantId + OzoneConsts.DEFAULT_TENANT_BUCKET_POLICY_SUFFIX;
  }

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
   * Passes check only when caller is an Ozone (cluster) admin, throws
   * OMException otherwise.
   * @throws OMException PERMISSION_DENIED
   */
  void checkAdmin() throws OMException;

  /**
   * Check if caller is a tenant admin of the specified tenant.
   * Ozone admins will always pass this check.
   * Throws PERMISSION_DENIED if the check failed.
   * @param tenantId tenant name
   * @param delegated if set to true, only delegated tenant admins can pass this
   *                  check; if false, both delegated and non-delegated tenant
   *                  admins will pass this check.
   * @throws OMException PERMISSION_DENIED
   */
  void checkTenantAdmin(String tenantId, boolean delegated) throws OMException;

  /**
   * Check if the tenantId exists in the table, throws TENANT_NOT_FOUND if not.
   */
  void checkTenantExistence(String tenantId) throws OMException;

  /**
   * Retrieve volume name of the tenant.
   *
   * Throws OMException TENANT_NOT_FOUND if tenantId doesn't exist.
   */
  String getTenantVolumeName(String tenantId) throws IOException;

  /**
   * Retrieve user role name of the given tenant.
   * @param tenantId tenant name
   * @return tenant user role name
   */
  String getTenantUserRoleName(String tenantId) throws IOException;

  /**
   * Retrieve admin role name of the given tenant.
   * @param tenantId tenant name
   * @return tenant user role name
   */
  String getTenantAdminRoleName(String tenantId) throws IOException;

  boolean isUserAccessIdPrincipalOrTenantAdmin(String accessId,
      UserGroupInformation ugi) throws IOException;

  /**
   * Returns true if the tenant doesn't have any accessIds assigned to it
   * Returns false otherwise.
   *
   * @param tenantId
   * @throws IOException
   */
  boolean isTenantEmpty(String tenantId) throws IOException;

  /**
   * Returns true if Multi-Tenancy can be successfully enabled given the OM
   * instance and conf; returns false if ozone.om.multitenancy.enabled = false
   *
   * Config validation will be performed on conf if the intent to enable
   * Multi-Tenancy is specified (i.e. ozone.om.multitenancy.enabled = true),
   * if the validation failed, an exception will be thrown to prevent OM from
   * starting up.
   */
  static boolean checkAndEnableMultiTenancy(
      OzoneManager ozoneManager, OzoneConfiguration conf) {

    // Borrow the logger from OM instance
    final Logger logger = OzoneManager.LOG;

    boolean isS3MultiTenancyEnabled = conf.getBoolean(
        OZONE_OM_MULTITENANCY_ENABLED, OZONE_OM_MULTITENANCY_ENABLED_DEFAULT);

    final boolean devSkipMTCheck = conf.getBoolean(
        OZONE_OM_TENANT_DEV_SKIP_RANGER, false);

    // If ozone.om.multitenancy.enabled = false, skip the validation
    // Or if dev skip check flag is set, skip the validation (used in UT)
    if (!isS3MultiTenancyEnabled || devSkipMTCheck) {
      return isS3MultiTenancyEnabled;
    }

    // Validate configs required to enable S3 multi-tenancy
    if (!ozoneManager.isSecurityEnabled()) {
      isS3MultiTenancyEnabled = false;
      logger.error("Ozone security is required to enable S3 Multi-Tenancy");
    } else if (!SecurityUtil.getAuthenticationMethod(conf).equals(
        AuthenticationMethod.KERBEROS)) {
      isS3MultiTenancyEnabled = false;
      logger.error("Kerberos authentication is required to enable S3 "
          + "Multi-Tenancy");
    }

    // TODO: Validate accessAuthorizer later. We can't do that for now:
    //  1. Tenant acceptance test env (ozonesecure) uses OzoneNativeAuthorizer
    //  2. RangerOzoneAuthorizer is external class

    final String rangerAddress = conf.get(OZONE_RANGER_HTTPS_ADDRESS_KEY);
    if (StringUtils.isBlank(rangerAddress)) {
      isS3MultiTenancyEnabled = false;
      logger.error("{} is required to enable S3 Multi-Tenancy but not set",
          OZONE_RANGER_HTTPS_ADDRESS_KEY);
    }

    // TODO: Do not check Ranger user/passwd if Ranger Java client is enabled
    final String rangerUser = conf.get(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER);
    if (StringUtils.isBlank(rangerUser)) {
      isS3MultiTenancyEnabled = false;
      logger.error("{} is required to enable S3 Multi-Tenancy but not set",
          OZONE_OM_RANGER_HTTPS_ADMIN_API_USER);
    }
    final String rangerPw = conf.get(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD);
    if (StringUtils.isBlank(rangerPw)) {
      isS3MultiTenancyEnabled = false;
      logger.error("{} is required to enable S3 Multi-Tenancy but not set",
          OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD);
    }

    if (!isS3MultiTenancyEnabled) {
      throw new RuntimeException("Failed to meet one or more requirements to "
          + "enable S3 Multi-Tenancy");
    }

    return true;
  }

  /**
   * Returns default VolumeAccess policy given tenant and role names.
   */
  AccessPolicy newDefaultVolumeAccessPolicy(String tenantId,
      OzoneTenantRolePrincipal userRole, OzoneTenantRolePrincipal adminRole)
      throws IOException;

  /**
   * Returns default BucketAccess policy given tenant and user role name.
   */
  AccessPolicy newDefaultBucketAccessPolicy(String tenantId,
      OzoneTenantRolePrincipal userRole) throws IOException;
}
