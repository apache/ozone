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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_TENANT_RANGER_POLICY_LABEL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_SERVICE;
import static org.apache.hadoop.ozone.om.OMMultiTenantManagerImpl.OZONE_OM_TENANT_DEV_SKIP_RANGER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ_ACL;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.multitenant.AuthorizerLock;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Acl;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Policy;
import org.apache.hadoop.ozone.om.multitenant.OzoneOwnerPrincipal;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.service.OMRangerBGSyncService;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.slf4j.Logger;

/**
 * OM MultiTenant manager interface.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface OMMultiTenantManager {

  String OZONE_TENANT_RANGER_POLICY_DESCRIPTION =
      "Created by Ozone. WARNING: "
          + "Changes will be lost when this tenant is deleted.";

  String OZONE_TENANT_RANGER_ROLE_DESCRIPTION =
      "Managed by Ozone. WARNING: "
          + "Changes will be overridden. "
          + "Use Ozone tenant CLI to manage users in this tenant role instead.";

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
  OMRangerBGSyncService getOMRangerBGSyncService();

  /**
   * Returns the corresponding OzoneManager instance.
   *
   * @return OMMetadataManager
   */
  OMMetadataManager getOmMetadataManager();

  TenantOp getAuthorizerOp();

  TenantOp getCacheOp();

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
   * List all the user and accessIDs of all users that belong to this Tenant.
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

  /**
   * Get Tenant object of given tenant name from OM DB.
   * @param tenantId tenant name
   * @return Tenant
   * @throws IOException
   */
  Tenant getTenantFromDBById(String tenantId) throws IOException;

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

    final String rangerService = conf.get(OZONE_RANGER_SERVICE);
    if (StringUtils.isBlank(rangerService)) {
      isS3MultiTenancyEnabled = false;
      logger.error("{} is required to enable S3 Multi-Tenancy but not set",
          OZONE_RANGER_SERVICE);
    }

    String fallbackUsername = conf.get(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER);
    String fallbackPassword = conf.get(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD);

    if (fallbackUsername != null && fallbackPassword != null) {
      logger.warn("Detected clear text username and password override configs. "
          + "These will be used to authenticate to Ranger Admin Server instead "
          + "of using the recommended Kerberos principal and keytab "
          + "authentication method. "
          + "This is NOT recommended on a production cluster.");
    } else {
      // Check Kerberos principal and keytab file path configs if not both
      // clear text username and password overrides are set.
      final String omKerbPrinc = conf.get(OZONE_OM_KERBEROS_PRINCIPAL_KEY);
      // Note: ozone.om.kerberos.keytab.file and ozone.om.kerberos.principal
      //  are not empty by default. The default values may or may not be valid.
      if (StringUtils.isBlank(omKerbPrinc)) {
        isS3MultiTenancyEnabled = false;
        logger.error("{} is required to enable S3 Multi-Tenancy but not set",
            OZONE_OM_KERBEROS_PRINCIPAL_KEY);
      }
      final String rangerPw = conf.get(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY);
      if (StringUtils.isBlank(rangerPw)) {
        isS3MultiTenancyEnabled = false;
        logger.error("{} is required to enable S3 Multi-Tenancy but not set",
            OZONE_OM_KERBEROS_KEYTAB_FILE_KEY);
      }
      if (!(new File(rangerPw).isFile())) {
        logger.error("{} = '{}' file path doesn't exist or is not a file",
            OZONE_OM_KERBEROS_KEYTAB_FILE_KEY, rangerPw);
      }
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
  static Policy getDefaultVolumeAccessPolicy(
      String tenantId, String volumeName,
      String userRoleName, String adminRoleName)
      throws IOException {

    final String volumePolicyName = OMMultiTenantManager
        .getDefaultBucketNamespacePolicyName(tenantId);

    return new Policy.Builder()
        .setName(volumePolicyName)
        .addVolume(volumeName)
        .setDescription(OZONE_TENANT_RANGER_POLICY_DESCRIPTION)
        .addLabel(OZONE_TENANT_RANGER_POLICY_LABEL)
        .addRoleAcl(userRoleName, Arrays.asList(
            Acl.allow(READ), Acl.allow(LIST), Acl.allow(READ_ACL)))
        .addRoleAcl(adminRoleName, Collections.singletonList(Acl.allow(ALL)))
        .build();
  }

  /**
   * Returns default BucketAccess policy given tenant and user role name.
   */
  static Policy getDefaultBucketAccessPolicy(
      String tenantId, String volumeName,
      String userRoleName) throws IOException {
    final String bucketPolicyName = OMMultiTenantManager
        .getDefaultBucketPolicyName(tenantId);

    return new Policy.Builder()
        .setName(bucketPolicyName)
        .addVolume(volumeName)
        .addBucket("*")
        .setDescription(OZONE_TENANT_RANGER_POLICY_DESCRIPTION)
        .addLabel(OZONE_TENANT_RANGER_POLICY_LABEL)
        .addRoleAcl(userRoleName,
            Collections.singletonList(Acl.allow(CREATE)))
        .addUserAcl(new OzoneOwnerPrincipal().getName(),
            Collections.singletonList(Acl.allow(ALL)))
        .build();
  }

  AuthorizerLock getAuthorizerLock();
}
