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

package org.apache.hadoop.ozone.om.request.s3.security;

import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ozone.om.OMMultiTenantManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common helper function for S3 secret requests.
 */
public final class S3SecretRequestHelper {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3SecretRequestHelper.class);

  private S3SecretRequestHelper() {
  }

  /**
   * Retrieves thread-local UGI for request or construct new one
   * based on provided accessId.
   *
   * @param accessId user identifier from request.
   * @return {@link UserGroupInformation} instance.
   */
  public static UserGroupInformation getOrCreateUgi(String accessId) {
    final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    if (ugi == null && Strings.isNotEmpty(accessId)) {
      return UserGroupInformation.createRemoteUser(accessId, KERBEROS);
    } else {
      return ugi;
    }
  }

  /**
   * Checks whether the ugi has the permission to operate (get secret,
   * set secret, revoke secret) on the given access ID.
   *
   * Throws OMException if the UGI doesn't have the permission.
   */
  public static void checkAccessIdSecretOpPermission(
      OzoneManager ozoneManager, UserGroupInformation ugi, String accessId)
      throws IOException {

    // Flag indicating whether the accessId is assigned to a tenant
    // (under S3 Multi-Tenancy feature) or not.
    boolean isAccessIdAssignedToTenant = false;

    // Permission check:
    //
    // 1. If multi-tenancy is enabled, caller ugi need to own the access ID or
    // have Ozone admin or tenant admin privilege to pass the check;
    if (ozoneManager.isS3MultiTenancyEnabled()) {
      final OMMultiTenantManager multiTenantManager =
          ozoneManager.getMultiTenantManager();

      final Optional<String> optionalTenantId =
          multiTenantManager.getTenantForAccessID(accessId);

      isAccessIdAssignedToTenant = optionalTenantId.isPresent();

      if (isAccessIdAssignedToTenant) {

        final String accessIdOwnerUsername =
            multiTenantManager.getUserNameGivenAccessId(accessId);
        final String tenantId = optionalTenantId.get();

        // Access ID owner is short name
        final String shortName = ugi.getShortUserName();

        // HDDS-6691: ugi should either own the access ID, or be an Ozone/tenant
        // admin to pass the check.
        if (!shortName.equals(accessIdOwnerUsername) &&
            !multiTenantManager.isTenantAdmin(ugi, tenantId, false)) {
          throw new OMException("Requested accessId '" + accessId + "' doesn't"
              + " belong to current user '" + shortName + "', nor does"
              + " current user have Ozone or tenant administrator privilege",
              ResultCodes.USER_MISMATCH);
          // Note: A more fitting result code could be PERMISSION_DENIED,
          //  but existing code already uses USER_MISMATCH. Maybe change this
          //  later -- could cause minor incompatibility.
        }
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("S3 Multi-Tenancy is enabled, but the requested accessId "
            + "'{}' is not assigned to a tenant. Falling back to the old "
            + "permission check", accessId);
      }
    }

    // 2. If S3 multi-tenancy is disabled (or the access ID is not assigned
    // to a tenant), fall back to the old permission check.
    final String fullPrincipal = ugi.getUserName();
    if (!isAccessIdAssignedToTenant &&
        !fullPrincipal.equals(accessId) && !ozoneManager.isS3Admin(ugi)) {

      throw new OMException("Requested accessId '" + accessId +
          "' doesn't match current user '" + fullPrincipal +
          "', nor does current user has administrator privilege.",
          OMException.ResultCodes.USER_MISMATCH);
    }
  }
}
