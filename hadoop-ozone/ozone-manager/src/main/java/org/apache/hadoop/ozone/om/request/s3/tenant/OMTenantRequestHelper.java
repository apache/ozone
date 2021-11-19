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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.request.s3.tenant;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantInfo;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Utility class that contains helper methods for OM tenant requests.
 */
public final class OMTenantRequestHelper {

  private OMTenantRequestHelper() {
  }

  /**
   * Passes check only when caller is an Ozone admin,
   * throws OMException otherwise.
   * @throws OMException PERMISSION_DENIED
   */
  static void checkAdmin(OzoneManager ozoneManager) throws OMException {

    final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    if (!ozoneManager.isAdmin(ugi)) {
      throw new OMException("User '" + ugi.getUserName() +
          "' is not an Ozone admin.",
          OMException.ResultCodes.PERMISSION_DENIED);
    }
  }

  /**
   * Passes check if caller is an Ozone cluster admin or tenant delegated admin,
   * throws OMException otherwise.
   * @throws OMException PERMISSION_DENIED
   */
  static void checkTenantAdmin(OzoneManager ozoneManager, String tenantName)
      throws OMException {

    final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    if (!ozoneManager.isAdmin(ugi) &&
        !ozoneManager.isTenantAdmin(ugi, tenantName, true)) {
      throw new OMException("User '" + ugi.getUserName() +
          "' is neither an Ozone admin nor a delegated admin of tenant '" +
          tenantName + "'.", OMException.ResultCodes.PERMISSION_DENIED);
    }
  }

  static void checkTenantExistence(OMMetadataManager omMetadataManager,
      String tenantName) throws OMException {

    try {
      if (!omMetadataManager.getTenantStateTable().isExist(tenantName)) {
        throw new OMException("Tenant '" + tenantName + "' doesn't exist.",
            OMException.ResultCodes.TENANT_NOT_FOUND);
      }
    } catch (IOException ex) {
      if (ex instanceof OMException) {
        final OMException omEx = (OMException) ex;
        if (omEx.getResult().equals(OMException.ResultCodes.TENANT_NOT_FOUND)) {
          throw omEx;
        }
      }
      throw new OMException("Unable to retrieve "
          + "OmDBTenantInfo entry for tenant '" + tenantName + "': "
          + ex.getMessage(), OMException.ResultCodes.METADATA_ERROR);
    }
  }

  /**
   * Retrieve volume name of the tenant.
   */
  static String getTenantVolumeName(OMMetadataManager omMetadataManager,
      String tenantName) {

    final OmDBTenantInfo tenantInfo;
    try {
      tenantInfo = omMetadataManager.getTenantStateTable().get(tenantName);
    } catch (IOException e) {
      throw new RuntimeException("Potential DB error. Unable to retrieve "
          + "OmDBTenantInfo entry for tenant '" + tenantName + "'.");
    }

    if (tenantInfo == null) {
      throw new RuntimeException("Potential DB error or race condition. "
          + "OmDBTenantInfo entry is missing for tenant '" + tenantName + "'.");
    }

    final String volumeName = tenantInfo.getAccountNamespaceName();

    if (StringUtils.isEmpty(tenantName)) {
      throw new RuntimeException("Potential DB error. volumeName "
          + "field is null or empty for tenantId '" + tenantName + "'.");
    }

    return volumeName;
  }

  public static String getTenantNameFromAccessId(
          OMMetadataManager omMetadataManager, String accessId,
          boolean throwOnError) throws IOException {

    final OmDBAccessIdInfo accessIdInfo = omMetadataManager
        .getTenantAccessIdTable().get(accessId);

    if (accessIdInfo == null) {
      if (throwOnError) {
        throw new OMException("OmDBAccessIdInfo is missing for accessId '" +
                accessId + "' in DB.", OMException.ResultCodes.METADATA_ERROR);
      } else {
        return null;
      }
    }

    final String tenantId = accessIdInfo.getTenantId();

    if (StringUtils.isEmpty(tenantId)) {
      if (throwOnError) {
        throw new OMException("tenantId field is null or empty for accessId '" +
                accessId + "'.", OMException.ResultCodes.METADATA_ERROR);
      } else {
        return null;
      }
    }

    return tenantId;
  }

  public static boolean isUserAccessIdPrincipalOrTenantAdmin(
          OzoneManager ozoneManager, String accessId,
          UserGroupInformation ugi) throws IOException {

    final OmDBAccessIdInfo accessIdInfo = ozoneManager.getMetadataManager()
            .getTenantAccessIdTable().get(accessId);

    if (accessIdInfo == null) {
      // Doesn't have the accessId entry in TenantAccessIdTable.
      // Probably came from `ozone s3 getsecret` with older OM.
      return false;
    }

    final String tenantName = accessIdInfo.getTenantId();
    // Sanity check
    if (tenantName == null) {
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

    // Check if ugi is an admin of this tenant
    if (ozoneManager.isTenantAdmin(ugi, tenantName, true)) {
      return true;
    }

    return false;
  }

}
