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

  static void checkTenantAdmin(OzoneManager ozoneManager, String tenantName)
      throws OMException {

    final UserGroupInformation ugi = ProtobufRpcEngine.Server.getRemoteUser();
    if (!ozoneManager.isAdmin(ugi) &&
        !ozoneManager.isTenantAdmin(ugi, tenantName, true)) {
      throw new OMException("Permission denied. User '" + ugi.getUserName() +
          "' is neither an Ozone admin nor a delegated admin of tenant '" +
          tenantName + "'.", OMException.ResultCodes.PERMISSION_DENIED);
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
      throw new RuntimeException("Potential DB error. OmDBTenantInfo "
          + "entry is missing for tenant '" + tenantName + "'.");
    }

    final String volumeName = tenantInfo.getAccountNamespaceName();

    if (StringUtils.isEmpty(tenantName)) {
      throw new RuntimeException("Potential DB error. volumeName "
          + "field is null or empty for tenantId '" + tenantName + "'.");
    }

    return volumeName;
  }

  static String getTenantNameFromAccessId(OMMetadataManager omMetadataManager,
      String accessId) throws IOException {

    final OmDBAccessIdInfo accessIdInfo = omMetadataManager
        .getTenantAccessIdTable().get(accessId);

    if (accessIdInfo == null) {
      throw new OMException("OmDBAccessIdInfo entry is missing for accessId '" +
          accessId + "'.", OMException.ResultCodes.METADATA_ERROR);
    }

    final String tenantName = accessIdInfo.getTenantId();

    if (StringUtils.isEmpty(tenantName)) {
      throw new OMException("tenantId field is null or empty for accessId '" +
          accessId + "'.", OMException.ResultCodes.METADATA_ERROR);
    }

    return tenantName;
  }

}
