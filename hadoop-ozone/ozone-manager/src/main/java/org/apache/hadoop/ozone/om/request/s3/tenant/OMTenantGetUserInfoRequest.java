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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.tenant.OMTenantGetUserInfoResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantGetUserInfoRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantGetUserInfoResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles TenantGetUserInfo request.
 */
public class OMTenantGetUserInfoRequest extends OMClientRequest {

  public OMTenantGetUserInfoRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    // No authorization necessary for GetUserInfo request.
    return getOmRequest();
  }

  @Override
  @SuppressWarnings("checkstyle:methodlength")
  public OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    OMClientResponse omClientResponse = null;
    final OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());

    final Map<String, String> auditMap = new HashMap<>();
    final OMMetadataManager omMetadataManager =
        ozoneManager.getMetadataManager();
    IOException exception = null;

    final TenantGetUserInfoRequest request =
        getOmRequest().getTenantGetUserInfoRequest();

    final String userPrincipal = request.getUserPrincipal();

    if (StringUtils.isEmpty(userPrincipal)) {
      // TODO: Error handling
      return omClientResponse;
    }

    try {
      omMetadataManager.getPrincipalToAccessIdsTable().get(userPrincipal);
      // TODO: Set omClientResponse
    } catch (IOException ex) {
      exception = ex;
      omResponse.setTenantGetUserInfoResponse(
          TenantGetUserInfoResponse.newBuilder().setSuccess(false).build()
      ).build();
      omClientResponse = new OMTenantGetUserInfoResponse(
          createErrorOMResponse(omResponse, ex));
    }

    // Audit
    auditMap.put(OzoneConsts.TENANT, "userPrincipal:" + userPrincipal);
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.TENANT_GET_USER_INFO, auditMap, exception,
            getOmRequest().getUserInfo()));

    return omClientResponse;
  }
}
