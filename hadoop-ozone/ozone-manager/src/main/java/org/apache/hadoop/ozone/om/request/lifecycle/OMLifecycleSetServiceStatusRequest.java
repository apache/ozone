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

package org.apache.hadoop.ozone.om.request.lifecycle;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.lifecycle.OMLifecycleSetServiceStatusResponse;
import org.apache.hadoop.ozone.om.service.KeyLifecycleService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetLifecycleServiceStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles SetLifecycleServiceStatus Request.
 * This request suspends or resumes the KeyLifecycleService.
 */
public class OMLifecycleSetServiceStatusRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMLifecycleSetServiceStatusRequest.class);

  public OMLifecycleSetServiceStatusRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    UserInfo userInfo = getOmRequest().getUserInfo();
    HashMap<String, String> auditMap = new HashMap<>();
    IOException exception = null;
    OMClientResponse omClientResponse;
    boolean suspend = getOmRequest().getSetLifecycleServiceStatusRequest().getSuspend();
    auditMap.put("suspend", String.valueOf(suspend));

    try {
      if (ozoneManager.getAclsEnabled()) {
        UserGroupInformation ugi = createUGIForApi();
        if (!ozoneManager.isAdmin(ugi)) {
          throw new OMException("Access denied for user " + ugi + ". "
              + "Superuser privilege is required to " + (suspend ? "suspend" : "resume") + " Lifecycle Service.",
              OMException.ResultCodes.ACCESS_DENIED);
        }
      }

      KeyLifecycleService keyLifecycleService = ozoneManager.getKeyManager().getKeyLifecycleService();
      if (keyLifecycleService != null) {
        if (suspend) {
          keyLifecycleService.suspend();
          LOG.info("KeyLifecycleService has been suspended by user: {}",
              userInfo != null ? userInfo.getUserName() : "unknown");
        } else {
          keyLifecycleService.resume();
          LOG.info("KeyLifecycleService resume called by user: {}",
              userInfo != null ? userInfo.getUserName() : "unknown");
        }
      } else {
        LOG.warn("KeyLifecycleService is not available");
      }

      omResponse.setSetLifecycleServiceStatusResponse(
          SetLifecycleServiceStatusResponse.newBuilder().build());
      omClientResponse = new OMLifecycleSetServiceStatusResponse(omResponse.build());
    } catch (IOException ex) {
      exception = ex;
      LOG.error("Failed to " + (suspend ? "suspend" : "resume") + " KeyLifecycleService", ex);
      omClientResponse = new OMLifecycleSetServiceStatusResponse(
          createErrorOMResponse(omResponse, ex));
    }

    markForAudit(auditLogger, buildAuditMessage(OMAction.SET_LIFECYCLE_SERVICE_STATUS,
        auditMap, exception, userInfo));
    return omClientResponse;
  }
}

