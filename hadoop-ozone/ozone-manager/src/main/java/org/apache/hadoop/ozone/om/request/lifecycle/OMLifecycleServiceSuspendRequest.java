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

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.lifecycle.OMLifecycleServiceSuspendResponse;
import org.apache.hadoop.ozone.om.service.KeyLifecycleService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SuspendLifecycleServiceResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles SuspendLifecycleService Request.
 */
public class OMLifecycleServiceSuspendRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMLifecycleServiceSuspendRequest.class);

  public OMLifecycleServiceSuspendRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    UserInfo userInfo = getOmRequest().getUserInfo();
    Map<String, String> auditMap = new HashMap<>();
    IOException exception = null;
    OMClientResponse omClientResponse;

    KeyLifecycleService keyLifecycleService = ozoneManager.getKeyManager().getKeyLifecycleService();
    if (keyLifecycleService != null) {
      keyLifecycleService.setServiceEnabled(false);
      LOG.info("KeyLifecycleService has been suspended by user: {}",
          userInfo != null ? userInfo.getUserName() : "unknown");
    } else {
      LOG.warn("KeyLifecycleService is not available");
    }

    omResponse.setSuspendLifecycleServiceResponse(
        SuspendLifecycleServiceResponse.newBuilder().build());
    omClientResponse = new OMLifecycleServiceSuspendResponse(omResponse.build());
    markForAudit(auditLogger, buildAuditMessage(
        OMAction.SUSPEND_LIFECYCLE_SERVICE, auditMap, exception, userInfo));
    return omClientResponse;
  }
}

