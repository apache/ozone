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

package org.apache.hadoop.ozone.om.request.upgrade;

import static org.apache.hadoop.ozone.OzoneConsts.APPARENT_VERSION_KEY;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CompleteFinalizeUpgrade;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMCompleteFinalizeUpgradeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CompleteFinalizeUpgradeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Completes OM finalization once SCM has finalized. Submitted internally over Ratis by
 * {@code OMUpgradeFinalizeService}; no client ever sends this request.
 */
public class OMCompleteFinalizeUpgradeRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMCompleteFinalizeUpgradeRequest.class);

  public OMCompleteFinalizeUpgradeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    LOG.trace("Request: {}", getOmRequest());
    AuditLogger auditLogger = ozoneManager.getSystemAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    OMResponse.Builder responseBuilder =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    responseBuilder.setCmdType(CompleteFinalizeUpgrade);
    OMClientResponse response = null;
    Exception exception = null;

    try {
      ozoneManager.finalizeUpgrade();

      OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
      int apparentVersion = ozoneManager.getVersionManager().getApparentVersion().serialize();
      omMetadataManager.getMetaTable().addCacheEntry(
          new CacheKey<>(APPARENT_VERSION_KEY),
          CacheValue.get(context.getIndex(), String.valueOf(apparentVersion)));
      // Clear the finalization_in_progress key from the cache
      omMetadataManager.getMetaTable().addCacheEntry(
          new CacheKey<>(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY), CacheValue.get(context.getIndex()));
      ozoneManager.getMetrics().setFinalizationInProgress(false);

      responseBuilder.setCompleteFinalizeUpgradeResponse(CompleteFinalizeUpgradeResponse.newBuilder().build());
      response = new OMCompleteFinalizeUpgradeResponse(responseBuilder.build(),
          ozoneManager.getVersionManager().getApparentVersion().serialize());
      LOG.trace("Returning response: {}", response);
    } catch (IOException e) {
      exception = e;
      response = new OMCompleteFinalizeUpgradeResponse(
          createErrorOMResponse(responseBuilder, e), -1);
    }

    markForAudit(auditLogger, buildAuditMessage(OMAction.UPGRADE_FINALIZE,
        new HashMap<>(), exception, userInfo));
    return response;
  }

}
