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

import static org.apache.hadoop.hdds.utils.HddsServerUtil.getRemoteUser;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.StartFinalizeUpgrade;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.upgrade.OMStartFinalizeUpgradeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts the cluster upgrade finalization process.
 */
public class OMStartFinalizeUpgradeRequest extends OMClientRequest {
  private static final Logger LOG = LoggerFactory.getLogger(OMStartFinalizeUpgradeRequest.class);

  public OMStartFinalizeUpgradeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest omRequest = super.preExecute(ozoneManager);
    if (ozoneManager.isAdminAuthorizationEnabled()) {
      UserGroupInformation ugi = createUGIForApi();
      if (!ozoneManager.isAdmin(ugi)) {
        throw new OMException("Access denied for user " + ugi + ". "
            + "Superuser privilege is required to start finalize upgrade.", OMException.ResultCodes.ACCESS_DENIED);
      }
    }
    boolean force = getOmRequest().getStartFinalizeUpgradeRequest().getForce();
    if (force) {
      LOG.warn("Forcing upgrade finalization by skipping OM peer software version checks");
    } else {
      validatePeerOmVersionsBeforeFinalize(ozoneManager.getPeerNodes(), ozoneManager.getConfiguration());
    }

    try {
      ozoneManager.getScmClient().getContainerClient().finalizeUpgrade();
    } catch (SCMException e) {
      if (e.getResult() == SCMException.ResultCodes.UNSUPPORTED_OPERATION) {
        throw new OMException(e.getMessage(), e, OMException.ResultCodes.NOT_SUPPORTED_OPERATION);
      }
      throw e;
    }
    LOG.info("Successfully triggered the finalize upgrade process in SCM");
    return omRequest;
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    LOG.trace("Request: {}", getOmRequest());
    AuditLogger auditLogger = ozoneManager.getSystemAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    OMResponse.Builder responseBuilder = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    responseBuilder.setCmdType(StartFinalizeUpgrade);
    OMClientResponse response = null;
    Exception exception = null;

    try {
      OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
      omMetadataManager.getMetaTable().addCacheEntry(
          new CacheKey<>(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY), CacheValue.get(context.getIndex(), "ignored"));
      ozoneManager.getMetrics().setFinalizationInProgress(true);


      OzoneManagerProtocolProtos.StartFinalizeUpgradeResponse omResponse =
          OzoneManagerProtocolProtos.StartFinalizeUpgradeResponse.newBuilder()
              .build();
      responseBuilder.setStartFinalizeUpgradeResponse(omResponse);
      response = new OMStartFinalizeUpgradeResponse(responseBuilder.build());
      LOG.trace("Returning response: {}", response);
    } catch (Exception e) {
      exception = e;
      response = new OMStartFinalizeUpgradeResponse(createErrorOMResponse(responseBuilder, e));
    }

    Map<String, String> auditMap = new HashMap<>();
    auditMap.put("force", String.valueOf(getOmRequest().getStartFinalizeUpgradeRequest().getForce()));
    markForAudit(auditLogger, buildAuditMessage(OMAction.UPGRADE_FINALIZE, auditMap, exception, userInfo));
    return response;
  }

  private static void validatePeerOmVersionsBeforeFinalize(List<OMNodeDetails> peerNodes,
      OzoneConfiguration configuration) throws OMException {
    if (peerNodes.isEmpty()) {
      return;
    }
    OzoneManagerVersion leaderVersion = OzoneManagerVersion.SOFTWARE_VERSION;
    List<String> failedPeers = new ArrayList<>();
    for (OMNodeDetails peerDetails : peerNodes) {
      String peerId = peerDetails.getNodeId();
      OMAdminProtocolClientSideImpl client = null;
      try {
        client = OMAdminProtocolClientSideImpl.createProxyForSingleOM(configuration, getRemoteUser(), peerDetails);
        OzoneManagerVersion peerVersion = client.getPeerUpgradeStatus();
        if (!peerVersion.equals(leaderVersion)) {
          LOG.warn("OM peer {} is running software version {} but leader is running version {}. "
              + "Rejecting finalize command.", peerId, peerVersion, leaderVersion);
          failedPeers.add(peerId + " (version: " + peerVersion + ")");
        }
      } catch (IOException e) {
        LOG.warn("Failed to contact OM peer {} to check software version before finalize.", peerId, e);
        failedPeers.add(peerId + " (unreachable: " + e.getMessage() + ")");
      } finally {
        IOUtils.cleanupWithLogger(LOG, client);
      }
    }
    if (!failedPeers.isEmpty()) {
      throw new OMException("Finalize rejected: the following OM peers did not confirm matching software version "
          + "(expected version=" + leaderVersion + "): " + String.join(", ", failedPeers),
          OMException.ResultCodes.NOT_SUPPORTED_OPERATION);
    }
  }

}
