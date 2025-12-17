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

package org.apache.hadoop.ozone.om.request.snapshot;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotInternalMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotSetPropertyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Updates the exclusive size of the snapshot.
 */
public class OMSnapshotSetPropertyRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotSetPropertyRequest.class);
  private static final AuditLogger AUDIT = new AuditLogger(AuditLoggerType.OMSYSTEMLOGGER);
  private static final String AUDIT_PARAM_SNAPSHOT_DB_KEY = "snapshotDBKey";
  private static final String AUDIT_PARAM_SNAPSHOT_EXCLUSIVE_SIZE = "snapshotExclusiveSize";
  private static final String AUDIT_PARAM_SNAPSHOT_EXCLUSIVE_REPL_SIZE = "snapshotExclusiveReplicatedSize";
  private static final String AUDIT_PARAM_DEEP_CLEAN_DEL_DIR = "deepCleanDeletedDir";
  private static final String AUDIT_PARAM_DEEP_CLEAN_DEL_KEY = "deepCleanDeletedKey";
  private static final String AUDIT_PARAM_EXCLUSIVE_SIZE_DELTA_FROM_DIR_DEEP_CLEAN =
      "exclusiveSizeDeltaFromDirDeepCleaning";
  private static final String AUDIT_PARAM_EXCLUSIVE_REPL_SIZE_DELTA_FROM_DIR_DEEP_CLEAN =
      "exclusiveReplicatedSizeDeltaFromDirDeepCleaning";

  public OMSnapshotSetPropertyRequest(OMRequest omRequest) {
    super(omRequest);
  }

  private void updateSnapshotProperty(
      SnapshotInfo snapInfo, OzoneManagerProtocolProtos.SetSnapshotPropertyRequest setSnapshotPropertyRequest,
      Map<String, String> auditParams) {
    if (setSnapshotPropertyRequest.hasDeepCleanedDeletedDir()) {
      snapInfo.setDeepCleanedDeletedDir(setSnapshotPropertyRequest
          .getDeepCleanedDeletedDir());
      auditParams.put(AUDIT_PARAM_DEEP_CLEAN_DEL_DIR, String.valueOf(setSnapshotPropertyRequest
          .getDeepCleanedDeletedDir()));
    }

    if (setSnapshotPropertyRequest.hasDeepCleanedDeletedKey()) {
      snapInfo.setDeepClean(setSnapshotPropertyRequest
          .getDeepCleanedDeletedKey());
      auditParams.put(AUDIT_PARAM_DEEP_CLEAN_DEL_KEY, String.valueOf(setSnapshotPropertyRequest
          .getDeepCleanedDeletedKey()));
    }

    if (setSnapshotPropertyRequest.hasSnapshotSize()) {
      SnapshotSize snapshotSize = setSnapshotPropertyRequest.getSnapshotSize();
      // Set Exclusive size.
      snapInfo.setExclusiveSize(snapshotSize.getExclusiveSize());
      snapInfo.setExclusiveReplicatedSize(snapshotSize.getExclusiveReplicatedSize());
      auditParams.put(AUDIT_PARAM_SNAPSHOT_EXCLUSIVE_SIZE, String.valueOf(snapshotSize.getExclusiveSize()));
      auditParams.put(AUDIT_PARAM_SNAPSHOT_EXCLUSIVE_REPL_SIZE,
          String.valueOf(snapshotSize.getExclusiveReplicatedSize()));
    }
    if (setSnapshotPropertyRequest.hasSnapshotSizeDeltaFromDirDeepCleaning()) {
      SnapshotSize snapshotSize = setSnapshotPropertyRequest.getSnapshotSizeDeltaFromDirDeepCleaning();
      // Set Exclusive size.
      snapInfo.setExclusiveSizeDeltaFromDirDeepCleaning(snapshotSize.getExclusiveSize());
      snapInfo.setExclusiveReplicatedSizeDeltaFromDirDeepCleaning(snapshotSize.getExclusiveReplicatedSize());
      auditParams.put(AUDIT_PARAM_EXCLUSIVE_SIZE_DELTA_FROM_DIR_DEEP_CLEAN,
          String.valueOf(snapshotSize.getExclusiveSize()));
      auditParams.put(AUDIT_PARAM_EXCLUSIVE_REPL_SIZE_DELTA_FROM_DIR_DEEP_CLEAN,
          String.valueOf(snapshotSize.getExclusiveReplicatedSize()));
    }
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OmSnapshotInternalMetrics omSnapshotIntMetrics = ozoneManager.getOmSnapshotIntMetrics();

    OMClientResponse omClientResponse;
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    List<OzoneManagerProtocolProtos.SetSnapshotPropertyRequest> setSnapshotPropertyRequests = Lists.newArrayList();
    if (getOmRequest().hasSetSnapshotPropertyRequest()) {
      setSnapshotPropertyRequests.add(getOmRequest().getSetSnapshotPropertyRequest());
    }
    setSnapshotPropertyRequests.addAll(getOmRequest().getSetSnapshotPropertyRequestsList());
    Set<String> snapshotKeys = new HashSet<>();
    Map<String, SnapshotInfo> snapshotInfoMap = new HashMap<>();
    Map<String, Map<String, String>> auditParamsMap = new HashMap<>();
    try {
      for (OzoneManagerProtocolProtos.SetSnapshotPropertyRequest setSnapshotPropertyRequest :
          setSnapshotPropertyRequests) {
        String snapshotKey = setSnapshotPropertyRequest.getSnapshotKey();
        if (snapshotKeys.contains(snapshotKey)) {
          OMException e = new OMException("Snapshot with snapshot key: " + snapshotKey +
              " added multiple times in the request. Request: " + setSnapshotPropertyRequests, INVALID_REQUEST);
          AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.SNAPSHOT_SET_PROPERTY,
              null, e));
          throw e;
        }
        snapshotKeys.add(snapshotKey);
        SnapshotInfo updatedSnapInfo = snapshotInfoMap.computeIfAbsent(snapshotKey,
                (k) -> {
                  try {
                    return metadataManager.getSnapshotInfoTable().get(k);
                  } catch (IOException e) {
                    throw new UncheckedIOException("Exception while getting key " + k, e);
                  }
                });
        if (updatedSnapInfo == null) {
          LOG.error("Snapshot: '{}' does not exist in snapshot table.", snapshotKey);
          OMException e = new OMException("Snapshot: '{}' does not exist in snapshot table." + snapshotKey
              + "Request: " + setSnapshotPropertyRequests, FILE_NOT_FOUND);
          AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.SNAPSHOT_SET_PROPERTY,
              null, e));
          throw e;
        }
        Map<String, String> auditParams = new LinkedHashMap<>();
        auditParams.put(AUDIT_PARAM_SNAPSHOT_DB_KEY, snapshotKey);
        updateSnapshotProperty(updatedSnapInfo, setSnapshotPropertyRequest, auditParams);
        auditParamsMap.put(snapshotKey, auditParams);
      }

      if (snapshotInfoMap.isEmpty()) {
        OMException e = new OMException("Snapshots: " + snapshotKeys + " don't not exist in snapshot table.",
            FILE_NOT_FOUND);
        AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.SNAPSHOT_SET_PROPERTY,
            null, e));
        throw e;
      }
      // Update Table Cache
      for (Map.Entry<String, SnapshotInfo> snapshot : snapshotInfoMap.entrySet()) {
        metadataManager.getSnapshotInfoTable().addCacheEntry(
            new CacheKey<>(snapshot.getKey()),
            CacheValue.get(context.getIndex(), snapshot.getValue()));
        omSnapshotIntMetrics.incNumSnapshotSetProperties();
        AUDIT.logWriteSuccess(ozoneManager.buildAuditMessageForSuccess(OMSystemAction.SNAPSHOT_SET_PROPERTY,
            auditParamsMap.get(snapshot.getKey())));
      }

      omClientResponse = new OMSnapshotSetPropertyResponse(omResponse.build(), snapshotInfoMap.values());
      LOG.info("Successfully executed snapshotSetPropertyRequest: {{}}.", setSnapshotPropertyRequests);
    } catch (UncheckedIOException | IOException ex) {
      omClientResponse = new OMSnapshotSetPropertyResponse(
          createErrorOMResponse(omResponse, ex));
      omSnapshotIntMetrics.incNumSnapshotSetPropertyFails();
      LOG.error("Failed to execute snapshotSetPropertyRequest: {{}}.", setSnapshotPropertyRequests, ex);
      AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.SNAPSHOT_SET_PROPERTY, null, ex));
    }

    return omClientResponse;
  }
}
