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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.validatePreviousSnapshotId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.om.DeletingServiceMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyPurgeResponse;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles purging of keys from OM DB.
 */
public class OMKeyPurgeRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyPurgeRequest.class);

  private static final AuditLogger AUDIT = new AuditLogger(AuditLoggerType.OMSYSTEMLOGGER);
  private static final String AUDIT_PARAM_KEYS_DELETED = "keysDeleted";
  private static final String AUDIT_PARAM_RENAMED_KEYS_PURGED = "renamedKeysPurged";
  private static final String AUDIT_PARAMS_DELETED_KEYS_LIST = "deletedKeysList";
  private static final String AUDIT_PARAMS_RENAMED_KEYS_LIST = "renamedKeysList";
  private static final String AUDIT_PARAM_SNAPSHOT_ID = "snapshotId";

  public OMKeyPurgeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    PurgeKeysRequest purgeKeysRequest = getOmRequest().getPurgeKeysRequest();
    List<DeletedKeys> bucketDeletedKeysList = purgeKeysRequest.getDeletedKeysList();
    List<SnapshotMoveKeyInfos> keysToUpdateList = purgeKeysRequest.getKeysToUpdateList();
    String fromSnapshot = purgeKeysRequest.hasSnapshotTableKey() ? purgeKeysRequest.getSnapshotTableKey() : null;
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl) ozoneManager.getMetadataManager();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    final SnapshotInfo fromSnapshotInfo;
    try {
      fromSnapshotInfo = fromSnapshot != null ? SnapshotUtils.getSnapshotInfo(ozoneManager,
          fromSnapshot) : null;
      // Checking if this request is an old request or new one.
      if (purgeKeysRequest.hasExpectedPreviousSnapshotID()) {
        // Validating previous snapshot since while purging deletes, a snapshot create request could make this purge
        // key request invalid on AOS since the deletedKey would be in the newly created snapshot. This would add an
        // redundant tombstone entry in the deletedTable. It is better to skip the transaction.
        UUID expectedPreviousSnapshotId = purgeKeysRequest.getExpectedPreviousSnapshotID().hasUuid()
            ? fromProtobuf(purgeKeysRequest.getExpectedPreviousSnapshotID().getUuid()) : null;
        if (!validatePreviousSnapshotId(fromSnapshotInfo, omMetadataManager.getSnapshotChainManager(),
            expectedPreviousSnapshotId)) {
          return new OMKeyPurgeResponse(createErrorOMResponse(omResponse,
              new OMException("Snapshot validation failed", OMException.ResultCodes.INVALID_REQUEST)));
        }
      }
    } catch (IOException e) {
      LOG.error("Error occurred while performing OmKeyPurge. ", e);
      AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.KEY_DELETION, null, e));
      return new OMKeyPurgeResponse(createErrorOMResponse(omResponse, e));
    }

    List<String> keysToBePurgedList = new ArrayList<>();

    int numKeysDeleted = 0;
    List<String> renamedKeysToBePurged = new ArrayList<>(purgeKeysRequest.getRenamedKeysList());
    for (DeletedKeys bucketWithDeleteKeys : bucketDeletedKeysList) {
      List<String> keysList = bucketWithDeleteKeys.getKeysList();
      keysToBePurgedList.addAll(keysList);
      numKeysDeleted = numKeysDeleted + keysList.size();
    }
    DeletingServiceMetrics deletingServiceMetrics = ozoneManager.getDeletionMetrics();
    deletingServiceMetrics.incrNumKeysPurged(numKeysDeleted);
    deletingServiceMetrics.incrNumRenameEntriesPurged(renamedKeysToBePurged.size());

    if (keysToBePurgedList.isEmpty() && renamedKeysToBePurged.isEmpty()) {
      OMException oe = new OMException("None of the keys can be purged be purged since a new snapshot was created " +
          "for all the buckets, making this request invalid", OMException.ResultCodes.KEY_DELETION_ERROR);
      AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.KEY_DELETION, null, oe));
      return new OMKeyPurgeResponse(createErrorOMResponse(omResponse, oe));
    }

    // Setting transaction info for snapshot, this is to prevent duplicate purge requests to OM from background
    // services.
    try {
      Map<String, String> auditParams = new LinkedHashMap<>();
      if (fromSnapshotInfo != null) {
        fromSnapshotInfo.setLastTransactionInfo(TransactionInfo.valueOf(context.getTermIndex()).toByteString());
        omMetadataManager.getSnapshotInfoTable().addCacheEntry(new CacheKey<>(fromSnapshotInfo.getTableKey()),
            CacheValue.get(context.getIndex(), fromSnapshotInfo));
        auditParams.put(AUDIT_PARAM_SNAPSHOT_ID, fromSnapshotInfo.getSnapshotId().toString());
      }
      auditParams.put(AUDIT_PARAM_KEYS_DELETED, String.valueOf(numKeysDeleted));
      auditParams.put(AUDIT_PARAM_RENAMED_KEYS_PURGED, String.valueOf(renamedKeysToBePurged.size()));
      if (!keysToBePurgedList.isEmpty()) {
        auditParams.put(AUDIT_PARAMS_DELETED_KEYS_LIST, String.join(",", keysToBePurgedList));
      }
      if (!renamedKeysToBePurged.isEmpty()) {
        auditParams.put(AUDIT_PARAMS_RENAMED_KEYS_LIST, String.join(",", renamedKeysToBePurged));
      }
      AUDIT.logWriteSuccess(ozoneManager.buildAuditMessageForSuccess(OMSystemAction.KEY_DELETION, auditParams));
    } catch (IOException e) {
      AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.KEY_DELETION, null, e));
      return new OMKeyPurgeResponse(createErrorOMResponse(omResponse, e));
    }

    return new OMKeyPurgeResponse(omResponse.build(),
        keysToBePurgedList, renamedKeysToBePurged, fromSnapshotInfo, keysToUpdateList);
  }

}
