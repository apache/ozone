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

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.FILESYSTEM_SNAPSHOT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotInternalMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotMoveTableKeysResponse;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveTableKeysRequest;

/**
 * Handles OMSnapshotMoveTableKeysRequest Request.
 * This is an OM internal request. Does not need @RequireSnapshotFeatureState.
 */
public class OMSnapshotMoveTableKeysRequest extends OMClientRequest {
  private static final AuditLogger AUDIT = new AuditLogger(AuditLoggerType.OMSYSTEMLOGGER);
  private static final String AUDIT_PARAM_FROM_SNAPSHOT_TABLE_KEY = "fromSnapshotTableKey";
  private static final String AUDIT_PARAM_TO_SNAPSHOT_TABLE_KEY_OR_AOS = "toSnapshotTableKeyOrAOS";
  private static final String AUDIT_PARAM_DEL_KEYS_MOVED = "deletedKeysMoved";
  private static final String AUDIT_PARAM_RENAMED_KEYS_MOVED = "renamedKeysMoved";
  private static final String AUDIT_PARAM_DEL_DIRS_MOVED = "deletedDirsMoved";
  private static final String AUDIT_PARAM_DEL_KEYS_MOVED_LIST = "deletedKeysMovedList";
  private static final String AUDIT_PARAM_RENAMED_KEYS_LIST = "renamedKeysList";
  private static final String AUDIT_PARAM_DEL_DIRS_MOVED_LIST = "deletedDirsMovedList";
  private static final String AOS = "AOS";

  public OMSnapshotMoveTableKeysRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl) ozoneManager.getMetadataManager();
    SnapshotChainManager snapshotChainManager = omMetadataManager.getSnapshotChainManager();
    SnapshotMoveTableKeysRequest moveTableKeysRequest = getOmRequest().getSnapshotMoveTableKeysRequest();
    UUID fromSnapshotID = fromProtobuf(moveTableKeysRequest.getFromSnapshotID());
    SnapshotInfo fromSnapshot = SnapshotUtils.getSnapshotInfo(ozoneManager,
        snapshotChainManager, fromSnapshotID);


    Set<String> keys = new HashSet<>();
    List<SnapshotMoveKeyInfos> deletedKeys = new ArrayList<>(moveTableKeysRequest.getDeletedKeysList().size());

    //validate deleted key starts with bucket prefix.[/<volName>/<bucketName>/]
    String deletedTablePrefix = omMetadataManager.getTableBucketPrefix(omMetadataManager.getDeletedTable().getName(),
        fromSnapshot.getVolumeName(), fromSnapshot.getBucketName());
    for (SnapshotMoveKeyInfos deletedKey : moveTableKeysRequest.getDeletedKeysList()) {
      // Filter only deleted keys with at least one keyInfo per key.
      if (!deletedKey.getKeyInfosList().isEmpty()) {
        deletedKeys.add(deletedKey);
        if (!deletedKey.getKey().startsWith(deletedTablePrefix)) {
          OMException ex = new OMException("Deleted Key: " + deletedKey + " doesn't start with prefix "
              + deletedTablePrefix, OMException.ResultCodes.INVALID_KEY_NAME);
          if (LOG.isDebugEnabled()) {
            AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.SNAPSHOT_MOVE_TABLE_KEYS,
                null, ex));
          }
          throw ex;
        }
        if (keys.contains(deletedKey.getKey())) {
          OMException ex = new OMException("Duplicate Deleted Key: " + deletedKey + " in request",
              OMException.ResultCodes.INVALID_REQUEST);
          if (LOG.isDebugEnabled()) {
            AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.SNAPSHOT_MOVE_TABLE_KEYS,
                null, ex));
          }
          throw ex;
        } else {
          keys.add(deletedKey.getKey());
        }
      }
    }

    keys.clear();
    String renamedTablePrefix = omMetadataManager.getTableBucketPrefix(
        omMetadataManager.getSnapshotRenamedTable().getName(), fromSnapshot.getVolumeName(),
        fromSnapshot.getBucketName());
    List<HddsProtos.KeyValue> renamedKeysList = new ArrayList<>(moveTableKeysRequest.getRenamedKeysList().size());
    //validate rename key starts with bucket prefix.[/<volName>/<bucketName>/]
    for (HddsProtos.KeyValue renamedKey : moveTableKeysRequest.getRenamedKeysList()) {
      if (renamedKey.hasKey() && renamedKey.hasValue()) {
        renamedKeysList.add(renamedKey);
        if (!renamedKey.getKey().startsWith(renamedTablePrefix)) {
          OMException ex = new OMException("Rename Key: " + renamedKey + " doesn't start with prefix "
              + renamedTablePrefix, OMException.ResultCodes.INVALID_KEY_NAME);
          if (LOG.isDebugEnabled()) {
            AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.SNAPSHOT_MOVE_TABLE_KEYS,
                null, ex));
          }
          throw ex;
        }
        if (keys.contains(renamedKey.getKey())) {
          OMException ex = new OMException("Duplicate rename Key: " + renamedKey + " in request",
              OMException.ResultCodes.INVALID_REQUEST);
          if (LOG.isDebugEnabled()) {
            AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.SNAPSHOT_MOVE_TABLE_KEYS,
                null, ex));
          }
          throw ex;
        } else {
          keys.add(renamedKey.getKey());
        }
      }
    }
    keys.clear();

    // Filter only deleted dirs with only one keyInfo per key.
    String deletedDirTablePrefix = omMetadataManager.getTableBucketPrefix(
        omMetadataManager.getDeletedDirTable().getName(), fromSnapshot.getVolumeName(), fromSnapshot.getBucketName());
    List<SnapshotMoveKeyInfos> deletedDirs = new ArrayList<>(moveTableKeysRequest.getDeletedDirsList().size());
    //validate deleted key starts with bucket FSO path prefix.[/<volId>/<bucketId>/]
    for (SnapshotMoveKeyInfos deletedDir : moveTableKeysRequest.getDeletedDirsList()) {
      // Filter deleted directories with exactly one keyInfo per key.
      if (deletedDir.getKeyInfosList().size() == 1) {
        deletedDirs.add(deletedDir);
        if (!deletedDir.getKey().startsWith(deletedDirTablePrefix)) {
          OMException ex = new OMException("Deleted dir: " + deletedDir + " doesn't start with prefix " +
              deletedDirTablePrefix, OMException.ResultCodes.INVALID_KEY_NAME);
          if (LOG.isDebugEnabled()) {
            AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.SNAPSHOT_MOVE_TABLE_KEYS,
                null, ex));
          }
          throw ex;
        }
        if (keys.contains(deletedDir.getKey())) {
          OMException ex = new OMException("Duplicate deleted dir Key: " + deletedDir + " in request",
              OMException.ResultCodes.INVALID_REQUEST);
          if (LOG.isDebugEnabled()) {
            AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.SNAPSHOT_MOVE_TABLE_KEYS,
                null, ex));
          }
          throw ex;
        } else {
          keys.add(deletedDir.getKey());
        }
      }
    }
    return getOmRequest().toBuilder().setSnapshotMoveTableKeysRequest(
        moveTableKeysRequest.toBuilder().clearDeletedDirs().clearDeletedKeys().clearRenamedKeys()
            .addAllDeletedKeys(deletedKeys).addAllDeletedDirs(deletedDirs)
            .addAllRenamedKeys(renamedKeysList).build()).build();
  }

  @Override
  @DisallowedUntilLayoutVersion(FILESYSTEM_SNAPSHOT)
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    OmSnapshotInternalMetrics omSnapshotIntMetrics = ozoneManager.getOmSnapshotIntMetrics();

    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl) ozoneManager.getMetadataManager();
    SnapshotChainManager snapshotChainManager = omMetadataManager.getSnapshotChainManager();

    SnapshotMoveTableKeysRequest moveTableKeysRequest = getOmRequest().getSnapshotMoveTableKeysRequest();

    OMClientResponse omClientResponse;
    OzoneManagerProtocolProtos.OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());

    UUID fromSnapshotID = fromProtobuf(moveTableKeysRequest.getFromSnapshotID());
    try {
      SnapshotInfo fromSnapshot = SnapshotUtils.getSnapshotInfo(ozoneManager,
          snapshotChainManager, fromProtobuf(moveTableKeysRequest.getFromSnapshotID()));
      OmBucketInfo omBucketInfo = OMKeyRequest.getBucketInfo(omMetadataManager, fromSnapshot.getVolumeName(),
          fromSnapshot.getBucketName());
      // If there is no snapshot in the chain after the current snapshot move the keys to Active Object Store.
      SnapshotInfo nextSnapshot = SnapshotUtils.getNextSnapshot(ozoneManager, snapshotChainManager, fromSnapshot);

      // If next snapshot is not active then ignore move. Since this could be a redundant operations.
      if (nextSnapshot != null && nextSnapshot.getSnapshotStatus() != SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE) {
        OMException ex = new OMException("Next snapshot : " + nextSnapshot + " in chain is not active.",
            OMException.ResultCodes.INVALID_SNAPSHOT_ERROR);
        if (LOG.isDebugEnabled()) {
          AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.SNAPSHOT_MOVE_TABLE_KEYS,
              null, ex));
        }
        throw ex;
      }

      List<SnapshotMoveKeyInfos> deletedKeysList = moveTableKeysRequest.getDeletedKeysList();
      List<SnapshotMoveKeyInfos> deletedDirsList = moveTableKeysRequest.getDeletedDirsList();
      List<HddsProtos.KeyValue> renamedKeysList = moveTableKeysRequest.getRenamedKeysList();
      OMSnapshotMoveUtils.updateCache(ozoneManager, fromSnapshot, nextSnapshot, context);
      omClientResponse = new OMSnapshotMoveTableKeysResponse(omResponse.build(),
          fromSnapshot, nextSnapshot, omBucketInfo.getObjectID(), moveTableKeysRequest.getDeletedKeysList(),
          moveTableKeysRequest.getDeletedDirsList(), moveTableKeysRequest.getRenamedKeysList());
      omSnapshotIntMetrics.incNumSnapshotMoveTableKeys();

      if (LOG.isDebugEnabled()) {
        Map<String, String> auditParams = new LinkedHashMap<>();
        auditParams.put(AUDIT_PARAM_FROM_SNAPSHOT_TABLE_KEY, snapshotChainManager.getTableKey(fromSnapshotID));
        if (nextSnapshot != null) {
          auditParams.put(AUDIT_PARAM_TO_SNAPSHOT_TABLE_KEY_OR_AOS, nextSnapshot.getTableKey());
        } else {
          auditParams.put(AUDIT_PARAM_TO_SNAPSHOT_TABLE_KEY_OR_AOS, AOS);
        }
        auditParams.put(AUDIT_PARAM_DEL_KEYS_MOVED, String.valueOf(deletedKeysList.size()));
        auditParams.put(AUDIT_PARAM_DEL_DIRS_MOVED, String.valueOf(deletedDirsList.size()));
        auditParams.put(AUDIT_PARAM_RENAMED_KEYS_MOVED, String.valueOf(renamedKeysList.size()));
        if (!deletedKeysList.isEmpty()) {
          auditParams.put(AUDIT_PARAM_DEL_KEYS_MOVED_LIST,
              deletedKeysList.stream().map(SnapshotMoveKeyInfos::getKey)
                  .collect(java.util.stream.Collectors.joining(",")));
        }
        if (!deletedDirsList.isEmpty()) {
          auditParams.put(AUDIT_PARAM_DEL_DIRS_MOVED_LIST,
              deletedDirsList.stream().map(SnapshotMoveKeyInfos::getKey)
                  .collect(java.util.stream.Collectors.joining(",")));
        }
        if (!renamedKeysList.isEmpty()) {
          auditParams.put(AUDIT_PARAM_RENAMED_KEYS_LIST, renamedKeysList.toString());
        }
        AUDIT.logWriteSuccess(ozoneManager.buildAuditMessageForSuccess(OMSystemAction.SNAPSHOT_MOVE_TABLE_KEYS,
            auditParams));
      }
    } catch (IOException ex) {
      omClientResponse = new OMSnapshotMoveTableKeysResponse(createErrorOMResponse(omResponse, ex));
      omSnapshotIntMetrics.incNumSnapshotMoveTableKeysFails();
      if (LOG.isDebugEnabled()) {
        AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.SNAPSHOT_MOVE_TABLE_KEYS,
            null, ex));
      }
    }
    return omClientResponse;
  }
}
