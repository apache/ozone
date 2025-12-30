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
import static org.apache.hadoop.ozone.OzoneConsts.DELETED_HSYNC_KEY;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.validatePreviousSnapshotId;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.om.DeletingServiceMetrics;
import org.apache.hadoop.ozone.om.OMMetadataManager.VolumeBucketId;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMDirectoriesPurgeResponseWithFSO;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketNameInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeDirectoriesRequest;

/**
 * Handles purging of keys from OM DB.
 */
public class OMDirectoriesPurgeRequestWithFSO extends OMKeyRequest {
  private static final AuditLogger AUDIT = new AuditLogger(AuditLoggerType.OMSYSTEMLOGGER);
  private static final String AUDIT_PARAM_DIRS_DELETED = "directoriesDeleted";
  private static final String AUDIT_PARAM_SUBDIRS_MOVED = "subdirectoriesMoved";
  private static final String AUDIT_PARAM_SUBFILES_MOVED = "subFilesMoved";
  private static final String AUDIT_PARAM_DIRS_DELETED_LIST = "directoriesDeletedList";
  private static final String AUDIT_PARAM_SUBDIRS_MOVED_LIST = "subdirectoriesMovedList";
  private static final String AUDIT_PARAM_SUBFILES_MOVED_LIST = "subFilesMovedList";
  private static final String AUDIT_PARAM_SNAPSHOT_ID = "snapshotId";

  public OMDirectoriesPurgeRequestWithFSO(OMRequest omRequest) {
    super(omRequest, BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    PurgeDirectoriesRequest purgeDirsRequest =
        getOmRequest().getPurgeDirectoriesRequest();
    String fromSnapshot = purgeDirsRequest.hasSnapshotTableKey() ?
        purgeDirsRequest.getSnapshotTableKey() : null;

    List<OzoneManagerProtocolProtos.PurgePathRequest> purgeRequests =
        purgeDirsRequest.getDeletedPathList();
    Map<Pair<String, String>, OmBucketInfo> volBucketInfoMap = new HashMap<>();
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl) ozoneManager.getMetadataManager();
    Map<String, OmKeyInfo> openKeyInfoMap = new HashMap<>();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    DeletingServiceMetrics deletingServiceMetrics = ozoneManager.getDeletionMetrics();
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    final SnapshotInfo fromSnapshotInfo;

    Set<String> subDirNames = new HashSet<>();
    Set<String> subFileNames = new HashSet<>();
    Set<String> deletedDirNames = new HashSet<>();

    try {
      fromSnapshotInfo = fromSnapshot != null ? SnapshotUtils.getSnapshotInfo(ozoneManager,
          fromSnapshot) : null;
      // Checking if this request is an old request or new one.
      if (purgeDirsRequest.hasExpectedPreviousSnapshotID()) {
        // Validating previous snapshot since while purging deletes, a snapshot create request could make this purge
        // directory request invalid on AOS since the deletedDirectory would be in the newly created snapshot. Adding
        // subdirectories could lead to not being able to reclaim sub-files and subdirectories since the
        // file/directory would be present in the newly created snapshot.
        // Validating previous snapshot can ensure the chain hasn't changed.
        UUID expectedPreviousSnapshotId = purgeDirsRequest.getExpectedPreviousSnapshotID().hasUuid()
            ? fromProtobuf(purgeDirsRequest.getExpectedPreviousSnapshotID().getUuid()) : null;
        validatePreviousSnapshotId(fromSnapshotInfo, omMetadataManager.getSnapshotChainManager(),
            expectedPreviousSnapshotId);
      }
    } catch (IOException e) {
      LOG.error("Error occurred while performing OMDirectoriesPurge. ", e);
      if (LOG.isDebugEnabled()) {
        AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.DIRECTORY_DELETION, null, e));
      }
      return new OMDirectoriesPurgeResponseWithFSO(createErrorOMResponse(omResponse, e));
    }
    List<String[]> bucketLockKeys = getBucketLockKeySet(purgeDirsRequest);
    mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLocks(BUCKET_LOCK, bucketLockKeys));
    boolean lockAcquired = getOmLockDetails().isLockAcquired();
    if (!lockAcquired && !purgeDirsRequest.getBucketNameInfosList().isEmpty()) {
      OMException oe = new OMException("Unable to acquire write locks on buckets while performing DirectoryPurge",
          OMException.ResultCodes.KEY_DELETION_ERROR);
      LOG.error("Error occurred while performing OMDirectoriesPurge. ", oe);
      AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.DIRECTORY_DELETION, null, oe));
      return new OMDirectoriesPurgeResponseWithFSO(createErrorOMResponse(omResponse, oe));
    }
    try {
      int numSubDirMoved = 0, numSubFilesMoved = 0, numDirsDeleted = 0;
      Map<VolumeBucketId, BucketNameInfo> volumeBucketIdMap = purgeDirsRequest.getBucketNameInfosList().stream()
          .collect(Collectors.toMap(bucketNameInfo ->
                  new VolumeBucketId(bucketNameInfo.getVolumeId(), bucketNameInfo.getBucketId()),
              Function.identity()));
      for (OzoneManagerProtocolProtos.PurgePathRequest path : purgeRequests) {
        for (OzoneManagerProtocolProtos.KeyInfo key : path.getMarkDeletedSubDirsList()) {
          ProcessedKeyInfo processed = processDeleteKey(key, path, omMetadataManager);
          subDirNames.add(processed.deleteKey);

          omMetrics.decNumKeys();
          omMetrics.incNumKeyDeletesInternal();
          OmBucketInfo omBucketInfo = getBucketInfo(omMetadataManager,
              processed.volumeName, processed.bucketName);
          // bucketInfo can be null in case of delete volume or bucket
          // or key does not belong to bucket as bucket is recreated
          if (null != omBucketInfo && omBucketInfo.getObjectID() == path.getBucketId()) {
            omBucketInfo.decrUsedNamespace(1L, true);
            String ozoneDbKey = omMetadataManager.getOzonePathKey(path.getVolumeId(),
                path.getBucketId(), processed.keyInfo.getParentObjectID(),
                processed.keyInfo.getFileName());
            omMetadataManager.getDirectoryTable().addCacheEntry(new CacheKey<>(ozoneDbKey),
                CacheValue.get(context.getIndex()));
            volBucketInfoMap.putIfAbsent(processed.volBucketPair, omBucketInfo);
          }
        }

        for (OzoneManagerProtocolProtos.KeyInfo key : path.getDeletedSubFilesList()) {
          ProcessedKeyInfo processed = processDeleteKey(key, path, omMetadataManager);
          subFileNames.add(processed.deleteKey);

          // If omKeyInfo has hsync metadata, delete its corresponding open key as well
          String dbOpenKey;
          String hsyncClientId = processed.keyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
          if (hsyncClientId != null) {
            long parentId = processed.keyInfo.getParentObjectID();
            dbOpenKey = omMetadataManager.getOpenFileName(path.getVolumeId(), path.getBucketId(),
                parentId, processed.keyInfo.getFileName(), hsyncClientId);
            OmKeyInfo openKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOpenKey);
            if (openKeyInfo != null) {
              openKeyInfo = openKeyInfo.withMetadataMutations(
                  metadata -> metadata.put(DELETED_HSYNC_KEY, "true"));
              openKeyInfoMap.put(dbOpenKey, openKeyInfo);
            }
          }

          omMetrics.decNumKeys();
          omMetrics.incNumKeyDeletesInternal();
          numSubFilesMoved++;
          OmBucketInfo omBucketInfo = getBucketInfo(omMetadataManager,
              processed.volumeName, processed.bucketName);
          // bucketInfo can be null in case of delete volume or bucket
          // or key does not belong to bucket as bucket is recreated
          if (null != omBucketInfo
              && omBucketInfo.getObjectID() == path.getBucketId()) {
            long totalSize = sumBlockLengths(processed.keyInfo);
            omBucketInfo.decrUsedBytes(totalSize, true);
            omBucketInfo.decrUsedNamespace(1L, true);
            String ozoneDbKey = omMetadataManager.getOzonePathKey(path.getVolumeId(),
                path.getBucketId(), processed.keyInfo.getParentObjectID(),
                processed.keyInfo.getFileName());
            omMetadataManager.getFileTable().addCacheEntry(new CacheKey<>(ozoneDbKey),
                CacheValue.get(context.getIndex()));
            volBucketInfoMap.putIfAbsent(processed.volBucketPair, omBucketInfo);
          }
        }
        if (path.hasDeletedDir()) {
          deletedDirNames.add(path.getDeletedDir());
          BucketNameInfo bucketNameInfo = volumeBucketIdMap.get(new VolumeBucketId(path.getVolumeId(),
              path.getBucketId()));
          OmBucketInfo omBucketInfo = getBucketInfo(omMetadataManager,
              bucketNameInfo.getVolumeName(), bucketNameInfo.getBucketName());
          if (omBucketInfo != null && omBucketInfo.getObjectID() == path.getBucketId()) {
            omBucketInfo.purgeSnapshotUsedNamespace(1);
            volBucketInfoMap.put(Pair.of(omBucketInfo.getVolumeName(), omBucketInfo.getBucketName()), omBucketInfo);
          }
          numDirsDeleted++;
        }
      }

      // Remove deletedDirNames from subDirNames to avoid duplication
      subDirNames.removeAll(deletedDirNames);
      numSubDirMoved = subDirNames.size();
      deletingServiceMetrics.incrNumSubDirectoriesMoved(numSubDirMoved);
      deletingServiceMetrics.incrNumSubFilesMoved(numSubFilesMoved);
      deletingServiceMetrics.incrNumDirPurged(numDirsDeleted);

      TransactionInfo transactionInfo = TransactionInfo.valueOf(context.getTermIndex());
      if (fromSnapshotInfo != null) {
        fromSnapshotInfo.setLastTransactionInfo(transactionInfo.toByteString());
        omMetadataManager.getSnapshotInfoTable().addCacheEntry(new CacheKey<>(fromSnapshotInfo.getTableKey()),
            CacheValue.get(context.getIndex(), fromSnapshotInfo));
      } else {
        // Update the deletingServiceMetrics with the transaction index to indicate the
        // last purge transaction when running for AOS
        deletingServiceMetrics.setLastAOSTransactionInfo(transactionInfo);
      }

      if (LOG.isDebugEnabled()) {
        Map<String, String> auditParams = new LinkedHashMap<>();
        if (fromSnapshotInfo != null) {
          auditParams.put(AUDIT_PARAM_SNAPSHOT_ID, fromSnapshotInfo.getSnapshotId().toString());
        }
        auditParams.put(AUDIT_PARAM_DIRS_DELETED, String.valueOf(numDirsDeleted));
        auditParams.put(AUDIT_PARAM_SUBDIRS_MOVED, String.valueOf(numSubDirMoved));
        auditParams.put(AUDIT_PARAM_SUBFILES_MOVED, String.valueOf(numSubFilesMoved));
        auditParams.put(AUDIT_PARAM_DIRS_DELETED_LIST, String.join(",", deletedDirNames));
        auditParams.put(AUDIT_PARAM_SUBDIRS_MOVED_LIST, String.join(",", subDirNames));
        auditParams.put(AUDIT_PARAM_SUBFILES_MOVED_LIST, String.join(",", subFileNames));
        AUDIT.logWriteSuccess(ozoneManager.buildAuditMessageForSuccess(OMSystemAction.DIRECTORY_DELETION, auditParams));
      }
    } catch (IOException ex) {
      // Case of IOException for fromProtobuf will not happen
      // as this is created and send within OM
      // only case of upgrade where compatibility is broken can have
      if (LOG.isDebugEnabled()) {
        AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.DIRECTORY_DELETION, null, ex));
      }
      throw new IllegalStateException(ex);
    } finally {
      for (Map.Entry<Pair<String, String>, OmBucketInfo> entry :
          volBucketInfoMap.entrySet()) {
        entry.setValue(entry.getValue().copyObject());
      }
      if (lockAcquired) {
        mergeOmLockDetails(omMetadataManager.getLock().releaseWriteLocks(BUCKET_LOCK, bucketLockKeys));
      }  
    }

    return new OMDirectoriesPurgeResponseWithFSO(
        omResponse.build(), purgeRequests,
        getBucketLayout(), volBucketInfoMap, fromSnapshotInfo, openKeyInfoMap);
  }

  /**
   * Helper class to hold processed key information.
   */
  private static class ProcessedKeyInfo {
    private final OmKeyInfo keyInfo;
    private final String deleteKey;
    private final String volumeName;
    private final String bucketName;
    private final Pair<String, String> volBucketPair;

    ProcessedKeyInfo(OmKeyInfo keyInfo, String deleteKey, String volumeName, String bucketName) {
      this.keyInfo = keyInfo;
      this.deleteKey = deleteKey;
      this.volumeName = volumeName;
      this.bucketName = bucketName;
      this.volBucketPair = Pair.of(volumeName, bucketName);
    }
  }

  /**
   * Process delete key info.
   * Returns ProcessedKeyInfo containing all the processed information.
   */
  private ProcessedKeyInfo processDeleteKey(OzoneManagerProtocolProtos.KeyInfo key,
                                            OzoneManagerProtocolProtos.PurgePathRequest path,
                                            OmMetadataManagerImpl omMetadataManager) {
    OmKeyInfo keyInfo = OmKeyInfo.getFromProtobuf(key);

    String pathKey = omMetadataManager.getOzonePathKey(path.getVolumeId(),
        path.getBucketId(), keyInfo.getParentObjectID(), keyInfo.getFileName());
    String deleteKey = omMetadataManager.getOzoneDeletePathKey(
        keyInfo.getObjectID(), pathKey);

    String volumeName = keyInfo.getVolumeName();
    String bucketName = keyInfo.getBucketName();

    return new ProcessedKeyInfo(keyInfo, deleteKey, volumeName, bucketName);
  }

  private List<String[]> getBucketLockKeySet(PurgeDirectoriesRequest purgeDirsRequest) {
    if (!purgeDirsRequest.getBucketNameInfosList().isEmpty()) {
      return purgeDirsRequest.getBucketNameInfosList().stream()
          .map(keyInfo -> Pair.of(keyInfo.getVolumeName(), keyInfo.getBucketName()))
          .distinct()
          .map(pair -> new String[]{pair.getLeft(), pair.getRight()})
          .collect(Collectors.toList());
    }

    return purgeDirsRequest.getDeletedPathList().stream()
        .flatMap(purgePathRequest -> Stream.concat(purgePathRequest.getDeletedSubFilesList().stream(),
            purgePathRequest.getMarkDeletedSubDirsList().stream()))
        .map(keyInfo -> Pair.of(keyInfo.getVolumeName(), keyInfo.getBucketName()))
        .distinct()
        .map(pair -> new String[]{pair.getLeft(), pair.getRight()})
        .collect(Collectors.toList());
  }

}
