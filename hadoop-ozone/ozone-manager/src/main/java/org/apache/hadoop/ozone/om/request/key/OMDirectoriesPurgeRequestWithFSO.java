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
import java.util.ArrayList;
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
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
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
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
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
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    PurgeDirectoriesRequest purgeDirsRequest =
        getOmRequest().getPurgeDirectoriesRequest();
    String fromSnapshot = purgeDirsRequest.hasSnapshotTableKey() ?
        purgeDirsRequest.getSnapshotTableKey() : null;

    List<OzoneManagerProtocolProtos.PurgePathRequest> purgeRequests =
        purgeDirsRequest.getDeletedPathList();
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl) ozoneManager.getMetadataManager();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    DeletingServiceMetrics deletingServiceMetrics = ozoneManager.getDeletionMetrics();
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    PurgeApplyState state = new PurgeApplyState(omMetadataManager, omMetrics, context.getIndex());
    final SnapshotInfo fromSnapshotInfo;

    try {
      fromSnapshotInfo = resolveFromSnapshotInfo(ozoneManager, omMetadataManager, purgeDirsRequest, fromSnapshot);
    } catch (IOException e) {
      LOG.error("Error occurred while performing OMDirectoriesPurge. ", e);
      if (LOG.isDebugEnabled()) {
        AUDIT.logWriteFailure(ozoneManager.buildAuditMessageForFailure(OMSystemAction.DIRECTORY_DELETION, null, e));
      }
      return new OMDirectoriesPurgeResponseWithFSO(createErrorOMResponse(omResponse, e));
    }
    // Phase 1 (no lock): parse every purge entry and precompute its delete key, path key, replicated size and any
    // hsync open-key name. None of this depends on the bucket write lock, so doing it up front keeps the string and
    // protobuf work out of the critical section and shortens the write-lock hold.
    Map<VolumeBucketId, BucketNameInfo> volumeBucketIdMap = purgeDirsRequest.getBucketNameInfosList().stream()
        .collect(Collectors.toMap(bucketNameInfo ->
                new VolumeBucketId(bucketNameInfo.getVolumeId(), bucketNameInfo.getBucketId()),
            Function.identity()));
    for (OzoneManagerProtocolProtos.PurgePathRequest path : purgeRequests) {
      prepareMarkDeletedSubDirs(path, state);
      prepareMoveDeletedSubFiles(path, state);
      if (path.hasDeletedDir()) {
        BucketNameInfo bucketNameInfo = volumeBucketIdMap.get(new VolumeBucketId(path.getVolumeId(),
            path.getBucketId()));
        prepareDeletedDir(path, bucketNameInfo, state);
      }
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
      // Phase 2 (under the bucket write lock): apply the prepared cache tombstones, hsync open-key cleanup and
      // aggregated per-bucket quota changes.
      applyPreparedEntries(state);

      int numSubDirMoved = recordDeletionMetrics(deletingServiceMetrics, state);

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
        logDirectoryDeletionAuditSuccess(ozoneManager, fromSnapshotInfo, numSubDirMoved, state);
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
      if (lockAcquired) {
        mergeOmLockDetails(omMetadataManager.getLock().releaseWriteLocks(BUCKET_LOCK, bucketLockKeys));
      }
      // Snapshot the mutated bucket infos for the response after releasing the lock. The single apply thread is the
      // only writer, so no other transaction can mutate them between release and copy.
      for (Map.Entry<Pair<String, String>, OmBucketInfo> entry :
          state.volBucketInfoMap.entrySet()) {
        entry.setValue(entry.getValue().copyObject());
      }
    }

    return new OMDirectoriesPurgeResponseWithFSO(
        omResponse.build(), purgeRequests,
        getBucketLayout(), state.volBucketInfoMap, fromSnapshotInfo, state.openKeyInfoMap);
  }

  /**
   * De-duplicates purged directories from the moved sub-directory set and updates the deletion service metrics for
   * this transaction, returning the resulting number of sub-directories moved (used for the success audit).
   */
  private int recordDeletionMetrics(DeletingServiceMetrics deletingServiceMetrics, PurgeApplyState state) {
    // Apply the per-entry global OM metric mutations in bulk (one increment each instead of once per entry) to
    // minimize work done while holding the bucket write lock. The per-entry calls were unconditional, so the count
    // is the total number of prepared sub-directories and sub-files.
    long numKeysProcessed = (long) state.preparedSubDirs.size() + state.preparedSubFiles.size();
    state.omMetrics.decNumKeys(numKeysProcessed);
    state.omMetrics.incNumKeyDeletesInternal(numKeysProcessed);
    // Remove deletedDirNames from subDirNames to avoid duplication
    state.subDirNames.removeAll(state.deletedDirNames);
    int numSubDirMoved = state.subDirNames.size();
    deletingServiceMetrics.incrNumSubDirectoriesMoved(numSubDirMoved);
    deletingServiceMetrics.incrNumSubFilesMoved(state.numSubFilesMoved);
    deletingServiceMetrics.incrNumDirPurged(state.numDirsDeleted);
    return numSubDirMoved;
  }

  /**
   * Builds and emits the success audit message for a directory purge. Kept separate so the per-entry parameter map is
   * only assembled when debug audit logging is enabled.
   */
  private void logDirectoryDeletionAuditSuccess(OzoneManager ozoneManager, SnapshotInfo fromSnapshotInfo,
      int numSubDirMoved, PurgeApplyState state) {
    Map<String, String> auditParams = new LinkedHashMap<>();
    if (fromSnapshotInfo != null) {
      auditParams.put(AUDIT_PARAM_SNAPSHOT_ID, fromSnapshotInfo.getSnapshotId().toString());
    }
    auditParams.put(AUDIT_PARAM_DIRS_DELETED, String.valueOf(state.numDirsDeleted));
    auditParams.put(AUDIT_PARAM_SUBDIRS_MOVED, String.valueOf(numSubDirMoved));
    auditParams.put(AUDIT_PARAM_SUBFILES_MOVED, String.valueOf(state.numSubFilesMoved));
    auditParams.put(AUDIT_PARAM_DIRS_DELETED_LIST, String.join(",", state.deletedDirNames));
    auditParams.put(AUDIT_PARAM_SUBDIRS_MOVED_LIST, String.join(",", state.subDirNames));
    auditParams.put(AUDIT_PARAM_SUBFILES_MOVED_LIST, String.join(",", state.subFileNames));
    AUDIT.logWriteSuccess(ozoneManager.buildAuditMessageForSuccess(OMSystemAction.DIRECTORY_DELETION, auditParams));
  }

  /**
   * Resolves the {@code fromSnapshot} this purge runs against and, for new-format requests, validates that the
   * previous snapshot chain has not changed. Extracted so {@link #validateAndUpdateCache} reads as setup → per-path
   * apply → bookkeeping.
   */
  private SnapshotInfo resolveFromSnapshotInfo(OzoneManager ozoneManager, OmMetadataManagerImpl omMetadataManager,
      PurgeDirectoriesRequest purgeDirsRequest, String fromSnapshot) throws IOException {
    SnapshotInfo fromSnapshotInfo = fromSnapshot != null
        ? SnapshotUtils.getSnapshotInfo(ozoneManager, fromSnapshot) : null;
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
    return fromSnapshotInfo;
  }

  /**
   * Phase 1 (no lock): parses a path's sub-directories into prepared entries, precomputing each delete/path key. The
   * directory-table tombstones and bucket namespace decrements are applied later under the lock in
   * {@link #applyPreparedEntries}.
   */
  private void prepareMarkDeletedSubDirs(OzoneManagerProtocolProtos.PurgePathRequest path, PurgeApplyState state) {
    OmMetadataManagerImpl omMetadataManager = state.omMetadataManager;
    for (OzoneManagerProtocolProtos.KeyInfo key : path.getMarkDeletedSubDirsList()) {
      ProcessedKeyInfo processed = processDeleteKey(key, path, omMetadataManager);
      state.preparedSubDirs.add(new PreparedEntry(processed, path.getBucketId(), 0L, null));
    }
  }

  /**
   * Phase 1 (no lock): parses a path's sub-files into prepared entries, precomputing each delete/path key, its
   * replicated size and, for hsync files, the open-key name to clean up. The file-table tombstones, hsync open-key
   * cleanup and bucket quota decrements are applied later under the lock in {@link #applyPreparedEntries}.
   */
  private void prepareMoveDeletedSubFiles(OzoneManagerProtocolProtos.PurgePathRequest path, PurgeApplyState state) {
    OmMetadataManagerImpl omMetadataManager = state.omMetadataManager;
    for (OzoneManagerProtocolProtos.KeyInfo key : path.getDeletedSubFilesList()) {
      ProcessedKeyInfo processed = processDeleteKey(key, path, omMetadataManager);
      long replicatedSize = sumBlockLengths(processed.keyInfo);
      // If omKeyInfo has hsync metadata, its corresponding open key is cleaned up under the lock.
      String hsyncClientId = getHsyncClientId(processed.keyInfo);
      String dbOpenKey = hsyncClientId == null ? null
          : omMetadataManager.getOpenFileName(path.getVolumeId(), path.getBucketId(),
          processed.parentObjectID, processed.fileName, hsyncClientId);
      state.preparedSubFiles.add(new PreparedEntry(processed, path.getBucketId(), replicatedSize, dbOpenKey));
    }
  }

  /**
   * Phase 1 (no lock): records a path's deleted directory for later purge under the lock in
   * {@link #applyPreparedEntries}.
   */
  private void prepareDeletedDir(OzoneManagerProtocolProtos.PurgePathRequest path, BucketNameInfo bucketNameInfo,
      PurgeApplyState state) {
    state.preparedDirPurges.add(new PreparedDirPurge(bucketNameInfo.getVolumeName(), bucketNameInfo.getBucketName(),
        path.getBucketId(), path.getDeletedDir()));
  }

  /**
   * Phase 2 (under the bucket write lock): applies the prepared directory/file tombstones, hsync open-key cleanup and
   * per-bucket quota changes. Quota deltas are accumulated per bucket and applied once, instead of once per entry, to
   * minimize the mutations done while holding the write lock.
   */
  private void applyPreparedEntries(PurgeApplyState state) throws IOException {
    OmMetadataManagerImpl omMetadataManager = state.omMetadataManager;
    Map<Pair<String, String>, QuotaDelta> quotaDeltas = new HashMap<>();

    for (PreparedEntry entry : state.preparedSubDirs) {
      ProcessedKeyInfo processed = entry.processed;
      state.subDirNames.add(processed.deleteKey);
      OmBucketInfo omBucketInfo = getBucketInfoCached(omMetadataManager,
          state.bucketInfoCache, processed.volumeName, processed.bucketName);
      // bucketInfo can be null in case of delete volume or bucket
      // or key does not belong to bucket as bucket is recreated
      if (null != omBucketInfo && omBucketInfo.getObjectID() == entry.bucketId) {
        omMetadataManager.getDirectoryTable().addCacheEntry(new CacheKey<>(processed.pathKey),
            CacheValue.get(state.trxnLogIndex));
        state.volBucketInfoMap.putIfAbsent(processed.volBucketPair, omBucketInfo);
        quotaDeltas.computeIfAbsent(processed.volBucketPair, k -> new QuotaDelta(omBucketInfo)).usedNamespace += 1L;
      }
    }

    for (PreparedEntry entry : state.preparedSubFiles) {
      ProcessedKeyInfo processed = entry.processed;
      state.subFileNames.add(processed.deleteKey);

      // If omKeyInfo has hsync metadata, delete its corresponding open key as well
      if (entry.dbOpenKey != null) {
        OmKeyInfo openKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout()).get(entry.dbOpenKey);
        if (openKeyInfo != null) {
          openKeyInfo = openKeyInfo.withMetadataMutations(
              metadata -> metadata.put(DELETED_HSYNC_KEY, "true"));
          state.openKeyInfoMap.put(entry.dbOpenKey, openKeyInfo);
        }
      }

      state.numSubFilesMoved++;
      OmBucketInfo omBucketInfo = getBucketInfoCached(omMetadataManager,
          state.bucketInfoCache, processed.volumeName, processed.bucketName);
      // bucketInfo can be null in case of delete volume or bucket
      // or key does not belong to bucket as bucket is recreated
      if (null != omBucketInfo && omBucketInfo.getObjectID() == entry.bucketId) {
        omMetadataManager.getFileTable().addCacheEntry(new CacheKey<>(processed.pathKey),
            CacheValue.get(state.trxnLogIndex));
        state.volBucketInfoMap.putIfAbsent(processed.volBucketPair, omBucketInfo);
        QuotaDelta quotaDelta = quotaDeltas.computeIfAbsent(processed.volBucketPair, k -> new QuotaDelta(omBucketInfo));
        quotaDelta.usedBytes += entry.replicatedSize;
        quotaDelta.usedNamespace += 1L;
      }
    }

    for (PreparedDirPurge dirPurge : state.preparedDirPurges) {
      state.deletedDirNames.add(dirPurge.deletedDir);
      OmBucketInfo omBucketInfo = getBucketInfoCached(omMetadataManager,
          state.bucketInfoCache, dirPurge.volumeName, dirPurge.bucketName);
      if (omBucketInfo != null && omBucketInfo.getObjectID() == dirPurge.bucketId) {
        Pair<String, String> volBucketPair = Pair.of(omBucketInfo.getVolumeName(), omBucketInfo.getBucketName());
        state.volBucketInfoMap.put(volBucketPair, omBucketInfo);
        quotaDeltas.computeIfAbsent(volBucketPair, k -> new QuotaDelta(omBucketInfo)).snapshotNamespacePurge += 1L;
      }
      state.numDirsDeleted++;
    }

    for (QuotaDelta quotaDelta : quotaDeltas.values()) {
      if (quotaDelta.usedBytes != 0L) {
        quotaDelta.bucketInfo.decrUsedBytes(quotaDelta.usedBytes, true);
      }
      if (quotaDelta.usedNamespace != 0L) {
        quotaDelta.bucketInfo.decrUsedNamespace(quotaDelta.usedNamespace, true);
      }
      if (quotaDelta.snapshotNamespacePurge != 0L) {
        quotaDelta.bucketInfo.purgeSnapshotUsedNamespace(quotaDelta.snapshotNamespacePurge);
      }
    }
  }

  /**
   * Mutable state accumulated while applying a directory purge, shared across the phase 1 prepare steps
   * ({@link #prepareMarkDeletedSubDirs}, {@link #prepareMoveDeletedSubFiles}, {@link #prepareDeletedDir}) and the
   * phase 2 apply step ({@link #applyPreparedEntries}).
   */
  private static final class PurgeApplyState {
    private final OmMetadataManagerImpl omMetadataManager;
    private final OMMetrics omMetrics;
    private final long trxnLogIndex;
    private final Map<Pair<String, String>, OmBucketInfo> volBucketInfoMap = new HashMap<>();
    // Memoizes getBucketInfo lookups within this apply so that a purge transaction touching many keys of the same
    // bucket resolves the bucket cache entry once instead of per key.
    private final Map<Pair<String, String>, OmBucketInfo> bucketInfoCache = new HashMap<>();
    private final Map<String, OmKeyInfo> openKeyInfoMap = new HashMap<>();
    // Prepared (lock-free) work built in phase 1 and applied under the bucket write lock in phase 2.
    private final List<PreparedEntry> preparedSubDirs = new ArrayList<>();
    private final List<PreparedEntry> preparedSubFiles = new ArrayList<>();
    private final List<PreparedDirPurge> preparedDirPurges = new ArrayList<>();
    private final Set<String> subDirNames = new HashSet<>();
    private final Set<String> subFileNames = new HashSet<>();
    private final Set<String> deletedDirNames = new HashSet<>();
    private int numSubFilesMoved;
    private int numDirsDeleted;

    PurgeApplyState(OmMetadataManagerImpl omMetadataManager, OMMetrics omMetrics, long trxnLogIndex) {
      this.omMetadataManager = omMetadataManager;
      this.omMetrics = omMetrics;
      this.trxnLogIndex = trxnLogIndex;
    }
  }

  /**
   * Helper class to hold processed key information.
   */
  private static class ProcessedKeyInfo {
    private final OzoneManagerProtocolProtos.KeyInfo keyInfo;
    private final String deleteKey;
    private final String pathKey;
    private final String volumeName;
    private final String bucketName;
    private final long parentObjectID;
    private final String fileName;
    private final Pair<String, String> volBucketPair;

    ProcessedKeyInfo(OzoneManagerProtocolProtos.KeyInfo keyInfo, String deleteKey, String pathKey, String volumeName,
                     String bucketName, long parentObjectID, String fileName) {
      this.keyInfo = keyInfo;
      this.deleteKey = deleteKey;
      this.pathKey = pathKey;
      this.volumeName = volumeName;
      this.bucketName = bucketName;
      this.parentObjectID = parentObjectID;
      this.fileName = fileName;
      this.volBucketPair = Pair.of(volumeName, bucketName);
    }
  }

  /**
   * A sub-directory or sub-file prepared (lock-free) in phase 1 for application under the bucket write lock in phase
   * 2. {@code replicatedSize} is the file's replicated byte usage (0 for directories) and {@code dbOpenKey} is the
   * hsync open-key to clean up, or {@code null} when the entry is not an hsync file.
   */
  private static final class PreparedEntry {
    private final ProcessedKeyInfo processed;
    private final long bucketId;
    private final long replicatedSize;
    private final String dbOpenKey;

    PreparedEntry(ProcessedKeyInfo processed, long bucketId, long replicatedSize, String dbOpenKey) {
      this.processed = processed;
      this.bucketId = bucketId;
      this.replicatedSize = replicatedSize;
      this.dbOpenKey = dbOpenKey;
    }
  }

  /**
   * A deleted directory prepared (lock-free) in phase 1 for its snapshot-namespace purge under the bucket write lock
   * in phase 2.
   */
  private static final class PreparedDirPurge {
    private final String volumeName;
    private final String bucketName;
    private final long bucketId;
    private final String deletedDir;

    PreparedDirPurge(String volumeName, String bucketName, long bucketId, String deletedDir) {
      this.volumeName = volumeName;
      this.bucketName = bucketName;
      this.bucketId = bucketId;
      this.deletedDir = deletedDir;
    }
  }

  /**
   * Accumulates a single bucket's quota changes across all entries in a purge so they can be applied once under the
   * write lock instead of once per entry.
   */
  private static final class QuotaDelta {
    private final OmBucketInfo bucketInfo;
    private long usedBytes;
    private long usedNamespace;
    private long snapshotNamespacePurge;

    QuotaDelta(OmBucketInfo bucketInfo) {
      this.bucketInfo = bucketInfo;
    }
  }

  /**
   * Process delete key info.
   * Reads only the fields the purge apply path needs directly from the protobuf, instead of building a full
   * {@link OmKeyInfo} (with its key-location, ACL, tag, encryption and checksum objects) for every entry on the
   * single-threaded apply path.
   */
  private ProcessedKeyInfo processDeleteKey(OzoneManagerProtocolProtos.KeyInfo key,
                                            OzoneManagerProtocolProtos.PurgePathRequest path,
                                            OmMetadataManagerImpl omMetadataManager) {
    long objectID = key.hasObjectID() ? key.getObjectID() : 0L;
    long parentObjectID = key.hasParentID() ? key.getParentID() : 0L;
    String fileName = OzoneFSUtils.getFileName(key.getKeyName());

    String pathKey = omMetadataManager.getOzonePathKey(path.getVolumeId(),
        path.getBucketId(), parentObjectID, fileName);
    String deleteKey = omMetadataManager.getOzoneDeletePathKey(objectID, pathKey);

    return new ProcessedKeyInfo(key, deleteKey, pathKey, key.getVolumeName(), key.getBucketName(),
        parentObjectID, fileName);
  }

  /**
   * Returns the cached bucket info for the given volume/bucket, memoizing the lookup within a single apply so that a
   * purge transaction touching many keys of the same bucket does the {@link #getBucketInfo} cache lookup once. The
   * returned instance is the same cached reference {@link #getBucketInfo} returns, so in-place quota mutations behave
   * identically. {@code null} results (deleted bucket) are memoized too.
   */
  private static OmBucketInfo getBucketInfoCached(OmMetadataManagerImpl omMetadataManager,
      Map<Pair<String, String>, OmBucketInfo> cache, String volumeName, String bucketName) {
    Pair<String, String> cacheKey = Pair.of(volumeName, bucketName);
    if (cache.containsKey(cacheKey)) {
      return cache.get(cacheKey);
    }
    OmBucketInfo omBucketInfo = getBucketInfo(omMetadataManager, volumeName, bucketName);
    cache.put(cacheKey, omBucketInfo);
    return omBucketInfo;
  }

  /**
   * Reads the HSYNC client id directly from the key's protobuf metadata, mirroring
   * {@code KeyValueUtil.getFromProtobuf(...).get(HSYNC_CLIENT_ID)} without building the full metadata map.
   */
  private static String getHsyncClientId(OzoneManagerProtocolProtos.KeyInfo key) {
    String hsyncClientId = null;
    for (HddsProtos.KeyValue kv : key.getMetadataList()) {
      if (OzoneConsts.HSYNC_CLIENT_ID.equals(kv.getKey())) {
        hsyncClientId = kv.getValue();
      }
    }
    return hsyncClientId;
  }

  /**
   * Computes replicated byte usage for a file directly from its protobuf key locations. This is equivalent to
   * {@link OMKeyRequest#sumBlockLengths(OmKeyInfo)} on the parsed key: {@code getFromProtobuf} only regroups the same
   * locations by create-version, so summing every {@code KeyLocation} length yields the identical total while avoiding
   * the full OmKeyInfo build on the apply path.
   */
  private static long sumBlockLengths(OzoneManagerProtocolProtos.KeyInfo key) {
    ReplicationConfig replicationConfig = ReplicationConfig.fromProto(key.getType(), key.getFactor(),
        key.getEcReplicationConfig());
    long bytesUsed = 0;
    for (OzoneManagerProtocolProtos.KeyLocationList group : key.getKeyLocationListList()) {
      for (OzoneManagerProtocolProtos.KeyLocation location : group.getKeyLocationsList()) {
        bytesUsed += QuotaUtil.getReplicatedSize(location.getLength(), replicationConfig);
      }
    }
    return bytesUsed;
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
