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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_CLOSED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_UNDER_LEASE_RECOVERY;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmFSOFile;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmKeyHSyncUtil;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles CommitKey request - prefix layout.
 */
public class OMKeyCommitRequestWithFSO extends OMKeyCommitRequest {

  @VisibleForTesting
  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCommitRequestWithFSO.class);

  public OMKeyCommitRequestWithFSO(OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long trxnLogIndex = context.getIndex();

    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();

    KeyArgs commitKeyArgs = commitKeyRequest.getKeyArgs();

    String volumeName = commitKeyArgs.getVolumeName();
    String bucketName = commitKeyArgs.getBucketName();
    String keyName = commitKeyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();

    Map<String, String> auditMap = buildKeyArgsAuditMap(commitKeyArgs);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
            getOmRequest());

    Exception exception = null;
    PreparedCommit prepared = new PreparedCommit();
    OMClientResponse omClientResponse = null;
    boolean bucketLockAcquired = false;
    Result result;
    boolean isHSync = commitKeyRequest.hasHsync() && commitKeyRequest.getHsync();
    boolean isRecovery = commitKeyRequest.hasRecovery() && commitKeyRequest.getRecovery();
    // isHsync = true, a commit request as a result of client side hsync call
    // isRecovery = true, a commit request as a result of client side recoverLease call
    // none of isHsync and isRecovery is true, a commit request as a result of client side normal
    // outputStream#close call.
    if (isHSync) {
      omMetrics.incNumKeyHSyncs();
    } else {
      omMetrics.incNumKeyCommits();
    }

    LOG.debug("isHSync = {}, isRecovery = {}, volumeName = {}, bucketName = {}, keyName = {}",
        isHSync, isRecovery, volumeName, bucketName, keyName);

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
      prepareCommit(ozoneManager, commitKeyRequest, prepared,
          getOmKeyLocationInfos(ozoneManager, commitKeyArgs), auditMap, trxnLogIndex);

      // Phase 2 (bucket write lock): re-check the commit target, then apply the quota
      // read-modify-publish and all cache mutations. The lock covers only this mutation tail, so
      // bucket read-lock holders still observe each transaction atomically (all mutations or none)
      // but are not blocked for the Phase 1 path walk.
      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(BUCKET_LOCK, volumeName, bucketName));
      bucketLockAcquired = getOmLockDetails().isLockAcquired();

      omClientResponse = applyKeyCommit(ozoneManager, commitKeyRequest, prepared,
          trxnLogIndex, omResponse);

      result = Result.SUCCESS;
    } catch (IOException | InvalidPathException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyCommitResponseWithFSO(createErrorOMResponse(
              omResponse, exception), getBucketLayout());
    } finally {
      if (bucketLockAcquired) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(BUCKET_LOCK, volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    auditAndLogResult(ozoneManager, commitKeyRequest, prepared, auditMap, exception, result);

    return omClientResponse;
  }

  /**
   * Phase 1 (no bucket lock): resolves the parent path, reads the open and committed keys and runs the
   * commit guards, filling {@code prepared} as it goes. All reads observe committed state only. The OM
   * apply path is single-threaded (OzoneManagerStateMachine uses a single-thread executor), so no other
   * transaction can change what these reads observe before Phase 2 mutates; the double-buffer
   * flush/cleanup threads only materialize already-committed epochs and never alter a key's visible
   * value.
   * <p>
   * Keeping the parent walk (OmFSOFile.build -> getParentID) out of the bucket write lock is the point
   * of HDDS-16289: it stops the lone apply thread from gating readers of a hot bucket during path
   * resolution. If OM ever applies transactions in parallel per bucket/key, these reads must be
   * re-validated under the lock in {@link #applyKeyCommit}.
   */
  private void prepareCommit(OzoneManager ozoneManager, CommitKeyRequest commitKeyRequest,
      PreparedCommit prepared, List<OmKeyLocationInfo> locationInfoList,
      Map<String, String> auditMap, long trxnLogIndex) throws IOException {
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    KeyArgs commitKeyArgs = commitKeyRequest.getKeyArgs();
    String volumeName = commitKeyArgs.getVolumeName();
    String bucketName = commitKeyArgs.getBucketName();
    String keyName = commitKeyArgs.getKeyName();
    final boolean isHSync = commitKeyRequest.hasHsync() && commitKeyRequest.getHsync();
    final boolean isRecovery = commitKeyRequest.hasRecovery() && commitKeyRequest.getRecovery();

    validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

    String errMsg = "Cannot create file : " + keyName
            + " as parent directory doesn't exist";
    OmFSOFile fsoFile =  new OmFSOFile.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOmMetadataManager(omMetadataManager)
        .setErrMsg(errMsg)
        .build();

    prepared.fileName = fsoFile.getFileName();
    prepared.volumeId = fsoFile.getVolumeId();
    String dbFileKey = fsoFile.getOzonePathKey();
    prepared.dbFileKey = dbFileKey;
    OmKeyInfo keyToDelete =
        omMetadataManager.getKeyTable(getBucketLayout()).get(dbFileKey);
    prepared.keyToDelete = keyToDelete;
    long writerClientId = commitKeyRequest.getClientID();
    final String clientIdString = String.valueOf(writerClientId);
    if (null != keyToDelete) {
      prepared.isSameHsyncKey = java.util.Optional.of(keyToDelete)
          .map(WithMetadata::getMetadata)
          .map(meta -> meta.get(OzoneConsts.HSYNC_CLIENT_ID))
          .filter(id -> id.equals(clientIdString))
          .isPresent();
      if (!prepared.isSameHsyncKey) {
        prepared.isOverwrittenHsyncKey = java.util.Optional.of(keyToDelete)
            .map(WithMetadata::getMetadata)
            .map(meta -> meta.get(OzoneConsts.HSYNC_CLIENT_ID))
            .filter(id -> !id.equals(clientIdString))
            .isPresent() && !isRecovery;
      }
    }

    if (isRecovery && keyToDelete != null) {
      String clientId = keyToDelete.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
      if (clientId == null) {
        throw new OMException("Failed to recovery key, as " +
            dbFileKey + " is already closed", KEY_ALREADY_CLOSED);
      }
      writerClientId = Long.parseLong(clientId);
    }
    final String dbOpenFileKey = fsoFile.getOpenFileName(writerClientId);
    prepared.dbOpenFileKey = dbOpenFileKey;
    prepared.omKeyInfo = OMFileRequest.getOmKeyInfoFromFileTable(true,
            omMetadataManager, dbOpenFileKey, keyName);
    if (prepared.omKeyInfo == null) {
      String action = isRecovery ? "recovery" : isHSync ? "hsync" : "commit";
      throw new OMException("Failed to " + action + " key, as " +
          dbOpenFileKey + " entry is not found in the OpenKey table", KEY_NOT_FOUND);
    } else if (prepared.omKeyInfo.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY) ||
        prepared.omKeyInfo.getMetadata().containsKey(OzoneConsts.OVERWRITTEN_HSYNC_KEY)) {
      throw new OMException("Open Key " + keyName + " is already deleted/overwritten",
          KEY_NOT_FOUND);
    }

    if (prepared.omKeyInfo.getMetadata().containsKey(OzoneConsts.LEASE_RECOVERY) &&
        prepared.omKeyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID)) {
      if (!isRecovery) {
        throw new OMException("Cannot commit key " + dbOpenFileKey + " with " + OzoneConsts.LEASE_RECOVERY +
            " metadata while recovery flag is not set in request", KEY_UNDER_LEASE_RECOVERY);
      }
    }

    if (prepared.isOverwrittenHsyncKey) {
      // find the overwritten openKey and add OVERWRITTEN_HSYNC_KEY to it.
      prepared.dbOpenKeyToDeleteKey = fsoFile.getOpenFileName(
          Long.parseLong(keyToDelete.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID)));
      OmKeyInfo openKeyToDelete = OMFileRequest.getOmKeyInfoFromFileTable(true,
          omMetadataManager, prepared.dbOpenKeyToDeleteKey, keyName);
      openKeyToDelete = openKeyToDelete.toBuilder()
          .addMetadata(OzoneConsts.OVERWRITTEN_HSYNC_KEY, "true")
          .setUpdateID(trxnLogIndex)
          .build();
      openKeyToDelete.setModificationTime(Time.now());
      prepared.openKeyToDelete = openKeyToDelete;
      // The cache write for this overwritten open key is deferred to the Phase 2 mutation tail, so it
      // happens under the narrowed bucket write lock with the other mutations.
    }

    buildCommittedKey(commitKeyRequest, prepared, locationInfoList, auditMap, trxnLogIndex);
  }

  /**
   * Still Phase 1 (no bucket lock): turns the open key read above into the key to commit - request
   * metadata, data size, update ID and block locations - and validates the atomic-rewrite expectation.
   * Kept separate from {@link #prepareCommit} so the resolution and the guards stay readable next to
   * the walk they belong to.
   */
  private void buildCommittedKey(CommitKeyRequest commitKeyRequest, PreparedCommit prepared,
      List<OmKeyLocationInfo> locationInfoList, Map<String, String> auditMap, long trxnLogIndex)
      throws IOException {
    KeyArgs commitKeyArgs = commitKeyRequest.getKeyArgs();
    final boolean isHSync = commitKeyRequest.hasHsync() && commitKeyRequest.getHsync();
    final String clientIdString = String.valueOf(commitKeyRequest.getClientID());

    prepared.omKeyInfo.setModificationTime(commitKeyArgs.getModificationTime());

    if (isHSync) {
      if (!OmKeyHSyncUtil.isHSyncedPreviously(prepared.omKeyInfo, clientIdString,
          prepared.dbOpenFileKey)) {
        // Update open key as well if it is the first hsync of this key. A non-null newOpenKeyInfo
        // indicates it is necessary to update the open key.
        prepared.omKeyInfo = prepared.omKeyInfo.withMetadataMutations(
            metadata -> metadata.put(OzoneConsts.HSYNC_CLIENT_ID, clientIdString));
        prepared.newOpenKeyInfo = prepared.omKeyInfo.copyObject();
      }
    }

    // Set the new metadata from the request and UpdateID to current
    // transactionLogIndex
    prepared.omKeyInfo = prepared.omKeyInfo.toBuilder()
        .addAllMetadata(KeyValueUtil.getFromProtobuf(
            commitKeyArgs.getMetadataList()))
        .setDataSize(commitKeyArgs.getDataSize())
        .setUpdateID(trxnLogIndex)
        .build();

    prepared.uncommitted =
        prepared.omKeyInfo.updateLocationInfoList(locationInfoList, false);

    validateAtomicRewrite(prepared.keyToDelete, prepared.omKeyInfo, auditMap);
    // Optimistic locking validation has passed. Now set the rewrite fields to null so they are
    // not persisted in the key table.
    prepared.omKeyInfo = prepared.omKeyInfo.toBuilder()
        .setExpectedDataGeneration(null)
        .build();
  }

  /**
   * Phase 2 (under the bucket write lock): re-checks the commit target resolved in Phase 1, then
   * applies the quota read-modify-publish and every cache mutation of the commit, and builds the
   * response.
   */
  private OMClientResponse applyKeyCommit(OzoneManager ozoneManager,
      CommitKeyRequest commitKeyRequest, PreparedCommit prepared, long trxnLogIndex,
      OMResponse.Builder omResponse) throws IOException {
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    KeyArgs commitKeyArgs = commitKeyRequest.getKeyArgs();
    String volumeName = commitKeyArgs.getVolumeName();
    String bucketName = commitKeyArgs.getBucketName();
    String keyName = commitKeyArgs.getKeyName();
    final boolean isHSync = commitKeyRequest.hasHsync() && commitKeyRequest.getHsync();
    final boolean isRecovery = commitKeyRequest.hasRecovery() && commitKeyRequest.getRecovery();

    // Cheap O(1) re-check of the commit target resolved in Phase 1. Under serial apply this always
    // holds; it is a tripwire that fails safe (as the KEY_NOT_FOUND checks in Phase 1) rather than
    // committing a stale open key if that invariant is ever broken by a concurrent writer.
    OmKeyInfo recheckOpenKey = OMFileRequest.getOmKeyInfoFromFileTable(true,
        omMetadataManager, prepared.dbOpenFileKey, keyName);
    if (recheckOpenKey == null
        || recheckOpenKey.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY)
        || recheckOpenKey.getMetadata().containsKey(OzoneConsts.OVERWRITTEN_HSYNC_KEY)) {
      throw new OMException("Open Key " + keyName + " is already deleted/overwritten",
          KEY_NOT_FOUND);
    }

    OmBucketInfo omBucketInfo = getBucketInfoForUpdate(omMetadataManager, volumeName, bucketName);

    // Deferred from Phase 1: mark the overwritten hsync open key, under the lock with the rest.
    if (prepared.isOverwrittenHsyncKey) {
      OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
          prepared.dbOpenKeyToDeleteKey, prepared.openKeyToDelete, keyName, trxnLogIndex);
    }

    CommitApplyState state = new CommitApplyState();
    applyCommitQuota(ozoneManager, commitKeyRequest, prepared, omBucketInfo, state, trxnLogIndex);

    // let the uncommitted blocks pretend as key's old version blocks
    // which will be deleted as RepeatedOmKeyInfo
    final OmKeyInfo pseudoKeyInfo = isHSync ? null
        : wrapUncommittedBlocksAsPseudoKey(prepared.uncommitted, prepared.omKeyInfo);
    if (pseudoKeyInfo != null) {
      String delKeyName = omMetadataManager
          .getOzoneKey(volumeName, bucketName, prepared.fileName);
      long pseudoObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
      delKeyName = omMetadataManager.getOzoneDeletePathKey(
          pseudoObjId, delKeyName);
      if (null == state.oldKeyVersionsToDeleteMap) {
        state.oldKeyVersionsToDeleteMap = new HashMap<>();
      }
      state.oldKeyVersionsToDeleteMap.computeIfAbsent(delKeyName,
          key -> new RepeatedOmKeyInfo(omBucketInfo.getObjectID())).addOmKeyInfo(pseudoKeyInfo);
    }

    // Add to cache of open key table and key table.
    if (!isHSync) {
      // If isHSync = false, put a tombstone in OpenKeyTable cache,
      // indicating the key is removed from OpenKeyTable.
      // So that this key can't be committed again.
      OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
          prepared.dbOpenFileKey, null, keyName, trxnLogIndex);

      // Prevent hsync metadata from getting committed to the final key
      prepared.omKeyInfo = prepared.omKeyInfo.withMetadataMutations(
          metadata -> metadata.remove(OzoneConsts.HSYNC_CLIENT_ID));
      if (isRecovery) {
        prepared.omKeyInfo = prepared.omKeyInfo.withMetadataMutations(
            metadata -> metadata.remove(OzoneConsts.LEASE_RECOVERY));
      }
    } else if (prepared.newOpenKeyInfo != null) {
      // isHSync is true and newOpenKeyInfo is set, update OpenKeyTable
      OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager,
          prepared.dbOpenFileKey, prepared.newOpenKeyInfo, keyName, trxnLogIndex);
    }

    OMFileRequest.addFileTableCacheEntry(omMetadataManager, prepared.dbFileKey,
            prepared.omKeyInfo, prepared.fileName, trxnLogIndex);

    omBucketInfo.incrUsedBytes(state.correctedSpace);

    omMetadataManager.getBucketTable().addCacheEntry(
        omMetadataManager.getBucketKey(volumeName, bucketName), omBucketInfo, trxnLogIndex);

    omResponse.setCommitKeyResponse(CommitKeyResponse.newBuilder()
        .setModificationTime(commitKeyArgs.getModificationTime())
        .build());

    return new OMKeyCommitResponseWithFSO(omResponse.build(),
        prepared.omKeyInfo, prepared.dbFileKey, prepared.dbOpenFileKey, omBucketInfo.copyObject(),
        state.oldKeyVersionsToDeleteMap, prepared.volumeId, isHSync, prepared.newOpenKeyInfo,
        prepared.dbOpenKeyToDeleteKey, prepared.openKeyToDelete);
  }

  /**
   * Phase 2 (under the bucket write lock): charges the commit against the bucket quota and collects the
   * old key versions to delete. Kept separate from {@link #applyKeyCommit} because the non-versioned
   * overwrite branch is the bulk of the quota arithmetic.
   */
  private void applyCommitQuota(OzoneManager ozoneManager, CommitKeyRequest commitKeyRequest,
      PreparedCommit prepared, OmBucketInfo omBucketInfo, CommitApplyState state, long trxnLogIndex)
      throws IOException {
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    KeyArgs commitKeyArgs = commitKeyRequest.getKeyArgs();
    String volumeName = commitKeyArgs.getVolumeName();
    String bucketName = commitKeyArgs.getBucketName();
    OmKeyInfo keyToDelete = prepared.keyToDelete;

    state.correctedSpace = prepared.omKeyInfo.getReplicatedSize();
    // Same-client hsync re-commit does not consume namespace.
    if (keyToDelete != null && prepared.isSameHsyncKey) {
      state.correctedSpace -= keyToDelete.getReplicatedSize();
      checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
          state.correctedSpace);
    } else if (keyToDelete != null && !omBucketInfo.getIsVersionEnabled()) {
      // If bucket versioning is turned on during the update, between key
      // creation and key commit, old versions will be just overwritten and
      // not kept. Bucket versioning will be effective from the first key
      // creation after the knob turned on.
      RepeatedOmKeyInfo oldVerKeyInfo = getOldVersionsToCleanUp(
          keyToDelete, omBucketInfo.getObjectID(), trxnLogIndex);
      String delKeyName = omMetadataManager
          .getOzoneKey(volumeName, bucketName, prepared.fileName);
      // using pseudoObjId as objectId can be same in case of overwrite key
      long pseudoObjId = ozoneManager.getObjectIdFromTxId(trxnLogIndex);
      delKeyName = omMetadataManager.getOzoneDeletePathKey(
          pseudoObjId, delKeyName);
      if (null == state.oldKeyVersionsToDeleteMap) {
        state.oldKeyVersionsToDeleteMap = new HashMap<>();
      }

      // Remove any block from oldVerKeyInfo that share the same container ID
      // and local ID with omKeyInfo blocks'.
      // Otherwise, it causes data loss once those shared blocks are added
      // to deletedTable and processed by KeyDeletingService for deletion.
      Pair<Map<OmKeyInfo, List<OmKeyLocationInfo>>, Integer> filteredUsedBlockCnt =
          filterOutBlocksStillInUse(prepared.omKeyInfo, oldVerKeyInfo);
      Map<OmKeyInfo, List<OmKeyLocationInfo>> blocks = filteredUsedBlockCnt.getLeft();
      state.correctedSpace -= blocks.entrySet().stream().mapToLong(filteredKeyBlocks ->
          filteredKeyBlocks.getValue().stream().mapToLong(block -> QuotaUtil.getReplicatedSize(
              block.getLength(), filteredKeyBlocks.getKey().getReplicationConfig())).sum()).sum();
      long totalSize = 0;
      long totalNamespace = 0;
      if (!oldVerKeyInfo.getOmKeyInfoList().isEmpty()) {
        state.oldKeyVersionsToDeleteMap.put(delKeyName, oldVerKeyInfo);
        List<OmKeyInfo> oldKeys = oldVerKeyInfo.getOmKeyInfoList();
        for (int i = 0; i < oldKeys.size(); i++) {
          OmKeyInfo updatedOlderKeyVersions =
              oldKeys.get(i).withCommittedKeyDeletedFlag(true);
          oldKeys.set(i, updatedOlderKeyVersions);
          totalSize += sumBlockLengths(updatedOlderKeyVersions);
          totalNamespace += 1;
        }
      }
      // Subtract the size of blocks to be overwritten.
      checkBucketQuotaInNamespace(omBucketInfo, 1L);
      checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
          state.correctedSpace);
      // Subtract the size of blocks to be overwritten.
      omBucketInfo.decrUsedNamespace(totalNamespace, true);
      omBucketInfo.decrUsedNamespace(filteredUsedBlockCnt.getRight(), false);
      omBucketInfo.decrUsedBytes(totalSize, true);
      omBucketInfo.incrUsedNamespace(1L);
    } else {
      checkBucketQuotaInNamespace(omBucketInfo, 1L);
      checkBucketQuotaInBytes(omMetadataManager, omBucketInfo,
          state.correctedSpace);
      omBucketInfo.incrUsedNamespace(1L);
    }
  }

  /**
   * Phase 1 output of {@link #prepareCommit}, consumed by {@link #applyKeyCommit} under the bucket
   * write lock: the resolved path keys, the key being overwritten and the hsync open keys. Mutable,
   * because {@code omKeyInfo} is rebuilt as the commit is prepared and again under the lock, and the
   * caller logs and audits the value reached.
   */
  private static final class PreparedCommit {
    private String fileName;
    private long volumeId;
    private String dbFileKey;
    private String dbOpenFileKey;
    private OmKeyInfo keyToDelete;
    private boolean isSameHsyncKey;
    private boolean isOverwrittenHsyncKey;
    private String dbOpenKeyToDeleteKey;
    private OmKeyInfo openKeyToDelete;
    private OmKeyInfo newOpenKeyInfo;
    private List<OmKeyLocationInfo> uncommitted;
    private OmKeyInfo omKeyInfo;
  }

  /**
   * Values produced under the bucket write lock and shared between {@link #applyCommitQuota} and the
   * rest of {@link #applyKeyCommit}: the bucket byte delta to charge and the old key versions to move
   * to the deleted table, which stays {@code null} while nothing has to be deleted.
   */
  private static final class CommitApplyState {
    private long correctedSpace;
    private Map<String, RepeatedOmKeyInfo> oldKeyVersionsToDeleteMap;
  }

  /**
   * Emits the debug, audit and result logs outside the bucket lock.
   */
  private void auditAndLogResult(OzoneManager ozoneManager, CommitKeyRequest commitKeyRequest,
      PreparedCommit prepared, Map<String, String> auditMap, Exception exception, Result result) {
    boolean isHSync = commitKeyRequest.hasHsync() && commitKeyRequest.getHsync();

    // Debug logging for any key commit operation, successful or not
    LOG.debug("Key commit {} with isHSync = {}, omKeyInfo = {}",
        result == Result.SUCCESS ? "succeeded" : "failed", isHSync, prepared.omKeyInfo);

    if (!isHSync) {
      KeyArgs commitKeyArgs = commitKeyRequest.getKeyArgs();
      markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(OMAction.COMMIT_KEY, auditMap,
              exception, getOmRequest().getUserInfo()));
      processResult(commitKeyRequest, commitKeyArgs.getVolumeName(), commitKeyArgs.getBucketName(),
          commitKeyArgs.getKeyName(), ozoneManager.getMetrics(), exception, prepared.omKeyInfo, result);
    }
  }
}
