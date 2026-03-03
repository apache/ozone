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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.apache.hadoop.ozone.util.ProtobufUtils.computeLongSizeWithTag;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PendingKeysDeletion;
import org.apache.hadoop.ozone.om.PendingKeysDeletion.PurgedKey;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableKeyFilter;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableRenameEntryFilter;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketNameInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketPurgeKeysSize;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NullableUUID;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetSnapshotPropertyRequest;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the background service to delete keys. Scan the metadata of om
 * periodically to get the keys from DeletedTable and ask scm to delete
 * metadata accordingly, if scm returns success for keys, then clean up those
 * keys.
 */
public class KeyDeletingService extends AbstractKeyDeletingService {
  private static final Logger LOG =
      LoggerFactory.getLogger(KeyDeletingService.class);
  private final ScmBlockLocationProtocol scmClient;

  private int keyLimitPerTask;
  private final AtomicLong deletedKeyCount;
  private final boolean deepCleanSnapshots;
  private final SnapshotChainManager snapshotChainManager;
  private int ratisByteLimit;
  private static final double RATIS_LIMIT_FACTOR = 0.9;

  // Track metrics for current task execution
  private long latestRunTimestamp = 0L;
  private final DeletionStats aosDeletionStats = new DeletionStats();
  private final DeletionStats snapshotDeletionStats = new DeletionStats();

  public KeyDeletingService(OzoneManager ozoneManager,
      ScmBlockLocationProtocol scmClient, long serviceInterval,
      long serviceTimeout, ConfigurationSource conf, int keyDeletionCorePoolSize,
      boolean deepCleanSnapshots) {
    super(KeyDeletingService.class.getSimpleName(), serviceInterval,
        TimeUnit.MILLISECONDS, keyDeletionCorePoolSize,
        serviceTimeout, ozoneManager);
    this.keyLimitPerTask = conf.getInt(OZONE_KEY_DELETING_LIMIT_PER_TASK,
        OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT);
    Preconditions.checkArgument(keyLimitPerTask >= 0,
        OZONE_KEY_DELETING_LIMIT_PER_TASK + " cannot be negative.");
    this.deletedKeyCount = new AtomicLong(0);
    this.deepCleanSnapshots = deepCleanSnapshots;
    this.snapshotChainManager = ((OmMetadataManagerImpl)ozoneManager.getMetadataManager()).getSnapshotChainManager();
    this.scmClient = scmClient;
    int limit = (int) ozoneManager.getConfiguration().getStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    // Use 90% of the actual Ratis limit to account for protobuf overhead and
    // prevent accidentally exceeding the hard limit during request serialization.
    this.ratisByteLimit = (int) Math.max(limit * RATIS_LIMIT_FACTOR, 1);
  }

  /**
   * Returns the number of keys deleted by the background service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public AtomicLong getDeletedKeyCount() {
    return deletedKeyCount;
  }

  Pair<Pair<Integer, Long>, Boolean> processKeyDeletes(Map<String, PurgedKey> keyBlocksList,
      Map<String, RepeatedOmKeyInfo> keysToModify, List<String> renameEntries,
      String snapTableKey, UUID expectedPreviousSnapshotId) throws IOException {
    long startTime = Time.monotonicNow();
    Pair<Pair<Integer, Long>, Boolean> purgeResult = Pair.of(Pair.of(0, 0L), false);
    
    // Filter out empty files (files with no blocks) before sending to SCM
    Map<String, PurgedKey> nonEmptyKeyBlocksList = keyBlocksList.entrySet().stream()
        .filter(entry -> entry.getValue().getBlockGroup() != null && 
                         entry.getValue().getBlockGroup().getDeletedBlocks() != null &&
                         !entry.getValue().getBlockGroup().getDeletedBlocks().isEmpty())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Send {} key(s) to SCM (filtered {} empty keys): {}",
          nonEmptyKeyBlocksList.size(), keyBlocksList.size() - nonEmptyKeyBlocksList.size(), nonEmptyKeyBlocksList);
    } else if (LOG.isInfoEnabled()) {
      int logSize = 10;
      if (nonEmptyKeyBlocksList.size() < logSize) {
        logSize = nonEmptyKeyBlocksList.size();
      }
      LOG.info("Send {} key(s) to SCM, first {} keys: {}",
          nonEmptyKeyBlocksList.size(), logSize, nonEmptyKeyBlocksList.entrySet().stream().limit(logSize)
              .map(Map.Entry::getValue).collect(Collectors.toSet()));
    }
    List<DeleteBlockGroupResult> blockDeletionResults;
    if (nonEmptyKeyBlocksList.isEmpty()) {
      // Skip SCM call if all files are empty
      blockDeletionResults = new ArrayList<>();
      LOG.info("Skipping SCM call as all {} keys are empty", keyBlocksList.size());
    } else {
      blockDeletionResults =
          scmClient.deleteKeyBlocks(nonEmptyKeyBlocksList.values().stream()
              .map(PurgedKey::getBlockGroup).collect(Collectors.toList()));
    }

    if (keyBlocksList.size() != nonEmptyKeyBlocksList.size()) {
      // Add successful results for empty files (no need to send to SCM)
      Map<String, PurgedKey> emptyKeyBlocksList = keyBlocksList.entrySet().stream()
          .filter(entry -> !nonEmptyKeyBlocksList.containsKey(entry.getKey()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      for (PurgedKey emptyKey : emptyKeyBlocksList.values()) {
        // Create a successful result for empty files
        DeleteBlockGroupResult emptyFileResult = new DeleteBlockGroupResult(
            emptyKey.getBlockGroup().getGroupID(), new ArrayList<>());
        blockDeletionResults.add(emptyFileResult);
      }
    }

    LOG.info("{} BlockGroup deletion are acked by SCM in {} ms",
        keyBlocksList.size(), Time.monotonicNow() - startTime);
    if (blockDeletionResults != null) {
      long purgeStartTime = Time.monotonicNow();
      purgeResult = submitPurgeKeysRequest(blockDeletionResults, keyBlocksList, keysToModify, renameEntries,
          snapTableKey, expectedPreviousSnapshotId, ratisByteLimit);
      int limit = getOzoneManager().getConfiguration().getInt(OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK,
          OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT);
      LOG.info("Blocks for {} (out of {}) keys are deleted from DB in {} ms. Limit per task is {}.",
          purgeResult.getKey().getKey(), blockDeletionResults.size(), Time.monotonicNow() - purgeStartTime, limit);
    }
    getPerfMetrics().setKeyDeletingServiceLatencyMs(Time.monotonicNow() - startTime);
    return purgeResult;
  }

  private static final class BucketPurgeSize {
    private BucketNameInfo bucket;
    private long purgedBytes;
    private long purgedNamespace;

    private BucketPurgeSize(String volume, String bucket, long bucketId) {
      this.bucket = BucketNameInfo.newBuilder().setBucketId(bucketId).setVolumeName(volume)
              .setBucketName(bucket).build();
      this.purgedBytes = 0;
      this.purgedNamespace = 0;
    }

    private BucketPurgeSize incrementPurgedBytes(long bytes) {
      purgedBytes += bytes;
      return this;
    }

    private BucketPurgeSize incrementPurgedNamespace(long namespace) {
      purgedNamespace += namespace;
      return this;
    }

    private BucketPurgeKeysSize toProtobuf() {
      return BucketPurgeKeysSize.newBuilder()
          .setBucketNameInfo(bucket)
          .setPurgedBytes(purgedBytes)
          .setPurgedNamespace(purgedNamespace)
          .build();
    }

    private int getEstimatedSize() {
      // Using -10 as the placeholder to get max size i.e. 10 bytes to store the long value in protobuf.
      // Field number 2 in BucketPurgeKeysSize proto corresponds to purgedBytes.
      return this.bucket.getSerializedSize() + computeLongSizeWithTag(2, -10)
      // Field number 3 in BucketPurgeKeysSize proto corresponds to purgedNamespace.
          + computeLongSizeWithTag(3, -10);
    }
  }

  private int increaseBucketPurgeSize(Map<Long, BucketPurgeSize> bucketPurgeSizeMap, PurgedKey purgedKey) {
    BucketPurgeSize bucketPurgeSize;
    int estimatedSize = 0;
    if (!bucketPurgeSizeMap.containsKey(purgedKey.getBucketId())) {
      bucketPurgeSize = bucketPurgeSizeMap.computeIfAbsent(purgedKey.getBucketId(),
          (bucketId) -> new BucketPurgeSize(purgedKey.getVolume(), purgedKey.getBucket(),
              purgedKey.getBucketId()));
      estimatedSize = bucketPurgeSize.getEstimatedSize();
    } else {
      bucketPurgeSize = bucketPurgeSizeMap.get(purgedKey.getBucketId());
    }
    bucketPurgeSize.incrementPurgedBytes(purgedKey.getPurgedBytes()).incrementPurgedNamespace(1);
    return estimatedSize;
  }

  /**
   * Submits PurgeKeys request for the keys whose blocks have been deleted
   * by SCM.
   * @param results DeleteBlockGroups returned by SCM.
   * @param keysToModify Updated list of RepeatedOmKeyInfo
   */
  @SuppressWarnings("checkstyle:MethodLength")
  private Pair<Pair<Integer, Long>, Boolean> submitPurgeKeysRequest(
      List<DeleteBlockGroupResult> results,
      Map<String, PurgedKey> purgedKeys,
      Map<String, RepeatedOmKeyInfo> keysToModify,
      List<String> renameEntriesToBeDeleted,
      String snapTableKey,
      UUID expectedPreviousSnapshotId,
      int ratisLimit) {

    Set<String> completePurgedKeys = new HashSet<>();

    // Put all keys to be purged in a list
    int deletedCount = 0;
    long deletedReplSize = 0;
    Set<String> failedDeletedKeys = new HashSet<>();
    boolean purgeSuccess = true;

    // Step 1: Process DeleteBlockGroupResults
    for (DeleteBlockGroupResult result : results) {
      String deletedKeyGroup = result.getObjectKey();
      PurgedKey purgedKey = purgedKeys.get(deletedKeyGroup);
      if (purgedKey != null) {
        String deletedKeyName = purgedKey.getDeleteKeyName();
        if (result.isSuccess()) {
          // Add key to PurgeKeys list.
          if (keysToModify == null || !keysToModify.containsKey(deletedKeyName)) {
            completePurgedKeys.add(deletedKeyName);
            LOG.debug("Key {} set to be purged from OM DB", deletedKeyName);
          } else {
            LOG.debug("Key {} set to be updated in OM DB, Other versions " +
                "of the key that are reclaimable are reclaimed.", deletedKeyName);
          }
          deletedReplSize += purgedKey.getPurgedBytes();
          deletedCount++;
        } else {
          // If the block deletion failed, then the deleted keys should also not be modified and
          // any other version of the key should also not be purged.
          failedDeletedKeys.add(deletedKeyName);
          purgeSuccess = false;
          if (LOG.isDebugEnabled()) {
            LOG.error("Failed Block Delete corresponding to Key {} with block result : {}.", deletedKeyName,
                result.getBlockResultList());
          } else {
            LOG.error("Failed Block Delete corresponding to Key {}.", deletedKeyName);
          }
        }
      } else {
        LOG.error("Key {} not found in the list of keys to be purged." +
            " Skipping purge for this entry. Result of delete blocks : {}", deletedKeyGroup, result.isSuccess());
      }
    }
    // Filter out the key even if one version of the key purge has failed. This is to prevent orphan blocks, and
    // this needs to be retried.
    completePurgedKeys = completePurgedKeys.stream()
        .filter(i -> !failedDeletedKeys.contains(i)).collect(Collectors.toSet());
    // Filter out any keys that have failed and sort the purge keys based on volume and bucket.
    List<PurgedKey> purgedKeyList = purgedKeys.values().stream()
        .filter(purgedKey -> !failedDeletedKeys.contains(purgedKey.getDeleteKeyName()))
        .collect(Collectors.toList());

    List<OzoneManagerProtocolProtos.SnapshotMoveKeyInfos> keysToUpdateList = new ArrayList<>();
    if (keysToModify != null) {
      for (Map.Entry<String, RepeatedOmKeyInfo> keyToModify : keysToModify.entrySet()) {
        if (failedDeletedKeys.contains(keyToModify.getKey())) {
          continue;
        }
        OzoneManagerProtocolProtos.SnapshotMoveKeyInfos.Builder keyToUpdate =
            OzoneManagerProtocolProtos.SnapshotMoveKeyInfos.newBuilder();
        keyToUpdate.setKey(keyToModify.getKey());
        List<OzoneManagerProtocolProtos.KeyInfo> keyInfos =
            keyToModify.getValue().getOmKeyInfoList().stream()
                .map(k -> k.getProtobuf(ClientVersion.CURRENT.serialize()))
                .collect(Collectors.toList());
        keyToUpdate.addAllKeyInfos(keyInfos);
        keyToUpdate.setBucketId(keyToModify.getValue().getBucketId());
        keysToUpdateList.add(keyToUpdate.build());
      }
    }

    if (purgedKeyList.isEmpty() && keysToUpdateList.isEmpty() &&
        (renameEntriesToBeDeleted == null || renameEntriesToBeDeleted.isEmpty())) {
      return Pair.of(Pair.of(deletedCount, deletedReplSize), purgeSuccess);
    }

    int purgeKeyIndex = 0, updateIndex = 0, renameIndex = 0;
    PurgeKeysRequest.Builder requestBuilder = getPurgeKeysRequest(snapTableKey, expectedPreviousSnapshotId);
    int currSize = requestBuilder.build().getSerializedSize();
    int baseSize = currSize;

    OzoneManagerProtocolProtos.DeletedKeys.Builder bucketDeleteKeys = null;
    Map<Long, BucketPurgeSize> bucketPurgeKeysSizeMap = new HashMap<>();

    Map<String, List<PurgedKey>> modifiedKeyPurgedKeys = new HashMap<>();
    while (purgeKeyIndex < purgedKeyList.size() || updateIndex < keysToUpdateList.size() ||
        (renameEntriesToBeDeleted != null && renameIndex < renameEntriesToBeDeleted.size())) {

      // 3.1 Purge keys (one at a time)
      if (purgeKeyIndex < purgedKeyList.size()) {
        PurgedKey purgedKey = purgedKeyList.get(purgeKeyIndex);
        if (bucketDeleteKeys == null) {
          bucketDeleteKeys = OzoneManagerProtocolProtos.DeletedKeys.newBuilder().setVolumeName("").setBucketName("");
          currSize += bucketDeleteKeys.buildPartial().getSerializedSize();
        }
        String deletedKey = purgedKey.getDeleteKeyName();
        // Add to purge keys only if there are no other version of key that needs to be retained.
        if (completePurgedKeys.contains(deletedKey)) {
          bucketDeleteKeys.addKeys(deletedKey);
          int estimatedKeySize = ProtobufUtils.computeRepeatedStringSize(deletedKey);
          currSize += estimatedKeySize;
          if (purgedKey.isCommittedKey()) {
            currSize += increaseBucketPurgeSize(bucketPurgeKeysSizeMap, purgedKey);
          }
        } else if (purgedKey.isCommittedKey()) {
          modifiedKeyPurgedKeys.computeIfAbsent(deletedKey, k -> new ArrayList<>()).add(purgedKey);
        }
        purgeKeyIndex++;
      } else if (updateIndex < keysToUpdateList.size()) {
        // 3.2 Add keysToUpdate
        OzoneManagerProtocolProtos.SnapshotMoveKeyInfos nextUpdate = keysToUpdateList.get(updateIndex);

        int estimatedSize = nextUpdate.getSerializedSize();

        requestBuilder.addKeysToUpdate(nextUpdate);
        if (modifiedKeyPurgedKeys.containsKey(nextUpdate.getKey())) {
          for (PurgedKey purgedKey : modifiedKeyPurgedKeys.get(nextUpdate.getKey())) {
            if (purgedKey.isCommittedKey()) {
              currSize += increaseBucketPurgeSize(bucketPurgeKeysSizeMap, purgedKey);
            }
          }
        }
        currSize += estimatedSize;
        updateIndex++;

      } else if (renameEntriesToBeDeleted != null && renameIndex < renameEntriesToBeDeleted.size()) {
        // 3.3 Add renamed keys
        String nextRename = renameEntriesToBeDeleted.get(renameIndex);

        int estimatedSize = ProtobufUtils.computeRepeatedStringSize(nextRename);

        requestBuilder.addRenamedKeys(nextRename);
        currSize += estimatedSize;
        renameIndex++;
      }

      // Flush either when limit is hit, or at the very end if items remain
      boolean allDone = purgeKeyIndex == purgedKeyList.size() && updateIndex == keysToUpdateList.size() &&
          (renameEntriesToBeDeleted == null || renameIndex == renameEntriesToBeDeleted.size());

      if (currSize >= ratisLimit || (allDone && (hasPendingItems(requestBuilder) || bucketDeleteKeys != null))) {
        if (bucketDeleteKeys != null) {
          requestBuilder.addDeletedKeys(bucketDeleteKeys.build());
          bucketDeleteKeys = null;
        }
        bucketPurgeKeysSizeMap.values().stream().map(BucketPurgeSize::toProtobuf)
            .forEach(requestBuilder::addBucketPurgeKeysSize);
        bucketPurgeKeysSizeMap.clear();
        purgeSuccess = submitPurgeRequest(purgeSuccess, requestBuilder);
        requestBuilder = getPurgeKeysRequest(snapTableKey, expectedPreviousSnapshotId);
        currSize = baseSize;
      }
    }

    return Pair.of(Pair.of(deletedCount, deletedReplSize), purgeSuccess);
  }

  private boolean hasPendingItems(PurgeKeysRequest.Builder builder) {
    return builder.getDeletedKeysCount() > 0
        || builder.getKeysToUpdateCount() > 0
        || builder.getRenamedKeysCount() > 0;
  }

  private static PurgeKeysRequest.Builder getPurgeKeysRequest(String snapTableKey,
      UUID expectedPreviousSnapshotId) {
    PurgeKeysRequest.Builder requestBuilder = PurgeKeysRequest.newBuilder();

    if (snapTableKey != null) {
      requestBuilder.setSnapshotTableKey(snapTableKey);
    }

    NullableUUID.Builder expectedPreviousSnapshotNullableUUID = NullableUUID.newBuilder();
    if (expectedPreviousSnapshotId != null) {
      expectedPreviousSnapshotNullableUUID.setUuid(HddsUtils.toProtobuf(expectedPreviousSnapshotId));
    }
    requestBuilder.setExpectedPreviousSnapshotID(expectedPreviousSnapshotNullableUUID.build());
    return requestBuilder;
  }

  private boolean submitPurgeRequest(boolean purgeSuccess, PurgeKeysRequest.Builder requestBuilder) {

    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder().setCmdType(OzoneManagerProtocolProtos.Type.PurgeKeys)
            .setPurgeKeysRequest(requestBuilder.build()).setClientId(getClientId().toString()).build();

    try {
      OzoneManagerProtocolProtos.OMResponse omResponse = submitRequest(omRequest);
      if (omResponse != null) {
        purgeSuccess = purgeSuccess && omResponse.getSuccess();
      }
    } catch (ServiceException e) {
      LOG.error("PurgeKey request failed in batch. Will retry at next run.", e);
      purgeSuccess = false;
      // Continue to next batch instead of returning immediately
    }
    return purgeSuccess;
  }

  /**
   * Updates ServiceMetrics for the last run of the service.
   */
  @Override
  protected void execTaskCompletion() {
    getMetrics().updateIntervalCumulativeMetrics(
        aosDeletionStats.reclaimedKeyCount.get() + snapshotDeletionStats.reclaimedKeyCount.get(),
        aosDeletionStats.reclaimedKeySize.get() + snapshotDeletionStats.reclaimedKeySize.get());
    getMetrics().updateAosLastRunMetrics(aosDeletionStats.reclaimedKeyCount.get(),
        aosDeletionStats.reclaimedKeySize.get(), aosDeletionStats.iteratedKeyCount.get(),
        aosDeletionStats.notReclaimableKeyCount.get());
    getMetrics().updateSnapLastRunMetrics(snapshotDeletionStats.reclaimedKeyCount.get(),
        snapshotDeletionStats.reclaimedKeySize.get(), snapshotDeletionStats.iteratedKeyCount.get(),
        snapshotDeletionStats.notReclaimableKeyCount.get());
    getMetrics().setKdsLastRunTimestamp(latestRunTimestamp);
  }

  /**
   * Resets ServiceMetrics for the current run of the service.
   */
  private void resetMetrics() {
    aosDeletionStats.reset();
    snapshotDeletionStats.reset();
    latestRunTimestamp = System.currentTimeMillis();
    getMetrics().setKdsCurRunTimestamp(latestRunTimestamp);
  }

  @Override
  public DeletingServiceTaskQueue getTasks() {
    resetMetrics();
    DeletingServiceTaskQueue queue = new DeletingServiceTaskQueue();
    queue.add(new KeyDeletingTask(null));
    if (deepCleanSnapshots) {
      Iterator<UUID> iterator = null;
      try {
        iterator = snapshotChainManager.iterator(true);
      } catch (IOException e) {
        LOG.error("Error while initializing snapshot chain iterator. DirDeletingTask will only process AOS this run.");
        return queue;
      }
      while (iterator.hasNext()) {
        UUID snapshotId = iterator.next();
        queue.add(new KeyDeletingTask(snapshotId));
      }
    }
    return queue;
  }

  public int getKeyLimitPerTask() {
    return keyLimitPerTask;
  }

  public void setKeyLimitPerTask(int keyLimitPerTask) {
    this.keyLimitPerTask = keyLimitPerTask;
  }

  /**
   * A key deleting task scans OM DB and looking for a certain number of
   * pending-deletion keys, sends these keys along with their associated blocks
   * to SCM for deletion. Once SCM confirms keys are deleted (once SCM persisted
   * the blocks info in its deletedBlockLog), it removes these keys from the
   * DB.
   */
  @VisibleForTesting
  final class KeyDeletingTask implements BackgroundTask {
    private final UUID snapshotId;

    KeyDeletingTask(UUID snapshotId) {
      this.snapshotId = snapshotId;
    }

    private OzoneManagerProtocolProtos.SetSnapshotPropertyRequest getSetSnapshotRequestUpdatingExclusiveSize(
        Map<UUID, Long> exclusiveSizeMap, Map<UUID, Long> exclusiveReplicatedSizeMap, UUID snapshotID) {
      OzoneManagerProtocolProtos.SnapshotSize snapshotSize = OzoneManagerProtocolProtos.SnapshotSize.newBuilder()
          .setExclusiveSize(
              exclusiveSizeMap.getOrDefault(snapshotID, 0L))
          .setExclusiveReplicatedSize(
              exclusiveReplicatedSizeMap.getOrDefault(
                  snapshotID, 0L))
          .build();

      return OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
          .setSnapshotKey(snapshotChainManager.getTableKey(snapshotID))
          .setSnapshotSize(snapshotSize)
          .build();
    }

    /**
     * @param currentSnapshotInfo if null, deleted directories in AOS should be processed.
     * @param keyManager KeyManager of the underlying store.
     */
    private void processDeletedKeysForStore(SnapshotInfo currentSnapshotInfo, KeyManager keyManager,
        int remainNum) throws IOException, InterruptedException {
      String volume = null, bucket = null, snapshotTableKey = null;
      if (currentSnapshotInfo != null) {
        volume = currentSnapshotInfo.getVolumeName();
        bucket = currentSnapshotInfo.getBucketName();
        snapshotTableKey = currentSnapshotInfo.getTableKey();
      }

      boolean successStatus = true;
      try {
        // TODO: [SNAPSHOT] HDDS-7968. Reclaim eligible key blocks in
        //  snapshot's deletedTable when active DB's deletedTable
        //  doesn't have enough entries left.
        //  OM would have to keep track of which snapshot the key is coming
        //  from if the above would be done inside getPendingDeletionKeys().
        OmSnapshotManager omSnapshotManager = getOzoneManager().getOmSnapshotManager();
        // This is to avoid race condition b/w purge request and snapshot chain update. For AOS taking the global
        // snapshotId since AOS could process multiple buckets in one iteration. While using path
        // previous snapshotId for a snapshot since it would process only one bucket.
        UUID expectedPreviousSnapshotId = currentSnapshotInfo == null ?
            snapshotChainManager.getLatestGlobalSnapshotId() :
            SnapshotUtils.getPreviousSnapshotId(currentSnapshotInfo, snapshotChainManager);

        IOzoneManagerLock lock = getOzoneManager().getMetadataManager().getLock();

        // Purge deleted Keys in the deletedTable && rename entries in the snapshotRenamedTable which doesn't have a
        // reference in the previous snapshot.
        try (ReclaimableKeyFilter reclaimableKeyFilter = new ReclaimableKeyFilter(getOzoneManager(),
            omSnapshotManager, snapshotChainManager, currentSnapshotInfo, keyManager, lock);
             ReclaimableRenameEntryFilter renameEntryFilter = new ReclaimableRenameEntryFilter(
                 getOzoneManager(), omSnapshotManager, snapshotChainManager, currentSnapshotInfo,
                 keyManager, lock)) {
          List<String> renamedTableEntries =
              keyManager.getRenamesKeyEntries(volume, bucket, null, renameEntryFilter, remainNum).stream()
                  .map(Table.KeyValue::getKey)
                  .collect(Collectors.toList());
          remainNum -= renamedTableEntries.size();

          // Get pending keys that can be deleted
          PendingKeysDeletion pendingKeysDeletion = currentSnapshotInfo == null
              ? keyManager.getPendingDeletionKeys(reclaimableKeyFilter, remainNum)
              : keyManager.getPendingDeletionKeys(volume, bucket, null, reclaimableKeyFilter, remainNum);
          Map<String, PurgedKey> purgedKeys = pendingKeysDeletion.getPurgedKeys();
          //submit purge requests if there are renamed entries to be purged or keys to be purged.
          if (!renamedTableEntries.isEmpty() || purgedKeys != null && !purgedKeys.isEmpty()) {
            // Validating if the previous snapshot is still the same before purging the blocks.
            SnapshotUtils.validatePreviousSnapshotId(currentSnapshotInfo, snapshotChainManager,
                expectedPreviousSnapshotId);
            Pair<Pair<Integer, Long>, Boolean> purgeResult = processKeyDeletes(purgedKeys,
                pendingKeysDeletion.getKeysToModify(), renamedTableEntries, snapshotTableKey,
                expectedPreviousSnapshotId);
            remainNum -= purgeResult.getKey().getKey();
            successStatus = purgeResult.getValue();
            getMetrics().incrNumKeysProcessed(purgedKeys.size());
            getMetrics().incrNumKeysSentForPurge(purgeResult.getKey().getKey());

            DeletionStats statsToUpdate = currentSnapshotInfo == null ? aosDeletionStats : snapshotDeletionStats;
            statsToUpdate.updateDeletionStats(purgeResult.getKey().getKey(), purgeResult.getKey().getValue(),
                purgedKeys.size() + pendingKeysDeletion.getNotReclaimableKeyCount(),
                pendingKeysDeletion.getNotReclaimableKeyCount()
            );
            if (successStatus) {
              deletedKeyCount.addAndGet(purgeResult.getKey().getKey());
            }
          }

          // Checking remainNum is greater than zero and not equal to the initial value if there were some keys to
          // reclaim. This is to check if all keys have been iterated over and all the keys necessary have been
          // reclaimed.
          if (remainNum > 0 && successStatus) {
            List<SetSnapshotPropertyRequest> setSnapshotPropertyRequests = new ArrayList<>();
            Map<UUID, Long> exclusiveReplicatedSizeMap = reclaimableKeyFilter.getExclusiveReplicatedSizeMap();
            Map<UUID, Long> exclusiveSizeMap = reclaimableKeyFilter.getExclusiveSizeMap();
            List<UUID> previousPathSnapshotsInChain =
                Stream.of(exclusiveSizeMap.keySet(), exclusiveReplicatedSizeMap.keySet())
                .flatMap(Collection::stream).distinct().collect(Collectors.toList());
            for (UUID snapshot : previousPathSnapshotsInChain) {
              setSnapshotPropertyRequests.add(getSetSnapshotRequestUpdatingExclusiveSize(exclusiveSizeMap,
                  exclusiveReplicatedSizeMap, snapshot));
            }

            // Updating directory deep clean flag of snapshot.
            if (currentSnapshotInfo != null) {
              setSnapshotPropertyRequests.add(OzoneManagerProtocolProtos.SetSnapshotPropertyRequest.newBuilder()
                  .setSnapshotKey(snapshotTableKey)
                  .setDeepCleanedDeletedKey(true)
                  .build());
            }
            submitSetSnapshotRequests(setSnapshotPropertyRequests);
          }
        }
      } catch (UncheckedIOException e) {
        throw e.getCause();
      }
    }

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() {
      // Check if this is the Leader OM. If not leader, no need to execute this
      // task.
      if (shouldRun()) {
        final long run = getRunCount().incrementAndGet();
        if (snapshotId == null) {
          LOG.debug("Running KeyDeletingService for active object store, {}", run);
        } else {
          LOG.debug("Running KeyDeletingService for snapshot : {}, {}", snapshotId, run);
        }
        int remainNum = keyLimitPerTask;
        OmSnapshotManager omSnapshotManager = getOzoneManager().getOmSnapshotManager();
        SnapshotInfo snapInfo = null;
        try {
          snapInfo = snapshotId == null ? null :
              SnapshotUtils.getSnapshotInfo(getOzoneManager(), snapshotChainManager, snapshotId);
          if (snapInfo != null) {
            if (snapInfo.isDeepCleaned()) {
              LOG.info("Snapshot '{}' ({}) has already been deep cleaned. Skipping the snapshot in this iteration.",
                  snapInfo.getTableKey(), snapInfo.getSnapshotId());
              return EmptyTaskResult.newResult();
            }
            if (!OmSnapshotManager.areSnapshotChangesFlushedToDB(getOzoneManager().getMetadataManager(), snapInfo)) {
              LOG.info("Skipping snapshot processing since changes to snapshot {} have not been flushed to disk",
                  snapInfo);
              return EmptyTaskResult.newResult();
            }
            if (!snapInfo.isDeepCleanedDeletedDir()) {
              LOG.debug("Snapshot {} hasn't done deleted directory deep cleaning yet. Skipping the snapshot in this" +
                  " iteration.", snapInfo);
              return EmptyTaskResult.newResult();
            }
          } else if (!isPreviousPurgeTransactionFlushed()) {
            return EmptyTaskResult.newResult();
          }
          try (UncheckedAutoCloseableSupplier<OmSnapshot> omSnapshot = snapInfo == null ? null :
              omSnapshotManager.getActiveSnapshot(snapInfo.getVolumeName(), snapInfo.getBucketName(),
                  snapInfo.getName())) {
            KeyManager keyManager = snapInfo == null ? getOzoneManager().getKeyManager()
                : omSnapshot.get().getKeyManager();
            processDeletedKeysForStore(snapInfo, keyManager, remainNum);
          }
        } catch (IOException e) {
          LOG.error("Error while running delete files background task for store {}. Will retry at next run.",
              snapInfo, e);
        } catch (InterruptedException e) {
          LOG.error("Interruption while running delete files background task for store {}.", snapInfo, e);
          Thread.currentThread().interrupt();
        }
      }
      // By design, no one cares about the results of this call back.
      return EmptyTaskResult.newResult();
    }
  }

  private static class DeletionStats {
    private final AtomicLong reclaimedKeyCount = new AtomicLong(0L);
    private final AtomicLong reclaimedKeySize = new AtomicLong(0L);
    private final AtomicLong iteratedKeyCount = new AtomicLong(0L);
    private final AtomicLong notReclaimableKeyCount = new AtomicLong(0L);

    private void updateDeletionStats(long reclaimedKeys, long reclaimedSize,
                                       long iteratedKeys, long notReclaimableKeys) {
      this.reclaimedKeyCount.addAndGet(reclaimedKeys);
      this.reclaimedKeySize.addAndGet(reclaimedSize);
      this.iteratedKeyCount.addAndGet(iteratedKeys);
      this.notReclaimableKeyCount.addAndGet(notReclaimableKeys);
    }

    private void reset() {
      reclaimedKeyCount.set(0L);
      reclaimedKeySize.set(0L);
      iteratedKeyCount.set(0L);
      notReclaimableKeyCount.set(0L);
    }
  }
}
