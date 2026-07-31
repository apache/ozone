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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.BucketVersioningStatus;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.VersionIdGenerator;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyDeleteMarkerResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeysDeleteMarkerResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Tests the S3-compatible versioning behaviour of key writes on a
 * versioning-enabled OBJECT_STORE bucket: a commit freezes a versionId on the
 * new current version and keeps the version it overwrote as a noncurrent
 * version in the versionedKeyTable instead of reclaiming it.
 */
public class TestOMKeyVersioningRequests extends OMKeyRequestTests {

  /**
   * versionIds a request proposes, deliberately unrelated to the transaction
   * index it is applied at: the id no longer comes from the replication log.
   */
  private static final long PROPOSED = 4_000_000L;
  private static final long LATER_PROPOSED = 5_000_000L;

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.OBJECT_STORE;
  }

  private void setupVersionedBucket() throws Exception {
    setupVersionedBucket(OzoneConsts.QUOTA_RESET, OzoneConsts.QUOTA_RESET);
  }

  private void setupVersionedBucket(long quotaInBytes, long quotaInNamespace)
      throws Exception {
    setupVersionedBucket(quotaInBytes, quotaInNamespace, 0L);
  }

  private void setupVersionedBucket(long quotaInBytes, long quotaInNamespace,
      long usedNamespace) throws Exception {
    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .setVersioningStatus(BucketVersioningStatus.ENABLED)
        .setQuotaInBytes(quotaInBytes)
        .setQuotaInNamespace(quotaInNamespace)
        .setUsedNamespace(usedNamespace)
        .setCreationTime(Time.now())
        .build();
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getBucketKey(volumeName, bucketName)),
        CacheValue.get(1L, bucketInfo));
  }

  /** Puts a current version into keyTable, as an earlier write would have. */
  private String seedCurrentVersion(Long versionId) throws Exception {
    return seedCurrentVersion(versionId, false);
  }

  /** Puts a current version of another key, to batch alongside the first. */
  private void seedCurrentVersion(String otherKey, Long versionId)
      throws Exception {
    OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, otherKey, replicationConfig)
        .setVersionId(versionId)
        .build();
    omMetadataManager.getKeyTable(getBucketLayout()).put(
        omMetadataManager.getOzoneKey(volumeName, bucketName, otherKey),
        keyInfo);
  }

  private String seedCurrentVersion(Long versionId, boolean deleteMarker)
      throws Exception {
    OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, keyName, replicationConfig)
        .setVersionId(versionId)
        .setDeleteMarker(deleteMarker)
        .build();
    String ozoneKey = omMetadataManager.getOzoneKey(
        volumeName, bucketName, keyName);
    omMetadataManager.getKeyTable(getBucketLayout()).put(ozoneKey, keyInfo);
    return ozoneKey;
  }

  private OMRequest deleteRequest(long proposedVersionId) {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setModificationTime(Time.now())
        .setProposedVersionId(proposedVersionId)
        .build();
    return OMRequest.newBuilder()
        .setDeleteKeyRequest(DeleteKeyRequest.newBuilder().setKeyArgs(keyArgs))
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  private OMClientResponse deleteAt(long trxnLogIndex, long proposedVersionId)
      throws Exception {
    OMClientResponse response =
        new OMKeyDeleteRequest(deleteRequest(proposedVersionId),
            getBucketLayout()).validateAndUpdateCache(ozoneManager,
                trxnLogIndex);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    return response;
  }

  private OMRequest deleteVersionRequest(Long versionId, boolean nullVersion) {
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setModificationTime(Time.now());
    if (versionId != null) {
      keyArgs.setVersionId(versionId);
    }
    if (nullVersion) {
      keyArgs.setNullVersion(true);
    }
    return OMRequest.newBuilder()
        .setDeleteKeyRequest(DeleteKeyRequest.newBuilder().setKeyArgs(keyArgs))
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  private OMClientResponse deleteVersionAt(Long versionId, boolean nullVersion,
      long trxnLogIndex) throws Exception {
    return new OMKeyDeleteRequest(deleteVersionRequest(versionId, nullVersion),
        getBucketLayout()).validateAndUpdateCache(ozoneManager, trxnLogIndex);
  }

  private void seedNoncurrentVersion(long versionId, boolean nullVersion)
      throws Exception {
    OmKeyInfo version = OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, keyName, replicationConfig)
        .setVersionId(versionId)
        .setNullVersion(nullVersion)
        .build();
    omMetadataManager.getVersionedKeyTable().put(
        omMetadataManager.getVersionedOzoneKey(
            volumeName, bucketName, keyName, versionId), version);
  }

  @Test
  public void testPermanentDeleteRemovesOnlyTheAddressedVersion()
      throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(300L);
    seedNoncurrentVersion(100L, false);
    seedNoncurrentVersion(200L, false);

    OMClientResponse response = deleteVersionAt(100L, false, 400L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());

    assertNull(noncurrentVersion(100L));
    assertNotNull(noncurrentVersion(200L));
    assertEquals(300L, currentVersion().getVersionId());
  }

  /**
   * Permanent delete is the one delete on a versioned bucket that destroys
   * data, so the version's blocks have to reach the deletedTable, which is the
   * only path that ever reclaims them.
   */
  @Test
  public void testPermanentDeleteSendsTheBlocksToTheDeletedTable()
      throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(300L);
    seedNoncurrentVersionWithBlocks(100L);

    OMClientResponse response = deleteVersionAt(100L, false, 400L);
    try (BatchOperation batch =
             omMetadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(omMetadataManager, batch);
      omMetadataManager.getStore().commitBatchOperation(batch);
    }

    assertNull(omMetadataManager.getVersionedKeyTable().getSkipCache(
        omMetadataManager.getVersionedOzoneKey(
            volumeName, bucketName, keyName, 100L)));
    assertFalse(deletedVersions().isEmpty(),
        "the deleted version's blocks never reached the deletedTable");
  }

  /**
   * A noncurrent version holding one block. A version with no blocks has
   * nothing to reclaim and never reaches the deletedTable.
   */
  private void seedNoncurrentVersionWithBlocks(long versionId)
      throws Exception {
    OmKeyInfo version = OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, keyName, replicationConfig)
        .setVersionId(versionId)
        .build();
    OMRequestTestUtils.addKeyLocationInfo(version, 0L, 100L);
    omMetadataManager.getVersionedKeyTable().put(
        omMetadataManager.getVersionedOzoneKey(
            volumeName, bucketName, keyName, versionId), version);
  }

  /** Every record the deletedTable holds for this key. */
  private List<OmKeyInfo> deletedVersions() throws Exception {
    List<OmKeyInfo> deleted = new ArrayList<>();
    try (Table.KeyValueIterator<String, RepeatedOmKeyInfo> entries =
             omMetadataManager.getDeletedTable().iterator()) {
      while (entries.hasNext()) {
        Table.KeyValue<String, RepeatedOmKeyInfo> entry = entries.next();
        if (entry.getKey().contains(keyName)) {
          deleted.addAll(entry.getValue().getOmKeyInfoList());
        }
      }
    }
    return deleted;
  }

  @Test
  public void testPermanentDeleteReleasesQuota() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(300L);
    seedNoncurrentVersion(100L, false);

    OmBucketInfo before = omMetadataManager.getBucketTable()
        .get(omMetadataManager.getBucketKey(volumeName, bucketName));
    long usedNamespaceBefore = before.getUsedNamespace();

    deleteVersionAt(100L, false, 400L);

    OmBucketInfo after = omMetadataManager.getBucketTable()
        .get(omMetadataManager.getBucketKey(volumeName, bucketName));
    assertEquals(usedNamespaceBefore - 1, after.getUsedNamespace());
  }

  @Test
  public void testPermanentDeleteOfNullVersion() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(300L);
    seedNoncurrentVersion(100L, true);
    seedNoncurrentVersion(200L, false);

    OMClientResponse response = deleteVersionAt(null, true, 400L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());

    assertNull(noncurrentVersion(100L));
    assertNotNull(noncurrentVersion(200L));
  }

  @Test
  public void testPermanentDeleteOfUnknownVersionIsNotFound() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(300L);

    OMClientResponse response = deleteVersionAt(999L, false, 400L);
    assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        response.getOMResponse().getStatus());
  }

  /** Removing the current version has to promote a successor; that is T4.3. */
  @Test
  public void testPermanentDeleteOfCurrentVersionIsRejectedForNow()
      throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(300L);
    seedNoncurrentVersion(100L, false);

    OMClientResponse response = deleteVersionAt(300L, false, 400L);
    assertEquals(OzoneManagerProtocolProtos.Status.NOT_SUPPORTED_OPERATION,
        response.getOMResponse().getStatus());
    assertEquals(300L, currentVersion().getVersionId());
    assertNotNull(noncurrentVersion(100L));
  }

  private OmKeyInfo currentVersion() throws Exception {
    return omMetadataManager.getKeyTable(getBucketLayout()).get(
        omMetadataManager.getOzoneKey(volumeName, bucketName, keyName));
  }

  private OMRequest commitRequest(boolean isHsync, long proposedVersionId,
      long dataSize, long writerClientId) {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setModificationTime(Time.now())
        .setDataSize(dataSize)
        .setProposedVersionId(proposedVersionId)
        .build();
    return OMRequest.newBuilder()
        .setCommitKeyRequest(CommitKeyRequest.newBuilder()
            .setKeyArgs(keyArgs)
            .setClientID(writerClientId)
            .setHsync(isHsync))
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitKey)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  private OMClientResponse commitAt(long trxnLogIndex, long proposedVersionId)
      throws Exception {
    return commitAt(trxnLogIndex, proposedVersionId, false);
  }

  private OMClientResponse commitAt(long trxnLogIndex, long proposedVersionId,
      boolean isHsync) throws Exception {
    OMClientResponse response =
        commitAt(trxnLogIndex, proposedVersionId, isHsync, 0L, clientID);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    return response;
  }

  /**
   * Commits the key as the writer identified by {@code writerClientId}. A
   * non-hsync commit tombstones its own open key, so a second write of the
   * same key comes from a different client, as it would in practice.
   */
  private OMClientResponse commitAt(long trxnLogIndex, long proposedVersionId,
      boolean isHsync, long dataSize, long writerClientId) throws Exception {
    OMRequestTestUtils.addKeyToTable(true, volumeName, bucketName, keyName,
        writerClientId, replicationConfig, omMetadataManager);
    return new OMKeyCommitRequest(
        commitRequest(isHsync, proposedVersionId, dataSize, writerClientId),
        getBucketLayout()).validateAndUpdateCache(ozoneManager, trxnLogIndex);
  }

  private OmKeyInfo noncurrentVersion(long versionId) throws Exception {
    return omMetadataManager.getVersionedKeyTable().get(
        omMetadataManager.getVersionedOzoneKey(
            volumeName, bucketName, keyName, versionId));
  }

  @Test
  public void testCommitOfNewKeyAssignsVersionIdAndHasNoNoncurrentVersion()
      throws Exception {
    setupVersionedBucket();

    commitAt(500L, PROPOSED);

    OmKeyInfo current = omMetadataManager.getKeyTable(getBucketLayout()).get(
        omMetadataManager.getOzoneKey(volumeName, bucketName, keyName));
    assertNotNull(current);
    assertEquals(PROPOSED, current.getVersionId());
    assertFalse(current.isDeleteMarker());
    assertFalse(current.isNullVersion());
    assertNull(noncurrentVersion(PROPOSED));
  }

  /**
   * The id is proposed on the OM that received the request, so it travels in
   * the replicated request rather than being generated during apply.
   */
  @Test
  public void testPreExecuteProposesAVersionId() throws Exception {
    setupVersionedBucket();

    OMRequest processed = new OMKeyCommitRequest(
        commitRequest(false, VersionIdGenerator.UNSET_VERSION_ID, 0L,
            clientID),
        getBucketLayout()).preExecute(ozoneManager);

    assertTrue(processed.getCommitKeyRequest().getKeyArgs()
        .getProposedVersionId() > VersionIdGenerator.UNSET_VERSION_ID);
  }

  /**
   * A proposal is only a floor. A current version numbered above it - which a
   * clock regression or a change of generator can produce - still has to be
   * superseded by a larger id, or the versionedKeyTable would order the key's
   * versions wrongly.
   */
  @Test
  public void testCommitRaisesAProposalBelowTheCurrentVersion()
      throws Exception {
    setupVersionedBucket();
    String ozoneKey = seedCurrentVersion(PROPOSED + 10);

    commitAt(500L, PROPOSED);

    assertEquals(PROPOSED + 11,
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey)
            .getVersionId());
  }

  @Test
  public void testOverwriteKeepsPreviousVersionAsNoncurrent()
      throws Exception {
    setupVersionedBucket();
    String ozoneKey = seedCurrentVersion(100L);

    commitAt(500L, PROPOSED);

    OmKeyInfo current =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertEquals(PROPOSED, current.getVersionId());

    OmKeyInfo noncurrent = noncurrentVersion(100L);
    assertNotNull(noncurrent);
    assertEquals(100L, noncurrent.getVersionId());
    assertFalse(noncurrent.isNullVersion());
  }

  /**
   * Both tables are updated in one WriteBatch, so what the table cache shows
   * during apply is what the DB holds once the batch is committed: a reader
   * that missed the cache finds the new current version and the version it
   * superseded, never one without the other.
   */
  @Test
  public void testBothTablesAreWrittenInOneBatch() throws Exception {
    setupVersionedBucket();
    String ozoneKey = seedCurrentVersion(100L);

    OMClientResponse response = commitAt(500L, PROPOSED);
    try (BatchOperation batch =
             omMetadataManager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(omMetadataManager, batch);
      omMetadataManager.getStore().commitBatchOperation(batch);
    }

    assertEquals(PROPOSED, omMetadataManager.getKeyTable(getBucketLayout())
        .getSkipCache(ozoneKey).getVersionId());
    assertNotNull(omMetadataManager.getVersionedKeyTable().getSkipCache(
        omMetadataManager.getVersionedOzoneKey(
            volumeName, bucketName, keyName, 100L)));
  }

  /**
   * A record written before versioning was enabled carries no versionId, so on
   * the first overwrite it becomes the key's single null version.
   */
  @Test
  public void testPreVersioningRecordBecomesNullVersion() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(null);

    commitAt(500L, PROPOSED);

    OmKeyInfo noncurrent =
        noncurrentVersion(VersionIdGenerator.UNSET_VERSION_ID);
    assertNotNull(noncurrent);
    assertTrue(noncurrent.isNullVersion());
    assertEquals(VersionIdGenerator.UNSET_VERSION_ID,
        noncurrent.getVersionId());
  }

  /**
   * The overwritten version keeps its blocks in the versionedKeyTable: they
   * must not be queued for reclamation, and they must not be carried into the
   * new current version's in-record block version list either.
   */
  @Test
  public void testOverwriteDoesNotReclaimOrInheritPreviousBlocks()
      throws Exception {
    setupVersionedBucket();
    String ozoneKey = seedCurrentVersion(100L);

    commitAt(500L, PROPOSED);

    assertNull(omMetadataManager.getDeletedTable().get(ozoneKey));
    OmKeyInfo current =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertEquals(1, current.getKeyLocationVersions().size());
    assertNotNull(noncurrentVersion(100L));
  }

  /**
   * An hsync re-commit keeps updating the version it opened rather than
   * creating a new one, so its versionId stays frozen and nothing moves to the
   * versionedKeyTable.
   */
  @Test
  public void testHsyncRecommitKeepsVersionIdFrozen() throws Exception {
    setupVersionedBucket();

    commitAt(500L, PROPOSED, true);
    OmKeyInfo firstCommit = omMetadataManager.getKeyTable(getBucketLayout())
        .get(omMetadataManager.getOzoneKey(volumeName, bucketName, keyName));
    assertEquals(PROPOSED, firstCommit.getVersionId());

    commitAt(600L, LATER_PROPOSED, true);
    OmKeyInfo recommitted = omMetadataManager.getKeyTable(getBucketLayout())
        .get(omMetadataManager.getOzoneKey(volumeName, bucketName, keyName));
    assertEquals(PROPOSED, recommitted.getVersionId());
    assertNull(noncurrentVersion(PROPOSED));
  }

  @Test
  public void testDeleteInsertsMarkerAndKeepsPreviousVersion()
      throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(100L);

    deleteAt(200L, PROPOSED);

    OmKeyInfo current = currentVersion();
    assertNotNull(current);
    assertTrue(current.isDeleteMarker());
    assertEquals(PROPOSED, current.getVersionId());
    assertEquals(0L, current.getDataSize());
    assertTrue(current.getLatestVersionLocations().getLocationList().isEmpty());

    OmKeyInfo noncurrent = noncurrentVersion(100L);
    assertNotNull(noncurrent);
    assertFalse(noncurrent.isDeleteMarker());
  }

  /** A marker is a version, so a delete proposes an id in preExecute too. */
  @Test
  public void testDeletePreExecuteProposesAVersionId() throws Exception {
    setupVersionedBucket();

    OMRequest processed = new OMKeyDeleteRequest(
        deleteRequest(VersionIdGenerator.UNSET_VERSION_ID), getBucketLayout())
        .preExecute(ozoneManager);

    assertTrue(processed.getDeleteKeyRequest().getKeyArgs()
        .getProposedVersionId() > VersionIdGenerator.UNSET_VERSION_ID);
  }

  /**
   * A batch delete is the same operation over more keys, so it has to leave
   * the bucket in the same shape a single delete would: a marker per key, the
   * version each one superseded kept. Hard-deleting instead would destroy the
   * current version and strand the versions under it.
   */
  @Test
  public void testBatchDeleteInsertsAMarkerPerKey() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(100L);
    String otherKey = keyName + "-other";
    seedCurrentVersion(otherKey, 150L);

    OMClientResponse response = new OMKeysDeleteRequest(
        batchDeleteRequest(PROPOSED, keyName, otherKey), getBucketLayout())
        .validateAndUpdateCache(ozoneManager, 200L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());

    // both keys are now markers, and neither superseded version was reclaimed
    assertTrue(currentVersion().isDeleteMarker());
    assertEquals(PROPOSED, currentVersion().getVersionId());
    assertNotNull(noncurrentVersion(100L));

    OmKeyInfo otherCurrent = omMetadataManager.getKeyTable(getBucketLayout())
        .get(omMetadataManager.getOzoneKey(volumeName, bucketName, otherKey));
    assertTrue(otherCurrent.isDeleteMarker());
    // one proposal covers the batch: ids only have to increase within a key
    assertEquals(PROPOSED, otherCurrent.getVersionId());
    assertNotNull(omMetadataManager.getVersionedKeyTable().get(
        omMetadataManager.getVersionedOzoneKey(volumeName, bucketName,
            otherKey, 150L)));

    assertInstanceOf(OMKeysDeleteMarkerResponse.class, response);
  }

  /** The proposal is a floor for each key independently. */
  @Test
  public void testBatchDeleteRaisesTheProposalPerKey() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(PROPOSED + 10);

    new OMKeysDeleteRequest(batchDeleteRequest(PROPOSED, keyName),
        getBucketLayout()).validateAndUpdateCache(ozoneManager, 200L);

    assertEquals(PROPOSED + 11, currentVersion().getVersionId());
  }

  /** A bucket without versioning keeps the old hard-delete behaviour. */
  @Test
  public void testBatchDeleteOnAnUnversionedBucketStillDeletes()
      throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    seedCurrentVersion(null);

    OMClientResponse response = new OMKeysDeleteRequest(
        batchDeleteRequest(PROPOSED, keyName), getBucketLayout())
        .validateAndUpdateCache(ozoneManager, 200L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    assertNull(currentVersion());
  }

  private OMRequest batchDeleteRequest(long proposedVersionId, String... keys) {
    return OMRequest.newBuilder()
        .setDeleteKeysRequest(DeleteKeysRequest.newBuilder()
            .setDeleteKeys(DeleteKeyArgs.newBuilder()
                .setVolumeName(volumeName)
                .setBucketName(bucketName)
                .addAllKeys(java.util.Arrays.asList(keys))
                .setProposedVersionId(proposedVersionId)))
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKeys)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  /** S3 inserts a delete marker even for a key that does not exist. */
  @Test
  public void testDeleteOfMissingKeyStillInsertsMarker() throws Exception {
    setupVersionedBucket();

    deleteAt(200L, PROPOSED);

    OmKeyInfo current = currentVersion();
    assertNotNull(current);
    assertTrue(current.isDeleteMarker());
    assertEquals(PROPOSED, current.getVersionId());
    assertNull(noncurrentVersion(VersionIdGenerator.UNSET_VERSION_ID));
  }

  /** Deleting a key whose current version is already a marker stacks another. */
  @Test
  public void testDeleteStacksAnotherMarker() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(100L, true);

    deleteAt(200L, PROPOSED);

    OmKeyInfo current = currentVersion();
    assertTrue(current.isDeleteMarker());
    assertEquals(PROPOSED, current.getVersionId());

    OmKeyInfo stacked = noncurrentVersion(100L);
    assertNotNull(stacked);
    assertTrue(stacked.isDeleteMarker());
  }

  @Test
  public void testDeleteOfPreVersioningRecordMovesItToNullVersion()
      throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(null);

    deleteAt(200L, PROPOSED);

    OmKeyInfo noncurrent =
        noncurrentVersion(VersionIdGenerator.UNSET_VERSION_ID);
    assertNotNull(noncurrent);
    assertTrue(noncurrent.isNullVersion());
    assertFalse(noncurrent.isDeleteMarker());
  }

  /**
   * Every version counts against the bucket's space quota: an overwrite adds
   * the new version's usage without releasing the version it supersedes.
   */
  @Test
  public void testEachVersionCountsAgainstUsedBytes() throws Exception {
    setupVersionedBucket();
    String bucketKey =
        omMetadataManager.getBucketKey(volumeName, bucketName);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        commitAt(500L, PROPOSED, false, 100L, clientID).getOMResponse().getStatus());
    long afterFirst = omMetadataManager.getBucketTable().get(bucketKey)
        .getUsedBytes();
    assertEquals(QuotaUtil.getReplicatedSize(100L, replicationConfig),
        afterFirst);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        commitAt(600L, LATER_PROPOSED, false, 300L, clientID + 1).getOMResponse().getStatus());
    long afterSecond = omMetadataManager.getBucketTable().get(bucketKey)
        .getUsedBytes();
    assertEquals(afterFirst
            + QuotaUtil.getReplicatedSize(300L, replicationConfig),
        afterSecond);
    assertEquals(2, omMetadataManager.getBucketTable().get(bucketKey)
        .getUsedNamespace());
  }

  @Test
  public void testVersionedWriteRejectedWhenSpaceQuotaExceeded()
      throws Exception {
    long quota = QuotaUtil.getReplicatedSize(150L, replicationConfig);
    setupVersionedBucket(quota, OzoneConsts.QUOTA_RESET);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        commitAt(500L, PROPOSED, false, 100L, clientID).getOMResponse().getStatus());
    // the first version is not released, so the second one no longer fits
    assertEquals(OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED,
        commitAt(600L, LATER_PROPOSED, false, 100L, clientID + 1)
            .getOMResponse().getStatus());

    OmKeyInfo current = currentVersion();
    assertEquals(PROPOSED, current.getVersionId());
    assertNull(noncurrentVersion(PROPOSED));
  }

  @Test
  public void testVersionedWriteRejectedWhenNamespaceQuotaExceeded()
      throws Exception {
    setupVersionedBucket(OzoneConsts.QUOTA_RESET, 1L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        commitAt(500L, PROPOSED, false, 0L, clientID).getOMResponse().getStatus());
    // each version is a record of its own, so the second one needs namespace
    assertEquals(OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED,
        commitAt(600L, LATER_PROPOSED, false, 0L, clientID + 1).getOMResponse().getStatus());
  }

  /** A delete marker is a record too, so it needs namespace quota. */
  @Test
  public void testDeleteMarkerRejectedWhenNamespaceQuotaExceeded()
      throws Exception {
    // the bucket already holds the one key its namespace quota allows
    setupVersionedBucket(OzoneConsts.QUOTA_RESET, 1L, 1L);
    seedCurrentVersion(100L);

    OMClientResponse response =
        new OMKeyDeleteRequest(deleteRequest(PROPOSED), getBucketLayout())
            .validateAndUpdateCache(ozoneManager, 200L);
    assertEquals(OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED,
        response.getOMResponse().getStatus());
    assertFalse(currentVersion().isDeleteMarker());
    // a rejected request must not leave the superseded version behind in the
    // versionedKeyTable cache, and its response has to declare that table so
    // that the double buffer cleans up whatever the request did touch
    assertNull(noncurrentVersion(100L));
    assertInstanceOf(OMKeyDeleteMarkerResponse.class, response);
  }

  /**
   * A delete marker holds no blocks, so it consumes namespace but no space,
   * and the superseded version keeps its own usage.
   */
  @Test
  public void testDeleteMarkerConsumesNamespaceButNoSpace() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(100L);
    String bucketKey =
        omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo before = omMetadataManager.getBucketTable().get(bucketKey);
    long usedBytes = before.getUsedBytes();
    long usedNamespace = before.getUsedNamespace();

    deleteAt(200L, PROPOSED);

    OmBucketInfo after = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(usedBytes, after.getUsedBytes());
    assertEquals(usedNamespace + 1, after.getUsedNamespace());
  }
}
