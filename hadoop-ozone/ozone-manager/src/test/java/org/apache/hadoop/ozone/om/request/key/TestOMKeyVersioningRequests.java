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

import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.BucketVersioningStatus;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.helpers.VersionIdGenerator;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyDeleteMarkerResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyRequest;
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

  private void setupSuspendedBucket() throws Exception {
    setupVersionedBucket(OzoneConsts.QUOTA_RESET, OzoneConsts.QUOTA_RESET, 0L,
        BucketVersioningStatus.SUSPENDED);
  }

  private void setupVersionedBucket(long quotaInBytes, long quotaInNamespace,
      long usedNamespace) throws Exception {
    setupVersionedBucket(quotaInBytes, quotaInNamespace, usedNamespace,
        BucketVersioningStatus.ENABLED);
  }

  private void setupVersionedBucket(long quotaInBytes, long quotaInNamespace,
      long usedNamespace, BucketVersioningStatus status) throws Exception {
    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .setVersioningStatus(status)
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

  private String seedCurrentVersion(Long versionId, boolean deleteMarker)
      throws Exception {
    return seedCurrentVersion(versionId, deleteMarker, false);
  }

  private String seedCurrentVersion(Long versionId, boolean deleteMarker,
      boolean nullVersion) throws Exception {
    return seedCurrentVersion(versionId, deleteMarker, nullVersion, false);
  }

  private String seedCurrentVersion(Long versionId, boolean deleteMarker,
      boolean nullVersion, boolean withBlocks) throws Exception {
    OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, keyName, replicationConfig)
        .setVersionId(versionId)
        .setDeleteMarker(deleteMarker)
        .setNullVersion(nullVersion)
        .build();
    if (withBlocks) {
      OMRequestTestUtils.addKeyLocationInfo(keyInfo, 0L, 1000L);
    }
    String ozoneKey = omMetadataManager.getOzoneKey(
        volumeName, bucketName, keyName);
    omMetadataManager.getKeyTable(getBucketLayout()).put(ozoneKey, keyInfo);
    return ozoneKey;
  }

  private OMRequest deleteRequest() {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setModificationTime(Time.now())
        .build();
    return OMRequest.newBuilder()
        .setDeleteKeyRequest(DeleteKeyRequest.newBuilder().setKeyArgs(keyArgs))
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  private OMClientResponse deleteAt(long trxnLogIndex) throws Exception {
    OMClientResponse response = new OMKeyDeleteRequest(deleteRequest(),
        getBucketLayout()).validateAndUpdateCache(ozoneManager, trxnLogIndex);
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

  /** A noncurrent version that only exists in the table cache, not in the DB. */
  private void cacheOnlyNoncurrentVersion(long versionId) throws Exception {
    OmKeyInfo version = OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, keyName, replicationConfig)
        .setVersionId(versionId)
        .build();
    omMetadataManager.getVersionedKeyTable().addCacheEntry(
        omMetadataManager.getVersionedOzoneKey(
            volumeName, bucketName, keyName, versionId), version, 350L);
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

  @Test
  public void testDeletingCurrentVersionPromotesTheNextNewest()
      throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(300L);
    seedNoncurrentVersion(100L, false);
    seedNoncurrentVersion(200L, false);

    OMClientResponse response = deleteVersionAt(300L, false, 400L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());

    // the newest remaining version takes over, unchanged
    OmKeyInfo current = currentVersion();
    assertNotNull(current);
    assertEquals(200L, current.getVersionId());
    assertFalse(current.isDeleteMarker());
    // and no longer counts as noncurrent
    assertNull(noncurrentVersion(200L));
    assertNotNull(noncurrentVersion(100L));
  }

  /**
   * A version written by a transaction that the double buffer has not flushed
   * yet lives only in the table cache. Promotion has to see it, otherwise an
   * older version takes over and the newest one is orphaned.
   */
  @Test
  public void testPromotionSeesVersionsStillInCache() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(300L);
    seedNoncurrentVersion(100L, false);
    cacheOnlyNoncurrentVersion(200L);

    deleteVersionAt(300L, false, 400L);

    assertEquals(200L, currentVersion().getVersionId());
    assertNotNull(noncurrentVersion(100L));
  }

  /**
   * The mirror case: a version removed by an unflushed transaction is a
   * tombstone in the cache while the DB still holds it. Promotion must not
   * bring it back.
   */
  @Test
  public void testPromotionSkipsVersionsTombstonedInCache() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(300L);
    seedNoncurrentVersion(100L, false);
    seedNoncurrentVersion(200L, false);
    // 200 is deleted but not flushed yet
    omMetadataManager.getVersionedKeyTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getVersionedOzoneKey(
            volumeName, bucketName, keyName, 200L)),
        CacheValue.get(350L));

    deleteVersionAt(300L, false, 400L);

    assertEquals(100L, currentVersion().getVersionId());
  }

  @Test
  public void testDeletingTheOnlyVersionRemovesTheKey() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(300L);

    OMClientResponse response = deleteVersionAt(300L, false, 400L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());

    assertNull(currentVersion());
  }

  /** Deleting a current delete marker is how S3 restores an object. */
  @Test
  public void testDeletingCurrentMarkerRestoresTheObject() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(300L, true);
    seedNoncurrentVersion(100L, false);

    OMClientResponse response = deleteVersionAt(300L, false, 400L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());

    OmKeyInfo current = currentVersion();
    assertNotNull(current);
    assertEquals(100L, current.getVersionId());
    assertFalse(current.isDeleteMarker());
  }

  private OmKeyInfo currentVersion() throws Exception {
    return omMetadataManager.getKeyTable(getBucketLayout()).get(
        omMetadataManager.getOzoneKey(volumeName, bucketName, keyName));
  }

  private OMRequest commitRequest(boolean isHsync, long dataSize,
      long writerClientId) {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setModificationTime(Time.now())
        .setDataSize(dataSize)
        .build();
    return OMRequest.newBuilder()
        .setCommitKeyRequest(CommitKeyRequest.newBuilder()
            .setKeyArgs(keyArgs)
            .setClientID(writerClientId)
            .setHsync(isHsync))
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitKey)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  private OMClientResponse commitAt(long trxnLogIndex) throws Exception {
    return commitAt(trxnLogIndex, false);
  }

  private OMClientResponse commitAt(long trxnLogIndex, boolean isHsync)
      throws Exception {
    OMClientResponse response =
        commitAt(trxnLogIndex, isHsync, 0L, clientID);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    return response;
  }

  /**
   * Commits the key as the writer identified by {@code writerClientId}. A
   * non-hsync commit tombstones its own open key, so a second write of the
   * same key comes from a different client, as it would in practice.
   */
  private OMClientResponse commitAt(long trxnLogIndex, boolean isHsync,
      long dataSize, long writerClientId) throws Exception {
    OMRequestTestUtils.addKeyToTable(true, volumeName, bucketName, keyName,
        writerClientId, replicationConfig, omMetadataManager);
    return new OMKeyCommitRequest(
        commitRequest(isHsync, dataSize, writerClientId),
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

    commitAt(500L);

    OmKeyInfo current = omMetadataManager.getKeyTable(getBucketLayout()).get(
        omMetadataManager.getOzoneKey(volumeName, bucketName, keyName));
    assertNotNull(current);
    assertEquals(500L, current.getVersionId());
    assertFalse(current.isDeleteMarker());
    assertFalse(current.isNullVersion());
    assertNull(noncurrentVersion(500L));
  }

  @Test
  public void testOverwriteKeepsPreviousVersionAsNoncurrent()
      throws Exception {
    setupVersionedBucket();
    String ozoneKey = seedCurrentVersion(100L);

    commitAt(500L);

    OmKeyInfo current =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertEquals(500L, current.getVersionId());

    OmKeyInfo noncurrent = noncurrentVersion(100L);
    assertNotNull(noncurrent);
    assertEquals(100L, noncurrent.getVersionId());
    assertFalse(noncurrent.isNullVersion());
  }

  /**
   * A record written before versioning was enabled carries no versionId, so on
   * the first overwrite it becomes the key's single null version.
   */
  @Test
  public void testPreVersioningRecordBecomesNullVersion() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(null);

    commitAt(500L);

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

    commitAt(500L);

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

    commitAt(500L, true);
    OmKeyInfo firstCommit = omMetadataManager.getKeyTable(getBucketLayout())
        .get(omMetadataManager.getOzoneKey(volumeName, bucketName, keyName));
    assertEquals(500L, firstCommit.getVersionId());

    commitAt(600L, true);
    OmKeyInfo recommitted = omMetadataManager.getKeyTable(getBucketLayout())
        .get(omMetadataManager.getOzoneKey(volumeName, bucketName, keyName));
    assertEquals(500L, recommitted.getVersionId());
    assertNull(noncurrentVersion(500L));
  }

  @Test
  public void testDeleteInsertsMarkerAndKeepsPreviousVersion()
      throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(100L);

    deleteAt(200L);

    OmKeyInfo current = currentVersion();
    assertNotNull(current);
    assertTrue(current.isDeleteMarker());
    assertEquals(200L, current.getVersionId());
    assertEquals(0L, current.getDataSize());
    assertTrue(current.getLatestVersionLocations().getLocationList().isEmpty());

    OmKeyInfo noncurrent = noncurrentVersion(100L);
    assertNotNull(noncurrent);
    assertFalse(noncurrent.isDeleteMarker());
  }

  /** S3 inserts a delete marker even for a key that does not exist. */
  @Test
  public void testDeleteOfMissingKeyStillInsertsMarker() throws Exception {
    setupVersionedBucket();

    deleteAt(200L);

    OmKeyInfo current = currentVersion();
    assertNotNull(current);
    assertTrue(current.isDeleteMarker());
    assertEquals(200L, current.getVersionId());
    assertNull(noncurrentVersion(VersionIdGenerator.UNSET_VERSION_ID));
  }

  /** Deleting a key whose current version is already a marker stacks another. */
  @Test
  public void testDeleteStacksAnotherMarker() throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(100L, true);

    deleteAt(200L);

    OmKeyInfo current = currentVersion();
    assertTrue(current.isDeleteMarker());
    assertEquals(200L, current.getVersionId());

    OmKeyInfo stacked = noncurrentVersion(100L);
    assertNotNull(stacked);
    assertTrue(stacked.isDeleteMarker());
  }

  @Test
  public void testDeleteOfPreVersioningRecordMovesItToNullVersion()
      throws Exception {
    setupVersionedBucket();
    seedCurrentVersion(null);

    deleteAt(200L);

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
        commitAt(500L, false, 100L, clientID).getOMResponse().getStatus());
    long afterFirst = omMetadataManager.getBucketTable().get(bucketKey)
        .getUsedBytes();
    assertEquals(QuotaUtil.getReplicatedSize(100L, replicationConfig),
        afterFirst);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        commitAt(600L, false, 300L, clientID + 1).getOMResponse().getStatus());
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
        commitAt(500L, false, 100L, clientID).getOMResponse().getStatus());
    // the first version is not released, so the second one no longer fits
    assertEquals(OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED,
        commitAt(600L, false, 100L, clientID + 1).getOMResponse().getStatus());

    OmKeyInfo current = currentVersion();
    assertEquals(500L, current.getVersionId());
    assertNull(noncurrentVersion(500L));
  }

  @Test
  public void testVersionedWriteRejectedWhenNamespaceQuotaExceeded()
      throws Exception {
    setupVersionedBucket(OzoneConsts.QUOTA_RESET, 1L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        commitAt(500L, false, 0L, clientID).getOMResponse().getStatus());
    // each version is a record of its own, so the second one needs namespace
    assertEquals(OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED,
        commitAt(600L, false, 0L, clientID + 1).getOMResponse().getStatus());
  }

  /** A delete marker is a record too, so it needs namespace quota. */
  @Test
  public void testDeleteMarkerRejectedWhenNamespaceQuotaExceeded()
      throws Exception {
    // the bucket already holds the one key its namespace quota allows
    setupVersionedBucket(OzoneConsts.QUOTA_RESET, 1L, 1L);
    seedCurrentVersion(100L);

    OMClientResponse response = new OMKeyDeleteRequest(deleteRequest(),
        getBucketLayout()).validateAndUpdateCache(ozoneManager, 200L);
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

    deleteAt(200L);

    OmBucketInfo after = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(usedBytes, after.getUsedBytes());
    assertEquals(usedNamespace + 1, after.getUsedNamespace());
  }

  /**
   * A write while versioning is suspended takes the key's null version slot
   * instead of creating a version of its own.
   */
  @Test
  public void testSuspendedWriteTakesTheNullVersionSlot() throws Exception {
    setupSuspendedBucket();

    commitAt(500L);

    OmKeyInfo current = currentVersion();
    assertNotNull(current);
    assertTrue(current.isNullVersion());
    assertEquals(500L, current.getVersionId());
  }

  /**
   * Repeated suspended writes replace each other: versions do not accumulate,
   * and the replaced record's blocks are queued for reclamation.
   */
  @Test
  public void testSuspendedWriteReplacesTheCurrentNullVersion()
      throws Exception {
    setupSuspendedBucket();
    seedCurrentVersion(100L, false, true, true);

    OMKeyCommitResponse response =
        (OMKeyCommitResponse) commitAt(500L);

    assertTrue(currentVersion().isNullVersion());
    assertEquals(500L, currentVersion().getVersionId());
    // the replaced null version is not kept as a noncurrent version
    assertNull(noncurrentVersion(100L));
    // its blocks are queued for reclamation instead
    assertReclaimed(response, 100L);
  }

  /** Asserts that the given version was queued for block reclamation. */
  private void assertReclaimed(OMKeyCommitResponse response, long versionId) {
    assertNotNull(response.getKeysToDelete());
    assertTrue(response.getKeysToDelete().values().stream()
        .flatMap(repeated -> repeated.getOmKeyInfoList().stream())
        .anyMatch(info -> info.getVersionId() != null
            && info.getVersionId() == versionId),
        "version " + versionId + " was not queued for reclamation");
  }

  /**
   * Versions created while versioning was enabled are not touched by a
   * suspended write: the one it supersedes becomes noncurrent as usual.
   */
  @Test
  public void testSuspendedWriteKeepsEnabledEraVersions() throws Exception {
    setupSuspendedBucket();
    seedCurrentVersion(300L);
    seedNoncurrentVersion(100L, false);

    commitAt(500L);

    assertTrue(currentVersion().isNullVersion());
    // the version it superseded is retained, as is the older one
    assertNotNull(noncurrentVersion(300L));
    assertFalse(noncurrentVersion(300L).isNullVersion());
    assertNotNull(noncurrentVersion(100L));
  }

  /**
   * The null version may be noncurrent, when versioning was enabled again
   * after the write that created it. A suspended write still replaces it, and
   * still becomes the current version.
   */
  @Test
  public void testSuspendedWriteReplacesANoncurrentNullVersion()
      throws Exception {
    setupSuspendedBucket();
    seedCurrentVersion(300L);
    seedNoncurrentVersion(200L, true);
    seedNoncurrentVersion(100L, false);

    commitAt(500L);

    OmKeyInfo current = currentVersion();
    assertTrue(current.isNullVersion());
    assertEquals(500L, current.getVersionId());
    // the old null version is gone, everything else is retained
    assertNull(noncurrentVersion(200L));
    assertNotNull(noncurrentVersion(300L));
    assertNotNull(noncurrentVersion(100L));
  }

  /**
   * A delete while versioning is suspended writes a marker into the key's null
   * version slot rather than creating a version.
   */
  @Test
  public void testSuspendedDeleteWritesANullMarker() throws Exception {
    setupSuspendedBucket();
    seedCurrentVersion(300L);

    deleteAt(500L);

    OmKeyInfo current = currentVersion();
    assertNotNull(current);
    assertTrue(current.isDeleteMarker());
    assertTrue(current.isNullVersion());
    // the version it superseded is retained, as under an enabled bucket
    assertNotNull(noncurrentVersion(300L));
  }

  /** The null marker replaces the null version that held the slot. */
  @Test
  public void testSuspendedDeleteReplacesTheCurrentNullVersion()
      throws Exception {
    setupSuspendedBucket();
    seedCurrentVersion(100L, false, true, true);

    OMKeyDeleteMarkerResponse response =
        (OMKeyDeleteMarkerResponse) deleteAt(500L);

    OmKeyInfo current = currentVersion();
    assertTrue(current.isDeleteMarker());
    assertTrue(current.isNullVersion());
    // the replaced record is not kept as a noncurrent version
    assertNull(noncurrentVersion(100L));
    assertNotNull(response.getKeysToDelete());
  }

  /**
   * Versions created while versioning was enabled stay readable and deletable
   * by versionId after a suspended delete.
   */
  @Test
  public void testSuspendedDeleteKeepsEnabledEraVersions() throws Exception {
    setupSuspendedBucket();
    seedCurrentVersion(300L);
    seedNoncurrentVersion(200L, true);
    seedNoncurrentVersion(100L, false);

    deleteAt(500L);

    assertTrue(currentVersion().isDeleteMarker());
    // the superseded version and the older one are retained
    assertNotNull(noncurrentVersion(300L));
    assertNotNull(noncurrentVersion(100L));
    // only the null version the marker replaced is gone
    assertNull(noncurrentVersion(200L));

    // and a retained version can still be deleted by versionId
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        deleteVersionAt(100L, false, 600L).getOMResponse().getStatus());
    assertNull(noncurrentVersion(100L));
  }
}
