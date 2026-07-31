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
}
