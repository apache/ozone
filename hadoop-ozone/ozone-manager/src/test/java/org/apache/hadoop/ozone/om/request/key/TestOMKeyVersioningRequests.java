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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.BucketVersioningStatus;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.VersionIdGenerator;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
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
    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .setVersioningStatus(BucketVersioningStatus.ENABLED)
        .setCreationTime(Time.now())
        .build();
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getBucketKey(volumeName, bucketName)),
        CacheValue.get(1L, bucketInfo));
  }

  /** Puts a current version into keyTable, as an earlier write would have. */
  private String seedCurrentVersion(Long versionId) throws Exception {
    OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, keyName, replicationConfig)
        .setVersionId(versionId)
        .build();
    String ozoneKey = omMetadataManager.getOzoneKey(
        volumeName, bucketName, keyName);
    omMetadataManager.getKeyTable(getBucketLayout()).put(ozoneKey, keyInfo);
    return ozoneKey;
  }

  private OMRequest commitRequest(boolean isHsync, long proposedVersionId) {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setModificationTime(Time.now())
        .setDataSize(0)
        .setProposedVersionId(proposedVersionId)
        .build();
    return OMRequest.newBuilder()
        .setCommitKeyRequest(CommitKeyRequest.newBuilder()
            .setKeyArgs(keyArgs)
            .setClientID(clientID)
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
    OMRequestTestUtils.addKeyToTable(true, volumeName, bucketName, keyName,
        clientID, replicationConfig, omMetadataManager);
    OMClientResponse response = new OMKeyCommitRequest(
        commitRequest(isHsync, proposedVersionId),
        getBucketLayout()).validateAndUpdateCache(ozoneManager, trxnLogIndex);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    return response;
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
        commitRequest(false, VersionIdGenerator.UNSET_VERSION_ID),
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
}
