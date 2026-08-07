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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.BucketVersioningStatus;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.key.OMObjectVersionsReclaimResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ObjectVersionsBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReclaimObjectVersionsRequest;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Tests the reclamation of noncurrent object versions selected by
 * VersionCleanupService: the versions leave the versionedKeyTable and their
 * blocks are queued in the deletedTable, with the bucket quota released.
 */
public class TestOMObjectVersionsReclaimRequest extends OMKeyRequestTests {

  private static final long BLOCK_LENGTH = 1000L;

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.OBJECT_STORE;
  }

  @Test
  public void testReclaimsSubmittedVersions() throws Exception {
    setupVersionedBucket(3 * BLOCK_LENGTH, 3L);
    seedNoncurrentVersion(100L);
    seedNoncurrentVersion(200L);
    seedNoncurrentVersion(300L);

    OMObjectVersionsReclaimResponse response = reclaim(500L,
        versionKeys(100L, 200L));

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());

    // the reclaimed versions are tombstoned in the versionedKeyTable cache
    assertReclaimedFromTable(100L);
    assertReclaimedFromTable(200L);
    // the version that was not submitted is untouched
    assertNull(omMetadataManager.getVersionedKeyTable().getCacheValue(
        new CacheKey<>(versionKey(300L))));

    // both are queued for block reclamation
    List<Long> queued = response.getKeysToDelete().values().stream()
        .flatMap(repeated -> repeated.getOmKeyInfoList().stream())
        .map(OmKeyInfo::getVersionId)
        .collect(Collectors.toList());
    assertEquals(Arrays.asList(100L, 200L), queued);

    // and their space and namespace are released
    OmBucketInfo bucketInfo = OMKeyRequest.getBucketInfo(omMetadataManager,
        volumeName, bucketName);
    assertEquals(BLOCK_LENGTH, bucketInfo.getUsedBytes());
    assertEquals(1L, bucketInfo.getUsedNamespace());
  }

  /**
   * Versions of one key share a deletedTable entry: it holds a
   * RepeatedOmKeyInfo list that KeyDeletingService evaluates one record at a
   * time.
   */
  @Test
  public void testVersionsOfOneKeyShareADeletedTableEntry() throws Exception {
    setupVersionedBucket(3 * BLOCK_LENGTH, 3L);
    seedNoncurrentVersion(100L);
    seedNoncurrentVersion(200L);

    OMObjectVersionsReclaimResponse response = reclaim(500L,
        versionKeys(100L, 200L));

    assertEquals(1, response.getKeysToDelete().size());
    assertEquals(2, response.getKeysToDelete().values().iterator().next()
        .getOmKeyInfoList().size());
  }

  /**
   * The service selects versions as of its own scan, so one may already be
   * gone - permanently deleted, or promoted into the keyTable because the
   * current version was deleted. Such a version is not this request's to
   * remove.
   */
  @Test
  public void testSkipsVersionsThatAreAlreadyGone() throws Exception {
    setupVersionedBucket(BLOCK_LENGTH, 1L);
    seedNoncurrentVersion(100L);

    OMObjectVersionsReclaimResponse response = reclaim(500L,
        versionKeys(100L, 200L));

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    assertReclaimedFromTable(100L);
    // only the version that still existed was reclaimed and accounted for
    assertEquals(1, response.getKeysToDelete().values().stream()
        .mapToInt(repeated -> repeated.getOmKeyInfoList().size()).sum());
    OmBucketInfo bucketInfo = OMKeyRequest.getBucketInfo(omMetadataManager,
        volumeName, bucketName);
    assertEquals(0L, bucketInfo.getUsedBytes());
    assertEquals(0L, bucketInfo.getUsedNamespace());
  }

  @Test
  public void testSkipsBucketThatNoLongerExists() throws Exception {
    setupVersionedBucket(BLOCK_LENGTH, 1L);
    seedNoncurrentVersion(100L);

    OMRequest request = reclaimRequest(ObjectVersionsBucket.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName("deleted-bucket")
        .addVersionKeys(versionKey(100L))
        .build());
    OMObjectVersionsReclaimResponse response =
        (OMObjectVersionsReclaimResponse) new OMObjectVersionsReclaimRequest(
            request).validateAndUpdateCache(ozoneManager, 500L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    assertTrue(response.getKeysToDelete().isEmpty());
    // the version of the bucket that does exist is left alone
    assertNull(omMetadataManager.getVersionedKeyTable().getCacheValue(
        new CacheKey<>(versionKey(100L))));
  }

  /** A delete marker holds no blocks: it releases namespace but no space. */
  @Test
  public void testDeleteMarkerReleasesNamespaceOnly() throws Exception {
    setupVersionedBucket(BLOCK_LENGTH, 1L);
    seedDeleteMarker(100L);

    OMObjectVersionsReclaimResponse response = reclaim(500L,
        versionKeys(100L));

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    assertReclaimedFromTable(100L);
    // an empty record is not queued for block reclamation at all
    assertTrue(response.getKeysToDelete().isEmpty());
    OmBucketInfo bucketInfo = OMKeyRequest.getBucketInfo(omMetadataManager,
        volumeName, bucketName);
    // the marker is a record of its own, so it held a namespace slot but no
    // space: reclaiming it gives the slot back and leaves usedBytes alone
    assertEquals(BLOCK_LENGTH, bucketInfo.getUsedBytes());
    assertEquals(0L, bucketInfo.getUsedNamespace());
  }

  /**
   * A key whose only remaining version is a delete marker is invisible to
   * reads and carries no versionId to address it by, so the whole key goes.
   */
  @Test
  public void testReclaimsExpiredDeleteMarker() throws Exception {
    setupVersionedBucket(BLOCK_LENGTH, 1L);
    String markerKey = seedCurrentDeleteMarker();

    OMObjectVersionsReclaimResponse response =
        reclaimMarkers(500L, markerKey);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    CacheValue<OmKeyInfo> cached = omMetadataManager
        .getKeyTable(getBucketLayout())
        .getCacheValue(new CacheKey<>(markerKey));
    assertNotNull(cached);
    assertNull(cached.getCacheValue(), "the marker was not tombstoned");

    // a marker holds no blocks, so nothing is queued for block reclamation
    assertTrue(response.getKeysToDelete().isEmpty());
    // it did hold a namespace slot of its own
    OmBucketInfo bucketInfo = OMKeyRequest.getBucketInfo(omMetadataManager,
        volumeName, bucketName);
    assertEquals(BLOCK_LENGTH, bucketInfo.getUsedBytes());
    assertEquals(0L, bucketInfo.getUsedNamespace());
  }

  /**
   * Removing the marker while a noncurrent version survives would promote that
   * version back to current, resurrecting an object the user deleted.
   */
  @Test
  public void testKeepsMarkerWhileAVersionSurvives() throws Exception {
    setupVersionedBucket(BLOCK_LENGTH, 2L);
    String markerKey = seedCurrentDeleteMarker();
    seedNoncurrentVersion(100L);

    OMObjectVersionsReclaimResponse response =
        reclaimMarkers(500L, markerKey);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    assertNull(omMetadataManager.getKeyTable(getBucketLayout())
        .getCacheValue(new CacheKey<>(markerKey)));
    OmBucketInfo bucketInfo = OMKeyRequest.getBucketInfo(omMetadataManager,
        volumeName, bucketName);
    assertEquals(2L, bucketInfo.getUsedNamespace());
  }

  /**
   * A write since the scan makes the key's current version a real object
   * again, so there is nothing expired to remove.
   */
  @Test
  public void testSkipsMarkerSupersededSinceTheScan() throws Exception {
    setupVersionedBucket(BLOCK_LENGTH, 1L);
    String objectKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
    OmKeyInfo live = OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, keyName, replicationConfig)
        .setVersionId(200L)
        .build();
    omMetadataManager.getKeyTable(getBucketLayout()).put(objectKey, live);

    OMObjectVersionsReclaimResponse response =
        reclaimMarkers(500L, objectKey);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    assertNull(omMetadataManager.getKeyTable(getBucketLayout())
        .getCacheValue(new CacheKey<>(objectKey)));
    OmBucketInfo bucketInfo = OMKeyRequest.getBucketInfo(omMetadataManager,
        volumeName, bucketName);
    assertEquals(1L, bucketInfo.getUsedNamespace());
  }

  private String seedCurrentDeleteMarker() throws Exception {
    OmKeyInfo marker = OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, keyName, replicationConfig)
        .setVersionId(300L)
        .setDeleteMarker(true)
        .build();
    String objectKey =
        omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
    omMetadataManager.getKeyTable(getBucketLayout()).put(objectKey, marker);
    return objectKey;
  }

  private OMObjectVersionsReclaimResponse reclaimMarkers(long trxnLogIndex,
      String... markerKeys) throws Exception {
    OMRequest request = reclaimRequest(ObjectVersionsBucket.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .addAllMarkerKeys(Arrays.asList(markerKeys))
        .build());
    return (OMObjectVersionsReclaimResponse)
        new OMObjectVersionsReclaimRequest(request)
            .validateAndUpdateCache(ozoneManager, trxnLogIndex);
  }

  private void setupVersionedBucket(long usedBytes, long usedNamespace)
      throws Exception {
    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .setVersioningStatus(BucketVersioningStatus.ENABLED)
        .setQuotaInBytes(OzoneConsts.QUOTA_RESET)
        .setQuotaInNamespace(OzoneConsts.QUOTA_RESET)
        .setUsedBytes(usedBytes)
        .setUsedNamespace(usedNamespace)
        .setCreationTime(Time.now())
        .build();
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getBucketKey(volumeName, bucketName)),
        CacheValue.get(1L, bucketInfo));
  }

  private void seedNoncurrentVersion(long versionId) throws Exception {
    seedNoncurrentVersion(versionId, false);
  }

  private void seedDeleteMarker(long versionId) throws Exception {
    seedNoncurrentVersion(versionId, true);
  }

  private void seedNoncurrentVersion(long versionId, boolean deleteMarker)
      throws Exception {
    OmKeyInfo version = OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, keyName, replicationConfig)
        .setVersionId(versionId)
        .setDeleteMarker(deleteMarker)
        .build();
    if (!deleteMarker) {
      OMRequestTestUtils.addKeyLocationInfo(version, 0L, BLOCK_LENGTH);
    }
    omMetadataManager.getVersionedKeyTable()
        .put(versionKey(versionId), version);
  }

  private String versionKey(long versionId) {
    return omMetadataManager.getVersionedOzoneKey(volumeName, bucketName,
        keyName, versionId);
  }

  private List<String> versionKeys(long... versionIds) {
    return Arrays.stream(versionIds).mapToObj(this::versionKey)
        .collect(Collectors.toList());
  }

  private void assertReclaimedFromTable(long versionId) {
    CacheValue<OmKeyInfo> cached = omMetadataManager.getVersionedKeyTable()
        .getCacheValue(new CacheKey<>(versionKey(versionId)));
    assertNotNull(cached, "version " + versionId + " was not reclaimed");
    assertNull(cached.getCacheValue(),
        "version " + versionId + " was not tombstoned");
  }

  private OMObjectVersionsReclaimResponse reclaim(long trxnLogIndex,
      List<String> keys) throws Exception {
    OMRequest request = reclaimRequest(ObjectVersionsBucket.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .addAllVersionKeys(keys)
        .build());
    return (OMObjectVersionsReclaimResponse)
        new OMObjectVersionsReclaimRequest(request)
            .validateAndUpdateCache(ozoneManager, trxnLogIndex);
  }

  private OMRequest reclaimRequest(ObjectVersionsBucket versionsBucket) {
    return OMRequest.newBuilder()
        .setReclaimObjectVersionsRequest(
            ReclaimObjectVersionsRequest.newBuilder()
                .addVersionsPerBucket(versionsBucket))
        .setCmdType(OzoneManagerProtocolProtos.Type.ReclaimObjectVersions)
        .setClientId(UUID.randomUUID().toString()).build();
  }
}
