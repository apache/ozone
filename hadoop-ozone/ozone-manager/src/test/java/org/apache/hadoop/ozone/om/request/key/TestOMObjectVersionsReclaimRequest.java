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

import java.util.ArrayList;
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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ObjectVersionRecord;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ObjectVersionsBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReclaimObjectVersionsRequest;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Tests the reclamation the lifecycle scan submits: noncurrent versions leave
 * the versionedKeyTable with their blocks queued in the deletedTable, expired
 * delete markers leave the keyTable, and the bucket quota is released for
 * both.
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
        .addVersions(versionRecord(versionKey(100L)))
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

  /**
   * A marker with nothing under it hides nothing and cannot be addressed by
   * versionId, so nothing else would ever remove it. It leaves the keyTable
   * and the key disappears with it, releasing the namespace slot it held.
   */
  @Test
  public void testReclaimsAMarkerWithNothingUnderIt() throws Exception {
    setupVersionedBucket(0L, 1L);
    seedCurrentMarker(300L);

    OMObjectVersionsReclaimResponse response = reclaimMarkers(500L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    CacheValue<OmKeyInfo> cached = omMetadataManager
        .getKeyTable(getBucketLayout()).getCacheValue(new CacheKey<>(ozoneKey()));
    assertNotNull(cached, "the marker was not reclaimed");
    assertNull(cached.getCacheValue(), "the marker was not tombstoned");

    // it holds no blocks, so nothing is queued and only namespace is released
    assertTrue(response.getKeysToDelete().isEmpty());
    OmBucketInfo bucketInfo = OMKeyRequest.getBucketInfo(omMetadataManager,
        volumeName, bucketName);
    assertEquals(0L, bucketInfo.getUsedNamespace());
  }

  /**
   * While a noncurrent version survives, removing the marker would promote it
   * and bring back an object the user deleted. The scan checked this too, but
   * a version can be written between the scan and this request, so the check
   * is what happens under the bucket lock that decides.
   */
  @Test
  public void testKeepsAMarkerThatStillHidesAVersion() throws Exception {
    setupVersionedBucket(BLOCK_LENGTH, 2L);
    seedCurrentMarker(300L);
    seedNoncurrentVersion(100L);

    OMObjectVersionsReclaimResponse response = reclaimMarkers(500L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    assertNull(omMetadataManager.getKeyTable(getBucketLayout())
        .getCacheValue(new CacheKey<>(ozoneKey())));
    OmBucketInfo bucketInfo = OMKeyRequest.getBucketInfo(omMetadataManager,
        volumeName, bucketName);
    assertEquals(2L, bucketInfo.getUsedNamespace());
  }

  /**
   * A write since the scan supersedes the marker: the key's current version is
   * a real object again, and there is nothing expired to remove.
   */
  @Test
  public void testSkipsAMarkerSupersededByAWrite() throws Exception {
    setupVersionedBucket(BLOCK_LENGTH, 1L);
    OmKeyInfo rewritten = OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, keyName, replicationConfig)
        .setVersionId(400L)
        .build();
    OMRequestTestUtils.addKeyLocationInfo(rewritten, 0L, BLOCK_LENGTH);
    omMetadataManager.getKeyTable(getBucketLayout())
        .put(ozoneKey(), rewritten);

    OMObjectVersionsReclaimResponse response = reclaimMarkers(500L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    assertNull(omMetadataManager.getKeyTable(getBucketLayout())
        .getCacheValue(new CacheKey<>(ozoneKey())));
  }

  private String ozoneKey() {
    return omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
  }

  /** A key whose current version is a delete marker. */
  private void seedCurrentMarker(long versionId) throws Exception {
    OmKeyInfo marker = OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, keyName, replicationConfig)
        .setVersionId(versionId)
        .setDeleteMarker(true)
        .build();
    omMetadataManager.getKeyTable(getBucketLayout()).put(ozoneKey(), marker);
  }

  private OMObjectVersionsReclaimResponse reclaimMarkers(long trxnLogIndex)
      throws Exception {
    return reclaimMarkers(trxnLogIndex, markerRecord(ozoneKey()));
  }

  private OMObjectVersionsReclaimResponse reclaimMarkers(long trxnLogIndex,
      ObjectVersionRecord marker) throws Exception {
    OMRequest request = reclaimRequest(ObjectVersionsBucket.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .addMarkers(marker)
        .build());
    return (OMObjectVersionsReclaimResponse)
        new OMObjectVersionsReclaimRequest(request)
            .validateAndUpdateCache(ozoneManager, trxnLogIndex);
  }

  /** A record naming what sits at the dbKey right now. */
  private ObjectVersionRecord versionRecord(String dbKey) throws Exception {
    OmKeyInfo version = omMetadataManager.getVersionedKeyTable().get(dbKey);
    return record(dbKey, version == null ? 0L : version.getUpdateID());
  }

  private ObjectVersionRecord markerRecord(String dbKey) throws Exception {
    OmKeyInfo marker =
        omMetadataManager.getKeyTable(getBucketLayout()).get(dbKey);
    return record(dbKey, marker == null ? 0L : marker.getUpdateID());
  }

  private static ObjectVersionRecord record(String dbKey, long updateId) {
    return ObjectVersionRecord.newBuilder()
        .setDbKey(dbKey)
        .setUpdateId(updateId)
        .build();
  }

  /**
   * A marker's dbKey is the plain key name, so it names whichever version of
   * the key is current. Deleting the key, writing it and deleting it again
   * leaves another marker there, which every other check here accepts: it is
   * a marker, and nothing survives under it. Only the updateID tells it from
   * the one the scan selected.
   */
  @Test
  public void testAMarkerReplacedSinceTheScanIsNotReclaimed()
      throws Exception {
    setupVersionedBucket(BLOCK_LENGTH, 1L);
    seedCurrentMarker(300L);

    // the scan read a marker that has since been replaced
    OMObjectVersionsReclaimResponse response =
        reclaimMarkers(500L, record(ozoneKey(), 4711L));

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    assertNotNull(
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey()),
        "a marker the scan never selected was reclaimed");
  }

  /**
   * A versionId is allocated against the key's current version alone, so one
   * that was permanently deleted can be handed out again, putting a different
   * version at the dbKey the scan selected.
   */
  @Test
  public void testAVersionReplacedSinceTheScanIsNotReclaimed()
      throws Exception {
    setupVersionedBucket(BLOCK_LENGTH, 1L);
    seedNoncurrentVersion(100L);

    OMRequest request = reclaimRequest(ObjectVersionsBucket.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .addVersions(record(versionKey(100L), 4711L))
        .build());
    OMObjectVersionsReclaimResponse response =
        (OMObjectVersionsReclaimResponse) new OMObjectVersionsReclaimRequest(
            request).validateAndUpdateCache(ozoneManager, 500L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    assertNotNull(
        omMetadataManager.getVersionedKeyTable().get(versionKey(100L)),
        "a version the scan never selected was reclaimed");
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

  /**
   * A version demoted by a transaction the double buffer has not flushed yet
   * lives in the table cache alone, and a prefix iteration over the DB reads
   * straight past it. Removing the marker then promotes that version back to
   * current, resurrecting an object the user deleted.
   */
  @Test
  public void testAMarkerIsKeptForAVersionOnlyInTheCache() throws Exception {
    setupVersionedBucket(BLOCK_LENGTH, 1L);
    seedCurrentMarker(300L);
    cacheOnlyNoncurrentVersion(100L);

    OMObjectVersionsReclaimResponse response = reclaimMarkers(500L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());
    assertNotNull(
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey()),
        "the marker was reclaimed while a version survived in the cache");
  }

  /** A noncurrent version that only exists in the table cache, not the DB. */
  private void cacheOnlyNoncurrentVersion(long versionId) throws Exception {
    OmKeyInfo version = OMRequestTestUtils.createOmKeyInfo(
            volumeName, bucketName, keyName, replicationConfig)
        .setVersionId(versionId)
        .build();
    OMRequestTestUtils.addKeyLocationInfo(version, 0L, BLOCK_LENGTH);
    omMetadataManager.getVersionedKeyTable()
        .addCacheEntry(versionKey(versionId), version, 350L);
  }

  private void seedNoncurrentVersion(long versionId) throws Exception {
    seedNoncurrentVersion(versionId, false);
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
    List<ObjectVersionRecord> records = new ArrayList<>();
    for (String key : keys) {
      records.add(versionRecord(key));
    }
    OMRequest request = reclaimRequest(ObjectVersionsBucket.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .addAllVersions(records)
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
