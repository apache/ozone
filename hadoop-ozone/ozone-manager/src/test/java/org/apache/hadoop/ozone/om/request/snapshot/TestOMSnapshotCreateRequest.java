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

package org.apache.hadoop.ozone.om.request.snapshot;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_FS_SNAPSHOT_MAX_LIMIT;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.getFromProtobuf;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.getTableKey;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.createSnapshotRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateSnapshot;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyRenameResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyRenameResponseWithFSO;
import org.apache.hadoop.ozone.om.snapshot.TestSnapshotRequestAndResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests OMSnapshotCreateRequest class, which handles CreateSnapshot request.
 */
public class TestOMSnapshotCreateRequest extends TestSnapshotRequestAndResponse {
  private String snapshotName1;
  private String snapshotName2;
  private String snapshotName3;
  private String snapshotName4;
  private String snapshotName5;

  @BeforeEach
  public void setup() throws Exception {
    snapshotName1 = UUID.randomUUID().toString();
    snapshotName2 = UUID.randomUUID().toString();
    snapshotName3 = UUID.randomUUID().toString();
    snapshotName4 = UUID.randomUUID().toString();
    snapshotName5 = UUID.randomUUID().toString();
  }

  @ValueSource(strings = {
      // '-' is allowed.
      "9cdf0e8a-6946-41ad-a2d1-9eb724fab126",
      // 3 chars name is allowed.
      "sn1",
      // less than or equal to 63 chars are allowed.
      "snap75795657617173401188448010125899089001363595171500499231286"
  })
  @ParameterizedTest
  public void testPreExecute(String snapshotName) throws Exception {
    when(getOzoneManager().isOwner(any(), any())).thenReturn(true);
    OMRequest omRequest = createSnapshotRequest(getVolumeName(),
        getBucketName(), snapshotName);
    doPreExecute(omRequest);
  }

  @ValueSource(strings = {
      // '-' is allowed.
      "9cdf0e8a-6946-41ad-a2d1-9eb724fab126",
      // 3 chars name is allowed.
      "sn1",
      // less than or equal to 63 chars are allowed.
      "snap75795657617173401188448010125899089001363595171500499231286"
  })
  @ParameterizedTest
  public void testPreExecuteWithLinkedBucket(String snapshotName) throws Exception {
    when(getOzoneManager().isOwner(any(), any())).thenReturn(true);
    String resolvedBucketName = getBucketName() + "1";
    String resolvedVolumeName = getVolumeName() + "1";
    when(getOzoneManager().resolveBucketLink(any(Pair.class), any(OMClientRequest.class)))
        .thenAnswer(i -> new ResolvedBucket(i.getArgument(0), Pair.of(resolvedVolumeName, resolvedBucketName),
            "owner", BucketLayout.FILE_SYSTEM_OPTIMIZED));
    OMRequest omRequest = createSnapshotRequest(getVolumeName(),
        getBucketName(), snapshotName);
    OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(omRequest);
    assertEquals(resolvedVolumeName, omSnapshotCreateRequest.getOmRequest().getCreateSnapshotRequest().getVolumeName());
    assertEquals(resolvedBucketName, omSnapshotCreateRequest.getOmRequest().getCreateSnapshotRequest().getBucketName());
  }

  @ValueSource(strings = {
      // ? is not allowed in snapshot name.
      "a?b",
      // only numeric name not allowed.
      "1234",
      // less than 3 chars are not allowed.
      "s1",
      // more than or equal to 64 chars are not allowed.
      "snap156808943643007724443266605711479126926050896107709081166294"
  })
  @ParameterizedTest
  public void testPreExecuteFailure(String snapshotName) {
    when(getOzoneManager().isOwner(any(), any())).thenReturn(true);
    OMRequest omRequest = createSnapshotRequest(getVolumeName(),
        getBucketName(), snapshotName);
    OMException omException =
        assertThrows(OMException.class, () -> doPreExecute(omRequest));
    assertTrue(omException.getMessage()
        .contains("Invalid snapshot name: " + snapshotName));
  }

  @Test
  public void testPreExecuteBadOwner() {
    // Owner is not set for the request.
    OMRequest omRequest = createSnapshotRequest(getVolumeName(),
        getBucketName(), snapshotName1);

    OMException omException = assertThrows(OMException.class,
        () -> doPreExecute(omRequest));
    assertEquals("Only bucket owners and Ozone admins can create snapshots",
        omException.getMessage());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    when(getOzoneManager().isAdmin(any())).thenReturn(true);
    OMRequest omRequest = createSnapshotRequest(getVolumeName(),
        getBucketName(), snapshotName1);
    OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(omRequest);
    String key = getTableKey(getVolumeName(), getBucketName(), snapshotName1);
    String bucketKey = getOmMetadataManager().getBucketKey(getVolumeName(), getBucketName());

    // Add a 1000-byte key to the bucket
    OmKeyInfo key1 = addKeyInBucket(getVolumeName(), getBucketName(), "key-testValidateAndUpdateCache", 12345L);
    addKeyToTable(key1);

    OmBucketInfo omBucketInfo = getOmMetadataManager().getBucketTable().get(
        bucketKey);
    long bucketDataSize = key1.getDataSize();
    long bucketUsedBytes = omBucketInfo.getUsedBytes();
    assertEquals(key1.getReplicatedSize(), bucketUsedBytes);

    // Value in cache should be null as of now.
    assertNull(getOmMetadataManager().getSnapshotInfoTable().get(key));

    // Run validateAndUpdateCache.
    OMClientResponse omClientResponse =
        omSnapshotCreateRequest.validateAndUpdateCache(getOzoneManager(), 1);

    assertNotNull(omClientResponse.getOMResponse());

    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getCreateSnapshotResponse());
    assertEquals(CreateSnapshot, omResponse.getCmdType());
    assertEquals(OK, omResponse.getStatus());

    // verify table data with response data.
    OzoneManagerProtocolProtos.SnapshotInfo snapshotInfoProto =
        omClientResponse
            .getOMResponse()
            .getCreateSnapshotResponse()
            .getSnapshotInfo();

    assertEquals(bucketDataSize, snapshotInfoProto.getReferencedSize());
    assertEquals(bucketUsedBytes,
        snapshotInfoProto.getReferencedReplicatedSize());

    SnapshotInfo snapshotInfoFromProto = getFromProtobuf(snapshotInfoProto);

    // Get value from cache
    SnapshotInfo snapshotInfoInCache =
        getOmMetadataManager().getSnapshotInfoTable().get(key);
    assertNotNull(snapshotInfoInCache);
    assertEquals(snapshotInfoFromProto, snapshotInfoInCache);
    assertEquals(snapshotInfoInCache.getLastTransactionInfo(),
        TransactionInfo.valueOf(TransactionInfo.getTermIndex(1L)).toByteString());
    assertEquals(0, getOmMetrics().getNumSnapshotCreateFails());
    assertEquals(1, getOmMetrics().getNumSnapshotActive());
    assertEquals(1, getOmMetrics().getNumSnapshotCreates());
  }

  @Test
  public void testEntryRenamedKeyTable() throws Exception {
    when(getOzoneManager().isAdmin(any())).thenReturn(true);
    Table<String, String> snapshotRenamedTable = getOmMetadataManager().getSnapshotRenamedTable();

    String bucket1Name = getBucketName();
    String bucket2Name = getBucketName() + "0";
    String volumeName = getVolumeName();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucket2Name, getOmMetadataManager());

    renameKeyInBucket(volumeName, bucket1Name, "key1", "key2", 0);
    renameDirInBucket(volumeName, bucket1Name, "dir1", "dir2", 5);
    renameKeyInBucket(volumeName, bucket2Name, "key10", "key20", 0);
    renameDirInBucket(volumeName, bucket2Name, "dir10", "dir20", 5);
    // Rename table should be empty as there is no rename happening in the snapshot scope.
    assertTrue(snapshotRenamedTable.isEmpty());

    // Create snapshot
    createSnapshotForBucket(volumeName, bucket1Name, snapshotName1);
    createSnapshotForBucket(volumeName, bucket2Name, snapshotName1 + "0");
    String bucket1SnapKey = getTableKey(volumeName, bucket1Name, snapshotName1);
    String bucket2SnapKey = getTableKey(volumeName, bucket2Name, snapshotName1 + "0");
    SnapshotInfo bucket1SnapshotInfo = getOmMetadataManager().getSnapshotInfoTable().get(bucket1SnapKey);
    SnapshotInfo bucket2SnapshotInfo = getOmMetadataManager().getSnapshotInfoTable().get(bucket2SnapKey);
    assertNotNull(bucket1SnapshotInfo);
    assertNotNull(bucket2SnapshotInfo);

    renameKeyInBucket(volumeName, bucket1Name, "key3", "key4", 10);
    renameDirInBucket(volumeName, bucket1Name, "dir3", "dir4", 15);
    renameKeyInBucket(volumeName, bucket2Name, "key30", "key40", 10);
    renameDirInBucket(volumeName, bucket2Name, "dir30", "dir40", 15);

    // Rename table should have four entries as rename is within snapshot scope.
    assertEquals(4, getOmMetadataManager().countRowsInTable(snapshotRenamedTable));

    // Create snapshot to clear snapshotRenamedTable of bucket1 entries.
    createSnapshotForBucket(volumeName, bucket1Name, snapshotName2);
    assertEquals(2, getOmMetadataManager().countRowsInTable(snapshotRenamedTable));
    // Verify the remaining entries are from bucket2
    try (TableIterator<String, ? extends Table.KeyValue<String, String>> iter =
             snapshotRenamedTable.iterator()) {
      iter.seekToFirst();
      while (iter.hasNext()) {
        String key = iter.next().getKey();
        assertTrue(key.startsWith(getOmMetadataManager().getBucketKey(volumeName, bucket2Name)),
            "Key should be from bucket2: " + key);
      }
    }
  }

  @Test
  public void testEntryExists() throws Exception {
    when(getOzoneManager().isAdmin(any())).thenReturn(true);

    String key = getTableKey(getVolumeName(), getBucketName(), snapshotName1);

    OMRequest omRequest =
        createSnapshotRequest(getVolumeName(), getBucketName(), snapshotName1);
    OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(omRequest);

    assertNull(getOmMetadataManager().getSnapshotInfoTable().get(key));
    omSnapshotCreateRequest.validateAndUpdateCache(getOzoneManager(), 1);

    assertNotNull(getOmMetadataManager().getSnapshotInfoTable().get(key));

    // Now try to create again to verify error
    omRequest = createSnapshotRequest(getVolumeName(), getBucketName(), snapshotName1);
    omSnapshotCreateRequest = doPreExecute(omRequest);
    OMClientResponse omClientResponse =
        omSnapshotCreateRequest.validateAndUpdateCache(getOzoneManager(), 2);

    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getCreateSnapshotResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS,
        omResponse.getStatus());

    assertEquals(1, getOmMetrics().getNumSnapshotCreateFails());
    assertEquals(1, getOmMetrics().getNumSnapshotActive());
    assertEquals(2, getOmMetrics().getNumSnapshotCreates());
  }

  @Test
  public void testSnapshotLimit() throws Exception {
    when(getOzoneManager().isAdmin(any())).thenReturn(true);
    getOzoneManager().getOmSnapshotManager().close();
    getOzoneManager().getConfiguration().setInt(OZONE_OM_FS_SNAPSHOT_MAX_LIMIT, 3);
    OmSnapshotManager omSnapshotManager = new OmSnapshotManager(getOzoneManager());
    when(getOzoneManager().getOmSnapshotManager()).thenReturn(omSnapshotManager);

    // Test Case 1: No snapshots in chain, no in-flight
    String key1 = getTableKey(getVolumeName(), getBucketName(), snapshotName1);
    OMRequest omRequest = createSnapshotRequest(getVolumeName(), getBucketName(), snapshotName1);
    OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(omRequest);
    assertNull(getOmMetadataManager().getSnapshotInfoTable().get(key1));
    omSnapshotCreateRequest.validateAndUpdateCache(getOzoneManager(), 1);
    assertNotNull(getOmMetadataManager().getSnapshotInfoTable().get(key1));

    // Test Case 2: One snapshot in chain, no in-flight
    String key2 = getTableKey(getVolumeName(), getBucketName(), snapshotName2);
    OMRequest snapshotRequest2 = createSnapshotRequest(getVolumeName(), getBucketName(), snapshotName2);
    OMSnapshotCreateRequest omSnapshotCreateRequest2 = doPreExecute(snapshotRequest2);
    omSnapshotCreateRequest2.validateAndUpdateCache(getOzoneManager(), 2);
    assertNotNull(getOmMetadataManager().getSnapshotInfoTable().get(key2));

    // Test Case 3: Two snapshots in chain, one in-flight
    // First create an in-flight snapshot
    String key3 = getTableKey(getVolumeName(), getBucketName(), snapshotName3);
    OMRequest snapshotRequest3 = createSnapshotRequest(getVolumeName(), getBucketName(), snapshotName3);
    OMSnapshotCreateRequest omSnapshotCreateRequest3 = doPreExecute(snapshotRequest3);
    // Don't call validateAndUpdateCache to keep it in-flight

    // Try to create another snapshot - should fail as total would be 4 (2 in chain + 1 in-flight + 1 new)
    OMRequest snapshotRequest4 = createSnapshotRequest(getVolumeName(), getBucketName(), snapshotName4);
    OMException omException = assertThrows(OMException.class, () -> doPreExecute(snapshotRequest4));
    assertEquals(OMException.ResultCodes.TOO_MANY_SNAPSHOTS, omException.getResult());

    // Complete the in-flight snapshot
    omSnapshotCreateRequest3.validateAndUpdateCache(getOzoneManager(), 3);
    assertNotNull(getOmMetadataManager().getSnapshotInfoTable().get(key3));

    // Test Case 4: Three snapshots in chain, no in-flight
    // Try to create another snapshot - should fail as we've reached the limit
    OMRequest snapshotRequest5 = createSnapshotRequest(getVolumeName(), getBucketName(), snapshotName5);
    omException = assertThrows(OMException.class, () -> doPreExecute(snapshotRequest5));
    assertEquals(OMException.ResultCodes.TOO_MANY_SNAPSHOTS, omException.getResult());
  }

  @DisplayName("Snapshot limit is enforced even after failed creation attempts")
  @Test
  public void testSnapshotLimitWithFailures() throws Exception {
    when(getOzoneManager().isAdmin(any())).thenReturn(true);
    getOzoneManager().getOmSnapshotManager().close();
    getOzoneManager().getConfiguration().setInt(OZONE_OM_FS_SNAPSHOT_MAX_LIMIT, 2);
    OmSnapshotManager omSnapshotManager = new OmSnapshotManager(getOzoneManager());
    when(getOzoneManager().getOmSnapshotManager()).thenReturn(omSnapshotManager);

    // Create first snapshot successfully
    String key1 = getTableKey(getVolumeName(), getBucketName(), snapshotName1);
    OMRequest omRequest = createSnapshotRequest(getVolumeName(), getBucketName(), snapshotName1);
    OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(omRequest);
    omSnapshotCreateRequest.validateAndUpdateCache(getOzoneManager(), 1);
    assertNotNull(getOmMetadataManager().getSnapshotInfoTable().get(key1));

    // Snapshot creation failure
    OMRequest snapshotRequestFail = createSnapshotRequest(getVolumeName(), getBucketName(), snapshotName1);
    OMSnapshotCreateRequest omSnapshotCreateRequestFail = doPreExecute(snapshotRequestFail);
    OMClientResponse omClientResponse =
        omSnapshotCreateRequestFail.validateAndUpdateCache(getOzoneManager(), 2);
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getCreateSnapshotResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS, omResponse.getStatus());

    // Second snapshot in-flight
    String key2 = getTableKey(getVolumeName(), getBucketName(), snapshotName2);
    OMRequest snapshotRequest2 = createSnapshotRequest(getVolumeName(), getBucketName(), snapshotName2);
    OMSnapshotCreateRequest omSnapshotCreateRequest2 = doPreExecute(snapshotRequest2);

    // Third snapshot should fail as total 3 > limit 2 (1 in chain + 1 in-flight + 1 new)
    OMRequest snapshotRequest3 = createSnapshotRequest(getVolumeName(), getBucketName(), snapshotName3);
    OMException omException = assertThrows(OMException.class, () -> doPreExecute(snapshotRequest3));
    assertEquals(OMException.ResultCodes.TOO_MANY_SNAPSHOTS, omException.getResult());

    // Complete the in-flight snapshot successfully
    omSnapshotCreateRequest2.validateAndUpdateCache(getOzoneManager(), 3);
    assertNotNull(getOmMetadataManager().getSnapshotInfoTable().get(key2));

    // Another snapshot should fail as total 3 > limit 2 (2 in chain + 1 new)
    OMRequest snapshotRequest4 = createSnapshotRequest(getVolumeName(), getBucketName(), snapshotName4);
    omException = assertThrows(OMException.class, () -> doPreExecute(snapshotRequest4));
    assertEquals(OMException.ResultCodes.TOO_MANY_SNAPSHOTS, omException.getResult());
  }

  @Test
  public void testEntryDeletedTable() throws Exception {
    when(getOzoneManager().isAdmin(any())).thenReturn(true);
    Table<String, RepeatedOmKeyInfo> deletedTable = getOmMetadataManager().getDeletedTable();

    // 1. Create a second bucket with lexicographically higher name
    String bucket1Name = getBucketName();
    String bucket2Name = getBucketName() + "0";
    String volumeName = getVolumeName();
    OmBucketInfo bucketInfo = OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucket2Name,
        getOmMetadataManager());

    // 2. Add and delete keys from both buckets
    OmKeyInfo key1 = addKeyInBucket(volumeName, bucket1Name, "key1", 100L);
    OmKeyInfo key2 = addKeyInBucket(volumeName, bucket2Name, "key2", 200L);
    deleteKey(key1, bucketInfo.getObjectID());
    deleteKey(key2, bucketInfo.getObjectID());

    // 3. Verify deletedTable contains both deleted keys (2 rows)
    assertEquals(2, getOmMetadataManager().countRowsInTable(deletedTable));

    // 4. Create a snapshot on bucket1
    createSnapshot(snapshotName1);

    // 5. Verify deletedTable now only contains the key from bucket2 (1 row)
    assertEquals(1, getOmMetadataManager().countRowsInTable(deletedTable));
    // Verify the remaining entry is from bucket2
    try (TableIterator<String, ? extends Table.KeyValue<String, RepeatedOmKeyInfo>> iter = deletedTable.iterator()) {
      iter.seekToFirst();
      while (iter.hasNext()) {
        String key = iter.next().getKey();
        assertTrue(key.startsWith(getOmMetadataManager().getBucketKeyPrefix(volumeName, bucket2Name)),
            "Key should be from bucket2: " + key);
      }
    }


  }

  @Test
  public void testEntryDeletedDirTable() throws Exception {
    when(getOzoneManager().isAdmin(any())).thenReturn(true);
    Table<String, OmKeyInfo> deletedDirTable = getOmMetadataManager().getDeletedDirTable();

    // 1. Create a second bucket with lexicographically higher name
    String bucket1Name = getBucketName();
    String bucket2Name = getBucketName() + "0";
    String volumeName = getVolumeName();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucket2Name, getOmMetadataManager());

    // 2. Add and delete keys from both buckets
    OmKeyInfo key1 = addKeyInBucket(volumeName, bucket1Name, "dir2", 100L);
    OmKeyInfo key2 = addKeyInBucket(volumeName, bucket2Name, "dir20", 200L);
    deleteDirectory(key1);
    deleteDirectory(key2);

    // 3. Verify deletedDirTable contains both deleted keys (2 rows)
    assertEquals(2, getOmMetadataManager().countRowsInTable(deletedDirTable));

    // 4. Create a snapshot on bucket1
    createSnapshotForBucket(volumeName, bucket1Name, snapshotName1);

    // 5. Verify deletedTable now only contains the key from bucket2 (1 row)
    assertEquals(1, getOmMetadataManager().countRowsInTable(deletedDirTable));
    // Verify the remaining entry is from bucket2
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> iter = deletedDirTable.iterator()) {
      while (iter.hasNext()) {
        String key = iter.next().getKey();
        assertTrue(key.startsWith(getOmMetadataManager().getBucketKeyPrefixFSO(volumeName, bucket2Name)),
            "Key should be from bucket2: " + key);
      }
    }
  }

  private void deleteDirectory(OmKeyInfo dirInfo) throws IOException {
    String dirKey = getOmMetadataManager().getOzonePathKey(
        getOmMetadataManager().getVolumeId(dirInfo.getVolumeName()),
        getOmMetadataManager().getBucketId(dirInfo.getVolumeName(), dirInfo.getBucketName()),
        dirInfo.getParentObjectID(), dirInfo.getKeyName());
    getOmMetadataManager().getDeletedDirTable().putWithBatch(getBatchOperation(),
        dirKey, dirInfo);
    getOmMetadataManager().getStore().commitBatchOperation(getBatchOperation());
  }

  private void deleteKey(OmKeyInfo keyInfo, long bucketId) throws IOException {
    String ozoneKey = getOmMetadataManager().getOzoneKey(keyInfo.getVolumeName(),
        keyInfo.getBucketName(), keyInfo.getKeyName());
    RepeatedOmKeyInfo repeatedOmKeyInfo = new RepeatedOmKeyInfo(keyInfo, bucketId);
    getOmMetadataManager().getDeletedTable().putWithBatch(getBatchOperation(),
        ozoneKey, repeatedOmKeyInfo);
    getOmMetadataManager().getStore().commitBatchOperation(getBatchOperation());
  }

  private void renameKeyInBucket(String volumeName, String bucketName, String fromKey, String toKey, long offset)
      throws IOException {
    OmKeyInfo toKeyInfo = addKeyInBucket(volumeName, bucketName, toKey, offset + 1L);
    OmKeyInfo fromKeyInfo = addKeyInBucket(volumeName, bucketName, fromKey, offset + 2L);

    OMResponse omResponse = OMResponse
        .newBuilder()
        .setRenameKeyResponse(
            OzoneManagerProtocolProtos.RenameKeyResponse.getDefaultInstance())
        .setStatus(OK)
        .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey)
        .build();
    OMKeyRenameResponse omKeyRenameResponse =
        new OMKeyRenameResponse(omResponse, fromKeyInfo.getKeyName(),
            toKeyInfo.getKeyName(), toKeyInfo);

    omKeyRenameResponse.addToDBBatch(getOmMetadataManager(), getBatchOperation());
    getOmMetadataManager().getStore().commitBatchOperation(getBatchOperation());
  }

  private void renameDirInBucket(String volumeName, String bucketName, String fromKey, String toKey, long offset)
      throws IOException {
    String fromKeyParentName = UUID.randomUUID().toString();
    OmKeyInfo fromKeyParent = OMRequestTestUtils.createOmKeyInfo(volumeName,
            bucketName, fromKeyParentName, RatisReplicationConfig.getInstance(THREE))
        .setObjectID(100L)
        .build();

    OmKeyInfo toKeyInfo = addKeyInBucket(volumeName, bucketName, toKey, offset + 4L);
    OmKeyInfo fromKeyInfo = addKeyInBucket(volumeName, bucketName, fromKey, offset + 5L);
    OMResponse omResponse = OMResponse
        .newBuilder()
        .setRenameKeyResponse(
            OzoneManagerProtocolProtos.RenameKeyResponse.getDefaultInstance())
        .setStatus(OK)
        .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey)
        .build();

    OMKeyRenameResponseWithFSO omKeyRenameResponse =
        new OMKeyRenameResponseWithFSO(omResponse, getDBKeyName(fromKeyInfo),
            getDBKeyName(toKeyInfo), fromKeyParent, null, toKeyInfo,
            null, true, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    omKeyRenameResponse.addToDBBatch(getOmMetadataManager(), getBatchOperation());
    getOmMetadataManager().getStore().commitBatchOperation(getBatchOperation());
  }

  protected String getDBKeyName(OmKeyInfo keyInfo) throws IOException {
    return getOmMetadataManager().getOzonePathKey(
        getOmMetadataManager().getVolumeId(getVolumeName()),
        getOmMetadataManager().getBucketId(getVolumeName(), getBucketName()),
        keyInfo.getParentObjectID(), keyInfo.getKeyName());
  }

  private void createSnapshot(String snapName) throws Exception {
    createSnapshotForBucket(getVolumeName(), getBucketName(), snapName);
  }

  private void createSnapshotForBucket(String volumeName, String bucketName, String snapName) throws Exception {
    OMRequest omRequest =
        createSnapshotRequest(
            volumeName, bucketName, snapName);
    OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(omRequest);
    //create entry
    OMClientResponse omClientResponse =
        omSnapshotCreateRequest.validateAndUpdateCache(getOzoneManager(), 1);
    omClientResponse.checkAndUpdateDB(getOmMetadataManager(), getBatchOperation());
    getOmMetadataManager().getStore().commitBatchOperation(getBatchOperation());
  }

  private OMSnapshotCreateRequest doPreExecute(
      OMRequest originalRequest) throws Exception {
    return doPreExecute(originalRequest, getOzoneManager());
  }

  /**
   * Static helper method so this could be used in TestOMSnapshotDeleteRequest.
   */
  public static OMSnapshotCreateRequest doPreExecute(
      OMRequest originalRequest, OzoneManager ozoneManager) throws Exception {
    OMSnapshotCreateRequest omSnapshotCreateRequest =
        new OMSnapshotCreateRequest(originalRequest);

    OMRequest modifiedRequest =
        omSnapshotCreateRequest.preExecute(ozoneManager);
    return new OMSnapshotCreateRequest(modifiedRequest);
  }

  private OmKeyInfo addKeyInBucket(String volumeName, String bucketName, String keyName, long objectId) {
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
            RatisReplicationConfig.getInstance(THREE)).setObjectID(objectId)
        .build();
  }

  protected String addKeyToTable(OmKeyInfo keyInfo) throws Exception {
    OMRequestTestUtils.addKeyToTable(false, true, keyInfo, 0, 0L,
        getOmMetadataManager());
    return getOmMetadataManager().getOzoneKey(keyInfo.getVolumeName(),
        keyInfo.getBucketName(), keyInfo.getKeyName());
  }
}
