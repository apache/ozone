/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
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

/**
 * Tests OMSnapshotCreateRequest class, which handles CreateSnapshot request.
 */
public class TestOMSnapshotCreateRequest extends TestSnapshotRequestAndResponse {
  private String snapshotName1;
  private String snapshotName2;

  @BeforeEach
  public void setup() throws Exception {
    snapshotName1 = UUID.randomUUID().toString();
    snapshotName2 = UUID.randomUUID().toString();
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
    OmKeyInfo key1 = addKey("key-testValidateAndUpdateCache", 12345L);
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
    Table<String, String> snapshotRenamedTable =
        getOmMetadataManager().getSnapshotRenamedTable();

    renameKey("key1", "key2", 0);
    renameDir("dir1", "dir2", 5);
    // Rename table should be empty as there is no rename happening in
    // the snapshot scope.
    assertTrue(snapshotRenamedTable.isEmpty());

    // Create snapshot
    createSnapshot(snapshotName1);
    String snapKey = getTableKey(getVolumeName(),
        getBucketName(), snapshotName1);
    SnapshotInfo snapshotInfo =
        getOmMetadataManager().getSnapshotInfoTable().get(snapKey);
    assertNotNull(snapshotInfo);

    renameKey("key3", "key4", 10);
    renameDir("dir3", "dir4", 15);

    // Rename table should have two entries as rename is within snapshot scope.
    assertEquals(2, getOmMetadataManager()
        .countRowsInTable(snapshotRenamedTable));

    // Create snapshot to clear snapshotRenamedTable
    createSnapshot(snapshotName2);
    assertTrue(snapshotRenamedTable.isEmpty());
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

  private void renameKey(String fromKey, String toKey, long offset)
      throws IOException {
    OmKeyInfo toKeyInfo = addKey(toKey, offset + 1L);
    OmKeyInfo fromKeyInfo = addKey(fromKey, offset + 2L);

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

  private void renameDir(String fromKey, String toKey, long offset)
      throws Exception {
    String fromKeyParentName = UUID.randomUUID().toString();
    OmKeyInfo fromKeyParent = OMRequestTestUtils.createOmKeyInfo(getVolumeName(),
            getBucketName(), fromKeyParentName, RatisReplicationConfig.getInstance(THREE))
        .setObjectID(100L)
        .build();

    OmKeyInfo toKeyInfo = addKey(toKey, offset + 4L);
    OmKeyInfo fromKeyInfo = addKey(fromKey, offset + 5L);
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
    OMRequest omRequest =
        createSnapshotRequest(
            getVolumeName(), getBucketName(), snapName);
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

  private OmKeyInfo addKey(String keyName, long objectId) {
    return OMRequestTestUtils.createOmKeyInfo(getVolumeName(), getBucketName(), keyName,
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
