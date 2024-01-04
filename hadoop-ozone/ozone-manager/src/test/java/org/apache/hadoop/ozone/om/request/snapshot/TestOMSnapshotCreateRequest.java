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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyRenameResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyRenameResponseWithFSO;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests OMSnapshotCreateRequest class, which handles CreateSnapshot request.
 */
public class TestOMSnapshotCreateRequest {
  @TempDir
  private File anotherTempDir;

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OmMetadataManagerImpl omMetadataManager;
  private BatchOperation batchOperation;

  private String volumeName;
  private String bucketName;
  private String snapshotName1;
  private String snapshotName2;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        anotherTempDir.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.isRatisEnabled()).thenReturn(true);
    when(ozoneManager.isFilesystemSnapshotEnabled()).thenReturn(true);
    when(ozoneManager.isAdmin(any())).thenReturn(false);
    when(ozoneManager.isOwner(any(), any())).thenReturn(false);
    when(ozoneManager.getBucketOwner(any(), any(),
        any(), any())).thenReturn("dummyBucketOwner");
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.isAllowed(anyString())).thenReturn(true);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    AuditLogger auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    batchOperation = omMetadataManager.getStore().initBatchOperation();

    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    snapshotName1 = UUID.randomUUID().toString();
    snapshotName2 = UUID.randomUUID().toString();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
  }

  @AfterEach
  public void stop() {
    omMetrics.unRegister();
    framework().clearInlineMocks();
    if (batchOperation != null) {
      batchOperation.close();
    }
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
    when(ozoneManager.isOwner(any(), any())).thenReturn(true);
    OMRequest omRequest = createSnapshotRequest(volumeName,
        bucketName, snapshotName);
    doPreExecute(omRequest);
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
    when(ozoneManager.isOwner(any(), any())).thenReturn(true);
    OMRequest omRequest = createSnapshotRequest(volumeName,
        bucketName, snapshotName);
    OMException omException =
        assertThrows(OMException.class, () -> doPreExecute(omRequest));
    assertEquals("Invalid snapshot name: " + snapshotName,
        omException.getMessage());
  }

  @Test
  public void testPreExecuteBadOwner() {
    // Owner is not set for the request.
    OMRequest omRequest = createSnapshotRequest(volumeName,
        bucketName, snapshotName1);

    OMException omException = assertThrows(OMException.class,
        () -> doPreExecute(omRequest));
    assertEquals("Only bucket owners and Ozone admins can create snapshots",
        omException.getMessage());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    OMRequest omRequest = createSnapshotRequest(volumeName,
        bucketName, snapshotName1);
    OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(omRequest);
    String key = getTableKey(volumeName, bucketName, snapshotName1);
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

    // Add a 1000-byte key to the bucket
    OmKeyInfo key1 = addKey("key-testValidateAndUpdateCache", 12345L);
    addKeyToTable(key1);

    OmBucketInfo omBucketInfo = omMetadataManager.getBucketTable().get(
        bucketKey);
    long bucketDataSize = key1.getDataSize();
    long bucketUsedBytes = omBucketInfo.getUsedBytes();
    assertEquals(key1.getReplicatedSize(), bucketUsedBytes);

    // Value in cache should be null as of now.
    assertNull(omMetadataManager.getSnapshotInfoTable().get(key));

    // Run validateAndUpdateCache.
    OMClientResponse omClientResponse =
        omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1);

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
        omMetadataManager.getSnapshotInfoTable().get(key);
    assertNotNull(snapshotInfoInCache);
    assertEquals(snapshotInfoFromProto, snapshotInfoInCache);

    assertEquals(0, omMetrics.getNumSnapshotCreateFails());
    assertEquals(1, omMetrics.getNumSnapshotActive());
    assertEquals(1, omMetrics.getNumSnapshotCreates());
  }

  @Test
  public void testEntryRenamedKeyTable() throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    Table<String, String> snapshotRenamedTable =
        omMetadataManager.getSnapshotRenamedTable();

    renameKey("key1", "key2", 0);
    renameDir("dir1", "dir2", 5);
    // Rename table should be empty as there is no rename happening in
    // the snapshot scope.
    assertTrue(snapshotRenamedTable.isEmpty());

    // Create snapshot
    createSnapshot(snapshotName1);
    String snapKey = getTableKey(volumeName,
        bucketName, snapshotName1);
    SnapshotInfo snapshotInfo =
        omMetadataManager.getSnapshotInfoTable().get(snapKey);
    assertNotNull(snapshotInfo);

    renameKey("key3", "key4", 10);
    renameDir("dir3", "dir4", 15);

    // Rename table should have two entries as rename is within snapshot scope.
    assertEquals(2, omMetadataManager
        .countRowsInTable(snapshotRenamedTable));

    // Create snapshot to clear snapshotRenamedTable
    createSnapshot(snapshotName2);
    assertTrue(snapshotRenamedTable.isEmpty());
  }

  @Test
  public void testEntryExists() throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);

    String key = getTableKey(volumeName, bucketName, snapshotName1);

    OMRequest omRequest =
        createSnapshotRequest(volumeName, bucketName, snapshotName1);
    OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(omRequest);

    assertNull(omMetadataManager.getSnapshotInfoTable().get(key));
    omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1);

    assertNotNull(omMetadataManager.getSnapshotInfoTable().get(key));

    // Now try to create again to verify error
    omRequest = createSnapshotRequest(volumeName, bucketName, snapshotName1);
    omSnapshotCreateRequest = doPreExecute(omRequest);
    OMClientResponse omClientResponse =
        omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 2);

    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getCreateSnapshotResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS,
        omResponse.getStatus());

    assertEquals(1, omMetrics.getNumSnapshotCreateFails());
    assertEquals(1, omMetrics.getNumSnapshotActive());
    assertEquals(2, omMetrics.getNumSnapshotCreates());
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

    omKeyRenameResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
  }

  private void renameDir(String fromKey, String toKey, long offset)
      throws Exception {
    String fromKeyParentName = UUID.randomUUID().toString();
    OmKeyInfo fromKeyParent = OMRequestTestUtils.createOmKeyInfo(volumeName,
        bucketName, fromKeyParentName, HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.THREE, 100L);

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
    omKeyRenameResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
  }

  protected String getDBKeyName(OmKeyInfo keyInfo) throws IOException {
    return omMetadataManager.getOzonePathKey(
        omMetadataManager.getVolumeId(volumeName),
        omMetadataManager.getBucketId(volumeName, bucketName),
        keyInfo.getParentObjectID(), keyInfo.getKeyName());
  }

  private void createSnapshot(String snapName) throws Exception {
    OMRequest omRequest =
        createSnapshotRequest(
            volumeName, bucketName, snapName);
    OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(omRequest);
    //create entry
    OMClientResponse omClientResponse =
        omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1);
    omClientResponse.checkAndUpdateDB(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
  }

  private OMSnapshotCreateRequest doPreExecute(
      OMRequest originalRequest) throws Exception {
    return doPreExecute(originalRequest, ozoneManager);
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
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
        HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.THREE,
        objectId);
  }

  protected String addKeyToTable(OmKeyInfo keyInfo) throws Exception {
    OMRequestTestUtils.addKeyToTable(false, true, keyInfo, 0, 0L,
        omMetadataManager);
    return omMetadataManager.getOzoneKey(keyInfo.getVolumeName(),
        keyInfo.getBucketName(), keyInfo.getKeyName());
  }
}
