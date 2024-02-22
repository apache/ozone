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

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.getFromProtobuf;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.getTableKey;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.createSnapshotRequest;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.renameSnapshotRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.RenameSnapshot;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests OMSnapshotRenameRequest class, which handles RenameSnapshot request.
 */
public class TestOMSnapshotRenameRequest {

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
    ozoneConfiguration.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
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
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    OmSnapshotManager omSnapshotManager = new OmSnapshotManager(ozoneManager);
    when(ozoneManager.getOmSnapshotManager()).thenReturn(omSnapshotManager);

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
  public void testPreExecute(String toSnapshotName) throws Exception {
    when(ozoneManager.isOwner(any(), any())).thenReturn(true);

    String currentSnapshotName = "current";
    OzoneManagerProtocolProtos.OMRequest omRequest = renameSnapshotRequest(volumeName,
        bucketName, currentSnapshotName, toSnapshotName);
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
      "snap156808943643007724443266605711479126926050896107709081166294",
      // Underscore is not allowed.
      "snap_1",
      // CamelCase is not allowed.
      "NewSnapshot"
  })
  @ParameterizedTest
  public void testPreExecuteFailure(String toSnapshotName) {
    when(ozoneManager.isOwner(any(), any())).thenReturn(true);
    String currentSnapshotName = "current";
    OzoneManagerProtocolProtos.OMRequest omRequest = renameSnapshotRequest(volumeName,
        bucketName, currentSnapshotName, toSnapshotName);
    OMException omException =
        assertThrows(OMException.class, () -> doPreExecute(omRequest));
    assertEquals("Invalid snapshot name: " + toSnapshotName,
        omException.getMessage());
  }

  @Test
  public void testPreExecuteBadOwner() {
    // Owner is not set for the request.
    OzoneManagerProtocolProtos.OMRequest omRequest = renameSnapshotRequest(volumeName,
        bucketName, snapshotName1, snapshotName2);

    OMException omException = assertThrows(OMException.class,
        () -> doPreExecute(omRequest));
    assertEquals("Only bucket owners and Ozone admins can rename snapshots",
        omException.getMessage());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    OzoneManagerProtocolProtos.OMRequest omRequest = renameSnapshotRequest(volumeName,
        bucketName, snapshotName1, snapshotName2);
    OMSnapshotRenameRequest omSnapshotRenameRequest = doPreExecute(omRequest);
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

    // Add key to cache.
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(volumeName, bucketName,
        snapshotName1, UUID.randomUUID(), Time.now());
    snapshotInfo.setReferencedSize(1000L);
    snapshotInfo.setReferencedReplicatedSize(3 * 1000L);
    assertEquals(SNAPSHOT_ACTIVE, snapshotInfo.getSnapshotStatus());
    omMetadataManager.getSnapshotInfoTable().addCacheEntry(
        new CacheKey<>(key),
        CacheValue.get(1L, snapshotInfo));

    // Run validateAndUpdateCache.
    OMClientResponse omClientResponse =
        omSnapshotRenameRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertNotNull(omClientResponse.getOMResponse());

    OzoneManagerProtocolProtos.OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getRenameSnapshotResponse());
    assertEquals(RenameSnapshot, omResponse.getCmdType());
    assertEquals(OK, omResponse.getStatus());

    // verify table data with response data.
    OzoneManagerProtocolProtos.SnapshotInfo snapshotInfoProto =
        omClientResponse
            .getOMResponse()
            .getRenameSnapshotResponse()
            .getSnapshotInfo();

    assertEquals(bucketDataSize, snapshotInfoProto.getReferencedSize());
    assertEquals(bucketUsedBytes,
        snapshotInfoProto.getReferencedReplicatedSize());

    SnapshotInfo snapshotInfoOldProto = getFromProtobuf(snapshotInfoProto);

    String key2 = getTableKey(volumeName, bucketName, snapshotName2);

    // Get value from cache
    SnapshotInfo snapshotInfoNewInCache =
        omMetadataManager.getSnapshotInfoTable().get(key2);
    assertNotNull(snapshotInfoNewInCache);
    assertEquals(snapshotInfoOldProto, snapshotInfoNewInCache);
    assertEquals(snapshotInfo.getSnapshotId(), snapshotInfoNewInCache.getSnapshotId());

    SnapshotInfo snapshotInfoOldInCache =
        omMetadataManager.getSnapshotInfoTable().get(key);
    assertNull(snapshotInfoOldInCache);
  }

  @Test
  public void testEntryExists() throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);

    String keyNameOld = getTableKey(volumeName, bucketName, snapshotName1);
    String keyNameNew = getTableKey(volumeName, bucketName, snapshotName2);

    assertNull(omMetadataManager.getSnapshotInfoTable().get(keyNameOld));
    assertNull(omMetadataManager.getSnapshotInfoTable().get(keyNameNew));

    // First make sure we have two snapshots.
    OzoneManagerProtocolProtos.OMRequest createOmRequest =
        createSnapshotRequest(volumeName, bucketName, snapshotName1);
    OMSnapshotCreateRequest omSnapshotCreateRequest =
        TestOMSnapshotCreateRequest.doPreExecute(createOmRequest, ozoneManager);
    omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1);

    createOmRequest =
        createSnapshotRequest(volumeName, bucketName, snapshotName2);
    omSnapshotCreateRequest =
        TestOMSnapshotCreateRequest.doPreExecute(createOmRequest, ozoneManager);
    omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 2);

    assertNotNull(omMetadataManager.getSnapshotInfoTable().get(keyNameOld));
    assertNotNull(omMetadataManager.getSnapshotInfoTable().get(keyNameNew));

    // Now try renaming and get an error.
    OzoneManagerProtocolProtos.OMRequest omRequest =
        renameSnapshotRequest(volumeName, bucketName, snapshotName1, snapshotName2);
    OMSnapshotRenameRequest omSnapshotRenameRequest = doPreExecute(omRequest);

    OMClientResponse omClientResponse =
        omSnapshotRenameRequest.validateAndUpdateCache(ozoneManager, 3);

    assertNotNull(omMetadataManager.getSnapshotInfoTable().get(keyNameOld));
    assertNotNull(omMetadataManager.getSnapshotInfoTable().get(keyNameNew));

    OzoneManagerProtocolProtos.OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getRenameSnapshotResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS,
        omResponse.getStatus());
  }

  @Test
  public void testEntryNotFound() throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);

    String keyNameOld = getTableKey(volumeName, bucketName, snapshotName1);
    String keyNameNew = getTableKey(volumeName, bucketName, snapshotName2);

    assertNull(omMetadataManager.getSnapshotInfoTable().get(keyNameOld));
    assertNull(omMetadataManager.getSnapshotInfoTable().get(keyNameNew));

    // Now try renaming and get an error.
    OzoneManagerProtocolProtos.OMRequest omRequest =
        renameSnapshotRequest(volumeName, bucketName, snapshotName1, snapshotName2);
    OMSnapshotRenameRequest omSnapshotRenameRequest = doPreExecute(omRequest);

    OMClientResponse omClientResponse =
        omSnapshotRenameRequest.validateAndUpdateCache(ozoneManager, 3);

    assertNull(omMetadataManager.getSnapshotInfoTable().get(keyNameOld));
    assertNull(omMetadataManager.getSnapshotInfoTable().get(keyNameNew));

    OzoneManagerProtocolProtos.OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getRenameSnapshotResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.FILE_NOT_FOUND,
        omResponse.getStatus());
  }

  private OMSnapshotRenameRequest doPreExecute(
      OzoneManagerProtocolProtos.OMRequest originalRequest) throws Exception {
    return doPreExecute(originalRequest, ozoneManager);
  }

  public static OMSnapshotRenameRequest doPreExecute(
      OzoneManagerProtocolProtos.OMRequest originalRequest, OzoneManager ozoneManager) throws Exception {
    OMSnapshotRenameRequest omSnapshotRenameRequest =
        new OMSnapshotRenameRequest(originalRequest);

    OzoneManagerProtocolProtos.OMRequest modifiedRequest =
        omSnapshotRenameRequest.preExecute(ozoneManager);
    return new OMSnapshotRenameRequest(modifiedRequest);
  }

  private OmKeyInfo addKey(String keyName, long objectId) {
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
            RatisReplicationConfig.getInstance(THREE)).setObjectID(objectId)
        .build();
  }

  protected String addKeyToTable(OmKeyInfo keyInfo) throws Exception {
    OMRequestTestUtils.addKeyToTable(false, true, keyInfo, 0, 0L,
        omMetadataManager);
    return omMetadataManager.getOzoneKey(keyInfo.getVolumeName(),
        keyInfo.getBucketName(), keyInfo.getKeyName());
  }

}
