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
 *
 */

package org.apache.hadoop.ozone.om.request.snapshot;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.File;
import java.util.UUID;

import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.createSnapshotRequest;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.deleteSnapshotRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteSnapshot;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests OMSnapshotDeleteRequest class, which handles DeleteSnapshot request.
 * Mostly mirrors TestOMSnapshotCreateRequest.
 * testEntryNotExist() and testEntryExists() are unique.
 */
public class TestOMSnapshotDeleteRequest {
  @TempDir
  private File folder;

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OmMetadataManagerImpl omMetadataManager;

  private String volumeName;
  private String bucketName;
  private String snapshotName;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.getAbsolutePath());
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
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));

    OmSnapshotManager omSnapshotManager = mock(OmSnapshotManager.class);
    when(ozoneManager.getOmSnapshotManager()).thenReturn(omSnapshotManager);

    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    snapshotName = UUID.randomUUID().toString();
    OMRequestTestUtils.addVolumeAndBucketToDB(
        volumeName, bucketName, omMetadataManager);

  }

  @AfterEach
  public void stop() {
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
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
  public void testPreExecute(String deleteSnapshotName) throws Exception {
    when(ozoneManager.isOwner(any(), any())).thenReturn(true);
    OMRequest omRequest = deleteSnapshotRequest(volumeName,
        bucketName, deleteSnapshotName);
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
  public void testPreExecuteFailure(String deleteSnapshotName) {
    when(ozoneManager.isOwner(any(), any())).thenReturn(true);
    OMRequest omRequest = deleteSnapshotRequest(volumeName,
        bucketName, deleteSnapshotName);
    OMException omException =
        assertThrows(OMException.class, () -> doPreExecute(omRequest));
    assertEquals("Invalid snapshot name: " + deleteSnapshotName,
        omException.getMessage());
  }

  @Test
  public void testPreExecuteBadOwner() {
    // Owner is not set for the request.
    OMRequest omRequest = deleteSnapshotRequest(volumeName,
        bucketName, snapshotName);

    OMException omException = assertThrows(OMException.class,
        () -> doPreExecute(omRequest));
    assertEquals("Only bucket owners and Ozone admins can delete snapshots",
        omException.getMessage());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    OMRequest omRequest =
        deleteSnapshotRequest(volumeName, bucketName, snapshotName);
    OMSnapshotDeleteRequest omSnapshotDeleteRequest = doPreExecute(omRequest);
    String key = SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.
    assertNull(omMetadataManager.getSnapshotInfoTable().get(key));

    // add key to cache
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(volumeName, bucketName,
        snapshotName, null, Time.now());
    assertEquals(SNAPSHOT_ACTIVE, snapshotInfo.getSnapshotStatus());
    omMetadataManager.getSnapshotInfoTable().addCacheEntry(
        new CacheKey<>(key),
        CacheValue.get(1L, snapshotInfo));

    // Trigger validateAndUpdateCache
    OMClientResponse omClientResponse =
        omSnapshotDeleteRequest.validateAndUpdateCache(ozoneManager, 2L);

    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse);
    assertTrue(omResponse.getSuccess());
    assertNotNull(omResponse.getDeleteSnapshotResponse());
    assertEquals(DeleteSnapshot, omResponse.getCmdType());
    assertEquals(OK, omResponse.getStatus());

    // check cache
    snapshotInfo = omMetadataManager.getSnapshotInfoTable().get(key);
    assertNotNull(snapshotInfo);
    assertEquals(SNAPSHOT_DELETED, snapshotInfo.getSnapshotStatus());
    assertEquals(0, omMetrics.getNumSnapshotCreates());
    // Expected -1 because no snapshot was created before.
    assertEquals(-1, omMetrics.getNumSnapshotActive());
    assertEquals(1, omMetrics.getNumSnapshotDeleted());
    assertEquals(0, omMetrics.getNumSnapshotDeleteFails());
  }

  /**
   * Expect snapshot deletion failure when snapshot entry does not exist.
   */
  @Test
  public void testEntryNotExist() throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    OMRequest omRequest = deleteSnapshotRequest(
        volumeName, bucketName, snapshotName);
    OMSnapshotDeleteRequest omSnapshotDeleteRequest = doPreExecute(omRequest);
    String key = SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName);

    // Entry does not exist
    assertNull(omMetadataManager.getSnapshotInfoTable().get(key));

    // Trigger delete snapshot validateAndUpdateCache
    OMClientResponse omClientResponse =
        omSnapshotDeleteRequest.validateAndUpdateCache(ozoneManager, 1L);

    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getDeleteSnapshotResponse());
    assertEquals(Status.FILE_NOT_FOUND, omResponse.getStatus());
    assertEquals(0, omMetrics.getNumSnapshotActive());
    assertEquals(0, omMetrics.getNumSnapshotDeleted());
    assertEquals(1, omMetrics.getNumSnapshotDeleteFails());
  }

  /**
   * Expect successful snapshot deletion when snapshot entry exists.
   * But a second deletion would result in an error.
   */
  @Test
  public void testEntryExist() throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    String key = SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName);

    OMRequest omRequest1 =
        createSnapshotRequest(volumeName, bucketName, snapshotName);
    OMSnapshotCreateRequest omSnapshotCreateRequest =
        TestOMSnapshotCreateRequest.doPreExecute(omRequest1, ozoneManager);

    assertNull(omMetadataManager.getSnapshotInfoTable().get(key));

    // Create snapshot entry
    omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1L);
    SnapshotInfo snapshotInfo =
        omMetadataManager.getSnapshotInfoTable().get(key);
    assertNotNull(snapshotInfo);
    assertEquals(SNAPSHOT_ACTIVE, snapshotInfo.getSnapshotStatus());

    assertEquals(1, omMetrics.getNumSnapshotActive());
    OMRequest omRequest2 =
        deleteSnapshotRequest(volumeName, bucketName, snapshotName);
    OMSnapshotDeleteRequest omSnapshotDeleteRequest = doPreExecute(omRequest2);

    // Delete snapshot entry
    OMClientResponse omClientResponse =
        omSnapshotDeleteRequest.validateAndUpdateCache(ozoneManager, 2L);
    // Response should be successful
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse);
    assertNotNull(omResponse.getDeleteSnapshotResponse());
    assertEquals(OK, omResponse.getStatus());

    snapshotInfo = omMetadataManager.getSnapshotInfoTable().get(key);
    // The snapshot entry should still exist in the table,
    // but marked as DELETED.
    assertNotNull(snapshotInfo);
    assertEquals(SNAPSHOT_DELETED, snapshotInfo.getSnapshotStatus());
    assertTrue(snapshotInfo.getDeletionTime() > 0L);
    assertEquals(0, omMetrics.getNumSnapshotActive());

    // Now delete snapshot entry again, expect error.
    omRequest2 = deleteSnapshotRequest(volumeName, bucketName, snapshotName);
    omSnapshotDeleteRequest = doPreExecute(omRequest2);
    omClientResponse =
        omSnapshotDeleteRequest.validateAndUpdateCache(ozoneManager, 3L);

    omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse);
    assertNotNull(omResponse.getDeleteSnapshotResponse());
    assertEquals(Status.FILE_NOT_FOUND, omResponse.getStatus());

    // Snapshot entry should still be there.
    snapshotInfo = omMetadataManager.getSnapshotInfoTable().get(key);
    assertNotNull(snapshotInfo);
    assertEquals(SNAPSHOT_DELETED, snapshotInfo.getSnapshotStatus());
    assertEquals(0, omMetrics.getNumSnapshotActive());
    assertEquals(1, omMetrics.getNumSnapshotDeleteFails());
  }

  private OMSnapshotDeleteRequest doPreExecute(
      OMRequest originalRequest) throws Exception {
    OMSnapshotDeleteRequest omSnapshotDeleteRequest =
        new OMSnapshotDeleteRequest(originalRequest);

    OMRequest modifiedRequest =
        omSnapshotDeleteRequest.preExecute(ozoneManager);
    return new OMSnapshotDeleteRequest(modifiedRequest);
  }

}
