
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

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests OMSnapshotDeleteRequest class, which handles DeleteSnapshot request.
 * Mostly mirrors TestOMSnapshotCreateRequest.
 * testEntryNotExist() and testEntryExists() are unique.
 */
public class TestOMSnapshotDeleteRequest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OmMetadataManagerImpl omMetadataManager;

  private String volumeName;
  private String bucketName;
  private String snapshotName;

  // Just setting ozoneManagerDoubleBuffer which does nothing.
  private final OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper =
      ((response, transactionIndex) -> null);

  @Before
  public void setup() throws Exception {

    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.isRatisEnabled()).thenReturn(true);
    when(ozoneManager.isAdmin(any())).thenReturn(false);
    when(ozoneManager.isOwner(any(), any())).thenReturn(false);
    when(ozoneManager.getBucketOwner(any(), any(),
        any(), any())).thenReturn("dummyBucketOwner");
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(0);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    AuditLogger auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));

    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    snapshotName = UUID.randomUUID().toString();
    OMRequestTestUtils.addVolumeAndBucketToDB(
        volumeName, bucketName, omMetadataManager);

  }

  @After
  public void stop() {
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
  }

  @Test
  public void testPreExecute() throws Exception {
    // set the owner
    when(ozoneManager.isOwner(any(), any())).thenReturn(true);
    OMRequest omRequest =
        OMRequestTestUtils.deleteSnapshotRequest(
        volumeName, bucketName, snapshotName);
    // should not throw
    doPreExecute(omRequest);
  }

  @Test
  public void testPreExecuteBadOwner() throws Exception {
    // owner not set
    OMRequest omRequest =
        OMRequestTestUtils.deleteSnapshotRequest(
        volumeName, bucketName, snapshotName);
    // Check bad owner
    LambdaTestUtils.intercept(OMException.class,
        "Only bucket owners and Ozone admins can delete snapshots",
        () -> doPreExecute(omRequest));
  }

  @Test
  public void testPreExecuteBadName() throws Exception {
    // check invalid snapshot name
    String badName = "a?b";
    OMRequest omRequest =
        OMRequestTestUtils.deleteSnapshotRequest(
        volumeName, bucketName, badName);
    LambdaTestUtils.intercept(OMException.class,
        "Invalid snapshot name: " + badName,
        () -> doPreExecute(omRequest));
  }

  @Test
  public void testPreExecuteNameOnlyNumbers() throws Exception {
    // check invalid snapshot name containing only numbers
    String badNameON = "1234";
    OMRequest omRequest = OMRequestTestUtils.deleteSnapshotRequest(
        volumeName, bucketName, badNameON);
    LambdaTestUtils.intercept(OMException.class,
        "Invalid snapshot name: " + badNameON,
        () -> doPreExecute(omRequest));
  }

  @Test
  public void testPreExecuteNameLength() throws Exception {
    // check snapshot name length
    String name63 =
            "snap75795657617173401188448010125899089001363595171500499231286";
    String name64 =
            "snap156808943643007724443266605711479126926050896107709081166294";

    // name length = 63
    when(ozoneManager.isOwner(any(), any())).thenReturn(true);
    OMRequest omRequest = OMRequestTestUtils.deleteSnapshotRequest(
        volumeName, bucketName, name63);
    // should not throw any error
    doPreExecute(omRequest);

    // name length = 64
    OMRequest omRequest2 = OMRequestTestUtils.deleteSnapshotRequest(
        volumeName, bucketName, name64);
    LambdaTestUtils.intercept(OMException.class,
        "Invalid snapshot name: " + name64,
        () -> doPreExecute(omRequest2));
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    OMRequest omRequest =
        OMRequestTestUtils.deleteSnapshotRequest(
        volumeName, bucketName, snapshotName);
    OMSnapshotDeleteRequest omSnapshotDeleteRequest =
        doPreExecute(omRequest);
    String key = SnapshotInfo.getTableKey(volumeName,
        bucketName, snapshotName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.
    Assert.assertNull(omMetadataManager.getSnapshotInfoTable().get(key));

    // add key to cache
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(
        volumeName, bucketName, snapshotName, null);
    Assert.assertEquals(SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE,
        snapshotInfo.getSnapshotStatus());
    omMetadataManager.getSnapshotInfoTable().addCacheEntry(
        new CacheKey<>(key),
        new CacheValue<>(Optional.of(snapshotInfo), 1L));

    // Trigger validateAndUpdateCache
    OMClientResponse omClientResponse =
        omSnapshotDeleteRequest.validateAndUpdateCache(ozoneManager, 2L,
            ozoneManagerDoubleBufferHelper);

    // check cache
    snapshotInfo = omMetadataManager.getSnapshotInfoTable().get(key);
    Assert.assertNotNull(snapshotInfo);
    Assert.assertEquals(SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED,
        snapshotInfo.getSnapshotStatus());

    OMResponse omResponse = omClientResponse.getOMResponse();

    // check response success flag
    Assert.assertTrue(omResponse.getSuccess());

    Assert.assertNotNull(omResponse.getDeleteSnapshotResponse());
    Assert.assertEquals(Type.DeleteSnapshot, omResponse.getCmdType());
    Assert.assertEquals(Status.OK, omResponse.getStatus());
  }

  /**
   * Expect snapshot deletion failure when snapshot entry does not exist.
   */
  @Test
  public void testEntryNotExist() throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    OMRequest omRequest = OMRequestTestUtils.deleteSnapshotRequest(
        volumeName, bucketName, snapshotName);
    OMSnapshotDeleteRequest omSnapshotDeleteRequest = doPreExecute(omRequest);
    String key = SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName);

    // Entry does not exist
    Assert.assertNull(omMetadataManager.getSnapshotInfoTable().get(key));

    // Trigger delete snapshot validateAndUpdateCache
    OMClientResponse omClientResponse =
        omSnapshotDeleteRequest.validateAndUpdateCache(ozoneManager, 1L,
            ozoneManagerDoubleBufferHelper);

    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getDeleteSnapshotResponse());
    Assert.assertEquals(Status.FILE_NOT_FOUND, omResponse.getStatus());
  }

  /**
   * Expect successful snapshot deletion when snapshot entry exists.
   * But a second deletion would result in an error.
   */
  @Test
  public void testEntryExists() throws Exception {
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    String key = SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName);

    OMRequest omRequest1 = OMRequestTestUtils.createSnapshotRequest(
        volumeName, bucketName, snapshotName);
    OMSnapshotCreateRequest omSnapshotCreateRequest =
        TestOMSnapshotCreateRequest.doPreExecute(omRequest1, ozoneManager);

    Assert.assertNull(omMetadataManager.getSnapshotInfoTable().get(key));

    // Create snapshot entry
    omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1L,
        ozoneManagerDoubleBufferHelper);
    SnapshotInfo snapshotInfo =
        omMetadataManager.getSnapshotInfoTable().get(key);
    Assert.assertNotNull(snapshotInfo);
    Assert.assertEquals(SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE,
        snapshotInfo.getSnapshotStatus());

    OMRequest omRequest2 = OMRequestTestUtils.deleteSnapshotRequest(
        volumeName, bucketName, snapshotName);
    OMSnapshotDeleteRequest omSnapshotDeleteRequest = doPreExecute(omRequest2);

    // Delete snapshot entry
    OMClientResponse omClientResponse =
        omSnapshotDeleteRequest.validateAndUpdateCache(ozoneManager, 2L,
            ozoneManagerDoubleBufferHelper);

    snapshotInfo = omMetadataManager.getSnapshotInfoTable().get(key);
    // The snapshot entry should still exist in the table,
    // but marked as DELETED.
    Assert.assertNotNull(snapshotInfo);
    Assert.assertEquals(SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED,
        snapshotInfo.getSnapshotStatus());
    Assert.assertTrue(snapshotInfo.getDeletionTime() > 0L);

    // Response should be successful
    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getDeleteSnapshotResponse());
    Assert.assertEquals(Status.OK, omResponse.getStatus());

    // Now delete snapshot entry again, expect error
    omRequest2 = OMRequestTestUtils.deleteSnapshotRequest(
        volumeName, bucketName, snapshotName);
    omSnapshotDeleteRequest = doPreExecute(omRequest2);
    omClientResponse =
        omSnapshotDeleteRequest.validateAndUpdateCache(ozoneManager, 3L,
            ozoneManagerDoubleBufferHelper);

    // Snapshot entry should still be there.
    snapshotInfo = omMetadataManager.getSnapshotInfoTable().get(key);
    Assert.assertNotNull(snapshotInfo);
    Assert.assertEquals(SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED,
        snapshotInfo.getSnapshotStatus());

    omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getDeleteSnapshotResponse());
    Assert.assertEquals(Status.FILE_NOT_FOUND, omResponse.getStatus());
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
