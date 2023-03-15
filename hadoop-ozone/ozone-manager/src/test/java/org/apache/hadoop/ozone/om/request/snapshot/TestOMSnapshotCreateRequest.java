
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

import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;

import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.response.key.OMKeyRenameResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;


import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests OMSnapshotCreateRequest class, which handles CreateSnapshot request.
 */
public class TestOMSnapshotCreateRequest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;

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
    batchOperation = omMetadataManager.getStore().initBatchOperation();

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
    if (batchOperation != null) {
      batchOperation.close();
    }
  }

  @Test
  public void testPreExecute() throws Exception {
    // set the owner
    when(ozoneManager.isOwner(any(), any())).thenReturn(true);
    OMRequest omRequest =
        OMRequestTestUtils.createSnapshotRequest(
        volumeName, bucketName, snapshotName);
    // should not throw
    doPreExecute(omRequest);
  }

  @Test
  public void testPreExecuteBadOwner() throws Exception {
    // owner not set
    OMRequest omRequest =
        OMRequestTestUtils.createSnapshotRequest(
        volumeName, bucketName, snapshotName);
    // Check bad owner
    LambdaTestUtils.intercept(OMException.class,
        "Only bucket owners and Ozone admins can create snapshots",
        () -> doPreExecute(omRequest));
  }

  @Test
  public void testPreExecuteBadName() throws Exception {
    // check invalid snapshot name
    String badName = "a?b";
    OMRequest omRequest =
        OMRequestTestUtils.createSnapshotRequest(
        volumeName, bucketName, badName);
    LambdaTestUtils.intercept(OMException.class,
        "Invalid snapshot name: " + badName,
        () -> doPreExecute(omRequest));
  }

  @Test
  public void testPreExecuteNameOnlyNumbers() throws Exception {
    // check invalid snapshot name containing only numbers
    String badNameON = "1234";
    OMRequest omRequest =
            OMRequestTestUtils.createSnapshotRequest(
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
    String name2 = "s1";
    String name3 = "sn1";

    // name length = 63
    when(ozoneManager.isOwner(any(), any())).thenReturn(true);
    OMRequest omRequest = OMRequestTestUtils.createSnapshotRequest(
                    volumeName, bucketName, name63);
    // should not throw any error
    doPreExecute(omRequest);

    // name length = 64
    OMRequest omRequest2 = OMRequestTestUtils.createSnapshotRequest(
                    volumeName, bucketName, name64);
    LambdaTestUtils.intercept(OMException.class,
            "Invalid snapshot name: " + name64,
            () -> doPreExecute(omRequest2));

    // name length = 3
    when(ozoneManager.isOwner(any(), any())).thenReturn(true);
    OMRequest omRequest3 = OMRequestTestUtils.createSnapshotRequest(
            volumeName, bucketName, name3);
    // should not throw any error
    doPreExecute(omRequest3);

    // name length = 2
    OMRequest omRequest4 = OMRequestTestUtils.createSnapshotRequest(
            volumeName, bucketName, name2);
    LambdaTestUtils.intercept(OMException.class,
            "Invalid snapshot name: " + name2,
            () -> doPreExecute(omRequest4));
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    SnapshotChainManager snapshotChainManager =
        new SnapshotChainManager(omMetadataManager);
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    when(ozoneManager.getSnapshotChainManager())
        .thenReturn(snapshotChainManager);
    OMRequest omRequest =
        OMRequestTestUtils.createSnapshotRequest(
        volumeName, bucketName, snapshotName);
    OMSnapshotCreateRequest omSnapshotCreateRequest =
        doPreExecute(omRequest);
    String key = SnapshotInfo.getTableKey(volumeName,
        bucketName, snapshotName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.
    Assert.assertNull(omMetadataManager.getSnapshotInfoTable().get(key));

    // add key to cache
    OMClientResponse omClientResponse =
        omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);
    
    // check cache
    SnapshotInfo snapshotInfo =
        omMetadataManager.getSnapshotInfoTable().get(key);
    Assert.assertNotNull(snapshotInfo);

    // verify table data with response data.
    SnapshotInfo snapshotInfoFromProto = SnapshotInfo.getFromProtobuf(
        omClientResponse.getOMResponse()
        .getCreateSnapshotResponse().getSnapshotInfo());
    Assert.assertEquals(snapshotInfoFromProto, snapshotInfo);

    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateSnapshotResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Type.CreateSnapshot,
        omResponse.getCmdType());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());
  }

  @Test
  public void testEmptyRenamedKeyTable() throws Exception {
    SnapshotChainManager snapshotChainManager =
        new SnapshotChainManager(omMetadataManager);
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    when(ozoneManager.getSnapshotChainManager())
        .thenReturn(snapshotChainManager);
    OmKeyInfo toKeyInfo = addKey("key1");
    OmKeyInfo fromKeyInfo = addKey("key2");

    OMResponse omResponse =
        OMResponse.newBuilder().setRenameKeyResponse(
            OzoneManagerProtocolProtos.RenameKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey)
            .build();
    OMKeyRenameResponse omKeyRenameResponse =
        new OMKeyRenameResponse(omResponse, fromKeyInfo.getKeyName(),
            toKeyInfo.getKeyName(), toKeyInfo);

    Assert.assertTrue(omMetadataManager.getRenamedKeyTable().isEmpty());
    omKeyRenameResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    Assert.assertFalse(omMetadataManager.getRenamedKeyTable().isEmpty());

    OMRequest omRequest =
        OMRequestTestUtils.createSnapshotRequest(
            volumeName, bucketName, snapshotName);
    OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(omRequest);
    String key = SnapshotInfo.getTableKey(volumeName,
        bucketName, snapshotName);

    Assert.assertNull(omMetadataManager.getSnapshotInfoTable().get(key));

    //create entry
    OMClientResponse omClientResponse = 
        omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1,
        ozoneManagerDoubleBufferHelper);
    omClientResponse.checkAndUpdateDB(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    SnapshotInfo snapshotInfo =
        omMetadataManager.getSnapshotInfoTable().get(key);
    Assert.assertNotNull(snapshotInfo);
    Assert.assertTrue(omMetadataManager.getRenamedKeyTable().isEmpty());

  }

  @Test
  public void testEntryExists() throws Exception {
    SnapshotChainManager snapshotChainManager =
        new SnapshotChainManager(omMetadataManager);
    when(ozoneManager.isAdmin(any())).thenReturn(true);
    when(ozoneManager.getSnapshotChainManager())
        .thenReturn(snapshotChainManager);
    OMRequest omRequest =
        OMRequestTestUtils.createSnapshotRequest(
        volumeName, bucketName, snapshotName);
    OMSnapshotCreateRequest omSnapshotCreateRequest = doPreExecute(omRequest);
    String key = SnapshotInfo.getTableKey(volumeName,
        bucketName, snapshotName);

    Assert.assertNull(omMetadataManager.getSnapshotInfoTable().get(key));

    //create entry
    omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 1,
        ozoneManagerDoubleBufferHelper);
    SnapshotInfo snapshotInfo =
        omMetadataManager.getSnapshotInfoTable().get(key);
    Assert.assertNotNull(snapshotInfo);

    // Now try to create again to verify error
    omRequest =
        OMRequestTestUtils.createSnapshotRequest(
        volumeName, bucketName, snapshotName);
    omSnapshotCreateRequest = doPreExecute(omRequest);
    OMClientResponse omClientResponse =
        omSnapshotCreateRequest.validateAndUpdateCache(ozoneManager, 2,
            ozoneManagerDoubleBufferHelper);
    
    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateSnapshotResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS,
        omResponse.getStatus());
  }

  private OMSnapshotCreateRequest doPreExecute(
      OMRequest originalRequest) throws Exception {
    return doPreExecute(originalRequest, ozoneManager);
  }

  /**
   * Static helper method so this could be used in TestOMSnapshotDeleteRequest.
   */
  static OMSnapshotCreateRequest doPreExecute(
      OMRequest originalRequest, OzoneManager ozoneManager) throws Exception {
    OMSnapshotCreateRequest omSnapshotCreateRequest =
        new OMSnapshotCreateRequest(originalRequest);

    OMRequest modifiedRequest =
        omSnapshotCreateRequest.preExecute(ozoneManager);
    return new OMSnapshotCreateRequest(modifiedRequest);
  }

  private OmKeyInfo addKey(String keyName) {
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
        HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.ONE, 0L);
  }

  protected String addKeyToTable(OmKeyInfo keyInfo) throws Exception {
    OMRequestTestUtils.addKeyToTable(false, false, keyInfo, 0, 0L,
        omMetadataManager);
    return omMetadataManager.getOzoneKey(keyInfo.getVolumeName(),
        keyInfo.getBucketName(), keyInfo.getKeyName());
  }

}
