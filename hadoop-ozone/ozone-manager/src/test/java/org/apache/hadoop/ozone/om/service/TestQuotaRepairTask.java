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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.OMKeyCommitRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequestTests;
import org.apache.hadoop.ozone.om.request.volume.OMQuotaRepairRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMQuotaRepairResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test class for quota repair.
 */
@Timeout(120)
public class TestQuotaRepairTask extends OMKeyRequestTests {

  /** Seconds; must match {@link Timeout} on this class. */
  private static final int REPAIR_TEST_TIMEOUT_SECONDS = 120;

  private static Boolean awaitRepair(CompletableFuture<Boolean> repair) throws Exception {
    return repair.get(REPAIR_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  public void testQuotaRepair() throws Exception {
    OzoneManagerProtocolProtos.OMResponse respMock = mock(OzoneManagerProtocolProtos.OMResponse.class);
    when(respMock.getSuccess()).thenReturn(true);
    OzoneManagerRatisServer ratisServerMock = mock(OzoneManagerRatisServer.class);
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref = new AtomicReference<>();
    doAnswer(invocation -> {
      ref.set(invocation.getArgument(0, OzoneManagerProtocolProtos.OMRequest.class));
      return respMock;
    }).when(ratisServerMock).submitRequest(any(), any(), anyLong());
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServerMock);
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.OBJECT_STORE);

    int count = 10;
    String parentDir = "/user";
    for (int i = 0; i < count; i++) {
      OMRequestTestUtils.addKeyToTableAndCache(volumeName, bucketName,
          parentDir.concat("/key" + i), -1, RatisReplicationConfig.getInstance(THREE), 150 + i, omMetadataManager);
    }

    String fsoBucketName = "fso" + bucketName;
    OMRequestTestUtils.addBucketToDB(volumeName, fsoBucketName,
        omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    long parentId = OMRequestTestUtils.addParentsToDirTable(volumeName,
        fsoBucketName, "c/d/e", omMetadataManager);
    for (int i = 0; i < count; i++) {
      String fileName = "file1" + i;
      OmKeyInfo omKeyInfo =
          OMRequestTestUtils.createOmKeyInfo(volumeName, fsoBucketName, fileName,
                  RatisReplicationConfig.getInstance(ONE))
              .setObjectID(parentId + 1 + i)
              .setParentObjectID(parentId)
              .setUpdateID(100L + i)
              .build();
      omKeyInfo.setKeyName(fileName);
      OMRequestTestUtils.addFileToKeyTable(false, false,
          fileName, omKeyInfo, -1, 50 + i, omMetadataManager);
    }

    // Intentionally zero out buckets' used bytes first
    zeroOutBucketUsedBytes(volumeName, bucketName, 1L);
    zeroOutBucketUsedBytes(volumeName, fsoBucketName, 2L);

    // all count is 0 as above is adding directly to key / file table
    // and directory table
    OmBucketInfo obsBucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    assertEquals(0, obsBucketInfo.getUsedNamespace());
    assertEquals(0, obsBucketInfo.getUsedBytes());
    OmBucketInfo fsoBucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, fsoBucketName));
    assertEquals(0, fsoBucketInfo.getUsedNamespace());
    assertEquals(0, fsoBucketInfo.getUsedBytes());
    
    QuotaRepairTask quotaRepairTask = new QuotaRepairTask(ozoneManager);
    CompletableFuture<Boolean> repair = quotaRepairTask.repair();
    Boolean repairStatus = awaitRepair(repair);
    assertTrue(repairStatus);

    OMQuotaRepairRequest omQuotaRepairRequest = new OMQuotaRepairRequest(ref.get());
    OMClientResponse omClientResponse = omQuotaRepairRequest.validateAndUpdateCache(ozoneManager, 1);
    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    ((OMQuotaRepairResponse)omClientResponse).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    // 10 files of each type, obs have replication of three and
    // fso have replication of one
    OmBucketInfo obsUpdateBucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    OmBucketInfo fsoUpdateBucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, fsoBucketName));
    assertEquals(10, obsUpdateBucketInfo.getUsedNamespace());
    assertEquals(30000, obsUpdateBucketInfo.getUsedBytes());
    assertEquals(13, fsoUpdateBucketInfo.getUsedNamespace());
    assertEquals(10000, fsoUpdateBucketInfo.getUsedBytes());
  }

  @Test
  public void testQuotaRepairForOldVersionVolumeBucket() throws Exception {
    OzoneManagerProtocolProtos.OMResponse respMock = mock(OzoneManagerProtocolProtos.OMResponse.class);
    when(respMock.getSuccess()).thenReturn(true);
    OzoneManagerRatisServer ratisServerMock = mock(OzoneManagerRatisServer.class);
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref = new AtomicReference<>();
    doAnswer(invocation -> {
      ref.set(invocation.getArgument(0, OzoneManagerProtocolProtos.OMRequest.class));
      return respMock;
    }).when(ratisServerMock).submitRequest(any(), any(), anyLong());
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServerMock);
    // add volume with -2 value
    OmVolumeArgs omVolumeArgs =
        OmVolumeArgs.newBuilder().setCreationTime(Time.now())
            .setVolume(volumeName).setAdminName(volumeName)
            .setOwnerName(volumeName).setQuotaInBytes(-2)
            .setQuotaInNamespace(-2).build();
    omMetadataManager.getVolumeTable().put(
        omMetadataManager.getVolumeKey(volumeName), omVolumeArgs);
    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getVolumeKey(volumeName)),
        CacheValue.get(1L, omVolumeArgs));
    
    // add bucket with -2 value and add to db
    OMRequestTestUtils.addBucketToDB(volumeName, bucketName,
        omMetadataManager, -2);
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    omMetadataManager.getBucketTable().put(bucketKey, omMetadataManager.getBucketTable().get(bucketKey));

    // pre check for quota flag
    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(-2, bucketInfo.getQuotaInBytes());
    
    omVolumeArgs = omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(volumeName));
    assertEquals(-2, omVolumeArgs.getQuotaInBytes());
    assertEquals(-2, omVolumeArgs.getQuotaInNamespace());

    QuotaRepairTask quotaRepairTask = new QuotaRepairTask(ozoneManager);
    CompletableFuture<Boolean> repair = quotaRepairTask.repair();
    Boolean repairStatus = awaitRepair(repair);
    assertTrue(repairStatus);

    OMQuotaRepairRequest omQuotaRepairRequest = new OMQuotaRepairRequest(ref.get());
    OMClientResponse omClientResponse = omQuotaRepairRequest.validateAndUpdateCache(ozoneManager, 1);
    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    ((OMQuotaRepairResponse)omClientResponse).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    bucketInfo = omMetadataManager.getBucketTable().get(
        bucketKey);
    assertEquals(-1, bucketInfo.getQuotaInBytes());
    OmVolumeArgs volArgsVerify = omMetadataManager.getVolumeTable()
        .get(omMetadataManager.getVolumeKey(volumeName));
    assertEquals(-1, volArgsVerify.getQuotaInBytes());
    assertEquals(-1, volArgsVerify.getQuotaInNamespace());
  }

  @Test
  public void testQuotaRepairDeletedTableSnapshotQuota() throws Exception {
    OzoneManagerProtocolProtos.OMResponse respMock = mock(OzoneManagerProtocolProtos.OMResponse.class);
    when(respMock.getSuccess()).thenReturn(true);
    OzoneManagerRatisServer ratisServerMock = mock(OzoneManagerRatisServer.class);
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref = new AtomicReference<>();
    doAnswer(invocation -> {
      ref.set(invocation.getArgument(0, OzoneManagerProtocolProtos.OMRequest.class));
      return respMock;
    }).when(ratisServerMock).submitRequest(any(), any(), anyLong());
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServerMock);

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.OBJECT_STORE);

    String keyName = "/user/snapKey";
    OMRequestTestUtils.addKeyToTableAndCache(volumeName, bucketName,
        keyName, -1, RatisReplicationConfig.getInstance(THREE), 1L, omMetadataManager);

    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    long bucketObjId = bucketInfo.getObjectID();

    OMRequestTestUtils.deleteKey(ozoneKey, bucketObjId, omMetadataManager, 2L);

    RepeatedOmKeyInfo deletedEntry = omMetadataManager.getDeletedTable().get(ozoneKey);
    long expectedSnapNs = deletedEntry.getOmKeyInfoList().size();

    bucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    OmBucketInfo corruptedSnapshot = bucketInfo.toBuilder()
        .setSnapshotUsedBytes(7L)
        .setSnapshotUsedNamespace(99L)
        .build();
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    omMetadataManager.getBucketTable().put(bucketKey, corruptedSnapshot);
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(bucketKey), CacheValue.get(3L, corruptedSnapshot));

    QuotaRepairTask quotaRepairTask = new QuotaRepairTask(ozoneManager);
    CompletableFuture<Boolean> repair = quotaRepairTask.repair();
    assertTrue(awaitRepair(repair));

    OMQuotaRepairRequest omQuotaRepairRequest = new OMQuotaRepairRequest(ref.get());
    OMClientResponse omClientResponse = omQuotaRepairRequest.validateAndUpdateCache(ozoneManager, 1);
    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    ((OMQuotaRepairResponse) omClientResponse).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    OmBucketInfo repaired = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(0, repaired.getUsedBytes());
    assertEquals(0, repaired.getUsedNamespace());
    assertEquals(expectedSnapNs, repaired.getSnapshotUsedNamespace());
    assertTrue(repaired.getSnapshotUsedBytes() > 0,
        "Snapshot pending-delete bytes must be recomputed from deletedTable");
  }

  @Test
  public void testQuotaRepairSnapshotDbDeletedTableQuota() throws Exception {
    OzoneManagerProtocolProtos.OMResponse respMock = mock(OzoneManagerProtocolProtos.OMResponse.class);
    when(respMock.getSuccess()).thenReturn(true);
    OzoneManagerRatisServer ratisServerMock = mock(OzoneManagerRatisServer.class);
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref = new AtomicReference<>();
    doAnswer(invocation -> {
      ref.set(invocation.getArgument(0, OzoneManagerProtocolProtos.OMRequest.class));
      return respMock;
    }).when(ratisServerMock).submitRequest(any(), any(), anyLong());
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServerMock);

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.OBJECT_STORE);

    String keyName = "/user/snapKey";
    OMRequestTestUtils.addKeyToTableAndCache(volumeName, bucketName,
        keyName, -1, RatisReplicationConfig.getInstance(THREE), 1L, omMetadataManager);

    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE).get(ozoneKey);
    long keyBytes = omKeyInfo.getReplicatedSize();

    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    OMRequestTestUtils.deleteKey(ozoneKey, bucketInfo.getObjectID(), omMetadataManager, 2L);

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo afterDelete = bucketInfo.toBuilder()
        .setUsedBytes(0)
        .setUsedNamespace(0)
        .setSnapshotUsedBytes(keyBytes)
        .setSnapshotUsedNamespace(1)
        .build();
    omMetadataManager.getBucketTable().put(bucketKey, afterDelete);

    when(ozoneManager.getDefaultReplicationConfig())
        .thenReturn(RatisReplicationConfig.getInstance(THREE));
    createSnapshot("snap1");

    assertNull(omMetadataManager.getDeletedTable().get(ozoneKey),
        "Deleted key should move out of active deletedTable after snapshot");
    assertEquals(0, omMetadataManager.countRowsInTable(omMetadataManager.getDeletedTable()));

    OmBucketInfo corrupted = afterDelete.toBuilder()
        .setSnapshotUsedBytes(7L)
        .build();
    omMetadataManager.getBucketTable().put(bucketKey, corrupted);
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(bucketKey), CacheValue.get(3L, corrupted));

    QuotaRepairTask quotaRepairTask = new QuotaRepairTask(ozoneManager);
    CompletableFuture<Boolean> repair = quotaRepairTask.repair();
    assertTrue(awaitRepair(repair));

    OMQuotaRepairRequest omQuotaRepairRequest = new OMQuotaRepairRequest(ref.get());
    OMClientResponse omClientResponse = omQuotaRepairRequest.validateAndUpdateCache(ozoneManager, 1);
    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    ((OMQuotaRepairResponse) omClientResponse).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    OmBucketInfo repaired = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(0, repaired.getUsedBytes());
    assertEquals(0, repaired.getUsedNamespace());
    assertEquals(keyBytes, repaired.getSnapshotUsedBytes());
    assertEquals(1, repaired.getSnapshotUsedNamespace());
  }

  @Test
  public void testQuotaRepairVersionedBucketUndercount() throws Exception {
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref = mockRatisSubmit();
    String bucketKey = addVersionedBucket(bucketName);
    ReplicationConfig ratisOne = RatisReplicationConfig.getInstance(ONE);

    // v0: 300 bytes, RATIS ONE so replicated size == data size
    commitVersion(ratisOne, 0L, 1L, null, 300L);
    OmBucketInfo afterV0 = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(300, afterV0.getUsedBytes());
    assertEquals(1, afterV0.getUsedNamespace());

    // v1: 600 bytes overwrite, old version stays on disk
    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
    OmKeyInfo committedV0 = omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE).get(ozoneKey);
    commitVersion(ratisOne, 1L, 2L, committedV0, 600L);

    OmBucketInfo live = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(900, live.getUsedBytes(), "live accounting keeps both versions");

    // key table entry keeps only the latest version's dataSize, but holds both location groups
    OmKeyInfo committedV1 = omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE).get(ozoneKey);
    assertEquals(600, committedV1.getDataSize());
    assertEquals(2, committedV1.getKeyLocationVersions().size());

    OmBucketInfo repaired = runRepair(ref, bucketKey);
    assertEquals(900, repaired.getUsedBytes(), "repair must not collapse multi-version keys");
    // usedNamespace counts keys, not versions: the delete path refunds one unit per key
    assertEquals(1, repaired.getUsedNamespace(), "a multi-version key uses one namespace unit");
  }

  /**
   * EC rounds a partial stripe up to a full parity chunk once per conversion, so converting a version as a whole
   * and converting its blocks one by one give different totals. {@link OMKeyCommitRequest} converts each commit as
   * a whole, so the recount has to do the same, otherwise repair would move a counter that live accounting had right.
   */
  @Test
  public void testQuotaRepairVersionedBucketWithECKey() throws Exception {
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref = mockRatisSubmit();
    String bucketKey = addVersionedBucket(bucketName);
    // data stripe is 3 * 1024 bytes, so both versions below carry a partial stripe
    ReplicationConfig ec = new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 1024);

    // v0: 2048 bytes of data over two blocks, charged as 2048 + 1024 * 2 parity
    commitVersion(ec, 0L, 1L, null, 1024L, 1024L);

    // v1: 3072 bytes of data in one block, charged as 3072 + 1024 * 2 parity
    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
    OmKeyInfo committedV0 = omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE).get(ozoneKey);
    commitVersion(ec, 1L, 2L, committedV0, 3072L);

    OmBucketInfo live = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(9216, live.getUsedBytes());

    // converting v0's two blocks separately would round the partial stripe up twice and report 11264 instead
    OmBucketInfo repaired = runRepair(ref, bucketKey);
    assertEquals(9216, repaired.getUsedBytes(),
        "repair must reproduce what commit charged, not re-derive it per block");
    assertEquals(1, repaired.getUsedNamespace());
  }

  /**
   * Keys written before HDDS-5472 carry copies of the earlier groups' blocks and were never migrated, so summing
   * whole groups would charge v0 twice here.
   */
  @Test
  public void testQuotaRepairLegacyKeyWithCopiedVersions() throws Exception {
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref = mockRatisSubmit();
    String bucketKey = addVersionedBucket(bucketName);

    List<OmKeyLocationInfo> v0 = blockLocations(0L, 300L);
    Map<Long, List<OmKeyLocationInfo>> legacyGroup = new HashMap<>();
    legacyGroup.put(0L, new ArrayList<>(v0));
    legacyGroup.put(1L, blockLocations(1L, 600L));

    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
            RatisReplicationConfig.getInstance(ONE))
        .setOmKeyLocationInfos(Arrays.asList(
            new OmKeyLocationInfoGroup(0L, v0),
            new OmKeyLocationInfoGroup(1L, legacyGroup)))
        .setDataSize(600)
        .setUpdateID(1L)
        .build();
    OMRequestTestUtils.addKeyToTable(false, false, omKeyInfo, clientID, 1L, omMetadataManager);

    zeroOutBucketUsedBytes(volumeName, bucketName, 1L);

    OmBucketInfo repaired = runRepair(ref, bucketKey);
    assertEquals(900, repaired.getUsedBytes(), "the copy of v0 in group 1 must not be charged again");
    assertEquals(1, repaired.getUsedNamespace());
  }

  /**
   * The FSO key table is keyed by volume and bucket id, so the recount looks the bucket up in a different map
   * than the OBS key table does. Build the multi-version entry directly and check the versioning aware branch
   * is reached for FSO too.
   */
  @Test
  public void testQuotaRepairVersionedFsoBucket() throws Exception {
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref = mockRatisSubmit();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
            .setIsVersionEnabled(true));
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);

    String fileName = "file0";
    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, fileName,
            RatisReplicationConfig.getInstance(ONE))
        .setObjectID(bucketId + 1)
        .setParentObjectID(bucketId)
        .setUpdateID(1L)
        .build();
    omKeyInfo.setKeyName(fileName);
    omKeyInfo.appendNewBlocks(blockLocations(0L, 300L), false);
    omKeyInfo.addNewVersion(blockLocations(1L, 600L), false, true);
    omKeyInfo.setDataSize(600);
    OMRequestTestUtils.addFileToKeyTable(false, false, fileName, omKeyInfo, -1, 1L, omMetadataManager);

    // start from zeroed counters so the assertion below is the recount itself, not a delta
    zeroOutBucketUsedBytes(volumeName, bucketName, 1L);
    OmBucketInfo zeroed = omMetadataManager.getBucketTable().get(bucketKey);
    assertEquals(0, zeroed.getUsedBytes());

    OmBucketInfo repaired = runRepair(ref, bucketKey);
    assertEquals(900, repaired.getUsedBytes(), "FSO recount must reach the versioning aware branch");
    assertEquals(1, repaired.getUsedNamespace());
  }

  private static List<OmKeyLocationInfo> blockLocations(long versionNum, long... blockLengths) {
    List<OmKeyLocationInfo> locations = new ArrayList<>(blockLengths.length);
    for (int i = 0; i < blockLengths.length; i++) {
      locations.add(new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(CONTAINER_ID + versionNum, LOCAL_ID + versionNum * 100 + i))
          .setLength(blockLengths[i])
          .setOffset(0)
          .setCreateVersion(versionNum)
          .build());
    }
    return locations;
  }

  private AtomicReference<OzoneManagerProtocolProtos.OMRequest> mockRatisSubmit() throws Exception {
    OzoneManagerProtocolProtos.OMResponse respMock = mock(OzoneManagerProtocolProtos.OMResponse.class);
    when(respMock.getSuccess()).thenReturn(true);
    OzoneManagerRatisServer ratisServerMock = mock(OzoneManagerRatisServer.class);
    AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref = new AtomicReference<>();
    doAnswer(invocation -> {
      ref.set(invocation.getArgument(0, OzoneManagerProtocolProtos.OMRequest.class));
      return respMock;
    }).when(ratisServerMock).submitRequest(any(), any(), anyLong());
    when(ozoneManager.getOmRatisServer()).thenReturn(ratisServerMock);
    return ref;
  }

  private String addVersionedBucket(String bucket) throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucket)
            .setBucketLayout(BucketLayout.OBJECT_STORE)
            .setIsVersionEnabled(true));
    return omMetadataManager.getBucketKey(volumeName, bucket);
  }

  private OmBucketInfo runRepair(AtomicReference<OzoneManagerProtocolProtos.OMRequest> ref,
      String bucketKey) throws Exception {
    QuotaRepairTask quotaRepairTask = new QuotaRepairTask(ozoneManager);
    assertTrue(awaitRepair(quotaRepairTask.repair()));

    OMQuotaRepairRequest omQuotaRepairRequest = new OMQuotaRepairRequest(ref.get());
    OMClientResponse omClientResponse = omQuotaRepairRequest.validateAndUpdateCache(ozoneManager, 3);
    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    ((OMQuotaRepairResponse) omClientResponse).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    return omMetadataManager.getBucketTable().get(bucketKey);
  }

  /**
   * Drive one real {@link OMKeyCommitRequest} on the versioning-enabled bucket. When {@code previous} is set,
   * the open key is built the way {@code OMKeyRequest.prepareFileInfo} builds it for an overwrite: old location
   * groups kept, dataSize accumulated.
   */
  private void commitVersion(ReplicationConfig repConfig, long versionNum, long trxnLogIndex,
      OmKeyInfo previous, long... blockLengths) throws Exception {
    long writerClientId = clientID + versionNum;
    long size = 0;
    for (long blockLength : blockLengths) {
      size += blockLength;
    }
    List<OmKeyLocationInfo> locations = blockLocations(versionNum, blockLengths);

    if (previous == null) {
      OMRequestTestUtils.addKeyToTable(true, false, volumeName, bucketName, keyName, writerClientId,
          repConfig, trxnLogIndex, omMetadataManager, locations, versionNum);
    } else {
      OmKeyInfo openKeyInfo = previous.copyObject();
      openKeyInfo.addNewVersion(locations, false, true);
      openKeyInfo.setDataSize(previous.getDataSize() + size);
      OMRequestTestUtils.addKeyToTable(true, false, openKeyInfo, writerClientId, trxnLogIndex, omMetadataManager);
    }

    OzoneManagerProtocolProtos.KeyArgs.Builder keyArgs = OzoneManagerProtocolProtos.KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(size)
        .setType(repConfig.getReplicationType())
        .addAllKeyLocations(locations.stream()
            .map(l -> l.getProtobuf(false, ClientVersion.CURRENT_VERSION))
            .collect(Collectors.toList()));
    if (repConfig.getReplicationType() == HddsProtos.ReplicationType.EC) {
      keyArgs.setEcReplicationConfig(((ECReplicationConfig) repConfig).toProto());
    } else {
      keyArgs.setFactor(ReplicationConfig.getLegacyFactor(repConfig));
    }
    OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.CommitKey)
        .setCommitKeyRequest(OzoneManagerProtocolProtos.CommitKeyRequest.newBuilder()
            .setKeyArgs(keyArgs.build())
            .setClientID(writerClientId)
            .build())
        .setClientId(UUID.randomUUID().toString())
        .build();

    OMKeyCommitRequest commitRequest = new OMKeyCommitRequest(omRequest, BucketLayout.OBJECT_STORE);
    OMClientResponse response = new OMKeyCommitRequest(commitRequest.preExecute(ozoneManager),
        BucketLayout.OBJECT_STORE).validateAndUpdateCache(ozoneManager, trxnLogIndex);
    assertEquals(OzoneManagerProtocolProtos.Status.OK, response.getOMResponse().getStatus());

    BatchOperation batch = omMetadataManager.getStore().initBatchOperation();
    response.checkAndUpdateDB(omMetadataManager, batch);
    omMetadataManager.getStore().commitBatchOperation(batch);
  }

  private void zeroOutBucketUsedBytes(String volumeName, String bucketName,
                                      long trxnLogIndex)
      throws IOException {
    String dbKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(dbKey);
    bucketInfo.decrUsedBytes(bucketInfo.getUsedBytes(), false);
    omMetadataManager.getBucketTable()
        .addCacheEntry(new CacheKey<>(dbKey),
            CacheValue.get(trxnLogIndex, bucketInfo));
    omMetadataManager.getBucketTable().put(dbKey, bucketInfo);
  }
}
