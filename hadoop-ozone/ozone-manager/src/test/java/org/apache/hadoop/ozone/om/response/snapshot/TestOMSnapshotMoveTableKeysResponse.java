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

package org.apache.hadoop.ozone.om.response.snapshot;

import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_DB_CONTENT_LOCK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.TestSnapshotRequestAndResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test class to test OMSnapshotMoveTableKeysResponse.
 */
public class TestOMSnapshotMoveTableKeysResponse extends TestSnapshotRequestAndResponse {

  private String snapshotName1;
  private String snapshotName2;
  private SnapshotInfo snapshotInfo1;
  private SnapshotInfo snapshotInfo2;

  @BeforeEach
  public void setup() throws Exception {
    snapshotName1 = UUID.randomUUID().toString();
    snapshotName2 = UUID.randomUUID().toString();
  }

  public TestOMSnapshotMoveTableKeysResponse() {
    super(true);
  }

  private void createSnapshots(boolean createSecondSnapshot, long bucketId) throws Exception {
    addDataToTable(getOmMetadataManager().getSnapshotRenamedTable(), getRenameKeys(getVolumeName(), getBucketName(), 0,
        10,  snapshotName1));
    addDataToTable(getOmMetadataManager().getDeletedTable(), getDeletedKeys(getVolumeName(), getBucketName(), 0,
        10, 10, 0).stream()
        .map(pair -> Pair.of(pair.getKey(), new RepeatedOmKeyInfo(pair.getRight(), bucketId)))
        .collect(Collectors.toList()));
    addDataToTable(getOmMetadataManager().getDeletedDirTable(),
        getDeletedDirKeys(getVolumeName(), getBucketName(), 0, 10, 1).stream()
        .map(pair -> Pair.of(pair.getKey(), pair.getRight().get(0))).collect(Collectors.toList()));
    createSnapshotCheckpoint(getVolumeName(), getBucketName(), snapshotName1);
    snapshotInfo1 = SnapshotUtils.getSnapshotInfo(getOzoneManager(), getVolumeName(), getBucketName(), snapshotName1);
    addDataToTable(getOmMetadataManager().getSnapshotRenamedTable(), getRenameKeys(getVolumeName(), getBucketName(), 5,
        15,  snapshotName2));
    addDataToTable(getOmMetadataManager().getDeletedTable(), getDeletedKeys(getVolumeName(), getBucketName(), 5,
        8, 10, 10).stream()
        .map(pair -> Pair.of(pair.getKey(), new RepeatedOmKeyInfo(pair.getRight(), bucketId)))
        .collect(Collectors.toList()));
    addDataToTable(getOmMetadataManager().getDeletedTable(), getDeletedKeys(getVolumeName(), getBucketName(), 8,
        15, 10, 0).stream()
        .map(pair -> Pair.of(pair.getKey(), new RepeatedOmKeyInfo(pair.getRight(), bucketId)))
        .collect(Collectors.toList()));
    addDataToTable(getOmMetadataManager().getDeletedDirTable(),
        getDeletedDirKeys(getVolumeName(), getBucketName(), 5, 15, 1).stream()
        .map(pair -> Pair.of(pair.getKey(), pair.getRight().get(0))).collect(Collectors.toList()));
    if (createSecondSnapshot) {
      createSnapshotCheckpoint(getVolumeName(), getBucketName(), snapshotName2);
      snapshotInfo2 = SnapshotUtils.getSnapshotInfo(getOzoneManager(), getVolumeName(), getBucketName(), snapshotName2);
    }
  }

  private <V> void addDataToTable(Table<String, V> table, List<Pair<String, V>> vals) throws IOException {
    for (Pair<String, V> pair : vals) {
      table.put(pair.getKey(), pair.getValue());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMoveTableKeysToNextSnapshot(boolean nextSnapshotExists) throws Exception {
    IOzoneManagerLock lock = spy(getOmMetadataManager().getLock());
    when(getOmMetadataManager().getLock()).thenReturn(lock);
    OmBucketInfo omBucketInfo = OMKeyRequest.getBucketInfo(getOmMetadataManager(), getVolumeName(), getBucketName());
    createSnapshots(nextSnapshotExists, omBucketInfo.getObjectID());
    try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshot1 = getOmSnapshotManager().getSnapshot(
        getVolumeName(), getBucketName(), snapshotName1);
         UncheckedAutoCloseableSupplier<OmSnapshot> snapshot2 = nextSnapshotExists ? getOmSnapshotManager().getSnapshot(
             getVolumeName(), getBucketName(), snapshotName2) : null) {
      List<List<String>> expectedSnapshotIdLocks =
          Arrays.asList(Collections.singletonList(snapshot1.get().getSnapshotID().toString()),
          nextSnapshotExists ? Collections.singletonList(snapshot2.get().getSnapshotID().toString()) : null);
      List<List<String>> locks = new ArrayList<>();
      doAnswer(i -> {
        for (String[] id : (Collection<String[]>)i.getArgument(1)) {
          locks.add(id == null ? null : Arrays.stream(id).collect(Collectors.toList()));
        }
        return i.callRealMethod();
      }).when(lock).acquireReadLocks(eq(SNAPSHOT_DB_CONTENT_LOCK), anyList());
      OmSnapshot snapshot = snapshot1.get();
      List<OzoneManagerProtocolProtos.SnapshotMoveKeyInfos> deletedTable = new ArrayList<>();
      List<OzoneManagerProtocolProtos.SnapshotMoveKeyInfos> deletedDirTable = new ArrayList<>();
      List<HddsProtos.KeyValue> renamedTable = new ArrayList<>();
      Map<String, String> renameEntries = new HashMap<>();
      snapshot.getMetadataManager().getDeletedTable().iterator()
          .forEachRemaining(entry -> {
            deletedTable.add(OzoneManagerProtocolProtos.SnapshotMoveKeyInfos.newBuilder().setKey(entry.getKey())
                .addAllKeyInfos(entry.getValue().getOmKeyInfoList().stream().map(omKeyInfo -> omKeyInfo.getProtobuf(
                    ClientVersion.CURRENT_VERSION)).collect(Collectors.toList())).build());
          });

      snapshot.getMetadataManager().getDeletedDirTable().iterator()
          .forEachRemaining(entry -> {
            deletedDirTable.add(OzoneManagerProtocolProtos.SnapshotMoveKeyInfos.newBuilder().setKey(entry.getKey())
                .addKeyInfos(entry.getValue().getProtobuf(ClientVersion.CURRENT_VERSION)).build());
          });
      snapshot.getMetadataManager().getSnapshotRenamedTable().iterator().forEachRemaining(entry -> {
        renamedTable.add(HddsProtos.KeyValue.newBuilder().setKey(entry.getKey()).setValue(entry.getValue()).build());
        renameEntries.put(entry.getKey(), entry.getValue());
      });
      OMSnapshotMoveTableKeysResponse response = new OMSnapshotMoveTableKeysResponse(
          OzoneManagerProtocolProtos.OMResponse.newBuilder().setStatus(OzoneManagerProtocolProtos.Status.OK)
              .setCmdType(OzoneManagerProtocolProtos.Type.SnapshotMoveTableKeys).build(),
          snapshotInfo1, nextSnapshotExists ? snapshotInfo2 : null, omBucketInfo.getObjectID(), deletedTable,
          deletedDirTable, renamedTable);
      CompletableFuture<Void> future = new CompletableFuture<>();
      CompletableFuture.runAsync(() -> {
        try (BatchOperation batchOperation = getOmMetadataManager().getStore().initBatchOperation()) {
          response.addToDBBatch(getOmMetadataManager(), batchOperation);
          getOmMetadataManager().getStore().commitBatchOperation(batchOperation);
        } catch (IOException e) {
          future.completeExceptionally(e);
          return;
        }
        future.complete(null);
      });
      future.get();
      assertEquals(expectedSnapshotIdLocks, locks);
      Assertions.assertTrue(snapshot.getMetadataManager().getDeletedTable().isEmpty());
      Assertions.assertTrue(snapshot.getMetadataManager().getDeletedDirTable().isEmpty());
      Assertions.assertTrue(snapshot.getMetadataManager().getSnapshotRenamedTable().isEmpty());
      OMMetadataManager nextMetadataManager =
          nextSnapshotExists ? snapshot2.get().getMetadataManager() : getOmMetadataManager();
      AtomicInteger count = new AtomicInteger();
      nextMetadataManager.getDeletedTable().iterator().forEachRemaining(entry -> {
        count.getAndIncrement();
        int maxCount = count.get() >= 6 && count.get() <= 8 ? 20 : 10;
        assertEquals(maxCount, entry.getValue().getOmKeyInfoList().size());
        List<Long> versions = entry.getValue().getOmKeyInfoList().stream().map(OmKeyInfo::getKeyLocationVersions)
            .map(omKeyInfo -> omKeyInfo.get(0).getVersion()).collect(Collectors.toList());
        List<Long> expectedVersions = new ArrayList<>();
        if (maxCount == 20) {
          expectedVersions.addAll(LongStream.range(10, 20).boxed().collect(Collectors.toList()));
        }
        expectedVersions.addAll(LongStream.range(0, 10).boxed().collect(Collectors.toList()));
        assertEquals(expectedVersions, versions);
      });
      assertEquals(15, count.get());
      count.set(0);

      nextMetadataManager.getDeletedDirTable().iterator().forEachRemaining(entry -> count.getAndIncrement());
      assertEquals(15, count.get());
      count.set(0);
      nextMetadataManager.getSnapshotRenamedTable().iterator().forEachRemaining(entry -> {
        String expectedValue = renameEntries.getOrDefault(entry.getKey(), entry.getValue());
        assertEquals(expectedValue, entry.getValue());
        count.getAndIncrement();
      });
      assertEquals(15, count.get());
    }

  }
}
