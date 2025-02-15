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
package org.apache.hadoop.ozone.om.response.snapshot;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.TestSnapshotRequestAndResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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

  private void createSnapshots(boolean createSecondSnapshot) throws Exception {
    addDataToTable(getOmMetadataManager().getSnapshotRenamedTable(), getRenameKeys(getVolumeName(), getBucketName(), 0,
        10,  snapshotName1));
    addDataToTable(getOmMetadataManager().getDeletedTable(), getDeletedKeys(getVolumeName(), getBucketName(), 0,
        10, 10, 0).stream()
        .map(pair -> Pair.of(pair.getKey(), new RepeatedOmKeyInfo(pair.getRight())))
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
        .map(pair -> Pair.of(pair.getKey(), new RepeatedOmKeyInfo(pair.getRight())))
        .collect(Collectors.toList()));
    addDataToTable(getOmMetadataManager().getDeletedTable(), getDeletedKeys(getVolumeName(), getBucketName(), 8,
        15, 10, 0).stream()
        .map(pair -> Pair.of(pair.getKey(), new RepeatedOmKeyInfo(pair.getRight())))
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
    createSnapshots(nextSnapshotExists);

    try (ReferenceCounted<OmSnapshot> snapshot1 = getOmSnapshotManager().getSnapshot(getVolumeName(), getBucketName(),
        snapshotName1);
         ReferenceCounted<OmSnapshot> snapshot2 = nextSnapshotExists ? getOmSnapshotManager().getSnapshot(
             getVolumeName(), getBucketName(), snapshotName2) : null) {
      OmSnapshot snapshot = snapshot1.get();
      List<OzoneManagerProtocolProtos.SnapshotMoveKeyInfos> deletedTable = new ArrayList<>();
      List<OzoneManagerProtocolProtos.SnapshotMoveKeyInfos> deletedDirTable = new ArrayList<>();
      List<HddsProtos.KeyValue> renamedTable = new ArrayList<>();
      Map<String, String> renameEntries = new HashMap<>();
      snapshot.getMetadataManager().getDeletedTable().iterator()
          .forEachRemaining(entry -> {
            try {
              deletedTable.add(OzoneManagerProtocolProtos.SnapshotMoveKeyInfos.newBuilder().setKey(entry.getKey())
                  .addAllKeyInfos(entry.getValue().getOmKeyInfoList().stream().map(omKeyInfo -> omKeyInfo.getProtobuf(
                      ClientVersion.CURRENT_VERSION)).collect(Collectors.toList())).build());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });

      snapshot.getMetadataManager().getDeletedDirTable().iterator()
          .forEachRemaining(entry -> {
            try {
              deletedDirTable.add(OzoneManagerProtocolProtos.SnapshotMoveKeyInfos.newBuilder().setKey(entry.getKey())
                  .addKeyInfos(entry.getValue().getProtobuf(ClientVersion.CURRENT_VERSION)).build());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
      snapshot.getMetadataManager().getSnapshotRenamedTable().iterator().forEachRemaining(entry -> {
        try {
          renamedTable.add(HddsProtos.KeyValue.newBuilder().setKey(entry.getKey()).setValue(entry.getValue()).build());
          renameEntries.put(entry.getKey(), entry.getValue());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      OMSnapshotMoveTableKeysResponse response = new OMSnapshotMoveTableKeysResponse(
          OzoneManagerProtocolProtos.OMResponse.newBuilder().setStatus(OzoneManagerProtocolProtos.Status.OK)
              .setCmdType(OzoneManagerProtocolProtos.Type.SnapshotMoveTableKeys).build(),
          snapshotInfo1, nextSnapshotExists ? snapshotInfo2 : null, deletedTable, deletedDirTable, renamedTable);
      try (BatchOperation batchOperation = getOmMetadataManager().getStore().initBatchOperation()) {
        response.addToDBBatch(getOmMetadataManager(), batchOperation);
        getOmMetadataManager().getStore().commitBatchOperation(batchOperation);
      }
      Assertions.assertTrue(snapshot.getMetadataManager().getDeletedTable().isEmpty());
      Assertions.assertTrue(snapshot.getMetadataManager().getDeletedDirTable().isEmpty());
      Assertions.assertTrue(snapshot.getMetadataManager().getSnapshotRenamedTable().isEmpty());
      OMMetadataManager nextMetadataManager =
          nextSnapshotExists ? snapshot2.get().getMetadataManager() : getOmMetadataManager();
      AtomicInteger count = new AtomicInteger();
      nextMetadataManager.getDeletedTable().iterator().forEachRemaining(entry -> {
        count.getAndIncrement();
        try {
          int maxCount = count.get() >= 6 && count.get() <= 8 ? 20 : 10;
          Assertions.assertEquals(maxCount, entry.getValue().getOmKeyInfoList().size());
          List<Long> versions = entry.getValue().getOmKeyInfoList().stream().map(OmKeyInfo::getKeyLocationVersions)
              .map(omKeyInfo -> omKeyInfo.get(0).getVersion()).collect(Collectors.toList());
          List<Long> expectedVersions = new ArrayList<>();
          if (maxCount == 20) {
            expectedVersions.addAll(LongStream.range(10, 20).boxed().collect(Collectors.toList()));
          }
          expectedVersions.addAll(LongStream.range(0, 10).boxed().collect(Collectors.toList()));
          Assertions.assertEquals(expectedVersions, versions);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      Assertions.assertEquals(15, count.get());
      count.set(0);

      nextMetadataManager.getDeletedDirTable().iterator().forEachRemaining(entry -> count.getAndIncrement());
      Assertions.assertEquals(15, count.get());
      count.set(0);
      nextMetadataManager.getSnapshotRenamedTable().iterator().forEachRemaining(entry -> {
        try {
          String expectedValue = renameEntries.getOrDefault(entry.getKey(), entry.getValue());
          Assertions.assertEquals(expectedValue, entry.getValue());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        count.getAndIncrement();
      });
      Assertions.assertEquals(15, count.get());
    }

  }
}
