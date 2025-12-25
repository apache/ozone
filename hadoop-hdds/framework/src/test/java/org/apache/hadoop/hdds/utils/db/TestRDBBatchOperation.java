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

package org.apache.hadoop.hdds.utils.db;

import static java.util.Arrays.asList;
import static org.apache.hadoop.hdds.StringUtils.bytes2String;
import static org.apache.hadoop.hdds.StringUtils.string2Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;

/**
 * Test class for verifying batch operations with delete ranges using the
 * RDBBatchOperation and MockedConstruction of ManagedWriteBatch.
 *
 * This test class includes:
 * - Mocking and tracking of operations including put, delete, and delete range
 *   within a batch operation.
 * - Validation of committed operations using assertions on collected data.
 * - Ensures that the batch operation interacts correctly with the
 *   RocksDatabase and ColumnFamilyHandle components.
 *
 * The test method includes:
 * 1. Setup of mocked ColumnFamilyHandle and RocksDatabase.ColumnFamily.
 * 2. Mocking of methods to track operations performed on*/
public class TestRDBBatchOperation {
  @Test
  public void testBatchOperationWithDeleteRange() throws RocksDatabaseException {
    final List<Pair<Pair<String, String>, Integer>> deleteKeyRangePairs = new ArrayList<>();
    final List<Pair<Pair<String, String>, Integer>> putKeys = new ArrayList<>();
    final List<Pair<String, Integer>> deleteKeys = new ArrayList<>();
    AtomicInteger cnt = new AtomicInteger(0);
    try (MockedConstruction<ManagedWriteBatch> mockedConstruction = Mockito.mockConstruction(ManagedWriteBatch.class,
        (writeBatch, context) -> {
          doAnswer(i -> {
            deleteKeyRangePairs.add(Pair.of(Pair.of(bytes2String((byte[]) i.getArgument(1)),
                bytes2String((byte[]) i.getArgument(2))), cnt.getAndIncrement()));
            return null;
          }).when(writeBatch).deleteRange(Mockito.any(ColumnFamilyHandle.class), Mockito.any(byte[].class),
              Mockito.any(byte[].class));
          doAnswer(i -> {
            putKeys.add(Pair.of(Pair.of(bytes2String((byte[]) i.getArgument(1)),
                    bytes2String((byte[]) i.getArgument(2))),
                cnt.getAndIncrement()));
            return null;
          }).when(writeBatch)
              .put(Mockito.any(ColumnFamilyHandle.class), Mockito.any(byte[].class), Mockito.any(byte[].class));
          doAnswer(i -> {
            deleteKeys.add(Pair.of(bytes2String((byte[]) i.getArgument(1)), cnt.getAndIncrement()));
            return null;
          }).when(writeBatch).delete(Mockito.any(ColumnFamilyHandle.class), Mockito.any(byte[].class));

        });
         RDBBatchOperation batchOperation = RDBBatchOperation.newAtomicOperation()) {
      ColumnFamilyHandle columnFamilyHandle = Mockito.mock(ColumnFamilyHandle.class);
      RocksDatabase.ColumnFamily columnFamily = Mockito.mock(RocksDatabase.ColumnFamily.class);
      doAnswer((i) -> {
        ((ManagedWriteBatch)i.getArgument(0))
            .put(columnFamilyHandle, (byte[]) i.getArgument(1), (byte[]) i.getArgument(2));
        return null;
      }).when(columnFamily).batchPut(any(ManagedWriteBatch.class), any(byte[].class), any(byte[].class));

      doAnswer((i) -> {
        ((ManagedWriteBatch)i.getArgument(0))
            .deleteRange(columnFamilyHandle, (byte[]) i.getArgument(1), (byte[]) i.getArgument(2));
        return null;
      }).when(columnFamily).batchDeleteRange(any(ManagedWriteBatch.class), any(byte[].class), any(byte[].class));

      doAnswer((i) -> {
        ((ManagedWriteBatch)i.getArgument(0))
            .delete(columnFamilyHandle, (byte[]) i.getArgument(1));
        return null;
      }).when(columnFamily).batchDelete(any(ManagedWriteBatch.class), any(byte[].class));

      when(columnFamily.getHandle()).thenReturn(columnFamilyHandle);
      when(columnFamily.getName()).thenReturn("test");
      batchOperation.put(columnFamily, string2Bytes("key01"), string2Bytes("value01"));
      batchOperation.put(columnFamily, string2Bytes("key02"), string2Bytes("value02"));
      batchOperation.put(columnFamily, string2Bytes("key03"), string2Bytes("value03"));
      batchOperation.put(columnFamily, string2Bytes("key03"), string2Bytes("value04"));
      batchOperation.delete(columnFamily, string2Bytes("key05"));
      batchOperation.deleteRange(columnFamily, string2Bytes("key01"), string2Bytes("key02"));
      batchOperation.deleteRange(columnFamily, string2Bytes("key02"), string2Bytes("key03"));
      batchOperation.put(columnFamily, string2Bytes("key04"), string2Bytes("value04"));
      batchOperation.put(columnFamily, string2Bytes("key06"), string2Bytes("value05"));
      batchOperation.deleteRange(columnFamily, string2Bytes("key06"), string2Bytes("key12"));
      batchOperation.deleteRange(columnFamily, string2Bytes("key09"), string2Bytes("key10"));
      RocksDatabase db = Mockito.mock(RocksDatabase.class);
      doNothing().when(db).batchWrite(any());
      batchOperation.commit(db);
      assertEquals(deleteKeys, Collections.singletonList(Pair.of("key05", 1)));
      assertEquals(deleteKeyRangePairs, asList(Pair.of(Pair.of("key01", "key02"), 2),
          Pair.of(Pair.of("key02", "key03"), 3),
          Pair.of(Pair.of("key06", "key12"), 5),
          Pair.of(Pair.of("key09", "key10"), 6)));
      assertEquals(putKeys, Arrays.asList(Pair.of(Pair.of("key03", "value04"), 0),
          Pair.of(Pair.of("key04", "value04"), 4)));
    }
  }
}
