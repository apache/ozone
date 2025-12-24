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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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
  public void testBatchOperationWithDeleteRange() throws RocksDatabaseException, CodecException {
    final List<Pair<Pair<String, String>, Pair<Integer, Boolean>>> deleteKeyRangePairs = new ArrayList<>();
    final List<Pair<Pair<String, String>, Pair<Integer, Boolean>>> putKeys = new ArrayList<>();
    final List<Pair<String, Pair<Integer, Boolean>>> deleteKeys = new ArrayList<>();
    AtomicInteger cnt = new AtomicInteger(0);
    try (MockedConstruction<ManagedWriteBatch> mockedConstruction = Mockito.mockConstruction(ManagedWriteBatch.class,
        (writeBatch, context) -> {
          doAnswer(i -> {
            deleteKeyRangePairs.add(Pair.of(Pair.of(bytes2String((byte[]) i.getArgument(1)),
                bytes2String((byte[]) i.getArgument(2))), Pair.of(cnt.getAndIncrement(), false)));
            return null;
          }).when(writeBatch).deleteRange(Mockito.any(ColumnFamilyHandle.class), Mockito.any(byte[].class),
              Mockito.any(byte[].class));
          doAnswer(i -> {
            putKeys.add(Pair.of(Pair.of(bytes2String((byte[]) i.getArgument(1)),
                    bytes2String((byte[]) i.getArgument(2))),
                Pair.of(cnt.getAndIncrement(), false)));
            return null;
          }).when(writeBatch)
              .put(Mockito.any(ColumnFamilyHandle.class), Mockito.any(byte[].class), Mockito.any(byte[].class));

          doAnswer(i -> {
            ByteBuffer key = i.getArgument(1);
            ByteBuffer value = i.getArgument(2);
            putKeys.add(Pair.of(Pair.of(bytes2String(key), bytes2String(value)),
                Pair.of(cnt.getAndIncrement(), true)));
            return null;
          }).when(writeBatch)
              .put(Mockito.any(ColumnFamilyHandle.class), Mockito.any(ByteBuffer.class), Mockito.any(ByteBuffer.class));

          doAnswer(i -> {
            deleteKeys.add(Pair.of(bytes2String((byte[]) i.getArgument(1)), Pair.of(cnt.getAndIncrement(), false)));
            return null;
          }).when(writeBatch).delete(Mockito.any(ColumnFamilyHandle.class), Mockito.any(byte[].class));

          doAnswer(i -> {
            ByteBuffer key = i.getArgument(1);
            deleteKeys.add(Pair.of(bytes2String(key), Pair.of(cnt.getAndIncrement(), true)));
            return null;
          }).when(writeBatch).delete(Mockito.any(ColumnFamilyHandle.class), Mockito.any(ByteBuffer.class));

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
            .put(columnFamilyHandle, (ByteBuffer) i.getArgument(1), (ByteBuffer) i.getArgument(2));
        return null;
      }).when(columnFamily).batchPut(any(ManagedWriteBatch.class), any(ByteBuffer.class), any(ByteBuffer.class));

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

      doAnswer((i) -> {
        ((ManagedWriteBatch)i.getArgument(0))
            .delete(columnFamilyHandle, (ByteBuffer) i.getArgument(1));
        return null;
      }).when(columnFamily).batchDelete(any(ManagedWriteBatch.class), any(ByteBuffer.class));

      when(columnFamily.getHandle()).thenReturn(columnFamilyHandle);
      when(columnFamily.getName()).thenReturn("test");
      Codec<String> codec = StringCodec.get();
      batchOperation.put(columnFamily, codec.toDirectCodecBuffer("key01"), codec.toDirectCodecBuffer("value01"));
      batchOperation.put(columnFamily, codec.toPersistedFormat("key02"), codec.toPersistedFormat("value02"));
      batchOperation.put(columnFamily, codec.toDirectCodecBuffer("key03"), codec.toDirectCodecBuffer("value03"));
      batchOperation.put(columnFamily, codec.toPersistedFormat("key03"), codec.toPersistedFormat("value04"));
      batchOperation.delete(columnFamily, codec.toDirectCodecBuffer("key05"));
      batchOperation.delete(columnFamily, codec.toPersistedFormat("key10"));
      batchOperation.deleteRange(columnFamily, codec.toPersistedFormat("key01"), codec.toPersistedFormat("key02"));
      batchOperation.deleteRange(columnFamily, codec.toPersistedFormat("key02"), codec.toPersistedFormat("key03"));
      batchOperation.put(columnFamily, codec.toDirectCodecBuffer("key04"), codec.toDirectCodecBuffer("value04"));
      batchOperation.put(columnFamily, codec.toPersistedFormat("key06"), codec.toPersistedFormat("value05"));
      batchOperation.deleteRange(columnFamily, codec.toPersistedFormat("key06"), codec.toPersistedFormat("key12"));
      batchOperation.deleteRange(columnFamily, codec.toPersistedFormat("key09"), codec.toPersistedFormat("key10"));
      RocksDatabase db = Mockito.mock(RocksDatabase.class);
      doNothing().when(db).batchWrite(any());
      batchOperation.commit(db);
      assertEquals(deleteKeys, ImmutableList.of(Pair.of("key05", Pair.of(1, true)),
          Pair.of("key10", Pair.of(2, false))));
      assertEquals(deleteKeyRangePairs, asList(Pair.of(Pair.of("key01", "key02"), Pair.of(3, false)),
          Pair.of(Pair.of("key02", "key03"), Pair.of(4, false)),
          Pair.of(Pair.of("key06", "key12"), Pair.of(6, false)),
          Pair.of(Pair.of("key09", "key10"), Pair.of(7, false))));
      assertEquals(putKeys, Arrays.asList(Pair.of(Pair.of("key03", "value04"), Pair.of(0, false)),
          Pair.of(Pair.of("key04", "value04"), Pair.of(5, true))));
    }
  }
}
