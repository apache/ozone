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

import static org.apache.hadoop.hdds.StringUtils.string2Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.TrackingUtilManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.TrackingUtilManagedWriteBatch.OpType;
import org.apache.hadoop.hdds.utils.db.managed.TrackingUtilManagedWriteBatch.Operation;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

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

  static {
    ManagedRocksObjectUtils.loadRocksDBLibrary();
  }

  private static Operation getOperation(String key, String value, OpType opType) {
    return new Operation(string2Bytes(key), value == null ? null : string2Bytes(value), opType);
  }

  @Test
  public void testBatchOperationWithDeleteRange() throws RocksDatabaseException, CodecException, RocksDBException {
    try (TrackingUtilManagedWriteBatch writeBatch = new TrackingUtilManagedWriteBatch();
        RDBBatchOperation batchOperation = RDBBatchOperation.newAtomicOperation(writeBatch)) {
      ColumnFamilyHandle columnFamilyHandle = Mockito.mock(ColumnFamilyHandle.class);
      RocksDatabase.ColumnFamily columnFamily = Mockito.mock(RocksDatabase.ColumnFamily.class);
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
            .delete(columnFamilyHandle, (ByteBuffer) i.getArgument(1));
        return null;
      }).when(columnFamily).batchDelete(any(ManagedWriteBatch.class), any(ByteBuffer.class));

      when(columnFamily.getHandle()).thenReturn(columnFamilyHandle);
      when(columnFamilyHandle.getName()).thenReturn(string2Bytes("test"));
      when(columnFamily.getName()).thenReturn("test");
      Codec<String> codec = StringCodec.get();
      // OP1
      batchOperation.put(columnFamily, codec.toDirectCodecBuffer("key01"), codec.toDirectCodecBuffer("value01"));
      // OP2
      batchOperation.put(columnFamily, codec.toPersistedFormat("key02"), codec.toPersistedFormat("value02"));
      // OP3
      batchOperation.put(columnFamily, codec.toDirectCodecBuffer("key03"), codec.toDirectCodecBuffer("value03"));
      // OP4
      batchOperation.put(columnFamily, codec.toPersistedFormat("key03"), codec.toPersistedFormat("value04"));
      // OP5
      batchOperation.delete(columnFamily, codec.toDirectCodecBuffer("key05"));
      // OP6 : This delete operation should get skipped because of OP11
      batchOperation.delete(columnFamily, codec.toPersistedFormat("key10"));
      // OP7
      batchOperation.deleteRange(columnFamily, codec.toPersistedFormat("key01"), codec.toPersistedFormat("key02"));
      // OP8
      batchOperation.deleteRange(columnFamily, codec.toPersistedFormat("key02"), codec.toPersistedFormat("key03"));
      // OP9
      batchOperation.put(columnFamily, codec.toDirectCodecBuffer("key04"), codec.toDirectCodecBuffer("value04"));
      // OP10
      batchOperation.put(columnFamily, codec.toPersistedFormat("key06"), codec.toPersistedFormat("value05"));
      // OP11
      batchOperation.deleteRange(columnFamily, codec.toPersistedFormat("key06"), codec.toPersistedFormat("key12"));
      // OP12
      batchOperation.deleteRange(columnFamily, codec.toPersistedFormat("key09"), codec.toPersistedFormat("key10"));

      RocksDatabase db = Mockito.mock(RocksDatabase.class);
      doNothing().when(db).batchWrite(any());
      batchOperation.commit(db);
      List<Operation> expectedOps = ImmutableList.of(
          getOperation("key03", "value04", OpType.PUT_DIRECT),
          getOperation("key05", null, OpType.DELETE_DIRECT),
          getOperation("key01", "key02", OpType.DELETE_RANGE_INDIRECT),
          getOperation("key02", "key03", OpType.DELETE_RANGE_INDIRECT),
          getOperation("key04", "value04", OpType.PUT_DIRECT),
          getOperation("key06", "key12", OpType.DELETE_RANGE_INDIRECT),
          getOperation("key09", "key10", OpType.DELETE_RANGE_INDIRECT));
      assertEquals(ImmutableMap.of("test", expectedOps), writeBatch.getOperations());
    }
  }
}
