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

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.TrackingUtilManagedWriteBatchForTesting;
import org.apache.hadoop.hdds.utils.db.managed.TrackingUtilManagedWriteBatchForTesting.OpType;
import org.apache.hadoop.hdds.utils.db.managed.TrackingUtilManagedWriteBatchForTesting.Operation;
import org.apache.ratis.util.function.CheckedConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * The TestRDBBatchOperation class provides test cases to validate the functionality of RDB batch operations
 * in a RocksDB-based backend. It verifies the correct behavior of write operations using batch processing
 * and ensures the integrity of operations like put and delete when performed in batch mode.
 */
public class TestRDBBatchOperation {

  static {
    ManagedRocksObjectUtils.loadRocksDBLibrary();
  }

  @TempDir
  private Path tempDir;

  private static Operation getOperation(String key, String value, OpType opType) {
    return new Operation(string2Bytes(key), value == null ? null : string2Bytes(value), opType);
  }

  @Test
  public void testBatchOperation() throws RocksDatabaseException, CodecException, RocksDBException {
    try (TrackingUtilManagedWriteBatchForTesting writeBatch = new TrackingUtilManagedWriteBatchForTesting();
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
            .delete(columnFamilyHandle, (ByteBuffer) i.getArgument(1));
        return null;
      }).when(columnFamily).batchDelete(any(ManagedWriteBatch.class), any(ByteBuffer.class));

      when(columnFamily.getHandle()).thenReturn(columnFamilyHandle);
      when(columnFamilyHandle.getName()).thenReturn(string2Bytes("test"));
      when(columnFamily.getName()).thenReturn("test");
      Codec<String> codec = StringCodec.get();
      // OP1: This should be skipped in favor of OP9.
      batchOperation.put(columnFamily, codec.toDirectCodecBuffer("key01"), codec.toDirectCodecBuffer("value01"));
      // OP2
      batchOperation.put(columnFamily, codec.toPersistedFormat("key02"), codec.toPersistedFormat("value02"));
      // OP3: This should be skipped in favor of OP4.
      batchOperation.put(columnFamily, codec.toDirectCodecBuffer("key03"), codec.toDirectCodecBuffer("value03"));
      // OP4
      batchOperation.put(columnFamily, codec.toPersistedFormat("key03"), codec.toPersistedFormat("value04"));
      // OP5
      batchOperation.delete(columnFamily, codec.toDirectCodecBuffer("key05"));
      // OP6
      batchOperation.delete(columnFamily, codec.toPersistedFormat("key10"));
      // OP7
      batchOperation.put(columnFamily, codec.toDirectCodecBuffer("key04"), codec.toDirectCodecBuffer("value04"));
      // OP8
      batchOperation.put(columnFamily, codec.toPersistedFormat("key06"), codec.toPersistedFormat("value05"));
      //OP9
      batchOperation.put(columnFamily, codec.toDirectCodecBuffer("key01"), codec.toDirectCodecBuffer("value011"));


      RocksDatabase db = Mockito.mock(RocksDatabase.class);
      doNothing().when(db).batchWrite(any());
      batchOperation.commit(db);
      Set<Operation> expectedOps = ImmutableSet.of(
          getOperation("key01", "value011", OpType.PUT_DIRECT),
          getOperation("key02", "value02", OpType.PUT_DIRECT),
          getOperation("key03", "value04", OpType.PUT_DIRECT),
          getOperation("key05", null, OpType.DELETE_DIRECT),
          getOperation("key10", null, OpType.DELETE_DIRECT),
          getOperation("key04", "value04", OpType.PUT_DIRECT),
          getOperation("key06", "value05", OpType.PUT_DIRECT));
      assertEquals(Collections.singleton("test"), writeBatch.getOperations().keySet());
      assertEquals(expectedOps, new HashSet<>(writeBatch.getOperations().get("test")));
    }
  }

  private DBStore getDBStore(OzoneConfiguration conf, String name, String tableName) throws RocksDatabaseException {
    return DBStoreBuilder.newBuilder(conf)
        .setName(name).setPath(tempDir).addTable(tableName).build();
  }

  private void performPut(Table<String, String> withBatchTable, BatchOperation batchOperation,
      Table<String, String> withoutBatchTable, String key) throws RocksDatabaseException, CodecException {
    String value = getRandomString();
    withBatchTable.putWithBatch(batchOperation, key, value);
    withoutBatchTable.put(key, value);
  }

  private void performDelete(Table<String, String> withBatchTable, BatchOperation batchOperation,
      Table<String, String> withoutBatchTable, String key) throws RocksDatabaseException, CodecException {
    withBatchTable.deleteWithBatch(batchOperation, key);
    withoutBatchTable.delete(key);
  }

  private String getRandomString() {
    int length = ThreadLocalRandom.current().nextInt(1, 1024);
    return RandomStringUtils.insecure().next(length);
  }

  private void performOpWithRandomKey(CheckedConsumer<String, IOException> op, Set<String> keySet,
      List<String> keyList) throws IOException {
    String key = getRandomString();
    op.accept(key);
    if (!keySet.contains(key)) {
      keyList.add(key);
      keySet.add(key);
    }
  }

  private void performOpWithRandomPreExistingKey(CheckedConsumer<String, IOException> op, List<String> keyList)
      throws IOException {
    int randomIndex = ThreadLocalRandom.current().nextInt(0, keyList.size());
    op.accept(keyList.get(randomIndex));
  }

  @Test
  public void testRDBBatchOperationWithRDB() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    String tableName = "test";
    try (DBStore dbStoreWithBatch = getDBStore(conf, "WithBatch.db", tableName);
         DBStore dbStoreWithoutBatch = getDBStore(conf, "WithoutBatch.db", tableName)) {
      try (BatchOperation batchOperation = dbStoreWithBatch.initBatchOperation()) {
        Table<String, String> withBatchTable = dbStoreWithBatch.getTable(tableName,
            StringCodec.get(), StringCodec.get());
        Table<String, String> withoutBatchTable = dbStoreWithoutBatch.getTable(tableName,
            StringCodec.get(), StringCodec.get());
        List<String> keyList = new ArrayList<>();
        Set<String> keySet = new HashSet<>();
        List<CheckedConsumer<String, IOException>> ops = Arrays.asList(
            (key) -> performPut(withBatchTable, batchOperation, withoutBatchTable, key),
            (key) -> performDelete(withBatchTable, batchOperation, withoutBatchTable, key));
        for (int i = 0; i < 30000; i++) {
          CheckedConsumer<String, IOException> op = ops.get(ThreadLocalRandom.current().nextInt(ops.size()));
          boolean performWithPreExistingKey = ThreadLocalRandom.current().nextBoolean();
          if (performWithPreExistingKey && !keyList.isEmpty()) {
            performOpWithRandomPreExistingKey(op, keyList);
          } else {
            performOpWithRandomKey(op, keySet, keyList);
          }
        }
        dbStoreWithBatch.commitBatchOperation(batchOperation);
      }
      Table<CodecBuffer, CodecBuffer> withBatchTable = dbStoreWithBatch.getTable(tableName,
          CodecBufferCodec.get(true), CodecBufferCodec.get(true));
      Table<CodecBuffer, CodecBuffer> withoutBatchTable = dbStoreWithoutBatch.getTable(tableName,
          CodecBufferCodec.get(true), CodecBufferCodec.get(true));
      try (Table.KeyValueIterator<CodecBuffer, CodecBuffer> itr1 = withBatchTable.iterator();
           Table.KeyValueIterator<CodecBuffer, CodecBuffer> itr2 = withoutBatchTable.iterator();) {
        while (itr1.hasNext() || itr2.hasNext()) {
          assertEquals(itr1.hasNext(), itr2.hasNext(), "Expected same number of entries");
          KeyValue<CodecBuffer, CodecBuffer> kv1 = itr1.next();
          KeyValue<CodecBuffer, CodecBuffer> kv2 = itr2.next();
          assertEquals(kv1.getKey().asReadOnlyByteBuffer(), kv2.getKey().asReadOnlyByteBuffer(),
              "Expected same keys");
          assertEquals(kv1.getValue().asReadOnlyByteBuffer(), kv2.getValue().asReadOnlyByteBuffer(),
              "Expected same keys");
        }
      }
    }
  }
}
