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

import static com.google.common.primitives.UnsignedBytes.lexicographicalComparator;
import static org.apache.hadoop.hdds.StringUtils.string2Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

  @TempDir
  private Path tempDir;

  private static Operation getOperation(String key, String value, OpType opType) {
    return new Operation(string2Bytes(key), value == null ? null : string2Bytes(value), opType);
  }

  @Test
  public void testBatchOperationWithDeleteRange() throws RocksDatabaseException, CodecException, RocksDBException {
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
      // OP1 should be skipped because of OP7
      batchOperation.put(columnFamily, codec.toDirectCodecBuffer("key01"), codec.toDirectCodecBuffer("value01"));
      // OP2 should be skipped because of OP8
      batchOperation.put(columnFamily, codec.toPersistedFormat("key02"), codec.toPersistedFormat("value02"));
      // OP3
      batchOperation.put(columnFamily, codec.toDirectCodecBuffer("key03"), codec.toDirectCodecBuffer("value03"));
      // OP4 would overwrite OP3
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
      // OP10 should be skipped because of OP11
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

  private void performDeleteRange(Table<String, String> withBatchTable, BatchOperation batchOperation,
      Table<String, String> withoutBatchTable, String startKey, String endKey)
      throws RocksDatabaseException, CodecException {
    withBatchTable.deleteRangeWithBatch(batchOperation, startKey, endKey);
    withoutBatchTable.deleteRange(startKey, endKey);
  }

  private String getRandomString() {
    int length = ThreadLocalRandom.current().nextInt(1, 1024);
    return RandomStringUtils.insecure().next(length);
  }

  private void performOpWithRandomKey(CheckedConsumer<List<String>, IOException> op, Set<String> keySet,
                                      List<String> keyList, int numberOfKeys) throws IOException {
    List<String> randomKeys = new ArrayList<>(numberOfKeys);
    for (int i = 0; i < numberOfKeys; i++) {
      randomKeys.add(getRandomString());
    }
    op.accept(randomKeys);
    for (String key : randomKeys) {
      if (!keySet.contains(key)) {
        keyList.add(key);
        keySet.add(key);
      }
    }
  }

  private void performOpWithRandomPreExistingKey(CheckedConsumer<List<String>, IOException> op, List<String> keyList,
      int numberOfKeys) throws IOException {
    op.accept(IntStream.range(0, numberOfKeys)
        .mapToObj(i -> keyList.get(ThreadLocalRandom.current().nextInt(0, keyList.size())))
        .collect(Collectors.toList()));
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
        NavigableMap<Double, Integer> opProbMap = new TreeMap<>();
        // Have a probablity map to run delete range only 2% of the times.
        // If there are too many delete range ops at once the table iteration can become very slow for
        // randomised operations.
        opProbMap.put(0.49, 0);
        opProbMap.put(0.98, 1);
        opProbMap.put(1.0, 2);

        Map<Integer, Integer> opIdxToNumKeyMap = ImmutableMap.of(0, 1, 1, 1, 2, 2);
        List<CheckedConsumer<List<String>, IOException>> ops = Arrays.asList(
            (key) -> performPut(withBatchTable, batchOperation, withoutBatchTable, key.get(0)),
            (key) -> performDelete(withBatchTable, batchOperation, withoutBatchTable, key.get(0)),
            (key) -> {
              key.sort((key1, key2) -> lexicographicalComparator().compare(string2Bytes(key1),
                      string2Bytes(key2)));
              performDeleteRange(withBatchTable, batchOperation, withoutBatchTable, key.get(0), key.get(1));
            });
        for (int i = 0; i < 30000; i++) {
          int opIdx = opProbMap.higherEntry(ThreadLocalRandom.current().nextDouble()).getValue();
          CheckedConsumer<List<String>, IOException> op = ops.get(opIdx);
          int numberOfKeys = opIdxToNumKeyMap.getOrDefault(opIdx, 1);
          boolean performWithPreExistingKey = ThreadLocalRandom.current().nextBoolean();
          if (performWithPreExistingKey && !keyList.isEmpty()) {
            performOpWithRandomPreExistingKey(op, keyList, numberOfKeys);
          } else {
            performOpWithRandomKey(op, keySet, keyList, numberOfKeys);
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
