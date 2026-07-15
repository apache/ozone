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

package org.apache.hadoop.hdds.utils.db.managed;

import static org.apache.hadoop.hdds.StringUtils.bytes2String;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * The TrackingUtilManagedWriteBatch class extends ManagedWriteBatch to provide functionality
 * for tracking operations in a managed write batch context. Operations such as put, delete,
 * merge, and delete range are managed and tracked, along with their corresponding operation types.
 *
 * This class supports direct and indirect operation types, delineated in the OpType enumeration.
 * Direct operations are created using ByteBuffers while indirect operations are created using
 * byte arrays.
 */
public class TrackingUtilManagedWriteBatchForTesting extends ManagedWriteBatch {

  private final Map<String, List<Operation>> operations = new HashMap<>();

  /**
   * The OpType enumeration defines the different types of operations performed in a batch.
   */
  public enum OpType {
    PUT_DIRECT,
    DELETE_DIRECT,
    MERGE_DIRECT,
    DELETE_RANGE_INDIRECT,
    PUT_NON_DIRECT,
    DELETE_NON_DIRECT,
    MERGE_NON_DIRECT,
  }

  /**
   * The Operation class represents an individual operation to be performed in the context of
   * a batch operation, such as a database write, delete, or merge. Each operation is characterized
   * by a key, value, and an operation type (OpType).
   *
   * Operations can be of different types, as defined in the OpType enumeration, which include
   * actions such as put, delete, merge, and delete range, either direct or indirect.
   */
  public static class Operation {
    private final byte[] key;
    private final byte[] value;
    private final OpType opType;

    public Operation(byte[] key, byte[] value, OpType opType) {
      this.key = Arrays.copyOf(key, key.length);
      this.value = value == null ? null : Arrays.copyOf(value, value.length);
      this.opType = opType;
    }

    public Operation(byte[] key, OpType opType) {
      this(key, null, opType);
    }

    @Override
    public final boolean equals(Object o) {
      if (!(o instanceof Operation)) {
        return false;
      }

      Operation operation = (Operation) o;
      return Arrays.equals(key, operation.key) && Arrays.equals(value, operation.value) &&
          opType == operation.opType;
    }

    @Override
    public final int hashCode() {
      return Arrays.hashCode(key) + Arrays.hashCode(value) + opType.hashCode();
    }

    @Override
    public String toString() {
      return "Operation{" +
          "key=" + bytes2String(key) +
          ", value=" + (value == null ? null : bytes2String(value)) +
          ", opType=" + opType +
          '}';
    }
  }

  public Map<String, List<Operation>> getOperations() {
    return operations;
  }

  public TrackingUtilManagedWriteBatchForTesting() {
    super();
  }

  private byte[] convert(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  @Override
  public void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
    operations.computeIfAbsent(bytes2String(columnFamilyHandle.getName()), k -> new ArrayList<>())
        .add(new Operation(key, OpType.DELETE_NON_DIRECT));
  }

  @Override
  public void delete(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key) throws RocksDBException {
    operations.computeIfAbsent(bytes2String(columnFamilyHandle.getName()), k -> new ArrayList<>())
        .add(new Operation(convert(key), OpType.DELETE_DIRECT));
  }

  @Override
  public void delete(byte[] key) throws RocksDBException {
    operations.computeIfAbsent("", k -> new ArrayList<>()).add(new Operation(key, OpType.DELETE_NON_DIRECT));
  }

  @Override
  public void delete(ByteBuffer key) throws RocksDBException {
    operations.computeIfAbsent("", k -> new ArrayList<>())
        .add(new Operation(convert(key), OpType.DELETE_DIRECT));
  }

  @Override
  public void deleteRange(byte[] beginKey, byte[] endKey) {
    operations.computeIfAbsent("", k -> new ArrayList<>())
        .add(new Operation(beginKey, endKey, OpType.DELETE_RANGE_INDIRECT));
  }

  @Override
  public void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey)
      throws RocksDBException {
    operations.computeIfAbsent(bytes2String(columnFamilyHandle.getName()), k -> new ArrayList<>())
        .add(new Operation(beginKey, endKey, OpType.DELETE_RANGE_INDIRECT));
  }

  @Override
  public void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
    operations.computeIfAbsent(bytes2String(columnFamilyHandle.getName()), k -> new ArrayList<>())
        .add(new Operation(key, value, OpType.MERGE_NON_DIRECT));
  }

  @Override
  public void merge(byte[] key, byte[] value) {
    operations.computeIfAbsent("", k -> new ArrayList<>())
        .add(new Operation(key, value, OpType.MERGE_NON_DIRECT));
  }

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
    operations.computeIfAbsent(bytes2String(columnFamilyHandle.getName()), k -> new ArrayList<>())
        .add(new Operation(key, value, OpType.PUT_NON_DIRECT));
  }

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key, ByteBuffer value) throws RocksDBException {
    operations.computeIfAbsent(bytes2String(columnFamilyHandle.getName()), k -> new ArrayList<>())
        .add(new Operation(convert(key), convert(value), OpType.PUT_DIRECT));
  }

  @Override
  public void put(byte[] key, byte[] value) throws RocksDBException {
    operations.computeIfAbsent("", k -> new ArrayList<>()).add(new Operation(key, value, OpType.PUT_NON_DIRECT));
  }

  @Override
  public void put(ByteBuffer key, ByteBuffer value) throws RocksDBException {
    operations.computeIfAbsent("", k -> new ArrayList<>())
        .add(new Operation(convert(key), convert(value), OpType.PUT_DIRECT));
  }

  @Override
  public void close() {
    super.close();
  }
}
