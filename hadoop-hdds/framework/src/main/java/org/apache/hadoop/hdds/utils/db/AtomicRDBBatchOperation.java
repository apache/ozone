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

import static org.apache.hadoop.hdds.StringUtils.bytes2String;

import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.ratis.util.TraditionalBinaryPrefix;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batch operation implementation for rocks db.
 * Note that a {@link AtomicRDBBatchOperation} object only for one batch.
 * Also, this class is not threadsafe.
 */
public class AtomicRDBBatchOperation extends RDBBatchOperation {
  static final Logger LOG = LoggerFactory.getLogger(AtomicRDBBatchOperation.class);

  private static final AtomicInteger BATCH_COUNT = new AtomicInteger();

  private final String name = "Batch-" + BATCH_COUNT.getAndIncrement();

  private final ManagedWriteBatch writeBatch;

  private final OpCache opCache = new OpCache();

  private enum Op { DELETE, PUT, DELETE_RANGE }

  private static void debug(Supplier<String> message) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("\n{}", message.get());
    }
  }

  private static String byteSize2String(long length) {
    return TraditionalBinaryPrefix.long2String(length, "B", 2);
  }

  private static String countSize2String(int count, long size) {
    return count + " (" + byteSize2String(size) + ")";
  }

  /**
   * The key type of {@link AtomicRDBBatchOperation.OpCache.FamilyCache#opsKeys}.
   * To implement {@link #equals(Object)} and {@link #hashCode()}
   * based on the contents of the bytes.
   */
  static final class Bytes implements Comparable<Bytes> {
    private final byte[] array;
    private final CodecBuffer buffer;
    /** Cache the hash value. */
    private final int hash;

    Bytes(CodecBuffer buffer) {
      this.array = null;
      this.buffer = Objects.requireNonNull(buffer, "buffer == null");
      this.hash = buffer.asReadOnlyByteBuffer().hashCode();
    }

    Bytes(byte[] array) {
      this.array = array;
      this.buffer = null;
      this.hash = ByteBuffer.wrap(array).hashCode();
    }

    ByteBuffer asReadOnlyByteBuffer() {
      return buffer.asReadOnlyByteBuffer();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof Bytes)) {
        return false;
      }
      final Bytes that = (Bytes) obj;
      if (this.hash != that.hash) {
        return false;
      }
      final ByteBuffer thisBuf = this.array != null ?
          ByteBuffer.wrap(this.array) : this.asReadOnlyByteBuffer();
      final ByteBuffer thatBuf = that.array != null ?
          ByteBuffer.wrap(that.array) : that.asReadOnlyByteBuffer();
      return thisBuf.equals(thatBuf);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public String toString() {
      return array != null ? bytes2String(array)
          : bytes2String(asReadOnlyByteBuffer());
    }

    // This method mimics the ByteWiseComparator in RocksDB.
    @Override
    public int compareTo(AtomicRDBBatchOperation.Bytes that) {
      final ByteBuffer thisBuf = this.array != null ?
          ByteBuffer.wrap(this.array) : this.asReadOnlyByteBuffer();
      final ByteBuffer thatBuf = that.array != null ?
          ByteBuffer.wrap(that.array) : that.asReadOnlyByteBuffer();

      for (int i = 0; i < Math.min(thisBuf.remaining(), thatBuf.remaining()); i++) {
        int cmp = UnsignedBytes.compare(thisBuf.get(i), thatBuf.get(i));
        if (cmp != 0) {
          return cmp;
        }
      }
      return thisBuf.remaining() - thatBuf.remaining();
    }
  }

  private abstract class Operation implements Closeable {
    private Bytes keyBytes;

    private Operation(Bytes keyBytes) {
      this.keyBytes = keyBytes;
    }

    abstract void apply(ColumnFamily family, ManagedWriteBatch batch) throws RocksDatabaseException;

    abstract int keyLen();

    abstract int valLen();

    Bytes getKey() {
      return keyBytes;
    }

    int totalLength() {
      return keyLen() + valLen();
    }

    abstract Op getOpType();

    @Override
    public void close() {
    }
  }

  /**
   * Delete operation to be applied to a {@link ColumnFamily} batch.
   */
  private final class DeleteOperation extends Operation {
    private final byte[] key;

    private DeleteOperation(byte[] key, Bytes keyBytes) {
      super(Objects.requireNonNull(keyBytes, "keyBytes == null"));
      this.key = Objects.requireNonNull(key, "key == null");
    }

    @Override
    public void apply(ColumnFamily family, ManagedWriteBatch batch) throws RocksDatabaseException {
      family.batchDelete(batch, this.key);
    }

    @Override
    public int keyLen() {
      return key.length;
    }

    @Override
    public int valLen() {
      return 0;
    }

    @Override
    public Op getOpType() {
      return Op.DELETE;
    }
  }

  /**
   * Put operation to be applied to a {@link ColumnFamily} batch using the CodecBuffer api.
   */
  private final class CodecBufferPutOperation extends Operation {
    private final CodecBuffer key;
    private final CodecBuffer value;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private CodecBufferPutOperation(CodecBuffer key, CodecBuffer value, Bytes keyBytes) {
      super(keyBytes);
      this.key = key;
      this.value = value;
    }

    @Override
    public void apply(ColumnFamily family, ManagedWriteBatch batch) throws RocksDatabaseException {
      family.batchPut(batch, key.asReadOnlyByteBuffer(), value.asReadOnlyByteBuffer());
    }

    @Override
    public int keyLen() {
      return key.readableBytes();
    }

    @Override
    public int valLen() {
      return value.readableBytes();
    }

    @Override
    public Op getOpType() {
      return Op.PUT;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        key.release();
        value.release();
      }
      super.close();
    }
  }

  /**
   * Put operation to be applied to a {@link ColumnFamily} batch using the byte array api.
   */
  private final class ByteArrayPutOperation extends Operation {
    private final byte[] key;
    private final byte[] value;

    private ByteArrayPutOperation(byte[] key, byte[] value, Bytes keyBytes) {
      super(Objects.requireNonNull(keyBytes));
      this.key = Objects.requireNonNull(key, "key == null");
      this.value = Objects.requireNonNull(value, "value == null");
    }

    @Override
    public void apply(ColumnFamily family, ManagedWriteBatch batch) throws RocksDatabaseException {
      family.batchPut(batch, key, value);
    }

    @Override
    public int keyLen() {
      return key.length;
    }

    @Override
    public int valLen() {
      return value.length;
    }

    @Override
    public Op getOpType() {
      return Op.PUT;
    }
  }

  /**
   * Delete range operation to be applied to a {@link ColumnFamily} batch.
   */
  private final class DeleteRangeOperation extends Operation {
    private final byte[] startKey;
    private final byte[] endKey;

    private DeleteRangeOperation(byte[] startKey, byte[] endKey) {
      super(null);
      this.startKey = Objects.requireNonNull(startKey, "startKey == null");
      this.endKey = Objects.requireNonNull(endKey, "endKey == null");
    }

    @Override
    public void apply(ColumnFamily family, ManagedWriteBatch batch) throws RocksDatabaseException {
      family.batchDeleteRange(batch, startKey, endKey);
    }

    @Override
    public int keyLen() {
      return startKey.length + endKey.length;
    }

    @Override
    public int valLen() {
      return 0;
    }

    @Override
    public Op getOpType() {
      return Op.DELETE_RANGE;
    }
  }

  /** Cache and deduplicate db ops (put/delete). */
  private class OpCache {
    /** A (family name -> {@link FamilyCache}) map. */
    private final Map<String, FamilyCache> name2cache = new HashMap<>();

    /** A cache for a {@link ColumnFamily}. */
    private class FamilyCache {
      private final ColumnFamily family;
      /**
       * A mapping of operation keys to their respective indices in {@code FamilyCache}.
       *
       * Key details:
       * - Maintains a mapping of unique operation keys to their insertion or processing order.
       * - Used internally to manage and sort operations during batch writes.
       * - Facilitates filtering, overwriting, or deletion of operations based on their keys.
       *
       * Constraints:
       * - Keys must be unique, represented using {@link Bytes}, to avoid collisions.
       * - Each key is associated with a unique integer index to track insertion order.
       *
       * This field plays a critical role in managing the logical consistency and proper execution
       * order of operations stored in the batch when interacting with a RocksDB-backed system.
       */
      private final Map<Bytes, Integer> opsKeys = new HashMap<>();
      /**
       * Maintains a mapping of unique operation indices to their corresponding {@code Operation} instances.
       *
       * This map serves as the primary container for recording operations in preparation for a batch write
       * within a RocksDB-backed system. Each operation is referenced by an integer index, which determines
       * its insertion order and ensures correct sequencing during batch execution.
       *
       * Key characteristics:
       * - Stores operations of type {@code Operation}.
       * - Uses a unique integer key (index) for mapping each operation.
       * - Serves as an intermediary structure during batch preparation and execution.
       *
       * Usage context:
       * - This map is managed as part of the batch-writing process, which involves organizing,
       *   filtering, and applying multiple operations in a single cohesive batch.
       * - Operations stored in this map are expected to define specific actions (e.g., put, delete,
       *   delete range) and their associated data (e.g., keys, values).
       */
      private final Map<Integer, Operation> batchOps = new HashMap<>();
      private boolean isCommit;

      private long batchSize;
      private long discardedSize;
      private int discardedCount;
      private int putCount;
      private int delCount;
      private int delRangeCount;
      private AtomicInteger opIndex;

      FamilyCache(ColumnFamily family) {
        this.family = family;
        this.opIndex = new AtomicInteger(0);
      }

      /**
       * Prepares a batch write operation for a RocksDB-backed system.
       *
       * This method ensures the orderly execution of operations accumulated in the batch,
       * respecting their respective types and order of insertion.
       *
       * Key functionalities:
       * 1. Ensures that the batch is not already committed before proceeding.
       * 2. Sorts all operations by their `opIndex` to maintain a consistent execution order.
       * 3. Filters and adapts operations to account for any delete range operations that might
       *    affect other operations in the batch:
       *    - Operations with keys that fall within the range specified by a delete range operation
       *      are discarded.
       *    - Delete range operations are executed in their correct order.
       * 4. Applies remaining operations to the write batch, ensuring proper filtering and execution.
       * 5. Logs a summary of the batch execution for debugging purposes.
       *
       * Throws:
       * - RocksDatabaseException if any error occurs while applying operations to the write batch.
       *
       * Prerequisites:
       * - The method assumes that the operations are represented by `Operation` objects, each of which
       *   encapsulates the logic for its specific type.
       * - Delete range operations must be represented by the `DeleteRangeOperation` class.
       */
      void prepareBatchWrite() throws RocksDatabaseException {
        Preconditions.checkState(!isCommit, "%s is already committed.", this);
        isCommit = true;
        // Sort Entries based on opIndex and flush the operation to the batch in the same order.
        List<Operation> ops = batchOps.entrySet().stream().sorted(Comparator.comparingInt(Map.Entry::getKey))
            .map(Map.Entry::getValue).collect(Collectors.toList());
        List<List<Integer>> deleteRangeIndices = new ArrayList<>();
        int index = 0;
        int prevIndex = -2;
        for (Operation op : ops) {
          if (Op.DELETE_RANGE == op.getOpType()) {
            if (index - prevIndex > 1) {
              deleteRangeIndices.add(new ArrayList<>());
            }
            List<Integer> continuousIndices = deleteRangeIndices.get(deleteRangeIndices.size() - 1);
            continuousIndices.add(index);
            prevIndex = index;
          }
          index++;
        }
        // This is to apply the last batch of entries after the last DeleteRangeOperation.
        deleteRangeIndices.add(Collections.emptyList());
        int startIndex = 0;
        for (List<Integer> continuousDeleteRangeIndices : deleteRangeIndices) {
          List<DeleteRangeOperation> deleteRangeOps = continuousDeleteRangeIndices.stream()
              .map(i -> (DeleteRangeOperation)ops.get(i))
              .collect(Collectors.toList());
          List<Pair<Bytes, Bytes>> deleteRangeOpsRanges = continuousDeleteRangeIndices.stream()
              .map(i -> (DeleteRangeOperation)ops.get(i))
              .map(i -> Pair.of(new Bytes(i.startKey), new Bytes(i.endKey)))
              .collect(Collectors.toList());
          int firstOpIndex = continuousDeleteRangeIndices.isEmpty() ? ops.size() : continuousDeleteRangeIndices.get(0);

          for (int i = startIndex; i < firstOpIndex; i++) {
            Operation op = ops.get(i);
            Bytes key = op.getKey();
            // Compare the key with the startKey and endKey of the delete range operation. Add to Batch if key
            // doesn't fall [startKey, endKey) range.
            boolean keyInRange = false;
            Pair<Bytes, Bytes> deleteRange = null;
            for (Pair<Bytes, Bytes> deleteRangeOp : deleteRangeOpsRanges) {
              if (key.compareTo(deleteRangeOp.getLeft()) >= 0 && key.compareTo(deleteRangeOp.getRight()) < 0) {
                keyInRange = true;
                deleteRange = deleteRangeOp;
                break;
              }
            }
            if (!keyInRange) {
              op.apply(family, writeBatch);
            } else {
              Pair<Bytes, Bytes> finalDeleteRange = deleteRange;
              debug(() -> String.format("Discarding Operation with Key: %s as it falls within the range of [%s, %s)",
                  bytes2String(key.asReadOnlyByteBuffer()),
                  bytes2String(finalDeleteRange.getKey().asReadOnlyByteBuffer()),
                  bytes2String(finalDeleteRange.getRight().asReadOnlyByteBuffer())));
              discardedCount++;
              discardedSize += op.totalLength();
            }
          }
          for (DeleteRangeOperation deleteRangeOp : deleteRangeOps) {
            // Apply the delete range operation to the batch.
            deleteRangeOp.apply(family, writeBatch);
          }
          // Update the startIndex to start from the next operation after the delete range operation.
          startIndex = firstOpIndex + continuousDeleteRangeIndices.size();
        }
        debug(this::summary);
      }

      private String summary() {
        return String.format("  %s %s, #put=%s, #del=%s", this,
            batchSizeDiscardedString(), putCount, delCount);
      }

      void clear() {
        final boolean warn = !isCommit && batchSize > 0;
        String details = warn ? summary() : null;

        IOUtils.close(LOG, batchOps.values());
        batchOps.clear();

        if (warn) {
          LOG.warn("discarding changes {}", details);
        }
      }

      private void deleteIfExist(Bytes key, boolean removeFromIndexMap) {
        // remove previous first in order to call release()
        if (opsKeys.containsKey(key)) {
          int previousIndex = removeFromIndexMap ? opsKeys.remove(key) : opsKeys.get(key);
          final Operation previous = batchOps.remove(previousIndex);
          previous.close();
          discardedSize += previous.totalLength();
          discardedCount++;
          debug(() -> String.format("%s overwriting a previous %s[valLen => %s]", this, previous.getOpType(),
              previous.valLen()));
        }
      }

      void overWriteOpIfExist(Bytes key, Operation operation) {
        Preconditions.checkState(!isCommit, "%s is already committed.", this);
        deleteIfExist(key, true);
        batchSize += operation.totalLength();
        int newIndex = opIndex.getAndIncrement();
        final Integer overwritten = opsKeys.put(key, newIndex);
        batchOps.put(newIndex, operation);
        Preconditions.checkState(overwritten == null || !batchOps.containsKey(overwritten));
        debug(() -> String.format("%s %s, %s; key=%s", this,
            Op.DELETE == operation.getOpType() ? delString(operation.totalLength()) : putString(operation.keyLen(),
                operation.valLen()),
            batchSizeDiscardedString(), key));
      }

      void put(CodecBuffer key, CodecBuffer value) {
        putCount++;

        // always release the key with the value
        Bytes keyBytes = new Bytes(key);
        overWriteOpIfExist(keyBytes, new CodecBufferPutOperation(key, value, keyBytes));
      }

      void put(byte[] key, byte[] value) {
        putCount++;
        Bytes keyBytes = new Bytes(key);
        overWriteOpIfExist(keyBytes, new ByteArrayPutOperation(key, value, keyBytes));
      }

      void delete(byte[] key) {
        delCount++;
        Bytes keyBytes = new Bytes(key);
        overWriteOpIfExist(keyBytes, new DeleteOperation(key, keyBytes));
      }

      void deleteRange(byte[] startKey, byte[] endKey) {
        delRangeCount++;
        batchOps.put(opIndex.getAndIncrement(), new DeleteRangeOperation(startKey, endKey));
      }

      String putString(int keySize, int valueSize) {
        return String.format("put(key: %s, value: %s), #put=%s",
            byteSize2String(keySize), byteSize2String(valueSize), putCount);
      }

      String delString(int keySize) {
        return String.format("del(key: %s), #del=%s",
            byteSize2String(keySize), delCount);
      }

      String batchSizeDiscardedString() {
        return String.format("batchSize=%s, discarded: %s",
            byteSize2String(batchSize),
            countSize2String(discardedCount, discardedSize));
      }

      @Override
      public String toString() {
        return name + ": " + family.getName();
      }
    }

    void put(ColumnFamily f, CodecBuffer key, CodecBuffer value) {
      name2cache.computeIfAbsent(f.getName(), k -> new FamilyCache(f))
          .put(key, value);
    }

    void put(ColumnFamily f, byte[] key, byte[] value) {
      name2cache.computeIfAbsent(f.getName(), k -> new FamilyCache(f))
          .put(key, value);
    }

    void delete(ColumnFamily family, byte[] key) {
      name2cache.computeIfAbsent(family.getName(), k -> new FamilyCache(family))
          .delete(key);
    }

    void deleteRange(ColumnFamily family, byte[] startKey, byte[] endKey) {
      name2cache.computeIfAbsent(family.getName(), k -> new FamilyCache(family))
          .deleteRange(startKey, endKey);
    }

    /** Prepare batch write for the entire cache. */
    UncheckedAutoCloseable prepareBatchWrite() throws RocksDatabaseException {
      for (Map.Entry<String, FamilyCache> e : name2cache.entrySet()) {
        e.getValue().prepareBatchWrite();
      }
      return this::clear;
    }

    void clear() {
      for (Map.Entry<String, FamilyCache> e : name2cache.entrySet()) {
        e.getValue().clear();
      }
      name2cache.clear();
    }

    String getCommitString() {
      int putCount = 0;
      int delCount = 0;
      int opSize = 0;
      int discardedCount = 0;
      int discardedSize = 0;
      int delRangeCount = 0;

      for (FamilyCache f : name2cache.values()) {
        putCount += f.putCount;
        delCount += f.delCount;
        opSize += f.batchSize;
        discardedCount += f.discardedCount;
        discardedSize += f.discardedSize;
        delRangeCount += f.delRangeCount;
      }

      final int opCount = putCount + delCount;
      return String.format(
          "#put=%s, #del=%s, #delRange=%s, batchSize: %s, discarded: %s, committed: %s",
          putCount, delCount, delRangeCount,
          countSize2String(opCount, opSize),
          countSize2String(discardedCount, discardedSize),
          countSize2String(opCount - discardedCount, opSize - discardedSize));
    }
  }

  public AtomicRDBBatchOperation() {
    this(new ManagedWriteBatch());
  }

  public AtomicRDBBatchOperation(ManagedWriteBatch writeBatch) {
    this.writeBatch = writeBatch;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  void commit(RocksDatabase db) throws RocksDatabaseException {
    debug(() -> String.format("%s: commit %s",
        name, opCache.getCommitString()));
    try (UncheckedAutoCloseable ignored = opCache.prepareBatchWrite()) {
      db.batchWrite(writeBatch);
    }
  }

  @Override
  void commit(RocksDatabase db, ManagedWriteOptions writeOptions) throws RocksDatabaseException {
    debug(() -> String.format("%s: commit-with-writeOptions %s",
        name, opCache.getCommitString()));
    try (UncheckedAutoCloseable ignored = opCache.prepareBatchWrite()) {
      db.batchWrite(writeBatch, writeOptions);
    }
  }

  @Override
  public void close() {
    debug(() -> String.format("%s: close", name));
    writeBatch.close();
    opCache.clear();
  }

  @Override
  void delete(ColumnFamily family, byte[] key) {
    opCache.delete(family, key);
  }

  @Override
  void put(ColumnFamily family, CodecBuffer key, CodecBuffer value) {
    opCache.put(family, key, value);
  }

  @Override
  void put(ColumnFamily family, byte[] key, byte[] value) {
    opCache.put(family, key, value);
  }

  @Override
  void deleteRange(ColumnFamily family, byte[] startKey, byte[] endKey) {
    opCache.deleteRange(family, startKey, endKey);
  }
}
