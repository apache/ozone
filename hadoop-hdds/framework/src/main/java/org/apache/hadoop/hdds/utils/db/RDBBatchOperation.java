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

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDirectSlice;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSlice;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.ratis.util.TraditionalBinaryPrefix;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.AbstractSlice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batch operation implementation for rocks db.
 * Note that a {@link RDBBatchOperation} object only for one batch.
 * Also, this class is not threadsafe.
 */
public final class RDBBatchOperation implements BatchOperation {
  static final Logger LOG = LoggerFactory.getLogger(RDBBatchOperation.class);

  private static final String PUT_OP = "PUT";
  private static final String DELETE_OP = "DELETE";
  private static final String DELETE_RANGE_OP = "DELETE_RANGE";

  private static final AtomicInteger BATCH_COUNT = new AtomicInteger();
  private static final CodecBufferCodec DIRECT_CODEC_BUFFER_CODEC = CodecBufferCodec.get(true);

  private final String name = "Batch-" + BATCH_COUNT.getAndIncrement();

  private final ManagedWriteBatch writeBatch;

  private final OpCache opCache = new OpCache();

  public static RDBBatchOperation newAtomicOperation() {
    return newAtomicOperation(new ManagedWriteBatch());
  }

  public static RDBBatchOperation newAtomicOperation(ManagedWriteBatch writeBatch) {
    return new RDBBatchOperation(writeBatch);
  }

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
   * The key type of {@link RDBBatchOperation.OpCache.FamilyCache#batchOps}.
   * To implement {@link #equals(Object)} and {@link #hashCode()}
   * based on the contents of the bytes.
   */
  static final class Bytes implements Comparable<Bytes>, Closeable {
    private AbstractSlice<?> slice;
    /** Cache the hash value. */
    private int hash;

    Bytes(CodecBuffer buffer) {
      Objects.requireNonNull(buffer, "buffer == null");
      if (buffer.isDirect()) {
        initWithDirectByteBuffer(buffer.asReadOnlyByteBuffer());
      } else {
        initWithByteArray(buffer.getArray());
      }
    }

    Bytes(byte[] array) {
      Objects.requireNonNull(array, "array == null");
      initWithByteArray(array);
    }

    private void initWithByteArray(byte[] array) {
      this.slice = new ManagedSlice(array);
      this.hash = ByteBuffer.wrap(array).hashCode();
    }

    private void initWithDirectByteBuffer(ByteBuffer byteBuffer) {
      this.slice = new ManagedDirectSlice(byteBuffer);
      this.hash = byteBuffer.hashCode();
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
      return slice.equals(that.slice);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public String toString() {
      return slice.toString();
    }

    // This method mimics the ByteWiseComparator in RocksDB.
    @Override
    public int compareTo(RDBBatchOperation.Bytes that) {
      return this.slice.compare(that.slice);
    }

    @Override
    public void close() {
      slice.close();
    }
  }

  private abstract class Operation implements Closeable {
    private final Bytes keyBytes;

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

    abstract String getOpType();

    @Override
    public void close() {
      if (keyBytes != null) {
        keyBytes.close();
      }
    }

    @Override
    public String toString() {
      return getOpType() + ", key=" + keyBytes;
    }
  }

  /**
   * Delete operation to be applied to a {@link ColumnFamily} batch.
   */
  private final class DeleteOperation extends Operation {
    private final CodecBuffer key;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private DeleteOperation(CodecBuffer key, Bytes keyBytes) {
      super(Objects.requireNonNull(keyBytes, "keyBytes == null"));
      this.key = Objects.requireNonNull(key, "key == null");
    }

    @Override
    public void apply(ColumnFamily family, ManagedWriteBatch batch) throws RocksDatabaseException {
      family.batchDelete(batch, this.key.asReadOnlyByteBuffer());
    }

    @Override
    public int keyLen() {
      return key.readableBytes();
    }

    @Override
    public int valLen() {
      return 0;
    }

    @Override
    public String getOpType() {
      return DELETE_OP;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        key.release();
      }
      super.close();
    }
  }

  /**
   * Put operation to be applied to a {@link ColumnFamily} batch using the CodecBuffer api.
   */
  private final class PutOperation extends Operation {
    private final CodecBuffer key;
    private final CodecBuffer value;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private PutOperation(CodecBuffer key, CodecBuffer value, Bytes keyBytes) {
      super(Objects.requireNonNull(keyBytes, "keyBytes == null"));
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
    public String getOpType() {
      return PUT_OP;
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
   * Delete range operation to be applied to a {@link ColumnFamily} batch.
   */
  private final class DeleteRangeOperation extends Operation {
    private final byte[] startKey;
    private final byte[] endKey;
    private final RangeQueryIndex.Range<Bytes> rangeEntry;

    private DeleteRangeOperation(byte[] startKey, byte[] endKey) {
      super(null);
      this.startKey = Objects.requireNonNull(startKey, "startKey == null");
      this.endKey = Objects.requireNonNull(endKey, "endKey == null");
      this.rangeEntry = new RangeQueryIndex.Range<>(new Bytes(startKey), new Bytes(endKey));
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
    public String getOpType() {
      return DELETE_RANGE_OP;
    }

    @Override
    public void close() {
      super.close();
      rangeEntry.getStartInclusive().close();
      rangeEntry.getEndExclusive().close();
    }

    @Override
    public String toString() {
      return getOpType() + ", rangeEntry=" + rangeEntry;
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
        Set<RangeQueryIndex.Range<Bytes>> deleteRangeEntries = new HashSet<>();
        for (Operation op : ops) {
          if (DELETE_RANGE_OP.equals(op.getOpType())) {
            DeleteRangeOperation deleteRangeOp = (DeleteRangeOperation) op;
            deleteRangeEntries.add(deleteRangeOp.rangeEntry);
          }
        }
        try {
          RangeQueryIndex<Bytes> rangeQueryIdx = new RangeQueryIndex<>(deleteRangeEntries);
          for (Operation op : ops) {
            if (DELETE_RANGE_OP.equals(op.getOpType())) {
              DeleteRangeOperation deleteRangeOp = (DeleteRangeOperation) op;
              rangeQueryIdx.removeRange(deleteRangeOp.rangeEntry);
              op.apply(family, writeBatch);
            } else {
              // Find a delete range op matching which would contain the key after the
              // operation has occurred. If there is no such operation then perform the operation otherwise discard the
              // op.
              if (!rangeQueryIdx.containsIntersectingRange(op.getKey())) {
                op.apply(family, writeBatch);
              } else {
                debug(() -> {
                  RangeQueryIndex.Range<Bytes> deleteRangeOp = rangeQueryIdx.getFirstIntersectingRange(op.getKey());
                  return String.format("Discarding Operation with Key: %s as it falls within the range of [%s, %s)",
                      op.getKey(), deleteRangeOp.getStartInclusive(), deleteRangeOp.getEndExclusive());
                });
                discardedCount++;
                discardedSize += op.totalLength();
              }
            }
          }
          debug(this::summary);
        } catch (IOException e) {
          throw new RocksDatabaseException("Failed to prepare batch write", e);
        }
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

      private void deleteIfExist(Bytes key) {
        // remove previous first in order to call release()
        if (opsKeys.containsKey(key)) {
          int previousIndex = opsKeys.remove(key);
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
        deleteIfExist(key);
        batchSize += operation.totalLength();
        int newIndex = opIndex.getAndIncrement();
        final Integer overwrittenOpKey = opsKeys.put(key, newIndex);
        final Operation overwrittenOp = batchOps.put(newIndex, operation);
        Preconditions.checkState(overwrittenOpKey == null && overwrittenOp == null);
        debug(() -> String.format("%s %s, %s; key=%s", this,
            DELETE_OP.equals(operation.getOpType()) ? delString(operation.totalLength()) : putString(operation.keyLen(),
                operation.valLen()),
            batchSizeDiscardedString(), key));
      }

      void put(CodecBuffer key, CodecBuffer value) {
        putCount++;

        // always release the key with the value
        Bytes keyBytes = new Bytes(key);
        overWriteOpIfExist(keyBytes, new PutOperation(key, value, keyBytes));
      }

      void put(byte[] key, byte[] value) {
        putCount++;
        CodecBuffer keyBuffer = DIRECT_CODEC_BUFFER_CODEC.fromPersistedFormat(key);
        CodecBuffer valueBuffer = DIRECT_CODEC_BUFFER_CODEC.fromPersistedFormat(value);
        Bytes keyBytes = new Bytes(keyBuffer);
        overWriteOpIfExist(keyBytes, new PutOperation(keyBuffer, valueBuffer, keyBytes));
      }

      void delete(byte[] key) {
        delCount++;
        CodecBuffer keyBuffer = DIRECT_CODEC_BUFFER_CODEC.fromPersistedFormat(key);
        Bytes keyBytes = new Bytes(keyBuffer);

        overWriteOpIfExist(keyBytes, new DeleteOperation(keyBuffer, keyBytes));
      }

      void delete(CodecBuffer key) {
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

    void delete(ColumnFamily family, CodecBuffer key) {
      name2cache.computeIfAbsent(family.getName(), k -> new FamilyCache(family)).delete(key);
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

  private RDBBatchOperation(ManagedWriteBatch writeBatch) {
    this.writeBatch = writeBatch;
  }

  @Override
  public String toString() {
    return name;
  }

  public void commit(RocksDatabase db) throws RocksDatabaseException {
    debug(() -> String.format("%s: commit %s",
        name, opCache.getCommitString()));
    try (UncheckedAutoCloseable ignored = opCache.prepareBatchWrite()) {
      db.batchWrite(writeBatch);
    }
  }

  public void commit(RocksDatabase db, ManagedWriteOptions writeOptions) throws RocksDatabaseException {
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

  public void delete(ColumnFamily family, byte[] key) {
    opCache.delete(family, key);
  }

  public void delete(ColumnFamily family, CodecBuffer key) {
    opCache.delete(family, key);
  }

  public void put(ColumnFamily family, CodecBuffer key, CodecBuffer value) {
    opCache.put(family, key, value);
  }

  public void put(ColumnFamily family, byte[] key, byte[] value) {
    opCache.put(family, key, value);
  }

  public void deleteRange(ColumnFamily family, byte[] startKey, byte[] endKey) {
    opCache.deleteRange(family, startKey, endKey);
  }
}
