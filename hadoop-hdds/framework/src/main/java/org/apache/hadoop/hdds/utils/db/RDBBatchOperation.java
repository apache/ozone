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
import static org.apache.ratis.util.Preconditions.assertInstanceOf;

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
 * Note that a {@link RDBBatchOperation} object only for one batch.
 * Also, this class is not threadsafe.
 */
public final class RDBBatchOperation implements BatchOperation {
  static final Logger LOG = LoggerFactory.getLogger(RDBBatchOperation.class);

  private static final AtomicInteger BATCH_COUNT = new AtomicInteger();

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

  static final class ByteArray extends Bytes<byte[]> {
    ByteArray(byte[] array) {
      super(array);
    }

    @Override
    int length() {
      return getBytes().length;
    }

    @Override
    ByteBuffer asReadOnlyByteBuffer() {
      return ByteBuffer.wrap(getBytes());
    }
  }

  static final class CodecBufferBytes extends Bytes<CodecBuffer> {
    CodecBufferBytes(CodecBuffer buffer) {
      super(buffer);
    }

    @Override
    int length() {
      return getBytes().readableBytes();
    }

    @Override
    ByteBuffer asReadOnlyByteBuffer() {
      return getBytes().asReadOnlyByteBuffer();
    }
  }

  /**
   * The key type of {@link RDBBatchOperation.OpCache.FamilyCache#opsKeys}.
   * To implement {@link #equals(Object)} and {@link #hashCode()}
   * based on the contents of the bytes.
   */
  abstract static class Bytes<T> {
    private final T byteObject;
    /** Cache the hash value. */
    private final int hash;

    Bytes(T byteObject) {
      this.byteObject = Objects.requireNonNull(byteObject, "byteObject == null");
      this.hash = asReadOnlyByteBuffer().hashCode();
    }

    T getBytes() {
      return byteObject;
    }

    abstract int length();

    abstract ByteBuffer asReadOnlyByteBuffer();

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof Bytes)) {
        return false;
      }
      final Bytes<?> that = (Bytes<?>) obj;
      if (this.hash != that.hash) {
        return false;
      }
      return this.asReadOnlyByteBuffer().equals(that.asReadOnlyByteBuffer());
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public String toString() {
      return bytes2String(asReadOnlyByteBuffer());
    }

    // TODO: use ByteWiseComparator directly and do not reimplement it.
    // This method mimics the ByteWiseComparator in RocksDB.
    int compareTo(ByteBuffer thatBuf) {
      final ByteBuffer thisBuf = asReadOnlyByteBuffer();

      for (int i = 0; i < Math.min(thisBuf.remaining(), thatBuf.remaining()); i++) {
        int cmp = UnsignedBytes.compare(thisBuf.get(i), thatBuf.get(i));
        if (cmp != 0) {
          return cmp;
        }
      }
      return thisBuf.remaining() - thatBuf.remaining();
    }
  }

  abstract static class Op<T extends Bytes<?>> implements Closeable {
    private final T keyBytes;

    private Op(T keyBytes) {
      this.keyBytes = Objects.requireNonNull(keyBytes, "keyBytes == null");
    }

    abstract void apply(ColumnFamily family, ManagedWriteBatch batch) throws RocksDatabaseException;

    int keyLen() {
      return getKey().length();
    }

    int valLen() {
      return 0;
    }

    T getKey() {
      return keyBytes;
    }

    int totalLength() {
      return keyLen() + valLen();
    }

    @Override
    public void close() {
    }
  }

  /**
   * Delete operation to be applied to a {@link ColumnFamily} batch.
   */
  static final class DeleteOp extends Op<ByteArray> {
    private DeleteOp(ByteArray key) {
      super(key);
    }

    @Override
    public void apply(ColumnFamily family, ManagedWriteBatch batch) throws RocksDatabaseException {
      family.batchDelete(batch, getKey().getBytes());
    }
  }

  /**
   * Put operation to be applied to a {@link ColumnFamily} batch using the CodecBuffer api.
   */
  static final class CodecBufferPutOp extends Op<CodecBufferBytes> {
    private final CodecBufferBytes value;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private CodecBufferPutOp(CodecBufferBytes key, CodecBufferBytes value) {
      super(key);
      this.value = value;
    }

    @Override
    public void apply(ColumnFamily family, ManagedWriteBatch batch) throws RocksDatabaseException {
      family.batchPut(batch, getKey().asReadOnlyByteBuffer(), value.asReadOnlyByteBuffer());
    }

    @Override
    public int valLen() {
      return value.length();
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        getKey().getBytes().release();
        value.getBytes().release();
      }
    }
  }

  /**
   * Put operation to be applied to a {@link ColumnFamily} batch using the byte array api.
   */
  static final class ByteArrayPutOp extends Op<ByteArray> {
    private final byte[] value;

    private ByteArrayPutOp(ByteArray key, byte[] value) {
      super(key);
      this.value = Objects.requireNonNull(value, "value == null");
    }

    @Override
    public void apply(ColumnFamily family, ManagedWriteBatch batch) throws RocksDatabaseException {
      family.batchPut(batch, getKey().getBytes(), value);
    }

    @Override
    public int valLen() {
      return value.length;
    }
  }

  static DeleteRangeOp findContainingRange(Bytes<?> key, Iterable<DeleteRangeOp> deleteRanges) {
    for (DeleteRangeOp op: deleteRanges) {
      if (op.containKey(key)) {
        return op;
      }
    }
    return null;
  }

  /**
   * Delete range operation to be applied to a {@link ColumnFamily} batch.
   */
  static final class DeleteRangeOp extends Op<ByteArray> {
    private final ByteArray endKey;

    private DeleteRangeOp(ByteArray startKey, ByteArray endKey) {
      super(startKey);
      this.endKey = Objects.requireNonNull(endKey, "endKey == null");
    }

    boolean containKey(Bytes<?> key) {
      final ByteBuffer keyBuf = ByteBuffer.wrap(assertInstanceOf(key.getBytes(), byte[].class));
      return getKey().compareTo(keyBuf) <= 0 && endKey.compareTo(keyBuf) > 0;
    }

    ByteBuffer getStartKey() {
      return getKey().asReadOnlyByteBuffer();
    }

    ByteBuffer getEndKey() {
      return endKey.asReadOnlyByteBuffer();
    }

    @Override
    public void apply(ColumnFamily family, ManagedWriteBatch batch) throws RocksDatabaseException {
      family.batchDeleteRange(batch, getKey().getBytes(), endKey.getBytes());
    }

    @Override
    public int keyLen() {
      return getKey().length() + endKey.length();
    }

    @Override
    public String toString() {
      return String.format("deleteRange[%s, %s)", bytes2String(getStartKey()), bytes2String(getEndKey()));
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
      private final Map<Bytes<?>, Integer> opsKeys = new HashMap<>();
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
      private final Map<Integer, Op<?>> batchOps = new HashMap<>();
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
        // TODO: use sorted map
        final List<Op<?>> ops = batchOps.entrySet().stream()
            .sorted(Comparator.comparingInt(Map.Entry::getKey))
            .map(Map.Entry::getValue).collect(Collectors.toList());
        List<List<Integer>> deleteRangeIndices = new ArrayList<>();
        int index = 0;
        int prevIndex = -2;
        for (Op<?> op : ops) {
          if (op instanceof DeleteRangeOp) {
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
          final List<DeleteRangeOp> deleteRangeOps = continuousDeleteRangeIndices.stream()
              .map(i -> (DeleteRangeOp)ops.get(i))
              .collect(Collectors.toList());
          int firstOpIndex = continuousDeleteRangeIndices.isEmpty() ? ops.size() : continuousDeleteRangeIndices.get(0);

          for (int i = startIndex; i < firstOpIndex; i++) {
            final Op<?> op = ops.get(i);
            final Bytes<?> key = op.getKey();
            // Add to Batch if the key is not contained in a deleteRange.
            final DeleteRangeOp containingRange = findContainingRange(key, deleteRangeOps);
            if (containingRange == null) {
              op.apply(family, writeBatch);
            } else {
              debug(() -> String.format("Discarding Operation with Key: %s as it falls within %s",
                  bytes2String(key.asReadOnlyByteBuffer()), containingRange));
              discardedCount++;
              discardedSize += op.totalLength();
            }
          }
          for (DeleteRangeOp deleteRangeOp : deleteRangeOps) {
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

      private void deleteIfExist(Bytes<?> key) {
        // remove previous first in order to call release()
        final Integer previousIndex = opsKeys.remove(key);
        if (previousIndex != null) {
          final Op<?> previous = batchOps.remove(previousIndex);
          previous.close();
          discardedSize += previous.totalLength();
          discardedCount++;
          debug(() -> String.format("%s overwriting a previous %s[valLen=%s]",
              this, previous.getClass().getSimpleName(), previous.valLen()));
        }
      }

      void overwriteIfExist(Bytes<?> key, Op<?> op) {
        Preconditions.checkState(!isCommit, "%s is already committed.", this);
        deleteIfExist(key);
        batchSize += op.totalLength();
        int newIndex = opIndex.getAndIncrement();
        final Integer overwritten = opsKeys.put(key, newIndex);
        batchOps.put(newIndex, op);
        Preconditions.checkState(overwritten == null || !batchOps.containsKey(overwritten));
        debug(() -> String.format("%s %s, %s; key=%s", this,
            op instanceof DeleteOp ? delString(op.totalLength()) : putString(op.keyLen(), op.valLen()),
            batchSizeDiscardedString(), key));
      }

      void put(CodecBuffer key, CodecBuffer value) {
        putCount++;

        // always release the key with the value
        final CodecBufferBytes keyBytes = new CodecBufferBytes(key);
        overwriteIfExist(keyBytes, new CodecBufferPutOp(keyBytes, new CodecBufferBytes(value)));
      }

      void put(byte[] key, byte[] value) {
        putCount++;
        final ByteArray keyBytes = new ByteArray(key);
        overwriteIfExist(keyBytes, new ByteArrayPutOp(keyBytes, value));
      }

      void delete(byte[] key) {
        delCount++;
        final ByteArray keyBytes = new ByteArray(key);
        overwriteIfExist(keyBytes, new DeleteOp(keyBytes));
      }

      void deleteRange(byte[] startKey, byte[] endKey) {
        delRangeCount++;
        batchOps.put(opIndex.getAndIncrement(), new DeleteRangeOp(new ByteArray(startKey), new ByteArray(endKey)));
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
