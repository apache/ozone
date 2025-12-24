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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
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
  static final class Bytes implements Closeable {
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
  }

  /**
   * Delete operation to be applied to a {@link ColumnFamily} batch.
   */
  private final class DeleteOperation extends Operation {
    private final CodecBuffer key;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private DeleteOperation(CodecBuffer key, Bytes keyBytes) {
      super(keyBytes);
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

  /** Cache and deduplicate db ops (put/delete). */
  private class OpCache {
    /** A (family name -> {@link FamilyCache}) map. */
    private final Map<String, FamilyCache> name2cache = new HashMap<>();

    /** A cache for a {@link ColumnFamily}. */
    private class FamilyCache {
      private final ColumnFamily family;

      /**
       * A mapping of keys to operations for batch processing in the {@link FamilyCache}.
       * The keys are represented as {@link Bytes} objects, encapsulating the byte array or buffer
       * for efficient equality and hashing. The values are instances of {@link Operation}, representing
       * different types of operations that can be applied to a {@link ColumnFamily}.
       *
       * This field is intended to store pending batch updates before they are written to the database.
       * It supports operations such as additions and deletions while maintaining the ability to overwrite
       * existing entries when necessary.
       */
      private final Map<Bytes, Operation> batchOps = new HashMap<>();
      private boolean isCommit;

      private long batchSize;
      private long discardedSize;
      private int discardedCount;
      private int putCount;
      private int delCount;

      FamilyCache(ColumnFamily family) {
        this.family = family;
      }

      /** Prepare batch write for the entire family. */
      void prepareBatchWrite() throws RocksDatabaseException {
        Preconditions.checkState(!isCommit, "%s is already committed.", this);
        isCommit = true;
        for (Operation op : batchOps.values()) {
          op.apply(family, writeBatch);
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

      private void deleteIfExist(Bytes key) {
        final Operation previous = batchOps.remove(key);
        if (previous != null) {
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
        Operation overwritten = batchOps.put(key, operation);
        Preconditions.checkState(overwritten == null);

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
        Bytes keyBytes = new Bytes(key);
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

      for (FamilyCache f : name2cache.values()) {
        putCount += f.putCount;
        delCount += f.delCount;
        opSize += f.batchSize;
        discardedCount += f.discardedCount;
        discardedSize += f.discardedSize;
      }

      final int opCount = putCount + delCount;
      return String.format(
          "#put=%s, #del=%s, batchSize: %s, discarded: %s, committed: %s",
          putCount, delCount,
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
}
