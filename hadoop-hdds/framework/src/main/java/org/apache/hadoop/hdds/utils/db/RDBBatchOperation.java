/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.utils.db;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.ratis.util.TraditionalBinaryPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Batch operation implementation for rocks db.
 * Note that a {@link RDBBatchOperation} object only for one batch.
 * Also, this class is not threadsafe.
 */
public class RDBBatchOperation implements BatchOperation {
  static final Logger LOG = LoggerFactory.getLogger(RDBBatchOperation.class);

  private enum Op { DELETE }

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
   * The key type of {@link RDBBatchOperation.OpCache.FamilyCache#ops}.
   * To implement {@link #equals(Object)} and {@link #hashCode()}
   * based on the contents of {@link #buffer}.
   */
  private static final class Bytes {
    private final byte[] array;
    private final CodecBuffer buffer;
    /** Cache the hash value. */
    private final int hash;

    Bytes(CodecBuffer buffer) {
      this.array = null;
      this.buffer = Objects.requireNonNull(buffer, "buffer == null");
      this.hash = buffer.hashCode();
    }

    Bytes(byte[] array) {
      this.array = array;
      this.buffer = null;
      this.hash = Arrays.hashCode(array);
    }

    byte[] array() {
      return array;
    }

    ByteBuffer asReadOnlyByteBuffer() {
      return buffer.asReadOnlyByteBuffer();
    }

    void release() {
      buffer.release();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final Bytes that = (Bytes) obj;
      if (this.hash != that.hash) {
        return false;
      }
      if (this.array != null) {
        return Arrays.equals(this.array, that.array);
      }
      return this.asReadOnlyByteBuffer().equals(that.asReadOnlyByteBuffer());
    }

    @Override
    public int hashCode() {
      return hash;
    }
  }

  /** Cache and deduplicate db ops (put/delete). */
  private class OpCache {
    /** A cache for a {@link ColumnFamily}. */
    private class FamilyCache {
      private final ColumnFamily family;
      /**
       * A (dbKey -> dbValue) map, where the dbKey type is {@link Bytes}
       * and the dbValue type is {@link Object}.
       * When dbValue is a byte[]/{@link ByteBuffer}, it represents a put-op.
       * Otherwise, it represents a delete-op (dbValue is {@link Op#DELETE}).
       */
      private final Map<Bytes, Object> ops = new HashMap<>();
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
      void prepareBatchWrite() throws IOException {
        Preconditions.checkState(!isCommit, "%s is already committed.", this);
        isCommit = true;
        for (Map.Entry<Bytes, Object> op : ops.entrySet()) {
          final Bytes key = op.getKey();
          final Object value = op.getValue();
          if (value instanceof byte[]) {
            family.batchPut(writeBatch, key.array(), (byte[]) value);
          } else if (value instanceof CodecBuffer) {
            final CodecBuffer buffer = (CodecBuffer) value;
            // always release the key with the value
            buffer.getReleased().thenAccept(r -> key.release());
            family.batchPut(writeBatch, key.asReadOnlyByteBuffer(),
                buffer.asReadOnlyByteBuffer());
          } else if (value == Op.DELETE) {
            family.batchDelete(writeBatch, key.array());
          } else {
            throw new IllegalStateException("Unexpected value: " + value
                + ", class=" + value.getClass().getSimpleName());
          }
        }

        debug(() -> String.format("  %s %s, #put=%s, #del=%s", this,
            batchSizeDiscardedString(), putCount, delCount));
      }

      void clear() {
        for (Object value : ops.values()) {
          if (value instanceof CodecBuffer) {
            // by the setup, the key will also be released
            ((CodecBuffer) value).release();
          }
        }
        ops.clear();
      }

      void putOrDelete(byte[] key, byte[] val) {
        putOrDelete(new Bytes(key), key.length, val, val.length);
      }

      void putOrDelete(CodecBuffer key, CodecBuffer val) {
        putOrDelete(new Bytes(key), key.readableBytes(),
            val, val.readableBytes());
      }

      void putOrDelete(Bytes key, int keyLen, Object val, int valLen) {
        Preconditions.checkState(!isCommit, "%s is already committed.", this);
        batchSize += keyLen + valLen;
        // remove previous first in order to do release
        final Object previous = ops.remove(key);
        if (previous != null) {
          final boolean isPut = previous != Op.DELETE;
          final int preLen;
          if (!isPut) {
            preLen = 0;
          } else if (previous instanceof CodecBuffer) {
            final CodecBuffer buffer = (CodecBuffer) previous;
            preLen = buffer.readableBytes();
            buffer.release(); // key will also be released
          } else if (previous instanceof byte[]) {
            preLen = ((byte[]) previous).length;
          } else {
            throw new IllegalStateException("Unexpected previous: " + previous
                + ", class=" + previous.getClass().getSimpleName());
          }
          discardedSize += keyLen + preLen;
          discardedCount++;
          debug(() -> String.format("%s overwriting a previous %s", this,
              isPut ? "put (value: " + byteSize2String(preLen) + ")" : "del"));
        }
        ops.put(key, val);

        debug(() -> String.format("%s %s, %s; key=%s", this,
            valLen == 0 ? delString(keyLen) : putString(keyLen, valLen),
            batchSizeDiscardedString(), null));
      }

      void put(CodecBuffer key, CodecBuffer value) {
        putCount++;
        putOrDelete(key, value);
      }

      void put(byte[] key, byte[] value) {
        putCount++;
        putOrDelete(key, value);
      }

      void delete(byte[] key) {
        delCount++;
        putOrDelete(new Bytes(key), key.length, Op.DELETE, 0);
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

    /** A (family name -> {@link FamilyCache}) map. */
    private final Map<String, FamilyCache> name2cache = new HashMap<>();

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

    /** Prepare batch write for the entire cache. */
    Closeable prepareBatchWrite() throws IOException {
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

  private static final AtomicInteger BATCH_COUNT = new AtomicInteger();

  private final String name = "Batch-" + BATCH_COUNT.getAndIncrement();
  private final ManagedWriteBatch writeBatch;
  private final OpCache opCache = new OpCache();

  public RDBBatchOperation() {
    writeBatch = new ManagedWriteBatch();
  }

  public RDBBatchOperation(ManagedWriteBatch writeBatch) {
    this.writeBatch = writeBatch;
  }

  @Override
  public String toString() {
    return name;
  }

  public void commit(RocksDatabase db) throws IOException {
    debug(() -> String.format("%s: commit %s",
        name, opCache.getCommitString()));
    try (Closeable autoClear = opCache.prepareBatchWrite()) {
      db.batchWrite(writeBatch);
    }
  }

  public void commit(RocksDatabase db, ManagedWriteOptions writeOptions)
      throws IOException {
    debug(() -> String.format("%s: commit-with-writeOptions %s",
        name, opCache.getCommitString()));
    try (Closeable autoClear = opCache.prepareBatchWrite()) {
      db.batchWrite(writeBatch, writeOptions);
    }
  }

  @Override
  public void close() {
    debug(() -> String.format("%s: close", name));
    writeBatch.close();
  }

  public void delete(ColumnFamily family, byte[] key) throws IOException {
    opCache.delete(family, key);
  }

  public void put(ColumnFamily family, CodecBuffer key, CodecBuffer value)
      throws IOException {
    opCache.put(family, key, value);
  }

  public void put(ColumnFamily family, byte[] key, byte[] value)
      throws IOException {
    opCache.put(family, key, value);
  }
}
