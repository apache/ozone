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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
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

  private enum Op { DELETE }

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
   * The key type of {@link RDBBatchOperation.OpCache.FamilyCache#ops}.
   * To implement {@link #equals(Object)} and {@link #hashCode()}
   * based on the contents of the bytes.
   */
  static final class Bytes {
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

    byte[] array() {
      return array;
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
  }

  /** Cache and deduplicate db ops (put/delete). */
  private class OpCache {
    /** A (family name -> {@link FamilyCache}) map. */
    private final Map<String, FamilyCache> name2cache = new HashMap<>();

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
      void prepareBatchWrite() throws RocksDatabaseException {
        Preconditions.checkState(!isCommit, "%s is already committed.", this);
        isCommit = true;
        for (Map.Entry<Bytes, Object> op : ops.entrySet()) {
          final Bytes key = op.getKey();
          final Object value = op.getValue();
          if (value instanceof byte[]) {
            family.batchPut(writeBatch, key.array(), (byte[]) value);
          } else if (value instanceof CodecBuffer) {
            family.batchPut(writeBatch, key.asReadOnlyByteBuffer(),
                ((CodecBuffer) value).asReadOnlyByteBuffer());
          } else if (value == Op.DELETE) {
            family.batchDelete(writeBatch, key.array());
          } else {
            throw new IllegalStateException("Unexpected value: " + value
                + ", class=" + value.getClass().getSimpleName());
          }
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

        for (Object value : ops.values()) {
          if (value instanceof CodecBuffer) {
            ((CodecBuffer) value).release(); // the key will also be released
          }
        }
        ops.clear();

        if (warn) {
          LOG.warn("discarding changes {}", details);
        }
      }

      void putOrDelete(Bytes key, int keyLen, Object val, int valLen) {
        Preconditions.checkState(!isCommit, "%s is already committed.", this);
        batchSize += keyLen + valLen;
        // remove previous first in order to call release()
        final Object previous = ops.remove(key);
        if (previous != null) {
          final boolean isPut = previous != Op.DELETE;
          final int preLen;
          if (!isPut) {
            preLen = 0;
          } else if (previous instanceof CodecBuffer) {
            final CodecBuffer previousValue = (CodecBuffer) previous;
            preLen = previousValue.readableBytes();
            previousValue.release(); // key will also be released
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
        final Object overwritten = ops.put(key, val);
        Preconditions.checkState(overwritten == null);

        debug(() -> String.format("%s %s, %s; key=%s", this,
            valLen == 0 ? delString(keyLen) : putString(keyLen, valLen),
            batchSizeDiscardedString(), key));
      }

      void put(CodecBuffer key, CodecBuffer value) {
        putCount++;

        // always release the key with the value
        value.getReleaseFuture().thenAccept(v -> key.release());
        putOrDelete(new Bytes(key), key.readableBytes(),
            value, value.readableBytes());
      }

      void put(byte[] key, byte[] value) {
        putCount++;
        putOrDelete(new Bytes(key), key.length, value, value.length);
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

  public void put(ColumnFamily family, CodecBuffer key, CodecBuffer value) {
    opCache.put(family, key, value);
  }

  public void put(ColumnFamily family, byte[] key, byte[] value) {
    opCache.put(family, key, value);
  }
}
