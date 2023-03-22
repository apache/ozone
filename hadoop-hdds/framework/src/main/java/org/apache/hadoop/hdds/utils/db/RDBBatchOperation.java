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
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TraditionalBinaryPrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Batch operation implementation for rocks db.
 */
public class RDBBatchOperation implements BatchOperation {
  static final Logger LOG = LoggerFactory.getLogger(RDBBatchOperation.class);

  private static final Object DELETE_OP = new Object();

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
   * To implement {@link #equals(Object)} and {@link #hashCode()}
   * based on the contents of {@link #bytes}.
   * <p>
   * Note that it is incorrect to directly use
   * {@link #bytes#equals(Object)} and {@link #bytes#hashCode()} here since
   * they do not use the contents of {@link #bytes} in the computations.
   * These methods simply inherit from {@link Object).
   */
  private static final class ByteArray {
    private final byte[] bytes;

    ByteArray(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final ByteArray that = (ByteArray) obj;
      return Arrays.equals(this.bytes, that.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }

  /** Cache and deduplicate db ops (put/delete). */
  private class OpCache {
    /** A cache for a {@link ColumnFamily}. */
    private class FamilyCache {
      private final ColumnFamily family;
      /**
       * A (dbKey -> dbValue) map, where the dbKey type is {@link ByteArray}
       * (see the javadoc of ByteArray) and the dbValue type is {@link Object}.
       * When dbValue is a byte[], it represents a put-op.
       * Otherwise, it represents a delete-op (dbValue is {@link #DELETE_OP)}).
       */
      private final Map<ByteArray, Object> ops = new HashMap<>();
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
        for (Map.Entry<ByteArray, Object> op : ops.entrySet()) {
          final byte[] key = op.getKey().bytes;
          final Object value = op.getValue();
          if (value instanceof byte[]) {
            family.batchPut(writeBatch, key, (byte[])value);
          } else {
            family.batchDelete(writeBatch, key);
          }
        }
        ops.clear();

        debug(() -> String.format("  %s %s, #put=%s, #del=%s", this,
            batchSizeDiscardedString(), putCount, delCount));
      }

      void putOrDelete(byte[] key, Object val) {
        Preconditions.checkState(!isCommit, "%s is already committed.", this);
        final int keyLen = key.length;
        final int valLen = val instanceof byte[] ? ((byte[]) val).length : 0;
        batchSize += keyLen + valLen;

        final Object previousVal = ops.put(new ByteArray(key), val);
        if (previousVal != null) {
          final boolean isPut = previousVal instanceof byte[];
          final int preLen = isPut ? ((byte[]) previousVal).length : 0;
          discardedSize += keyLen + preLen;
          discardedCount++;
          debug(() -> String.format("%s overwriting a previous %s", this,
              isPut ? "put (value: " + byteSize2String(preLen) + ")" : "del"));
        }

        debug(() -> String.format("%s %s, %s; key=%s", this,
            valLen == 0 ? delString(keyLen) : putString(keyLen, valLen),
            batchSizeDiscardedString(),
            StringUtils.bytes2HexString(key).toUpperCase()));
      }

      void put(byte[] key, byte[] value) {
        putCount++;
        putOrDelete(key, value);
      }

      void delete(byte[] key) {
        delCount++;
        putOrDelete(key, DELETE_OP);
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

    void put(ColumnFamily f, byte[] key, byte[] value) {
      name2cache.computeIfAbsent(f.getName(), k -> new FamilyCache(f))
          .put(key, value);
    }

    void delete(ColumnFamily family, byte[] key) {
      name2cache.computeIfAbsent(family.getName(), k -> new FamilyCache(family))
          .delete(key);
    }

    /** Prepare batch write for the entire cache. */
    void prepareBatchWrite() throws IOException {
      for (Map.Entry<String, FamilyCache> e : name2cache.entrySet()) {
        e.getValue().prepareBatchWrite();
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
    opCache.prepareBatchWrite();
    db.batchWrite(writeBatch);
  }

  public void commit(RocksDatabase db, ManagedWriteOptions writeOptions)
      throws IOException {
    debug(() -> String.format("%s: commit-with-writeOptions %s",
        name, opCache.getCommitString()));
    opCache.prepareBatchWrite();
    db.batchWrite(writeBatch, writeOptions);
  }

  @Override
  public void close() {
    debug(() -> String.format("%s: close", name));
    writeBatch.close();
  }

  public void delete(ColumnFamily family, byte[] key) throws IOException {
    opCache.delete(family, key);
  }

  public void put(ColumnFamily family, byte[] key, byte[] value)
      throws IOException {
    opCache.put(family, key, value);
  }
}
