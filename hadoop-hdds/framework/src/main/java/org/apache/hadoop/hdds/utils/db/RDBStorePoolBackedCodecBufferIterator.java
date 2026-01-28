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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hdds.utils.db.Table.CloseableKeyValue;
import org.apache.hadoop.hdds.utils.db.Table.CloseableKeyValueIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.ratis.util.function.CheckedFunction;

/**
 * A concrete implementation of {@link RDBStoreAbstractIterator} that provides an iterator
 * for {@link CodecBuffer} keys and values managed within a RocksDB store. The iterator
 * leverages an object pool to manage reusable {@link CloseableKeyValue<Buffer, Buffer>} objects,
 * enabling efficient memory and resource management by reusing buffers for keys and values
 * during iteration.
 *
 * This iterator supports operations such as seeking to a specific key, retrieving key-value
 * pairs from the database, and removing entries from the database, while encapsulating
 * all RocksDB-specific logic internally.
 */
class RDBStorePoolBackedCodecBufferIterator extends RDBStoreAbstractIterator<CodecBuffer,
    CloseableKeyValue<CodecBuffer, CodecBuffer>> implements CloseableKeyValueIterator<CodecBuffer, CodecBuffer> {

  private final GenericObjectPool<CloseableKeyValue<Buffer, Buffer>> kvBufferPool;

  RDBStorePoolBackedCodecBufferIterator(
      CheckedFunction<ManagedReadOptions, ManagedRocksIterator, RocksDatabaseException> itrSupplier, RDBTable table,
      byte[] prefix, IteratorType type, int maxNumberOfObjectsNeededConcurrently)
      throws RocksDatabaseException {
    super(itrSupplier, table, prefix, type);
    GenericObjectPoolConfig<CloseableKeyValue<Buffer, Buffer>> config = new GenericObjectPoolConfig<>();
    config.setMaxTotal(Math.max(maxNumberOfObjectsNeededConcurrently, 1));
    config.setBlockWhenExhausted(true);

    final String name = table != null ? table.getName() : null;
    this.kvBufferPool = new GenericObjectPool<>(new KeyValueBufferFactory(name), config);
    seekToFirst();
  }

  private final class KeyValueBufferFactory extends BasePooledObjectFactory<CloseableKeyValue<Buffer, Buffer>> {

    private final String iteratorName;

    private KeyValueBufferFactory(String iteratorName) {
      this.iteratorName = iteratorName;
    }

    @Override
    public CloseableKeyValue<Buffer, Buffer> create() {
      Buffer keyBuffer = new Buffer(
          new CodecBuffer.Capacity(iteratorName + "-iterator-key", 1 << 10),
          getType().readKey() ? buffer -> getRocksDBIterator().get().key(buffer) : null);
      Buffer valueBuffer = new Buffer(
          new CodecBuffer.Capacity(iteratorName + "-iterator-value", 4 << 10),
          getType().readValue() ? buffer -> getRocksDBIterator().get().value(buffer) : null);
      Runnable closeAction = () -> {
        keyBuffer.release();
        valueBuffer.release();
      };
      return Table.newCloseableKeyValue(keyBuffer, valueBuffer, closeAction);
    }

    @Override
    public void destroyObject(PooledObject<CloseableKeyValue<Buffer, Buffer>> p) {
      p.getObject().close();
    }

    @Override
    public PooledObject<CloseableKeyValue<Buffer, Buffer>> wrap(
        CloseableKeyValue<Buffer, Buffer> bufferBufferCloseableKeyValue) {
      return new DefaultPooledObject<>(bufferBufferCloseableKeyValue);
    }
  }

  @Override
  Table.CloseableKeyValue<CodecBuffer, CodecBuffer> getKeyValue() {
    try {
      AtomicBoolean closed = new AtomicBoolean(false);
      CloseableKeyValue<Buffer, Buffer> kvBuffers = kvBufferPool.borrowObject();
      return Table.newCloseableKeyValue(kvBuffers.getKey().getFromDb(), kvBuffers.getValue().getFromDb(),
          () -> {
            // To handle multiple close calls.
            if (closed.compareAndSet(false, true)) {
              kvBufferPool.returnObject(kvBuffers);
            }
          });
    } catch (Exception e) {
      // Ideally, this should never happen since we have a generic object pool where Buffers would always be created
      // and the wait for object borrow never times out.
      throw new IllegalStateException("Failed to borrow key/value buffer from pool", e);
    }
  }

  @Override
  void seek0(CodecBuffer key) {
    getRocksDBIterator().get().seek(key.asReadOnlyByteBuffer());
  }

  @Override
  void delete(CodecBuffer key) throws RocksDatabaseException {
    getRocksDBTable().delete(key.asReadOnlyByteBuffer());
  }

  @Override
  public void close() {
    super.close();
    this.kvBufferPool.close();
  }
}
