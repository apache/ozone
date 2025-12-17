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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.ratis.util.Preconditions;

/**
 * Implement {@link RDBStoreAbstractIterator} using {@link CodecBuffer}.
 */
class RDBStoreCodecBufferIterator extends RDBStoreAbstractIterator<CodecBuffer> {

  private final Buffer keyBuffer;
  private final Buffer valueBuffer;
  private final AtomicBoolean closed = new AtomicBoolean();

  RDBStoreCodecBufferIterator(ManagedRocksIterator iterator, RDBTable table,
      CodecBuffer prefix, IteratorType type) {
    super(iterator, table, prefix, type);

    final String name = table != null ? table.getName() : null;
    this.keyBuffer = new Buffer(
        new CodecBuffer.Capacity(name + "-iterator-key", 1 << 10),
        // it has to read key for matching prefix.
        getType().readKey() || prefix != null ? buffer -> getRocksDBIterator().get().key(buffer) : null);
    this.valueBuffer = new Buffer(
        new CodecBuffer.Capacity(name + "-iterator-value", 4 << 10),
        getType().readValue() ? buffer -> getRocksDBIterator().get().value(buffer) : null);
    seekToFirst();
  }

  void assertOpen() {
    Preconditions.assertTrue(!closed.get(), "Already closed");
  }

  @Override
  CodecBuffer key() {
    assertOpen();
    return keyBuffer.getFromDb();
  }

  @Override
  Table.KeyValue<CodecBuffer, CodecBuffer> getKeyValue() {
    assertOpen();
    final CodecBuffer key = getType().readKey() ? key() : null;
    return Table.newKeyValue(key, valueBuffer.getFromDb());
  }

  @Override
  void seek0(CodecBuffer key) {
    assertOpen();
    getRocksDBIterator().get().seek(key.asReadOnlyByteBuffer());
  }

  @Override
  void delete(CodecBuffer key) throws RocksDatabaseException {
    assertOpen();
    getRocksDBTable().delete(key.asReadOnlyByteBuffer());
  }

  @Override
  boolean startsWithPrefix(CodecBuffer key) {
    assertOpen();
    final CodecBuffer prefix = getPrefix();
    if (prefix == null) {
      return true;
    }
    if (key == null) {
      return false;
    }
    return key.startsWith(prefix);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      super.close();
      Optional.ofNullable(getPrefix()).ifPresent(CodecBuffer::release);
      keyBuffer.release();
      valueBuffer.release();
    }
  }

  static class Buffer {
    private final CodecBuffer.Capacity initialCapacity;
    private final PutToByteBuffer<RuntimeException> source;
    private CodecBuffer buffer;

    Buffer(CodecBuffer.Capacity initialCapacity,
           PutToByteBuffer<RuntimeException> source) {
      this.initialCapacity = initialCapacity;
      this.source = source;
    }

    void release() {
      if (buffer != null) {
        buffer.release();
      }
    }

    private void prepare() {
      if (buffer == null) {
        allocate();
      } else {
        buffer.clear();
      }
    }

    private void allocate() {
      if (buffer != null) {
        buffer.release();
      }
      buffer = CodecBuffer.allocateDirect(-initialCapacity.get());
    }

    CodecBuffer getFromDb() {
      if (source == null) {
        return null;
      }

      for (prepare(); ; allocate()) {
        final Integer required = buffer.putFromSource(source);
        if (required == null) {
          return null; // the source is unavailable
        } else if (required == buffer.readableBytes()) {
          return buffer; // buffer size is big enough
        }
        // buffer size too small, try increasing the capacity.
        if (buffer.setCapacity(required)) {
          buffer.clear();
          // retry with the new capacity
          final int retried = buffer.putFromSource(source);
          Preconditions.assertSame(required.intValue(), retried, "required");
          return buffer;
        }

        // failed to increase the capacity
        // increase initial capacity and reallocate it
        initialCapacity.increase(required);
      }
    }
  }
}
