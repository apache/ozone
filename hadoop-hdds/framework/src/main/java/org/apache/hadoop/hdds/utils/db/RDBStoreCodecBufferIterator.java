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
}
