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
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.Table.KeyValueIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedFunction;

/**
 * Implement {@link RDBStoreAbstractIterator} using {@link CodecBuffer}.
 */
class RDBStoreCodecBufferIterator extends RDBStoreAbstractIterator<CodecBuffer, KeyValue<CodecBuffer, CodecBuffer>>
    implements KeyValueIterator<CodecBuffer, CodecBuffer> {

  private final Buffer keyBuffer;
  private final Buffer valueBuffer;
  private final AtomicBoolean closed = new AtomicBoolean();

  RDBStoreCodecBufferIterator(
      CheckedFunction<ManagedReadOptions, ManagedRocksIterator, RocksDatabaseException> itrSupplier, RDBTable table,
      byte[] prefix, IteratorType type) throws RocksDatabaseException {
    super(itrSupplier, table, prefix, type);

    final String name = table != null ? table.getName() : null;
    this.keyBuffer = new Buffer(
        new CodecBuffer.Capacity(name + "-iterator-key", 1 << 10),
        getType().readKey() ? buffer -> getRocksDBIterator().get().key(buffer) : null);
    this.valueBuffer = new Buffer(
        new CodecBuffer.Capacity(name + "-iterator-value", 4 << 10),
        getType().readValue() ? buffer -> getRocksDBIterator().get().value(buffer) : null);
    seekToFirst();
  }

  void assertOpen() {
    Preconditions.assertTrue(!closed.get(), "Already closed");
  }

  @Override
  KeyValue<CodecBuffer, CodecBuffer> getKeyValue() {
    assertOpen();
    return Table.newKeyValue(keyBuffer.getFromDb(), valueBuffer.getFromDb());
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
  public void close() {
    if (closed.compareAndSet(false, true)) {
      super.close();
      keyBuffer.release();
      valueBuffer.release();
    }
  }
}
