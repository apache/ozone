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

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;

/**
 * RocksDB store iterator using the byte[] API.
 */
class RDBStoreByteArrayIterator extends RDBStoreAbstractIterator<byte[]> {
  private static final byte[] EMPTY = {};

  private final Type type;

  RDBStoreByteArrayIterator(ManagedRocksIterator iterator,
      RDBTable table, byte[] prefix, Type type) {
    super(iterator, table,
        prefix == null ? null : Arrays.copyOf(prefix, prefix.length));
    this.type = type;
    seekToFirst();
  }

  @Override
  byte[] key() {
    return getRocksDBIterator().get().key();
  }

  @Override
  Table.KeyValue<byte[], byte[]> getKeyValue() {
    final ManagedRocksIterator i = getRocksDBIterator();
    final byte[] key = type.readKey() ? i.get().key() : EMPTY;
    final byte[] value = type.readValue() ? i.get().value() : EMPTY;
    return Table.newKeyValue(key, value);
  }

  @Override
  void seek0(byte[] key) {
    getRocksDBIterator().get().seek(key);
  }

  @Override
  void delete(byte[] key) throws IOException {
    getRocksDBTable().delete(key);
  }

  @Override
  boolean startsWithPrefix(byte[] value) {
    final byte[] prefix = getPrefix();
    if (prefix == null) {
      return true;
    }
    if (value == null) {
      return false;
    }

    int length = prefix.length;
    if (value.length < length) {
      return false;
    }

    for (int i = 0; i < length; i++) {
      if (value[i] != prefix[i]) {
        return false;
      }
    }
    return true;
  }
}
