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

import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.ratis.util.function.CheckedFunction;

/**
 * RocksDB store iterator using the byte[] API.
 */
class RDBStoreByteArrayIterator extends RDBStoreAbstractIterator<byte[]> {

  RDBStoreByteArrayIterator(
      CheckedFunction<ManagedReadOptions, ManagedRocksIterator, RocksDatabaseException> itrSupplier,
      RDBTable table, byte[] prefix, IteratorType type) throws RocksDatabaseException {
    super(itrSupplier, table, prefix, type);
    seekToFirst();
  }

  @Override
  Table.KeyValue<byte[], byte[]> getKeyValue() {
    final ManagedRocksIterator i = getRocksDBIterator();
    final byte[] key = getType().readKey() ? i.get().key() : null;
    final byte[] value = getType().readValue() ? i.get().value() : null;
    return Table.newKeyValue(key, value);
  }

  @Override
  void seek0(byte[] key) {
    getRocksDBIterator().get().seek(key);
  }

  @Override
  void delete(byte[] key) throws RocksDatabaseException {
    getRocksDBTable().delete(key);
  }
}
