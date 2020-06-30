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

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import org.iq80.leveldb.DBIterator;

/**
 * RocksDB store iterator.
 */
public class LDBStoreIterator
        implements TableIterator<byte[], ByteArrayKeyValue> {

  private final DBIterator ldbIterator;

  public LDBStoreIterator(DBIterator iterator) {
    this.ldbIterator = iterator;
    ldbIterator.seekToFirst();
  }

  @Override
  public void forEachRemaining(
          Consumer<? super ByteArrayKeyValue> action) {
    while (hasNext()) {
      action.accept(next());
    }
  }

  @Override
  public boolean hasNext() {
    return ldbIterator.hasNext();
  }

  @Override
  public ByteArrayKeyValue next() {
    if (ldbIterator.hasNext()) {
      ByteArrayKeyValue value = toByteArrayKeyValue(ldbIterator.peekNext());
      ldbIterator.next();
      return value;
    }
    throw new NoSuchElementException("LevelDB Store has no more elements");
  }

  @Override
  public void seekToFirst() {
    ldbIterator.seekToFirst();
  }

  @Override
  public void seekToLast() {
    ldbIterator.seekToLast();
  }

  @Override
  public ByteArrayKeyValue seek(byte[] key) {
    ldbIterator.seek(key);
    if (ldbIterator.hasNext()) {
      return toByteArrayKeyValue(ldbIterator.peekNext());
    }
    return null;
  }

  @Override
  public byte[] key() {
    if (ldbIterator.hasNext()) {
      return ldbIterator.peekNext().getKey();
    }
    return null;
  }

  @Override
  public ByteArrayKeyValue value() {
    if (ldbIterator.hasNext()) {
      return toByteArrayKeyValue(ldbIterator.peekNext());
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    ldbIterator.close();
  }

  private ByteArrayKeyValue toByteArrayKeyValue(Map.Entry<byte[], byte[]> entry) {
      return ByteArrayKeyValue.create(entry.getKey(), entry.getValue());
  }
}
