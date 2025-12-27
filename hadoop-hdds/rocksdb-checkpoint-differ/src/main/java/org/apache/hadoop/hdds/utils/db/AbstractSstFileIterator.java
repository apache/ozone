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

import java.util.function.Function;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReader;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReaderIterator;
import org.apache.hadoop.ozone.util.ClosableIterator;
import org.rocksdb.RocksDBException;

/**
 * ManagedSstFileIterator is an abstract class designed to provide a managed, resource-safe
 * iteration over SST (Sorted String Table) files leveraging RocksDB. It implements the
 * {@link ClosableIterator} interface to support resource management and ensures proper
 * cleanup of resources upon closure. This class binds together a ManagedSstFileReader,
 * ManagedSstFileReaderIterator, and Buffers for keys and values, while allowing specific
 * implementations to define how the iterator values are derived.
 *
 * @param <T> The type of the element to be returned by the iterator.
 */
abstract class AbstractSstFileIterator<T> implements ClosableIterator<T> {
  private final ManagedSstFileReader fileReader;
  private final ManagedSstFileReaderIterator fileReaderIterator;
  private final IteratorType type;
  private final ManagedOptions options;
  private boolean closed;
  private final Buffer keyBuffer;
  private final Buffer valueBuffer;

  AbstractSstFileIterator(String path, ManagedOptions options, IteratorType type,
      Function<ManagedSstFileReader, ManagedSstFileReaderIterator> itrInitFunction) throws RocksDatabaseException {
    try {
      this.fileReader = new ManagedSstFileReader(options);
      this.fileReader.open(path);
      this.fileReaderIterator = itrInitFunction.apply(fileReader);
      fileReaderIterator.get().seekToFirst();
      this.closed = false;
      this.type = type;
      this.keyBuffer = new Buffer(
          new CodecBuffer.Capacity(path + " iterator-key", 1 << 10),
          this.type.readKey() ? buffer -> fileReaderIterator.get().key(buffer) : null);
      this.valueBuffer = new Buffer(
          new CodecBuffer.Capacity(path + " iterator-value", 4 << 10),
          this.type.readValue() ? buffer -> fileReaderIterator.get().value(buffer) : null);
      this.options = options;
    } catch (RocksDBException e) {
      throw new RocksDatabaseException("Failed to open SST file: " + path, e);
    }
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      this.fileReaderIterator.close();
      this.fileReader.close();
      keyBuffer.release();
      valueBuffer.release();
    }
    closed = true;
  }

  @Override
  public synchronized boolean hasNext() {
    return fileReaderIterator.get().isValid();
  }

  abstract T getIteratorValue(CodecBuffer key, CodecBuffer value);

  @Override
  public synchronized T next() {
    T value = getIteratorValue(this.type.readKey() ? keyBuffer.getFromDb() : null,
        this.type.readValue() ? valueBuffer.getFromDb() : null);
    fileReaderIterator.get().next();
    return value;
  }

  ManagedOptions getOptions() {
    return options;
  }
}
