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

import java.io.Closeable;
import java.io.File;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDirectSlice;
import org.apache.hadoop.hdds.utils.db.managed.ManagedEnvOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileWriter;
import org.rocksdb.RocksDBException;

/**
 * DumpFileWriter using rocksdb sst files.
 */
public class RDBSstFileWriter implements Closeable {

  private ManagedSstFileWriter sstFileWriter;
  private File sstFile;
  private AtomicLong keyCounter;
  private ManagedOptions emptyOption = new ManagedOptions();
  private final ManagedEnvOptions emptyEnvOptions = new ManagedEnvOptions();

  public RDBSstFileWriter(File externalFile) throws RocksDatabaseException {
    this.sstFileWriter = new ManagedSstFileWriter(emptyEnvOptions, emptyOption);
    this.keyCounter = new AtomicLong(0);
    this.sstFile = externalFile;
    try {
      // Here will create a new sst file each time, not append to existing
      sstFileWriter.open(sstFile.getAbsolutePath());
    } catch (RocksDBException e) {
      closeOnFailure();
      throw new RocksDatabaseException("Failed to open " + sstFile, e);
    }
  }

  public void put(byte[] key, byte[] value) throws RocksDatabaseException {
    try {
      sstFileWriter.put(key, value);
      keyCounter.incrementAndGet();
    } catch (RocksDBException e) {
      closeOnFailure();
      throw new RocksDatabaseException("Failed to put key (length=" + key.length
          + ") and value (length=" + value.length + "), sstFile=" + sstFile.getAbsolutePath(), e);
    }
  }

  public void put(CodecBuffer key, CodecBuffer value) throws RocksDatabaseException {
    try {
      sstFileWriter.put(key.asReadOnlyByteBuffer(), value.asReadOnlyByteBuffer());
      keyCounter.incrementAndGet();
    } catch (RocksDBException e) {
      closeOnFailure();
      throw new RocksDatabaseException("Failed to put key (length=" + key.readableBytes()
          + ") and value (length=" + value.readableBytes() + "), sstFile=" + sstFile.getAbsolutePath(), e);
    }
  }

  public void delete(byte[] key) throws RocksDatabaseException {
    try {
      sstFileWriter.delete(key);
      keyCounter.incrementAndGet();
    } catch (RocksDBException e) {
      closeOnFailure();
      throw new RocksDatabaseException("Failed to delete key (length=" + key.length
          + "), sstFile=" + sstFile.getAbsolutePath(), e);
    }
  }

  public void delete(CodecBuffer key) throws RocksDatabaseException {
    try (ManagedDirectSlice slice = new ManagedDirectSlice(key.asReadOnlyByteBuffer())) {
      sstFileWriter.delete(slice);
      keyCounter.incrementAndGet();
    } catch (RocksDBException e) {
      closeOnFailure();
      throw new RocksDatabaseException("Failed to delete key (length=" + key.readableBytes()
          + "), sstFile=" + sstFile.getAbsolutePath(), e);
    }
  }

  @Override
  public void close() throws RocksDatabaseException {
    if (sstFileWriter != null) {
      try {
        // We should check for empty sst file, or we'll get exception.
        if (keyCounter.get() > 0) {
          sstFileWriter.finish();
        }
      } catch (RocksDBException e) {
        throw new RocksDatabaseException("Failed to finish writing to " + sstFile, e);
      } finally {
        closeResources();
      }

      keyCounter.set(0);
    }
  }

  private void closeResources() {
    sstFileWriter.close();
    sstFileWriter = null;
    emptyOption.close();
    emptyEnvOptions.close();
  }

  private void closeOnFailure() {
    if (sstFileWriter != null) {
      closeResources();
    }
  }
}
