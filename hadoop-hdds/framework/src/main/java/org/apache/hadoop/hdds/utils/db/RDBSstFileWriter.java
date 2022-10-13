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
 */
package org.apache.hadoop.hdds.utils.db;

import org.apache.hadoop.hdds.utils.db.managed.ManagedEnvOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileWriter;
import org.rocksdb.RocksDBException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.toIOException;

/**
 * DumpFileWriter using rocksdb sst files.
 */
public class RDBSstFileWriter implements DumpFileWriter, Closeable {

  private ManagedSstFileWriter sstFileWriter;
  private File sstFile;
  private AtomicLong keyCounter;
  private ManagedOptions emptyOption = new ManagedOptions();
  private final ManagedEnvOptions emptyEnvOptions = new ManagedEnvOptions();

  public RDBSstFileWriter() {
    this.sstFileWriter = new ManagedSstFileWriter(emptyEnvOptions, emptyOption);
    this.keyCounter = new AtomicLong(0);
  }

  @Override
  public void open(File externalFile) throws IOException {
    this.sstFile = externalFile;
    try {
      // Here will create a new sst file each time, not append to existing
      sstFileWriter.open(sstFile.getAbsolutePath());
    } catch (RocksDBException e) {
      closeOnFailure();
      throw toIOException("Failed to open external file for dump "
          + sstFile.getAbsolutePath(), e);
    }
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    try {
      sstFileWriter.put(key, value);
      keyCounter.incrementAndGet();
    } catch (RocksDBException e) {
      closeOnFailure();
      throw toIOException("Failed to put kv into dump file "
          + sstFile.getAbsolutePath(), e);
    }
  }

  @Override
  public void close() throws IOException {
    if (sstFileWriter != null) {
      try {
        // We should check for empty sst file, or we'll get exception.
        if (keyCounter.get() > 0) {
          sstFileWriter.finish();
        }
      } catch (RocksDBException e) {
        throw toIOException("Failed to finish dumping into file "
            + sstFile.getAbsolutePath(), e);
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