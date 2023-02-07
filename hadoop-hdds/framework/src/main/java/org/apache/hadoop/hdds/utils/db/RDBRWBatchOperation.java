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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.rocksdb.ColumnFamilyHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator.managed;

/**
 * Read Write Batch operation implementation for rocks db.
 */
public class RDBRWBatchOperation implements RWBatchOperation {
  public static final Logger LOG =
      LoggerFactory.getLogger(RDBRWBatchOperation.class);
  private final ManagedReadWriteBatch writeBatch;
  
  private final AtomicLong refCount = new AtomicLong(1);
  
  public RDBRWBatchOperation() {
    writeBatch = new ManagedReadWriteBatch();
  }

  @Override
  public void commit(RocksDatabase db) throws IOException {
    db.batchWrite(writeBatch);
  }

  @Override
  public void commit(RocksDatabase db, ManagedWriteOptions writeOptions)
      throws IOException {
    db.batchWrite(writeBatch, writeOptions);
  }

  @Override
  public void close() {
    decrementRefCount();
  }

  @Override
  public void delete(ColumnFamily family, byte[] key) throws IOException {
    family.batchDelete(writeBatch, key);
  }

  @Override
  public void put(ColumnFamily family, byte[] key, byte[] value)
      throws IOException {
    family.batchPut(writeBatch, key, value);
  }

  @Override
  public ManagedRocksIterator newIteratorWithBase(
      ColumnFamilyHandle handle, ManagedRocksIterator newIterator)
      throws IOException {
    return managed(writeBatch.newIteratorWithBase(handle, newIterator.get()));
  }

  @Override
  public void incrementRefCount() {
    refCount.getAndIncrement();
  }

  @Override
  public void decrementRefCount() {
    if (refCount.decrementAndGet() == 0) {
      writeBatch.close();
    }
  }
}
