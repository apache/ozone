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

import org.iq80.leveldb.DBException;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.WriteOptions;

import java.io.IOException;

/**
 * Batch operation implementation for rocks db.
 */
public class LDBBatchOperation implements BatchOperation {

  private final WriteBatch writeBatch;

  public LDBBatchOperation(DB levelDB) {
    writeBatch = levelDB.createWriteBatch();
  }

  public LDBBatchOperation(WriteBatch writeBatch) {
    this.writeBatch = writeBatch;
  }

  public void commit(DB db, WriteOptions writeOptions) throws IOException {
    try {
      db.write(writeBatch, writeOptions);
    } catch (DBException e) {
      throw new IOException("Unable to write the batch.", e);
    }
  }

  @Override
  public void close() {
    try {
      writeBatch.close();
    }
    catch (IOException e) {
      // TODO : Determine how to handle this exception since the parent interface did not declare
      //  any exceptions thrown from this method (unlike its parent) and adding the exception
      //  would break compatibility with the classes already using it.
    }
  }

  public void delete(byte[] key) throws IOException {
    try {
      writeBatch.delete(key);
    } catch (DBException e) {
      throw new IOException("Can't record batch delete operation.", e);
    }
  }

  public void put(byte[] key, byte[] value)
          throws IOException {
    try {
      writeBatch.put(key, value);
    } catch (DBException e) {
      throw new IOException("Can't record batch put operation.", e);
    }
  }
}
