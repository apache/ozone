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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;

/**
 * Interface for performing batch operations on a RocksDB database.
 *
 * Provides methods to perform operations on a specific column family within
 * a database, such as inserting or deleting a key-value pair or deleting
 * a range of keys. These batch operations are designed for use in scenarios
 * where multiple database modifications need to be grouped together, ensuring
 * efficiency and atomicity.
 *
 * This interface extends {@link BatchOperation}, inheriting functionality for
 * managing batch sizes and allowing cleanup of batch resources via the
 * {@link #close()} method.
 */
public abstract class RDBBatchOperation implements BatchOperation {

  abstract void delete(ColumnFamily family, byte[] key) throws RocksDatabaseException;

  abstract void put(ColumnFamily family, CodecBuffer key, CodecBuffer value) throws RocksDatabaseException;

  abstract void put(ColumnFamily family, byte[] key, byte[] value) throws RocksDatabaseException;

  abstract void deleteRange(ColumnFamily family, byte[] startKey, byte[] endKey) throws RocksDatabaseException;

  abstract void commit(RocksDatabase db) throws RocksDatabaseException;

  abstract void commit(RocksDatabase db, ManagedWriteOptions writeOptions) throws RocksDatabaseException;

  // TODO: Remove this once recon components code implements BatchOperationHandler and make use of mocked batch
  //  operation.
  public static RDBBatchOperation newAtomicOperation() {
    return new AtomicRDBBatchOperation();
  }

  public static RDBBatchOperation newAtomicOperation(ManagedWriteBatch writeBatch) {
    return new AtomicRDBBatchOperation(writeBatch);
  }
}
