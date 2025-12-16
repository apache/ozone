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

import java.util.function.Consumer;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;

/**
 * Abstract base class for performing bulk ingestion operations on a database using batch processing.
 *
 * This class implements the {@link AbstractRDBBatchOperation} interface, enabling bulk operations
 * such as insert, delete, and delete range using a batch mechanism. The operations are performed
 * sequentially and automatically flushed when the maximum batch size is reached, as defined during
 * object instantiation.
 *
 * Subclasses must provide a concrete implementation of the {@link #opOnBatchFull(RDBBatchOperation)}
 * method to handle custom actions when the batch is full.
 */
public abstract class BulkIngestRDBBatchOperation implements AbstractRDBBatchOperation {
  private RDBBatchOperation rdbBatchOperation;
  private final int maxBatchSize;

  public BulkIngestRDBBatchOperation(int batchSize) {
    this.rdbBatchOperation = new RDBBatchOperation();
    this.maxBatchSize = batchSize;
  }

  @Override
  public void delete(ColumnFamily family, byte[] key) throws RocksDatabaseException {
    performOpAndFlushBatchIfFull(batch -> batch.delete(family, key));
  }

  @Override
  public void deleteRange(ColumnFamily family, byte[] startKey, byte[] endKey) throws RocksDatabaseException {
    performOpAndFlushBatchIfFull(batch -> batch.deleteRange(family, startKey, endKey));
  }

  @Override
  public void put(ColumnFamily family, byte[] key, byte[] value) throws RocksDatabaseException {
    performOpAndFlushBatchIfFull(batch -> batch.put(family, key, value));
  }

  @Override
  public void put(ColumnFamily family, CodecBuffer key, CodecBuffer value) throws RocksDatabaseException {
    performOpAndFlushBatchIfFull(batch -> batch.put(family, key, value));
  }

  @Override
  public long size() {
    return 0;
  }

  private void performOpAndFlushBatchIfFull(Consumer<RDBBatchOperation> op) throws RocksDatabaseException {
    op.accept(rdbBatchOperation);
    if (rdbBatchOperation.size() >= maxBatchSize) {
      opOnBatchFull(this.rdbBatchOperation);
      rdbBatchOperation.close();
      rdbBatchOperation = new RDBBatchOperation();
    }
  }

  @Override
  public void close() throws RocksDatabaseException {
    opOnBatchFull(rdbBatchOperation);
    rdbBatchOperation.close();
  }

  public abstract void opOnBatchFull(RDBBatchOperation batch) throws RocksDatabaseException;
}
