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

import java.util.function.Supplier;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;

/**
 * A concrete implementation of {@link BulkIngestRDBBatchOperation} for performing bulk
 * database operations with auto-commit functionality.
 *
 * This class is designed for batch processing on a RocksDB database. It extends the
 * {@code BulkIngestRDBBatchOperation} base class and provides automatic commit
 * behavior for each batch when the maximum batch size is reached.
 */
public class AutoCommitRDBBatchOperation extends BulkIngestRDBBatchOperation {
  private final RocksDatabase db;
  private final ManagedWriteOptions writeOptions;

  public AutoCommitRDBBatchOperation(int batchSize, RocksDatabase db) {
    this(batchSize, db, ManagedWriteOptions::new);
  }

  public AutoCommitRDBBatchOperation(int batchSize, RocksDatabase db, Supplier<ManagedWriteOptions> writeOptions) {
    super(batchSize);
    this.db = db;
    this.writeOptions = writeOptions.get();
  }

  @Override
  public void opOnBatchFull(RDBBatchOperation batch) throws RocksDatabaseException {
    batch.commit(this.db, writeOptions);
  }

  @Override
  public void close() throws RocksDatabaseException {
    super.close();
    this.writeOptions.close();
  }
}
