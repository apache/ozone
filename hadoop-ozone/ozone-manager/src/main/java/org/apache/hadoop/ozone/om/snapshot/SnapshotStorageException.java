/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.snapshot;

import java.io.UncheckedIOException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.rocksdb.RocksDBException;

/**
 * Unchecked wrapper for snapshot metadata store failures.
 */
public class SnapshotStorageException extends RuntimeException {

  public SnapshotStorageException(String message, Exception cause) {
    super(message, cause);
  }

  public static SnapshotStorageException fromRocksDB(String op,
                                                      RocksDBException e) {
    return new SnapshotStorageException("Failed to " + op,
        new RocksDatabaseException("Failed to " + op, e));
  }

  public static SnapshotStorageException fromIO(String op,
                                                java.io.IOException e) {
    return new SnapshotStorageException("Failed to " + op,
        new UncheckedIOException(e));
  }
}
