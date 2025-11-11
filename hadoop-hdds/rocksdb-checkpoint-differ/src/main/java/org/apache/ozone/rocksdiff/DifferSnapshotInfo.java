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

package org.apache.ozone.rocksdiff;

import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;

/**
 * Snapshot information node class for the differ.
 */
public class DifferSnapshotInfo {
  private final String dbPath;
  private final UUID snapshotId;
  private final long snapshotGeneration;

  private final TablePrefixInfo tablePrefixes;

  private final ManagedRocksDB rocksDB;

  public DifferSnapshotInfo(String db, UUID id, long gen,
                            TablePrefixInfo tablePrefixInfo,
                            ManagedRocksDB rocksDB) {
    dbPath = db;
    snapshotId = id;
    snapshotGeneration = gen;
    tablePrefixes = tablePrefixInfo;
    this.rocksDB = rocksDB;
  }

  public String getDbPath() {
    return dbPath;
  }

  public UUID getSnapshotId() {
    return snapshotId;
  }

  public long getSnapshotGeneration() {
    return snapshotGeneration;
  }

  public TablePrefixInfo getTablePrefixes() {
    return tablePrefixes;
  }

  @Override
  public String toString() {
    return String.format("DifferSnapshotInfo{dbPath='%s', snapshotID='%s', " +
            "snapshotGeneration=%d, tablePrefixes size=%s}",
        dbPath, snapshotId, snapshotGeneration, tablePrefixes.size());
  }

  public ManagedRocksDB getRocksDB() {
    return rocksDB;
  }
}
