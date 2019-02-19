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

package org.apache.hadoop.utils.db;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB Checkpoint Manager, used to create and cleanup checkpoints.
 */
public class RDBCheckpointManager {

  private final Checkpoint checkpoint;
  private final RocksDB db;
  public static final String RDB_CHECKPOINT_DIR_PREFIX = "rdb_checkpoint_";
  private static final Logger LOG =
      LoggerFactory.getLogger(RDBCheckpointManager.class);
  public static final String JAVA_TMP_DIR = "java.io.tmpdir";
  private String checkpointNamePrefix = "";

  public RDBCheckpointManager(RocksDB rocksDB) {
    this.db = rocksDB;
    this.checkpoint = Checkpoint.create(rocksDB);
  }

  /**
   * Create a checkpoint manager with a prefix to be added to the
   * snapshots created.
   *
   * @param rocksDB          DB instance
   * @param checkpointPrefix prefix string.
   */
  public RDBCheckpointManager(RocksDB rocksDB, String checkpointPrefix) {
    this.db = rocksDB;
    this.checkpointNamePrefix = checkpointPrefix;
    this.checkpoint = Checkpoint.create(rocksDB);
  }

  /**
   * Create RocksDB snapshot by saving a checkpoint to a directory.
   *
   * @param parentDir The directory where the checkpoint needs to be created.
   * @return RocksDB specific Checkpoint information object.
   */
  public RocksDBCheckpointSnapshot createCheckpointSnapshot(String parentDir)
      throws IOException {
    try {
      long currentTime = System.currentTimeMillis();

      String checkpointDir = StringUtils.EMPTY;
      if (StringUtils.isNotEmpty(checkpointNamePrefix)) {
        checkpointDir += checkpointNamePrefix;
      }
      checkpointDir += "_" + RDB_CHECKPOINT_DIR_PREFIX + currentTime;

      Path checkpointPath = Paths.get(parentDir, checkpointDir);
      checkpoint.createCheckpoint(checkpointPath.toString());

      return new RocksDBCheckpointSnapshot(
          checkpointPath,
          currentTime,
          db.getLatestSequenceNumber()); //Best guesstimate here. Not accurate.

    } catch (RocksDBException e) {
      LOG.error("Unable to create RocksDB Snapshot.", e);
    }
    return null;
  }

  class RocksDBCheckpointSnapshot implements DBCheckpointSnapshot {

    private Path checkpointLocation;
    private long checkpointTimestamp;
    private long latestSequenceNumber;

    RocksDBCheckpointSnapshot(Path checkpointLocation,
                              long snapshotTimestamp,
                              long latestSequenceNumber) {
      this.checkpointLocation = checkpointLocation;
      this.checkpointTimestamp = snapshotTimestamp;
      this.latestSequenceNumber = latestSequenceNumber;
    }

    @Override
    public Path getCheckpointLocation() {
      return this.checkpointLocation;
    }

    @Override
    public long getCheckpointTimestamp() {
      return this.checkpointTimestamp;
    }

    @Override
    public long getLatestSequenceNumber() {
      return this.latestSequenceNumber;
    }

    @Override
    public void cleanupCheckpoint() throws IOException {
      FileUtils.deleteDirectory(checkpointLocation.toFile());
    }
  }
}
