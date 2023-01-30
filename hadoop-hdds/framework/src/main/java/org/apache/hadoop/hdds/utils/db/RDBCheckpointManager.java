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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.RocksCheckpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB Checkpoint Manager, used to create and cleanup checkpoints.
 */
public class RDBCheckpointManager implements Closeable {

  private final RocksCheckpoint checkpoint;
  public static final String RDB_CHECKPOINT_DIR_PREFIX = "checkpoint_";
  private static final Logger LOG =
      LoggerFactory.getLogger(RDBCheckpointManager.class);
  private final String checkpointNamePrefix;

  /**
   * Create a checkpoint manager with a prefix to be added to the
   * snapshots created.
   *
   * @param checkpointPrefix prefix string.
   */
  public RDBCheckpointManager(RocksDatabase db, String checkpointPrefix) {
    this.checkpointNamePrefix = checkpointPrefix;
    this.checkpoint = db.createCheckpoint();
  }

  /**
   * Create RocksDB snapshot by saving a checkpoint to a directory.
   *
   * @param parentDir The directory where the checkpoint needs to be created.
   * @return RocksDB specific Checkpoint information object.
   */
  public RocksDBCheckpoint createCheckpoint(String parentDir) {
    try {
      long currentTime = System.currentTimeMillis();

      String checkpointDir = StringUtils.EMPTY;
      if (StringUtils.isNotEmpty(checkpointNamePrefix)) {
        checkpointDir += checkpointNamePrefix;
      }
      checkpointDir += "_" + RDB_CHECKPOINT_DIR_PREFIX + currentTime;

      Path checkpointPath = Paths.get(parentDir, checkpointDir);
      Instant start = Instant.now();
      checkpoint.createCheckpoint(checkpointPath);
      //Best guesstimate here. Not accurate.
      final long latest = checkpoint.getLatestSequenceNumber();
      Instant end = Instant.now();

      long duration = Duration.between(start, end).toMillis();
      LOG.info("Created checkpoint at {} in {} milliseconds",
              checkpointPath, duration);

      return new RocksDBCheckpoint(
          checkpointPath,
          currentTime,
          latest,
          duration);
    } catch (IOException e) {
      LOG.error("Unable to create RocksDB Snapshot.", e);
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    checkpoint.close();
  }
}
