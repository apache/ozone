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
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.RocksCheckpoint;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.awaitility.Awaitility.with;

/**
 * RocksDB Checkpoint Manager, used to create and cleanup checkpoints.
 */
public class RDBCheckpointManager implements Closeable {
  private final RocksDatabase db;
  private final RocksCheckpoint checkpoint;
  public static final String RDB_CHECKPOINT_DIR_PREFIX = "checkpoint_";
  private static final Logger LOG =
      LoggerFactory.getLogger(RDBCheckpointManager.class);
  private final String checkpointNamePrefix;
  private static final Duration POLL_DELAY_DURATION = Duration.ZERO;
  private static final Duration POLL_INTERVAL_DURATION = Duration.ofMillis(100);
  private static final Duration POLL_MAX_DURATION = Duration.ofSeconds(5);

  /**
   * Create a checkpoint manager with a prefix to be added to the
   * snapshots created.
   *
   * @param checkpointPrefix prefix string.
   */
  public RDBCheckpointManager(RocksDatabase db, String checkpointPrefix) {
    this.db = db;
    this.checkpointNamePrefix = checkpointPrefix;
    this.checkpoint = db.createCheckpoint();
  }

  /**
   * Create Ozone snapshot by saving a RocksDb checkpoint to a directory.
   *
   * @param parentDir The directory where the checkpoint needs to be created.
   * @param name name of checkpoint dir, (null for default name)
   * @return RocksDB specific Checkpoint information object.
   */
  public RocksDBCheckpoint createCheckpoint(String parentDir, String name) {
    try {
      long currentTime = System.currentTimeMillis();

      String checkpointDir = StringUtils.EMPTY;
      if (StringUtils.isNotEmpty(checkpointNamePrefix)) {
        checkpointDir += checkpointNamePrefix;
      }
      if (name == null) {
        name = "_" + RDB_CHECKPOINT_DIR_PREFIX + currentTime;
      }
      checkpointDir += name;

      Path checkpointPath = Paths.get(parentDir, checkpointDir);
      Instant start = Instant.now();

      // Flush the DB WAL and mem table.
      db.flushWal(true);
      db.flush();

      checkpoint.createCheckpoint(checkpointPath);
      // Best guesstimate here. Not accurate.
      final long latest = checkpoint.getLatestSequenceNumber();

      Instant end = Instant.now();
      long duration = Duration.between(start, end).toMillis();
      LOG.info("Created checkpoint in rocksDB at {} in {} milliseconds",
              checkpointPath, duration);

      waitForCheckpointDirectoryExist(checkpointPath.toFile());

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

  /**
   * Wait for checkpoint directory to be created for 5 secs with 100 millis
   * poll interval.
   */
  public static void waitForCheckpointDirectoryExist(File file)
      throws IOException {
    Instant start = Instant.now();
    try {
      with().atMost(POLL_MAX_DURATION)
          .pollDelay(POLL_DELAY_DURATION)
          .pollInterval(POLL_INTERVAL_DURATION)
          .await()
          .until(file::exists);
      LOG.info("Waited for {} milliseconds for checkpoint directory {}" +
              " availability.",
          Duration.between(start, Instant.now()).toMillis(),
          file.getAbsoluteFile());
    } catch (ConditionTimeoutException exception) {
      LOG.info("Checkpoint directory: {} didn't get created in 5 secs.",
          file.getAbsolutePath());
    }
  }

  /**
   * Create RocksDB snapshot by saving a checkpoint to a directory.
   *
   * @param parentDir The directory where the checkpoint needs to be created.
   * @return RocksDB specific Checkpoint information object.
   */
  public RocksDBCheckpoint createCheckpoint(String parentDir) {
    return createCheckpoint(parentDir, null);
  }

  @Override
  public void close() throws IOException {
    checkpoint.close();
  }
}
