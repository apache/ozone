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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db;

import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import static org.awaitility.Awaitility.with;

/**
 * RocksDB Checkpoint Utilities.
 */
public final class RDBCheckpointUtils {
  static final Logger LOG =
      LoggerFactory.getLogger(RDBCheckpointUtils.class);
  private static final Duration POLL_DELAY_DURATION = Duration.ZERO;
  private static final Duration POLL_INTERVAL_DURATION = Duration.ofMillis(100);
  private static final Duration POLL_MAX_DURATION = Duration.ofSeconds(20);

  private RDBCheckpointUtils() { }

  /**
   * Wait for checkpoint directory to be created for the given duration with
   * 100 millis poll interval.
   * @param file Checkpoint directory.
   * @param maxWaitTimeout wait at most before request timeout.
   * @return true if found within given timeout else false.
   */
  public static boolean waitForCheckpointDirectoryExist(File file,
      Duration maxWaitTimeout) {
    Instant start = Instant.now();
    try {
      with().atMost(maxWaitTimeout)
          .pollDelay(POLL_DELAY_DURATION)
          .pollInterval(POLL_INTERVAL_DURATION)
          .await()
          .until(file::exists);
      LOG.info("Waited for {} milliseconds for checkpoint directory {}" +
              " availability.",
          Duration.between(start, Instant.now()).toMillis(),
          file.getAbsoluteFile());
      return true;
    } catch (ConditionTimeoutException exception) {
      LOG.info("Checkpoint directory: {} didn't get created in {} secs.",
          maxWaitTimeout.getSeconds(), file.getAbsolutePath());
      return false;
    }
  }

  /**
   * Wait for checkpoint directory to be created for 5 secs with 100 millis
   * poll interval.
   * @param file Checkpoint directory.
   * @return true if found.
   */
  public static boolean waitForCheckpointDirectoryExist(File file)
      throws IOException {
    return waitForCheckpointDirectoryExist(file, POLL_MAX_DURATION);
  }
}
