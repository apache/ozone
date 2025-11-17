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

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB Checkpoint Utilities.
 */
public final class RDBCheckpointUtils {
  static final Logger LOG =
      LoggerFactory.getLogger(RDBCheckpointUtils.class);
  public static final Duration POLL_INTERVAL_DURATION = Duration.ofMillis(100);
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
    final boolean success = RatisHelper.attemptUntilTrue(file::exists, POLL_INTERVAL_DURATION, maxWaitTimeout);
    if (!success) {
      LOG.info("Checkpoint directory: {} didn't get created in {} secs.",
          file.getAbsolutePath(), maxWaitTimeout.getSeconds());
    }
    return success;
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
