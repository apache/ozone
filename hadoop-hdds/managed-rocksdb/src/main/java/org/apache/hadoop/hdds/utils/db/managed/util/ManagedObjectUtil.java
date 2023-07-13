/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.utils.db.managed.util;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;


/**
 * Util classes for managed objects.
 */
public final class ManagedObjectUtil {

  static final Logger LOG =
      LoggerFactory.getLogger(ManagedObjectUtil.class);
  private static final Duration POLL_DELAY_DURATION = Duration.ZERO;
  private static final Duration POLL_INTERVAL_DURATION = Duration.ofMillis(100);
  private static final Duration POLL_MAX_DURATION = Duration.ofSeconds(5);

  private ManagedObjectUtil() {
  }

  /**
   * Wait for file to be deleted.
   * @param file File to be deleted.
   * @param maxDuration poll max duration.
   * @param interval poll interval.
   * @param pollDelayDuration poll delay val.
   * @return true if deleted.
   */
  public static boolean waitForFileDelete(File file, Duration maxDuration,
                                          Duration interval,
                                          Duration pollDelayDuration)
      throws IOException {
    Instant start = Instant.now();
    try {
      Awaitility.with().atMost(maxDuration)
          .pollDelay(pollDelayDuration)
          .pollInterval(interval)
          .await()
          .until(() -> !file.exists());
      LOG.info("Waited for {} milliseconds for file {} deletion.",
          Duration.between(start, Instant.now()).toMillis(),
          file.getAbsoluteFile());
      return true;
    } catch (ConditionTimeoutException exception) {
      LOG.info("File: {} didn't get deleted in {} secs.",
          file.getAbsolutePath(), maxDuration.getSeconds());
      throw new IOException(exception);
    }
  }

  /**
   * Wait for file to be deleted.
   * @param file File to be deleted.
   * @param maxDuration poll max duration.
   * @return true if deleted.
   */
  public static boolean waitForFileDelete(File file, Duration maxDuration)
      throws IOException {
    return waitForFileDelete(file, maxDuration, POLL_INTERVAL_DURATION,
        POLL_DELAY_DURATION);
  }
}
