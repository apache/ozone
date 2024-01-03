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
package org.apache.hadoop.hdds.utils.db.managed;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.utils.LeakDetector;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

/**
 * Utilities to help assert RocksObject closures.
 */
public final class ManagedRocksObjectUtils {
  private ManagedRocksObjectUtils() {
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(ManagedRocksObjectUtils.class);

  private static final Duration POLL_DELAY_DURATION = Duration.ZERO;
  private static final Duration POLL_INTERVAL_DURATION = Duration.ofMillis(100);


  private static final LeakDetector LEAK_DETECTOR = new LeakDetector("ManagedRocksObject");

  static UncheckedAutoCloseable track(AutoCloseable object) {
    ManagedRocksObjectMetrics.INSTANCE.increaseManagedObject();
    final Class<?> clazz = object.getClass();
    final StackTraceElement[] stackTrace = getStackTrace();
    return LEAK_DETECTOR.track(object, () -> reportLeak(clazz, formatStackTrace(stackTrace)));
  }

  static void reportLeak(Class<?> clazz, String stackTrace) {
    ManagedRocksObjectMetrics.INSTANCE.increaseLeakObject();
    String warning = String.format("%s is not closed properly", clazz.getSimpleName());
    if (stackTrace != null && LOG.isDebugEnabled()) {
      String debugMessage = String.format("%nStackTrace for unclosed instance: %s", stackTrace);
      warning = warning.concat(debugMessage);
    }
    LOG.warn(warning);
  }

  private static @Nullable StackTraceElement[] getStackTrace() {
    return HddsUtils.getStackTrace(LOG);
  }

  static String formatStackTrace(@Nullable StackTraceElement[] elements) {
    return HddsUtils.formatStackTrace(elements, 4);
  }

  /**
   * Wait for file to be deleted.
   * @param file File to be deleted.
   * @param maxDuration poll max duration.
   * @param interval poll interval.
   * @param pollDelayDuration poll delay val.
   */
  public static void waitForFileDelete(File file, Duration maxDuration,
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
   * @throws IOException in case of failure.
   */
  public static void waitForFileDelete(File file, Duration maxDuration)
      throws IOException {
    waitForFileDelete(file, maxDuration, POLL_INTERVAL_DURATION,
        POLL_DELAY_DURATION);
  }

  /**
   * Ensures that the RocksDB native library is loaded.
   * This method should be called before performing any operations
   * that require the RocksDB native library.
   */
  public static void loadRocksDBLibrary() {
    RocksDB.loadLibrary();
  }
}
