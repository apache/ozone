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

package org.apache.hadoop.hdds.utils.db.managed;

import jakarta.annotation.Nullable;
import java.io.File;
import java.time.Duration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.utils.LeakDetector;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.RocksDB;
import org.rocksdb.util.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities to help assert RocksObject closures.
 */
public final class ManagedRocksObjectUtils {

  static final Logger LOG =
      LoggerFactory.getLogger(ManagedRocksObjectUtils.class);

  private static final Duration POLL_INTERVAL_DURATION = Duration.ofMillis(100);

  private static final LeakDetector LEAK_DETECTOR = new LeakDetector("ManagedRocksObject");

  private ManagedRocksObjectUtils() {
  }

  static UncheckedAutoCloseable track(AutoCloseable object) {
    ManagedRocksObjectMetrics.INSTANCE.increaseManagedObject();
    final Class<?> clazz = object.getClass();
    final StackTraceElement[] stackTrace = getStackTrace();
    return LEAK_DETECTOR.track(object, () -> reportLeak(clazz, formatStackTrace(stackTrace)));
  }

  static void reportLeak(Class<?> clazz, String stackTrace) {
    ManagedRocksObjectMetrics.INSTANCE.increaseLeakObject();
    HddsUtils.reportLeak(clazz, stackTrace, LOG);
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
   * @throws RocksDatabaseException in case of failure.
   */
  public static void waitForFileDelete(File file, Duration maxDuration)
      throws RocksDatabaseException {
    if (!RatisHelper.attemptUntilTrue(() -> !file.exists(), POLL_INTERVAL_DURATION, maxDuration)) {
      String msg = String.format("File: %s didn't get deleted in %s secs.",
          file.getAbsolutePath(), maxDuration.getSeconds());
      LOG.warn(msg);
      throw new RocksDatabaseException(msg);
    }
  }

  /**
   * Ensures that the RocksDB native library is loaded.
   * This method should be called before performing any operations
   * that require the RocksDB native library.
   */
  public static void loadRocksDBLibrary() {
    RocksDB.loadLibrary();
  }

  /**
   * Returns RocksDB library file name.
   */
  public static String getRocksDBLibFileName() {
    return Environment.getJniLibraryFileName("rocksdb");
  }
}
