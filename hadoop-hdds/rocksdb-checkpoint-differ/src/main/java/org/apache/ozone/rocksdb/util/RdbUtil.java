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

package org.apache.ozone.rocksdb.util;

import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.with;

/**
 * Temporary class to test snapshot diff functionality.
 * This should be removed later.
 */
public final class RdbUtil {

  static final Logger LOG =
      LoggerFactory.getLogger(RdbUtil.class);
  private static final Duration POLL_DELAY_DURATION = Duration.ZERO;
  private static final Duration POLL_INTERVAL_DURATION = Duration.ofMillis(100);
  private static final Duration POLL_MAX_DURATION = Duration.ofSeconds(5);

  private RdbUtil() { }

  public static Set<String> getSSTFilesForComparison(final String dbLocation,
      List<String> cfs) throws RocksDBException {
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    final List<ColumnFamilyDescriptor> cfd = new ArrayList<>();
    for (String columnFamily : cfs) {
      cfd.add(new ColumnFamilyDescriptor(
          columnFamily.getBytes(StandardCharsets.UTF_8)));
    }
    cfd.add(
        new ColumnFamilyDescriptor("default".getBytes(StandardCharsets.UTF_8)));
    try (ManagedDBOptions options = new ManagedDBOptions();
         ManagedRocksDB rocksDB = ManagedRocksDB.openReadOnly(options,
             dbLocation, cfd, columnFamilyHandles)) {
      return rocksDB.get().getLiveFilesMetaData().stream()
          .map(lfm -> new File(lfm.path(), lfm.fileName()).getPath())
          .collect(Collectors.toCollection(HashSet::new));
    }
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
      Duration interval, Duration pollDelayDuration) {
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
      return false;
    }
  }

  /**
   * Wait for file to be deleted.
   * @param file File to be deleted.
   * @param maxDuration poll max duration.
   * @return true if deleted.
   */
  public static boolean waitForFileDelete(File file, Duration maxDuration) {
    return waitForFileDelete(file, maxDuration, POLL_INTERVAL_DURATION,
        POLL_DELAY_DURATION);
  }
}
