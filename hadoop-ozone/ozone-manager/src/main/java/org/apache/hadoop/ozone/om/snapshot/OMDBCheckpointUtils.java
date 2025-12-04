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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.commons.io.filefilter.TrueFileFilter.TRUE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA;
import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.file.Counters;
import org.apache.commons.io.file.CountingPathVisitor;
import org.apache.commons.io.file.PathFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for handling operations related to OM DB Checkpoints.
 * This includes extracting metadata directory paths, handling snapshot data,
 * and logging estimated sizes of checkpoint tarball streams.
 */
public final class OMDBCheckpointUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBCheckpointUtils.class);

  private static final PathFilter SST_FILE_FILTER =
      new SuffixFileFilter(ROCKSDB_SST_SUFFIX, IOCase.INSENSITIVE);

  private OMDBCheckpointUtils() {
  }

  public static boolean includeSnapshotData(HttpServletRequest request) {
    String includeParam =
        request.getParameter(OZONE_DB_CHECKPOINT_INCLUDE_SNAPSHOT_DATA);
    return Boolean.parseBoolean(includeParam);
  }

  public static void logEstimatedTarballSize(Path dbLocation, Collection<Path> snapshotPaths) {
    try {
      Counters.PathCounters counters = Counters.longPathCounters();
      CountingPathVisitor visitor = new CountingPathVisitor(
          counters, SST_FILE_FILTER, TRUE);
      Files.walkFileTree(dbLocation, visitor);
      boolean includeSnapshotData = !snapshotPaths.isEmpty();
      long totalSnapshots = snapshotPaths.size();
      if (includeSnapshotData) {
        for (Path snapshotDir: snapshotPaths) {
          Files.walkFileTree(snapshotDir, visitor);
        }
      }
      LOG.info("Estimates for Checkpoint Tarball Stream - Data size: {} KB, SST files: {}{}",
          counters.getByteCounter().get() / (1024),
          counters.getFileCounter().get(),
          (includeSnapshotData ? ", snapshots: " + totalSnapshots : ""));
    } catch (Exception e) {
      LOG.error("Could not estimate size of transfer to Checkpoint Tarball Stream for dbLocation:{} snapshotPaths:{}",
          dbLocation, snapshotPaths, e);
    }
  }
}
