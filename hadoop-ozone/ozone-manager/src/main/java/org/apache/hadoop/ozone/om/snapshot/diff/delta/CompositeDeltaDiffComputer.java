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

package org.apache.hadoop.ozone.om.snapshot.diff.delta;

import static org.apache.hadoop.ozone.om.snapshot.diff.delta.FullDiffComputer.getSSTFileSetForSnapshot;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CompositeDeltaDiffComputer is responsible for computing the delta file
 * differences between two snapshots, utilizing different strategies such
 * as partial differ computation and full differ computation.
 *
 * It serves as an orchestrator to decide whether to perform a full diff
 * or a more efficient partial diff, and handles fallback mechanisms if
 * the chosen method fails.
 *
 * The class leverages two main difference computation strategies:
 * - {@code RDBDifferComputer} for partial diff computation
 * - {@code FullDiffComputer} for exhaustive diff
 *
 * This class also includes support for handling non-native diff scenarios
 * through additional processing of input files from the "from" snapshot
 * when native RocksDB tools are not used.
 *
 * Inherits from {@code FileLinkDeltaFileComputer} and implements the
 * functionality for computing delta files and resource management.
 */
public class CompositeDeltaDiffComputer extends FileLinkDeltaFileComputer {

  private static final Logger LOG = LoggerFactory.getLogger(CompositeDeltaDiffComputer.class);

  private final RDBDifferComputer differComputer;
  private final FullDiffComputer fullDiffComputer;
  private final boolean nonNativeDiff;

  public CompositeDeltaDiffComputer(OmSnapshotManager snapshotManager,
      OMMetadataManager activeMetadataManager, Path deltaDirPath,
      Consumer<SnapshotDiffResponse.SubStatus> activityReporter, boolean fullDiff,
      boolean nonNativeDiff) throws IOException {
    super(snapshotManager, activeMetadataManager, deltaDirPath, activityReporter);
    differComputer = fullDiff ? null : new RDBDifferComputer(snapshotManager, activeMetadataManager,
        deltaDirPath.resolve("rdbDiffer"), activityReporter);
    fullDiffComputer = new FullDiffComputer(snapshotManager, activeMetadataManager,
        deltaDirPath.resolve("fullDiff"), activityReporter);
    this.nonNativeDiff = nonNativeDiff;
  }

  @Override
  Optional<Map<Path, Pair<Path, SstFileInfo>>> computeDeltaFiles(SnapshotInfo fromSnapshotInfo,
      SnapshotInfo toSnapshotInfo, Set<String> tablesToLookup, TablePrefixInfo tablePrefixInfo) throws IOException {
    Map<Path, Pair<Path, SstFileInfo>> deltaFiles = null;
    try {
      if (differComputer != null) {
        updateActivity(SnapshotDiffResponse.SubStatus.SST_FILE_DELTA_DAG_WALK);
        deltaFiles = differComputer.computeDeltaFiles(fromSnapshotInfo, toSnapshotInfo, tablesToLookup,
            tablePrefixInfo).orElse(null);
      }
    } catch (Exception e) {
      LOG.warn("Falling back to full diff.", e);
    }
    if (deltaFiles == null) {
      updateActivity(SnapshotDiffResponse.SubStatus.SST_FILE_DELTA_FULL_DIFF);
      deltaFiles = fullDiffComputer.computeDeltaFiles(fromSnapshotInfo, toSnapshotInfo, tablesToLookup,
              tablePrefixInfo).orElse(null);
      if (deltaFiles == null) {
        // FileLinkDeltaFileComputer would throw an exception in this case.
        return Optional.empty();
      }
    }
    // Workaround to handle deletes if native rocksDb tool for reading
    // tombstone is not loaded.
    // When performing non native diff, input files of from snapshot needs to be added.
    if (nonNativeDiff) {
      try (UncheckedAutoCloseableSupplier<OmSnapshot> fromSnapshot = getSnapshot(fromSnapshotInfo)) {
        Set<SstFileInfo> fromSnapshotFiles = getSSTFileSetForSnapshot(fromSnapshot.get(), tablesToLookup,
            tablePrefixInfo);
        Path fromSnapshotPath = fromSnapshot.get().getMetadataManager().getStore().getDbLocation()
            .getAbsoluteFile().toPath();
        for (SstFileInfo sstFileInfo : fromSnapshotFiles) {
          Path source = sstFileInfo.getFilePath(fromSnapshotPath);
          deltaFiles.put(source, Pair.of(createLink(source), sstFileInfo));
        }
      }
    }
    return Optional.of(deltaFiles);
  }

  @Override
  public void close() throws IOException {
    if (differComputer != null) {
      differComputer.close();
    }
    if (fullDiffComputer != null) {
      fullDiffComputer.close();
    }
    super.close();
  }
}
