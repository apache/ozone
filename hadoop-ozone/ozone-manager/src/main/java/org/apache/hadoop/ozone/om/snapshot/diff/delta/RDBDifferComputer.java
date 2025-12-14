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

import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshotLocalData;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.OmSnapshotLocalDataManager;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.SubStatus;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ozone.rocksdiff.DifferSnapshotInfo;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;

/**
 * Computes RocksDB SST file differences between two snapshots and materializes
 * differing SST files as hard links in the configured delta directory.
 *
 * <p>This class uses {@link RocksDBCheckpointDiffer} to obtain the list of SST
 * files that differ between a \"from\" and a \"to\" snapshot. It opens local
 * snapshot metadata via {@link #getLocalDataProvider}, and delegates the
 * comparison to the differ to compute the delta files.</p>
 *
 * <p>Each source SST file returned by the differ is linked into the delta
 * directory using {@link FileLinkDeltaFileComputer#createLink(Path)}, and the
 * returned value from {@link #computeDeltaFiles} is a list of those link
 * paths. The implementation synchronizes on the internal {@code differ}
 * instance because the differ is not assumed to be thread-safe.</p>
 */
class RDBDifferComputer extends FileLinkDeltaFileComputer {

  private final RocksDBCheckpointDiffer differ;

  RDBDifferComputer(OmSnapshotManager omSnapshotManager, OMMetadataManager activeMetadataManager,
      Path deltaDirPath, Consumer<SubStatus> activityReporter) throws IOException {
    super(omSnapshotManager, activeMetadataManager, deltaDirPath, activityReporter);
    this.differ = activeMetadataManager.getStore().getRocksDBCheckpointDiffer();
  }

  @Override
  public Optional<Map<Path, Pair<Path, SstFileInfo>>> computeDeltaFiles(SnapshotInfo fromSnapshot,
      SnapshotInfo toSnapshot, Set<String> tablesToLookup, TablePrefixInfo tablePrefixInfo) throws IOException {
    if (differ != null) {
      try (OmSnapshotLocalDataManager.ReadableOmSnapshotLocalDataProvider snapProvider =
               getLocalDataProvider(toSnapshot.getSnapshotId(), fromSnapshot.getSnapshotId())) {
        final DifferSnapshotInfo fromDSI = toDifferSnapshotInfo(getActiveMetadataManager(), fromSnapshot,
            snapProvider.getPreviousSnapshotLocalData().orElseThrow(() -> new IOException("Missing previous snapshot " +
                "local data " + fromSnapshot.getSnapshotId())));
        final DifferSnapshotInfo toDSI = toDifferSnapshotInfo(getActiveMetadataManager(), toSnapshot,
            snapProvider.getSnapshotLocalData());
        final Map<Integer, Integer> versionMap = snapProvider.getSnapshotLocalData().getVersionSstFileInfos().entrySet()
            .stream().collect(toMap(Map.Entry::getKey, entry -> entry.getValue().getPreviousSnapshotVersion()));
        synchronized (differ) {
          Optional<Map<Path, SstFileInfo>> paths = differ.getSSTDiffListWithFullPath(toDSI, fromDSI, versionMap,
              tablePrefixInfo, tablesToLookup);
          if (paths.isPresent()) {
            Map<Path, Pair<Path, SstFileInfo>> links = new HashMap<>(paths.get().size());
            for (Map.Entry<Path, SstFileInfo> source : paths.get().entrySet()) {
              links.put(source.getKey(), Pair.of(createLink(source.getKey()), source.getValue()));
            }
            return Optional.of(links);
          }
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Convert from SnapshotInfo to DifferSnapshotInfo.
   */
  private static DifferSnapshotInfo toDifferSnapshotInfo(OMMetadataManager activeOmMetadataManager,
      SnapshotInfo snapshotInfo, OmSnapshotLocalData snapshotLocalData) throws IOException {
    final UUID snapshotId = snapshotInfo.getSnapshotId();
    final long dbTxSequenceNumber = snapshotLocalData.getDbTxSequenceNumber();
    NavigableMap<Integer, List<SstFileInfo>> versionSstFiles = snapshotLocalData.getVersionSstFileInfos().entrySet()
        .stream().collect(toMap(Map.Entry::getKey,
            entry -> entry.getValue().getSstFiles(), (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
          }, TreeMap::new));
    if (versionSstFiles.isEmpty()) {
      throw new IOException(String.format("No versions found corresponding to %s", snapshotId));
    }
    return new DifferSnapshotInfo(
        version -> OmSnapshotManager.getSnapshotPath(activeOmMetadataManager, snapshotId, version),
        snapshotId, dbTxSequenceNumber, versionSstFiles);
  }
}
