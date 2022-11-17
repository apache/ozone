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

package org.apache.hadoop.ozone.om.snapshot;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffReport.DiffType;
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffReport.DiffReportEntry;

import org.apache.ozone.rocksdb.util.ManagedSstFileReader;
import org.apache.ozone.rocksdb.util.RdbUtil;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Class to generate snapshot diff.
 */
public class SnapshotDiffManager {

  public SnapshotDiffReport getSnapshotDiffReport(final String volume,
                                                  final String bucket,
                                                  final OmSnapshot fromSnapshot,
                                                  final OmSnapshot toSnapshot)
      throws IOException, RocksDBException {

    // TODO: Once RocksDBCheckpointDiffer exposes method to get list
    //  of delta SST files, plug it in here.

    Set<String> fromSnapshotFiles = RdbUtil.getKeyTableSSTFiles(fromSnapshot
        .getMetadataManager().getStore().getDbLocation().getPath());
    Set<String> toSnapshotFiles = RdbUtil.getKeyTableSSTFiles(toSnapshot
        .getMetadataManager().getStore().getDbLocation().getPath());

    final Set<String> deltaFiles = new HashSet<>();
    deltaFiles.addAll(fromSnapshotFiles);
    deltaFiles.addAll(toSnapshotFiles);

    // TODO: Filter out the files.

    final Stream<String> keysToCheck = new ManagedSstFileReader(deltaFiles)
        .getKeyStream();

    final BucketLayout bucketLayout = getBucketLayout(volume, bucket,
        fromSnapshot.getMetadataManager());

    final Table<String, OmKeyInfo> fsKeyTable = fromSnapshot
        .getMetadataManager().getKeyTable(bucketLayout);
    final Table<String, OmKeyInfo> tsKeyTable = toSnapshot
        .getMetadataManager().getKeyTable(bucketLayout);

    /*
     * The reason for having ObjectID to KeyName mapping instead of OmKeyInfo
     * is to reduce the memory footprint.
     */
    final Map<Long, String> oldObjIdToKeyMap = new HashMap<>();
    // Long --> const. length
    // String --> var. length "/dir1/dir2/dir3/dir4/dir5/key1"
    final Map<Long, String> newObjIdToKeyMap = new HashMap<>();

    final Set<Long> objectIDsToCheck = new HashSet<>();

    keysToCheck.forEach(key -> {
      try {
        final OmKeyInfo oldKey = fsKeyTable.get(key);
        final OmKeyInfo newKey = tsKeyTable.get(key);
        if (areKeysEqual(oldKey, newKey)) {
          // We don't have to do anything.
          return;
        }
        if (oldKey != null) {
          final long oldObjId = oldKey.getObjectID();
          oldObjIdToKeyMap.put(oldObjId, oldKey.getKeyName());
          objectIDsToCheck.add(oldObjId);
        }
        if (newKey != null) {
          final long newObjId = newKey.getObjectID();
          newObjIdToKeyMap.put(newObjId, newKey.getKeyName());
          objectIDsToCheck.add(newObjId);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    keysToCheck.close();

    return new SnapshotDiffReport(volume, bucket, fromSnapshot.getName(),
        toSnapshot.getName(), generateDiffReport(objectIDsToCheck,
        oldObjIdToKeyMap, newObjIdToKeyMap));
  }

  private List<DiffReportEntry> generateDiffReport(
      final Set<Long> objectIDsToCheck,
      final Map<Long, String> oldObjIdToKeyMap,
      final Map<Long, String> newObjIdToKeyMap) {

    final List<DiffReportEntry> deleteDiffs = new ArrayList<>();
    final List<DiffReportEntry> renameDiffs = new ArrayList<>();
    final List<DiffReportEntry> createDiffs = new ArrayList<>();
    final List<DiffReportEntry> modifyDiffs = new ArrayList<>();


    for (Long id : objectIDsToCheck) {
      /*
       * This key can be
       * -> Created after the old snapshot was taken, which means it will be
       *    missing in oldKeyTable and present in newKeyTable.
       * -> Deleted after the old snapshot was taken, which means it will be
       *    present in oldKeyTable and missing in newKeyTable.
       * -> Modified after the old snapshot was taken, which means it will be
       *    present in oldKeyTable and present in newKeyTable with same
       *    Object ID but with different metadata.
       * -> Renamed after the old snapshot was taken, which means it will be
       *    present in oldKeyTable and present in newKeyTable but with different
       *    name and same Object ID.
       */

      final String oldKeyName = oldObjIdToKeyMap.get(id);
      final String newKeyName = newObjIdToKeyMap.get(id);

      if (oldKeyName == null && newKeyName == null) {
        // This cannot happen.
        continue;
      }

      // Key Created.
      if (oldKeyName == null) {
        createDiffs.add(DiffReportEntry.of(DiffType.CREATE, newKeyName));
        continue;
      }

      // Key Deleted.
      if (newKeyName == null) {
        deleteDiffs.add(DiffReportEntry.of(DiffType.DELETE, oldKeyName));
        continue;
      }

      // Key modified.
      if (oldKeyName.equals(newKeyName)) {
        modifyDiffs.add(DiffReportEntry.of(DiffType.MODIFY, newKeyName));
        continue;
      }

      // Key Renamed.
      renameDiffs.add(DiffReportEntry.of(DiffType.RENAME,
          oldKeyName, newKeyName));
    }
    /*
     * The order in which snap-diff should be applied
     *
     *     1. Delete diffs
     *     2. Rename diffs
     *     3. Create diffs
     *     4. Modified diffs
     *
     * Consider the following scenario
     *
     *    1. File "A" is created.
     *    2. File "B" is created.
     *    3. File "C" is created.
     *    Snapshot "1" is taken.
     *
     * Case 1:
     *   1. File "A" is deleted.
     *   2. File "B" is renamed to "A".
     *   Snapshot "2" is taken.
     *
     *   Snapshot diff should be applied in the following order:
     *    1. Delete "A"
     *    2. Rename "B" to "A"
     *
     *
     * Case 2:
     *    1. File "B" is renamed to "C".
     *    2. File "B" is created.
     *    Snapshot "2" is taken.
     *
     *   Snapshot diff should be applied in the following order:
     *    1. Rename "B" to "C"
     *    2. Create "B"
     *
     */

    final List<DiffReportEntry> snapshotDiffs = new ArrayList<>();
    snapshotDiffs.addAll(deleteDiffs);
    snapshotDiffs.addAll(renameDiffs);
    snapshotDiffs.addAll(createDiffs);
    snapshotDiffs.addAll(modifyDiffs);
    return snapshotDiffs;
  }

  private BucketLayout getBucketLayout(final String volume,
                                       final String bucket,
                                       final OMMetadataManager mManager)
      throws IOException {
    final String bucketTableKey = mManager.getBucketKey(volume, bucket);
    return mManager.getBucketTable().get(bucketTableKey).getBucketLayout();
  }

  private boolean areKeysEqual(OmKeyInfo oldKey, OmKeyInfo newKey) {
    if (oldKey == null && newKey == null) {
      return true;
    }
    if (oldKey != null) {
      return oldKey.equals(newKey);
    }
    return false;
  }
}
