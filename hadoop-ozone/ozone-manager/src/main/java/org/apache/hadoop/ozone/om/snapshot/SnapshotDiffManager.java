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

import java.util.Iterator;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.IntegerCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.codec.OmDBDiffReportEntryCodec;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReport;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReport.DiffType;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReport.DiffReportEntry;

import org.apache.ozone.rocksdb.util.ManagedSstFileReader;
import org.apache.ozone.rocksdb.util.RdbUtil;
import org.apache.ozone.rocksdiff.DifferSnapshotInfo;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Class to generate snapshot diff.
 */
public class SnapshotDiffManager {

  private static final Logger LOG =
          LoggerFactory.getLogger(SnapshotDiffManager.class);
  private final RocksDBCheckpointDiffer differ;
  private final ManagedRocksDB db;
  private final CodecRegistry codecRegistry;

  public SnapshotDiffManager(ManagedRocksDB db,
                             RocksDBCheckpointDiffer differ) {
    this.db = db;
    this.differ = differ;
    this.codecRegistry = new CodecRegistry();

    // Integers are used for indexing persistent list.
    this.codecRegistry.addCodec(Integer.class,
        new IntegerCodec());
    // Need for Diff Report
    this.codecRegistry.addCodec(DiffReportEntry.class,
        new OmDBDiffReportEntryCodec());
  }

  private Map<String, String> getTablePrefixes(
      OMMetadataManager omMetadataManager,
      String volumeName, String bucketName) throws IOException {
    // Copied straight from TestOMSnapshotDAG. TODO: Dedup. Move this to util.
    Map<String, String> tablePrefixes = new HashMap<>();
    String volumeId = String.valueOf(omMetadataManager.getVolumeId(volumeName));
    String bucketId = String.valueOf(
        omMetadataManager.getBucketId(volumeName, bucketName));
    tablePrefixes.put(OmMetadataManagerImpl.KEY_TABLE,
        OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName);
    tablePrefixes.put(OmMetadataManagerImpl.FILE_TABLE,
        OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId);
    tablePrefixes.put(OmMetadataManagerImpl.DIRECTORY_TABLE,
        OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId);
    return tablePrefixes;
  }

  /**
   * Convert from SnapshotInfo to DifferSnapshotInfo.
   */
  private DifferSnapshotInfo getDSIFromSI(SnapshotInfo snapshotInfo,
      OmSnapshot omSnapshot, final String volumeName, final String bucketName)
      throws IOException {

    final OMMetadataManager snapshotOMMM = omSnapshot.getMetadataManager();
    final String checkpointPath =
        snapshotOMMM.getStore().getDbLocation().getPath();
    final String snapshotId = snapshotInfo.getSnapshotID();
    final long dbTxSequenceNumber = snapshotInfo.getDbTxSequenceNumber();

    return new DifferSnapshotInfo(
        checkpointPath,
        snapshotId,
        dbTxSequenceNumber,
        getTablePrefixes(snapshotOMMM, volumeName, bucketName));
  }

  public SnapshotDiffReport getSnapshotDiffReport(final String volume,
                                                  final String bucket,
                                                  final OmSnapshot fromSnapshot,
                                                  final OmSnapshot toSnapshot,
                                                  final SnapshotInfo fsInfo,
                                                  final SnapshotInfo tsInfo)
      throws IOException, RocksDBException {

    final BucketLayout bucketLayout = getBucketLayout(volume, bucket,
        fromSnapshot.getMetadataManager());

    // TODO: This should comes from request itself.
    String requestId = UUID.randomUUID().toString();

    ColumnFamilyHandle fromSnapshotColumnFamily = null;
    ColumnFamilyHandle toSnapshotColumnFamily = null;
    ColumnFamilyHandle objectIDsColumnFamily = null;
    ColumnFamilyHandle diffReportColumnFamily = null;

    try {
      // RequestId is prepended to column family name to make it unique
      // for request.
      fromSnapshotColumnFamily = db.get().createColumnFamily(
          new ColumnFamilyDescriptor(
              codecRegistry.asRawData(requestId + "-fromSnapshot"),
              new ManagedColumnFamilyOptions()));
      toSnapshotColumnFamily = db.get().createColumnFamily(
          new ColumnFamilyDescriptor(
              codecRegistry.asRawData(requestId + "-toSnapshot"),
              new ManagedColumnFamilyOptions()));
      objectIDsColumnFamily = db.get().createColumnFamily(
          new ColumnFamilyDescriptor(
              codecRegistry.asRawData(requestId + "-objectIDs"),
              new ManagedColumnFamilyOptions()));
      diffReportColumnFamily = db.get().createColumnFamily(
          new ColumnFamilyDescriptor(
              codecRegistry.asRawData(requestId + "-diffReport"),
              new ManagedColumnFamilyOptions()));

      // ObjectId to keyName map to keep key info for fromSnapshot.
      // objectIdToKeyNameMap is used to identify what keys were touched
      // in which snapshot and to know the difference if operation was
      // creation, deletion, modify or rename.
      // Stores only keyName instead of OmKeyInfo to reduce the memory
      // footprint.
      final PersistentMap<Long, String> objectIdToKeyNameMapForFromSnapshot =
          new RocksDbPersistentMap<>(db,
              fromSnapshotColumnFamily,
              codecRegistry,
              Long.class,
              String.class);

      // ObjectId to keyName map to keep key info for toSnapshot.
      final PersistentMap<Long, String> objectIdToKeyNameMapForToSnapshot =
          new RocksDbPersistentMap<>(db,
              toSnapshotColumnFamily,
              codecRegistry,
              Long.class,
              String.class);

      // Set of unique objectId between fromSnapshot and toSnapshot.
      final PersistentSet<Long> objectIDsToCheckMap =
          new RocksDbPersistentSet<>(db,
              objectIDsColumnFamily,
              codecRegistry,
              Long.class);

      // Final diff report.
      final PersistentList<DiffReportEntry> diffReport =
          new RocksDbPersistentList<>(db,
              diffReportColumnFamily,
              codecRegistry,
              DiffReportEntry.class);

      final Table<String, OmKeyInfo> fsKeyTable = fromSnapshot
          .getMetadataManager().getKeyTable(bucketLayout);
      final Table<String, OmKeyInfo> tsKeyTable = toSnapshot
          .getMetadataManager().getKeyTable(bucketLayout);
      final Set<String> deltaFilesForKeyOrFileTable =
          getDeltaFiles(fromSnapshot, toSnapshot,
              Collections.singletonList(fsKeyTable.getName()),
              fsInfo, tsInfo, volume, bucket);

      addToObjectIdMap(fsKeyTable,
          tsKeyTable,
          deltaFilesForKeyOrFileTable,
          objectIdToKeyNameMapForFromSnapshot,
          objectIdToKeyNameMapForToSnapshot,
          objectIDsToCheckMap,
          false);

      if (bucketLayout.isFileSystemOptimized()) {
        // add to object ID map for directory.
        final Table<String, OmDirectoryInfo> fsDirTable =
            fromSnapshot.getMetadataManager().getDirectoryTable();
        final Table<String, OmDirectoryInfo> tsDirTable =
            toSnapshot.getMetadataManager().getDirectoryTable();
        final Set<String> deltaFilesForDirTable =
            getDeltaFiles(fromSnapshot, toSnapshot,
                Collections.singletonList(fsDirTable.getName()),
                fsInfo, tsInfo, volume, bucket);

        addToObjectIdMap(fsDirTable,
            tsDirTable,
            deltaFilesForDirTable,
            objectIdToKeyNameMapForFromSnapshot,
            objectIdToKeyNameMapForToSnapshot,
            objectIDsToCheckMap,
            true);
      }

      generateDiffReport(requestId,
          objectIDsToCheckMap,
          objectIdToKeyNameMapForFromSnapshot,
          objectIdToKeyNameMapForToSnapshot,
          diffReport);

      // TODO: Need to change it to pagination.
      //  https://issues.apache.org/jira/browse/HDDS-7548
      List<DiffReportEntry> diffReportList = new ArrayList<>();
      diffReport.iterator().forEachRemaining(diffReportList::add);

      return new SnapshotDiffReport(volume,
          bucket,
          fromSnapshot.getName(),
          toSnapshot.getName(),
          diffReportList);

    } finally {
      // Clean up: drop the intermediate column family and close them.
      if (fromSnapshotColumnFamily != null) {
        db.get().dropColumnFamily(fromSnapshotColumnFamily);
        fromSnapshotColumnFamily.close();
      }
      if (toSnapshotColumnFamily != null) {
        db.get().dropColumnFamily(toSnapshotColumnFamily);
        toSnapshotColumnFamily.close();
      }
      if (objectIDsColumnFamily != null) {
        db.get().dropColumnFamily(objectIDsColumnFamily);
        objectIDsColumnFamily.close();
      }
      // Drop diff report table for now. Otherwise, rocksDb creation will
      // fail on OM restart because column family already exists.
      // Will be fixed in pagination PR (Jira: HDDS-7548).
      if (diffReportColumnFamily != null) {
        db.get().dropColumnFamily(diffReportColumnFamily);
        diffReportColumnFamily.close();
      }
    }
  }

  private void addToObjectIdMap(Table<String, ? extends WithObjectID> fsTable,
                                Table<String, ? extends WithObjectID> tsTable,
                                Set<String> deltaFiles,
                                PersistentMap<Long, String> oldObjIdToKeyMap,
                                PersistentMap<Long, String> newObjIdToKeyMap,
                                PersistentSet<Long> objectIDsToCheck,
                                boolean isDirectoryTable) {

    if (deltaFiles.isEmpty()) {
      return;
    }
    try (Stream<String> keysToCheck = new ManagedSstFileReader(deltaFiles)
            .getKeyStream()) {
      keysToCheck.forEach(key -> {
        try {
          final WithObjectID oldKey = fsTable.get(key);
          final WithObjectID newKey = tsTable.get(key);
          if (areKeysEqual(oldKey, newKey)) {
            // We don't have to do anything.
            return;
          }
          if (oldKey != null) {
            final long oldObjId = oldKey.getObjectID();
            oldObjIdToKeyMap.put(oldObjId,
                    getKeyOrDirectoryName(isDirectoryTable, oldKey));
            objectIDsToCheck.add(oldObjId);
          }
          if (newKey != null) {
            final long newObjId = newKey.getObjectID();
            newObjIdToKeyMap.put(newObjId,
                    getKeyOrDirectoryName(isDirectoryTable, newKey));
            objectIDsToCheck.add(newObjId);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (RocksDBException rocksDBException) {
      // TODO: Gracefully handle exception e.g. when input files do not exist
      throw new RuntimeException(rocksDBException);
    }
  }

  private String getKeyOrDirectoryName(boolean isDirectory,
      WithObjectID object) {
    if (isDirectory) {
      OmDirectoryInfo directoryInfo = (OmDirectoryInfo) object;
      return directoryInfo.getName();
    }
    OmKeyInfo keyInfo = (OmKeyInfo) object;
    return keyInfo.getKeyName();
  }

  @NotNull
  @SuppressWarnings("parameternumber")
  private Set<String> getDeltaFiles(OmSnapshot fromSnapshot,
      OmSnapshot toSnapshot, List<String> tablesToLookUp,
      SnapshotInfo fsInfo, SnapshotInfo tsInfo,
      String volume, String bucket)
      throws RocksDBException, IOException {
    // TODO: Refactor the parameter list

    final Set<String> deltaFiles = new HashSet<>();

    // Check if compaction DAG is available, use that if so
    if (differ != null && fsInfo != null && tsInfo != null) {
      // Construct DifferSnapshotInfo
      final DifferSnapshotInfo fromDSI =
          getDSIFromSI(fsInfo, fromSnapshot, volume, bucket);
      final DifferSnapshotInfo toDSI =
          getDSIFromSI(tsInfo, toSnapshot, volume, bucket);

      LOG.debug("Calling RocksDBCheckpointDiffer");
      List<String> sstDiffList =
          differ.getSSTDiffListWithFullPath(toDSI, fromDSI);
      deltaFiles.addAll(sstDiffList);

      // TODO: Remove the workaround below when the SnapDiff logic can read
      //  tombstones in SST files.
      // Workaround: Append "From DB" SST files to the deltaFiles list so that
      //  the current SnapDiff logic correctly handles deleted keys.
      if (!deltaFiles.isEmpty()) {
        Set<String> fromSnapshotFiles = RdbUtil.getSSTFilesForComparison(
            fromSnapshot.getMetadataManager()
                .getStore().getDbLocation().getPath(),
            tablesToLookUp);
        deltaFiles.addAll(fromSnapshotFiles);
      }
      // End of Workaround
    }

    if (deltaFiles.isEmpty()) {
      // If compaction DAG is not available (already cleaned up), fall back to
      //  the slower approach.
      LOG.warn("RocksDBCheckpointDiffer is not available, falling back to" +
          " slow path");

      Set<String> fromSnapshotFiles =
          RdbUtil.getSSTFilesForComparison(
              fromSnapshot.getMetadataManager().getStore()
                  .getDbLocation().getPath(),
              tablesToLookUp);
      Set<String> toSnapshotFiles =
          RdbUtil.getSSTFilesForComparison(
              toSnapshot.getMetadataManager().getStore()
                  .getDbLocation().getPath(),
              tablesToLookUp);

      deltaFiles.addAll(fromSnapshotFiles);
      deltaFiles.addAll(toSnapshotFiles);
    }

    return deltaFiles;
  }

  private void generateDiffReport(
      final String requestId,
      final PersistentSet<Long> objectIDsToCheck,
      final PersistentMap<Long, String> oldObjIdToKeyMap,
      final PersistentMap<Long, String> newObjIdToKeyMap,
      final PersistentList<DiffReportEntry> diffReport
  ) throws RocksDBException, IOException {

    ColumnFamilyHandle deleteDiffColumnFamily = null;
    ColumnFamilyHandle renameDiffColumnFamily = null;
    ColumnFamilyHandle createDiffColumnFamily = null;
    ColumnFamilyHandle modifyDiffColumnFamily = null;

    try {
      // RequestId is prepended to column family name to make it unique
      // for request.
      deleteDiffColumnFamily = db.get().createColumnFamily(
          new ColumnFamilyDescriptor(
              codecRegistry.asRawData(requestId + "-deleteDiff"),
              new ManagedColumnFamilyOptions()));
      renameDiffColumnFamily = db.get().createColumnFamily(
          new ColumnFamilyDescriptor(
              codecRegistry.asRawData(requestId + "-renameDiff"),
              new ManagedColumnFamilyOptions()));
      createDiffColumnFamily = db.get().createColumnFamily(
          new ColumnFamilyDescriptor(
              codecRegistry.asRawData(requestId + "-createDiff"),
              new ManagedColumnFamilyOptions()));
      modifyDiffColumnFamily = db.get().createColumnFamily(
          new ColumnFamilyDescriptor(
              codecRegistry.asRawData(requestId + "-modifyDiff"),
              new ManagedColumnFamilyOptions()));

      final PersistentList<DiffReportEntry> deleteDiffs =
          createDiffReportPersistentList(deleteDiffColumnFamily);
      final PersistentList<DiffReportEntry> renameDiffs =
          createDiffReportPersistentList(renameDiffColumnFamily);
      final PersistentList<DiffReportEntry> createDiffs =
          createDiffReportPersistentList(createDiffColumnFamily);
      final PersistentList<DiffReportEntry> modifyDiffs =
          createDiffReportPersistentList(modifyDiffColumnFamily);

      Iterator<Long> objectIdsIterator = objectIDsToCheck.iterator();
      while (objectIdsIterator.hasNext()) {
        Long id = objectIdsIterator.next();
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
         *    present in oldKeyTable and present in newKeyTable but with
         *    different name and same Object ID.
         */

        final String oldKeyName = oldObjIdToKeyMap.get(id);
        final String newKeyName = newObjIdToKeyMap.get(id);

        if (oldKeyName == null && newKeyName == null) {
          // This cannot happen.
          throw new IllegalStateException("Old and new key name both are null");
        } else if (oldKeyName == null) { // Key Created.
          createDiffs.add(DiffReportEntry.of(DiffType.CREATE, newKeyName));
        } else if (newKeyName == null) { // Key Deleted.
          deleteDiffs.add(DiffReportEntry.of(DiffType.DELETE, oldKeyName));
        } else if (oldKeyName.equals(newKeyName)) { // Key modified.
          modifyDiffs.add(DiffReportEntry.of(DiffType.MODIFY, newKeyName));
        } else { // Key Renamed.
          renameDiffs.add(
              DiffReportEntry.of(DiffType.RENAME, oldKeyName, newKeyName));
        }
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

      diffReport.addAll(deleteDiffs);
      diffReport.addAll(renameDiffs);
      diffReport.addAll(createDiffs);
      diffReport.addAll(modifyDiffs);
    } finally {
      if (deleteDiffColumnFamily != null) {
        db.get().dropColumnFamily(deleteDiffColumnFamily);
        deleteDiffColumnFamily.close();
      }
      if (renameDiffColumnFamily != null) {
        db.get().dropColumnFamily(renameDiffColumnFamily);
        renameDiffColumnFamily.close();
      }
      if (createDiffColumnFamily != null) {
        db.get().dropColumnFamily(createDiffColumnFamily);
        createDiffColumnFamily.close();
      }
      if (modifyDiffColumnFamily != null) {
        db.get().dropColumnFamily(modifyDiffColumnFamily);
        modifyDiffColumnFamily.close();
      }
    }
  }

  private PersistentList<DiffReportEntry> createDiffReportPersistentList(
      ColumnFamilyHandle columnFamilyHandle
  ) {
    return new RocksDbPersistentList<>(db,
        columnFamilyHandle,
        codecRegistry,
        DiffReportEntry.class);
  }

  private BucketLayout getBucketLayout(final String volume,
                                       final String bucket,
                                       final OMMetadataManager mManager)
      throws IOException {
    final String bucketTableKey = mManager.getBucketKey(volume, bucket);
    return mManager.getBucketTable().get(bucketTableKey).getBucketLayout();
  }

  private boolean areKeysEqual(WithObjectID oldKey, WithObjectID newKey) {
    if (oldKey == null && newKey == null) {
      return true;
    }
    if (oldKey != null) {
      return oldKey.equals(newKey);
    }
    return false;
  }
}
