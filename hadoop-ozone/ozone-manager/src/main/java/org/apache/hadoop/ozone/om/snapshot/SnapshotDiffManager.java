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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.IntegerCodec;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConfigKeys;
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
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReport.DiffType;
import org.apache.hadoop.util.ClosableIterator;
import org.apache.ozone.rocksdb.util.ManagedSstFileReader;
import org.apache.ozone.rocksdb.util.RdbUtil;
import org.apache.ozone.rocksdiff.DifferSnapshotInfo;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.apache.ozone.rocksdiff.RocksDiffUtils;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Class to generate snapshot diff.
 */
public class SnapshotDiffManager {
  private static final Logger LOG =
          LoggerFactory.getLogger(SnapshotDiffManager.class);
  private static final String DELIMITER = "-";
  private static final String FROM_SNAP_TABLE_SUFFIX = "-from-snap";
  private static final String TO_SNAP_TABLE_SUFFIX = "-to-snap";
  private static final String UNIQUE_IDS_TABLE_SUFFIX = "-unique-ids";
  private static final String DELETE_DIFF_TABLE_SUFFIX = "-delete-diff";
  private static final String RENAME_DIFF_TABLE_SUFFIX = "-rename-diff";
  private static final String CREATE_DIFF_TABLE_SUFFIX = "-create-diff";
  private static final String MODIFY_DIFF_TABLE_SUFFIX = "-modify-diff";

  private final RocksDBCheckpointDiffer differ;
  private final ManagedRocksDB db;
  private final CodecRegistry codecRegistry;
  private final ManagedColumnFamilyOptions familyOptions;

  /**
   * Global table to keep the diff report. Each key is prefixed by the jobID
   * to improve look up and clean up.
   * Note that byte array is used to reduce the unnecessary serialization and
   * deserialization during intermediate steps.
   */
  private final PersistentMap<byte[], byte[]> snapDiffReportTable;

  /**
   * Contains all the snap diff jobs which are either queued, in_progress or
   * done. This table is used to make sure that there is only single job for
   * similar type of request at any point of time.
   */
  private final PersistentMap<String, String> snapDiffJobTable;

  private final OzoneConfiguration configuration;


  public SnapshotDiffManager(ManagedRocksDB db,
                             RocksDBCheckpointDiffer differ,
                             OzoneConfiguration configuration,
                             ColumnFamilyHandle snapDiffJobCfh,
                             ColumnFamilyHandle snapDiffReportCfh,
                             ManagedColumnFamilyOptions familyOptions) {
    this.db = db;
    this.differ = differ;
    this.configuration = configuration;
    this.familyOptions = familyOptions;
    this.codecRegistry = new CodecRegistry();

    // Integers are used for indexing persistent list.
    this.codecRegistry.addCodec(Integer.class, new IntegerCodec());
    // DiffReportEntry codec for Diff Report.
    this.codecRegistry.addCodec(DiffReportEntry.class,
        new OmDBDiffReportEntryCodec());

    this.snapDiffJobTable = new RocksDbPersistentMap<>(db,
        snapDiffJobCfh,
        codecRegistry,
        String.class,
        String.class);

    this.snapDiffReportTable = new RocksDbPersistentMap<>(db,
        snapDiffReportCfh,
        codecRegistry,
        byte[].class,
        byte[].class);
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

  @SuppressWarnings("parameternumber")
  public SnapshotDiffReport getSnapshotDiffReport(final String volume,
                                                  final String bucket,
                                                  final OmSnapshot fromSnapshot,
                                                  final OmSnapshot toSnapshot,
                                                  final SnapshotInfo fsInfo,
                                                  final SnapshotInfo tsInfo,
                                                  final int index,
                                                  final int pageSize,
                                                  final boolean forceFullDiff)
      throws IOException, RocksDBException {
    String diffJobKey = fsInfo.getSnapshotID() + DELIMITER +
        tsInfo.getSnapshotID();

    Pair<String, Boolean> jobIdToJobExist = getOrCreateJobId(diffJobKey);
    String jobId = jobIdToJobExist.getLeft();
    boolean jobExist = jobIdToJobExist.getRight();

    // If snapshot diff doesn't exist, we generate the diff report first
    // and add it to the table for future requests.
    // This needs to be updated to queuing and job status base.
    if (!jobExist) {
      generateSnapshotDiffReport(jobId, volume, bucket, fromSnapshot,
          toSnapshot, fsInfo, tsInfo, forceFullDiff);
    }

    List<DiffReportEntry> diffReportList = new ArrayList<>();

    boolean hasMoreEntries = true;

    for (int idx = index; idx - index < pageSize; idx++) {
      byte[] rawKey = codecRegistry.asRawData(jobId + DELIMITER + idx);
      byte[] bytes = snapDiffReportTable.get(rawKey);
      if (bytes == null) {
        hasMoreEntries = false;
        break;
      }
      diffReportList.add(codecRegistry.asObject(bytes, DiffReportEntry.class));
    }

    String tokenString = hasMoreEntries ?
        String.valueOf(index + pageSize) : null;

    return new SnapshotDiffReport(volume, bucket, fromSnapshot.getName(),
        toSnapshot.getName(), diffReportList, tokenString);
  }

  /**
   * Return the jobId from the table if it exists otherwise create a new one,
   * add to the table and return that.
   */
  private synchronized Pair<String, Boolean> getOrCreateJobId(
      String diffJobKey) {
    String jobId = snapDiffJobTable.get(diffJobKey);

    if (jobId != null) {
      return Pair.of(jobId, true);
    } else {
      jobId = UUID.randomUUID().toString();
      snapDiffJobTable.put(diffJobKey, jobId);
      return Pair.of(jobId, false);
    }
  }

  @SuppressWarnings("parameternumber")
  private void generateSnapshotDiffReport(final String jobId,
                                          final String volume,
                                          final String bucket,
                                          final OmSnapshot fromSnapshot,
                                          final OmSnapshot toSnapshot,
                                          final SnapshotInfo fsInfo,
                                          final SnapshotInfo tsInfo,
                                          final boolean forceFullDiff)
      throws RocksDBException {
    ColumnFamilyHandle fromSnapshotColumnFamily = null;
    ColumnFamilyHandle toSnapshotColumnFamily = null;
    ColumnFamilyHandle objectIDsColumnFamily = null;

    try {
      // JobId is prepended to column families name to make them unique
      // for request.
      fromSnapshotColumnFamily =
          createColumnFamily(jobId + FROM_SNAP_TABLE_SUFFIX);
      toSnapshotColumnFamily =
          createColumnFamily(jobId + TO_SNAP_TABLE_SUFFIX);
      objectIDsColumnFamily =
          createColumnFamily(jobId + UNIQUE_IDS_TABLE_SUFFIX);
      // ReportId is prepended to column families name to make them unique
      // for request.

      // ObjectId to keyName map to keep key info for fromSnapshot.
      // objectIdToKeyNameMap is used to identify what keys were touched
      // in which snapshot and to know the difference if operation was
      // creation, deletion, modify or rename.
      // Stores only keyName instead of OmKeyInfo to reduce the memory
      // footprint.
      // Note: Store objectId and keyName as byte array to reduce unnecessary
      // serialization and deserialization.
      final PersistentMap<byte[], byte[]> objectIdToKeyNameMapForFromSnapshot =
          new RocksDbPersistentMap<>(db,
              fromSnapshotColumnFamily,
              codecRegistry,
              byte[].class,
              byte[].class);

      // ObjectId to keyName map to keep key info for toSnapshot.
      final PersistentMap<byte[], byte[]> objectIdToKeyNameMapForToSnapshot =
          new RocksDbPersistentMap<>(db,
              toSnapshotColumnFamily,
              codecRegistry,
              byte[].class,
              byte[].class);

      // Set of unique objectId between fromSnapshot and toSnapshot.
      final PersistentSet<byte[]> objectIDsToCheckMap =
          new RocksDbPersistentSet<>(db,
              objectIDsColumnFamily,
              codecRegistry,
              byte[].class);

      final BucketLayout bucketLayout = getBucketLayout(volume, bucket,
          fromSnapshot.getMetadataManager());

      final Table<String, OmKeyInfo> fsKeyTable =
          fromSnapshot.getMetadataManager().getKeyTable(bucketLayout);
      final Table<String, OmKeyInfo> tsKeyTable =
          toSnapshot.getMetadataManager().getKeyTable(bucketLayout);

      boolean useFullDiff = configuration.getBoolean(
          OzoneConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF,
          OzoneConfigKeys.OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF_DEFAULT);
      if (forceFullDiff) {
        useFullDiff = true;
      }

      Map<String, String> tablePrefixes =
          getTablePrefixes(toSnapshot.getMetadataManager(), volume, bucket);

      final Set<String> deltaFilesForKeyOrFileTable =
          getDeltaFiles(fromSnapshot, toSnapshot,
              Collections.singletonList(fsKeyTable.getName()), fsInfo, tsInfo,
              useFullDiff, tablePrefixes);

      addToObjectIdMap(fsKeyTable,
          tsKeyTable,
          deltaFilesForKeyOrFileTable,
          objectIdToKeyNameMapForFromSnapshot,
          objectIdToKeyNameMapForToSnapshot,
          objectIDsToCheckMap,
          tablePrefixes);

      if (bucketLayout.isFileSystemOptimized()) {
        final Table<String, OmDirectoryInfo> fsDirTable =
            fromSnapshot.getMetadataManager().getDirectoryTable();
        final Table<String, OmDirectoryInfo> tsDirTable =
            toSnapshot.getMetadataManager().getDirectoryTable();
        final Set<String> deltaFilesForDirTable =
            getDeltaFiles(fromSnapshot, toSnapshot,
                Collections.singletonList(fsDirTable.getName()), fsInfo, tsInfo,
                useFullDiff, tablePrefixes);
        addToObjectIdMap(fsDirTable,
            tsDirTable,
            deltaFilesForDirTable,
            objectIdToKeyNameMapForFromSnapshot,
            objectIdToKeyNameMapForToSnapshot,
            objectIDsToCheckMap,
            tablePrefixes);
      }

      generateDiffReport(jobId,
          objectIDsToCheckMap,
          objectIdToKeyNameMapForFromSnapshot,
          objectIdToKeyNameMapForToSnapshot);
    } catch (IOException | RocksDBException exception) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(exception);
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
    }
  }

  private void addToObjectIdMap(Table<String, ? extends WithObjectID> fsTable,
                                Table<String, ? extends WithObjectID> tsTable,
                                Set<String> deltaFiles,
                                PersistentMap<byte[], byte[]> oldObjIdToKeyMap,
                                PersistentMap<byte[], byte[]> newObjIdToKeyMap,
                                PersistentSet<byte[]> objectIDsToCheck,
                                Map<String, String> tablePrefixes)
      throws IOException {

    if (deltaFiles.isEmpty()) {
      return;
    }

    boolean isDirectoryTable =
        fsTable.getName().equals(OmMetadataManagerImpl.DIRECTORY_TABLE);

    try (Stream<String> keysToCheck = new ManagedSstFileReader(deltaFiles)
            .getKeyStream()) {
      keysToCheck.forEach(key -> {
        try {
          final WithObjectID oldKey = fsTable.get(key);
          final WithObjectID newKey = tsTable.get(key);
          if (areKeysEqual(oldKey, newKey) || !isKeyInBucket(key, tablePrefixes,
              fsTable.getName())) {
            // We don't have to do anything.
            return;
          }
          if (oldKey != null) {
            byte[] rawObjId = codecRegistry.asRawData(oldKey.getObjectID());
            byte[] rawValue = codecRegistry.asRawData(
                getKeyOrDirectoryName(isDirectoryTable, oldKey));
            oldObjIdToKeyMap.put(rawObjId, rawValue);
            objectIDsToCheck.add(rawObjId);
          }
          if (newKey != null) {
            byte[] rawObjId = codecRegistry.asRawData(newKey.getObjectID());
            byte[] rawValue = codecRegistry.asRawData(
                getKeyOrDirectoryName(isDirectoryTable, newKey));
            newObjIdToKeyMap.put(rawObjId, rawValue);
            objectIDsToCheck.add(rawObjId);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (RocksDBException rocksDBException) {
      // TODO: [SNAPSHOT] Gracefully handle exception
      //  e.g. when input files do not exist
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
      boolean useFullDiff, Map<String, String> tablePrefixes)
      throws RocksDBException, IOException {
    // TODO: [SNAPSHOT] Refactor the parameter list

    final Set<String> deltaFiles = new HashSet<>();

    // Check if compaction DAG is available, use that if so
    if (differ != null && fsInfo != null && tsInfo != null && !useFullDiff) {
      String volume = fsInfo.getVolumeName();
      String bucket = fsInfo.getBucketName();
      // Construct DifferSnapshotInfo
      final DifferSnapshotInfo fromDSI =
          getDSIFromSI(fsInfo, fromSnapshot, volume, bucket);
      final DifferSnapshotInfo toDSI =
          getDSIFromSI(tsInfo, toSnapshot, volume, bucket);

      LOG.debug("Calling RocksDBCheckpointDiffer");
      List<String> sstDiffList =
          differ.getSSTDiffListWithFullPath(toDSI, fromDSI);
      deltaFiles.addAll(sstDiffList);

      // TODO: [SNAPSHOT] Remove the workaround below when the SnapDiff logic
      //  can read tombstones in SST files.
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

    if (useFullDiff || deltaFiles.isEmpty()) {
      // If compaction DAG is not available (already cleaned up), fall back to
      //  the slower approach.
      if (!useFullDiff) {
        LOG.warn("RocksDBCheckpointDiffer is not available, falling back to" +
                " slow path");
      }

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
      RocksDiffUtils.filterRelevantSstFiles(deltaFiles, tablePrefixes);
    }

    return deltaFiles;
  }

  private void generateDiffReport(
      final String jobId,
      final PersistentSet<byte[]> objectIDsToCheck,
      final PersistentMap<byte[], byte[]> oldObjIdToKeyMap,
      final PersistentMap<byte[], byte[]> newObjIdToKeyMap
  ) throws RocksDBException {

    ColumnFamilyHandle deleteDiffColumnFamily = null;
    ColumnFamilyHandle renameDiffColumnFamily = null;
    ColumnFamilyHandle createDiffColumnFamily = null;
    ColumnFamilyHandle modifyDiffColumnFamily = null;

    // RequestId is prepended to column family name to make it unique
    // for request.
    try {
      deleteDiffColumnFamily =
          createColumnFamily(jobId + DELETE_DIFF_TABLE_SUFFIX);
      renameDiffColumnFamily =
          createColumnFamily(jobId + RENAME_DIFF_TABLE_SUFFIX);
      createDiffColumnFamily =
          createColumnFamily(jobId + CREATE_DIFF_TABLE_SUFFIX);
      modifyDiffColumnFamily =
          createColumnFamily(jobId + MODIFY_DIFF_TABLE_SUFFIX);

      // Keep byte array instead of storing as DiffReportEntry to avoid
      // unnecessary serialization and deserialization.
      final PersistentList<byte[]> deleteDiffs =
          createDiffReportPersistentList(deleteDiffColumnFamily);
      final PersistentList<byte[]> renameDiffs =
          createDiffReportPersistentList(renameDiffColumnFamily);
      final PersistentList<byte[]> createDiffs =
          createDiffReportPersistentList(createDiffColumnFamily);
      final PersistentList<byte[]> modifyDiffs =
          createDiffReportPersistentList(modifyDiffColumnFamily);

      try (ClosableIterator<byte[]>
               objectIdsIterator = objectIDsToCheck.iterator()) {
        while (objectIdsIterator.hasNext()) {
          byte[] id = objectIdsIterator.next();
          /*
           * This key can be
           * -> Created after the old snapshot was taken, which means it will be
           *    missing in oldKeyTable and present in newKeyTable.
           * -> Deleted after the old snapshot was taken, which means it will be
           *    present in oldKeyTable and missing in newKeyTable.
           * -> Modified after the old snapshot was taken, which means it will
           *    be present in oldKeyTable and present in newKeyTable with same
           *    Object ID but with different metadata.
           * -> Renamed after the old snapshot was taken, which means it will be
           *    present in oldKeyTable and present in newKeyTable but with
           *    different name and same Object ID.
           */

          byte[] oldKeyName = oldObjIdToKeyMap.get(id);
          byte[] newKeyName = newObjIdToKeyMap.get(id);

          if (oldKeyName == null && newKeyName == null) {
            // This cannot happen.
            throw new IllegalStateException(
                "Old and new key name both are null");
          } else if (oldKeyName == null) { // Key Created.
            String key = codecRegistry.asObject(newKeyName, String.class);
            DiffReportEntry entry = DiffReportEntry.of(DiffType.CREATE, key);
            createDiffs.add(codecRegistry.asRawData(entry));
          } else if (newKeyName == null) { // Key Deleted.
            String key = codecRegistry.asObject(oldKeyName, String.class);
            DiffReportEntry entry = DiffReportEntry.of(DiffType.DELETE, key);
            deleteDiffs.add(codecRegistry.asRawData(entry));
          } else if (Arrays.equals(oldKeyName, newKeyName)) { // Key modified.
            String key = codecRegistry.asObject(newKeyName, String.class);
            DiffReportEntry entry = DiffReportEntry.of(DiffType.MODIFY, key);
            modifyDiffs.add(codecRegistry.asRawData(entry));
          } else { // Key Renamed.
            String oldKey = codecRegistry.asObject(oldKeyName, String.class);
            String newKey = codecRegistry.asObject(newKeyName, String.class);
            renameDiffs.add(codecRegistry.asRawData(
                DiffReportEntry.of(DiffType.RENAME, oldKey, newKey)));
          }
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

      int index = 0;
      index = addToReport(jobId, index, deleteDiffs);
      index = addToReport(jobId, index, renameDiffs);
      index = addToReport(jobId, index, createDiffs);
      addToReport(jobId, index, modifyDiffs);
    } catch (IOException e) {
      // TODO: [SNAPSHOT] Fail gracefully.
      throw new RuntimeException(e);
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

  private PersistentList<byte[]> createDiffReportPersistentList(
      ColumnFamilyHandle columnFamilyHandle
  ) {
    return new RocksDbPersistentList<>(db,
        columnFamilyHandle,
        codecRegistry,
        byte[].class);
  }

  private ColumnFamilyHandle createColumnFamily(String columnFamilyName)
      throws RocksDBException {
    return db.get().createColumnFamily(
        new ColumnFamilyDescriptor(
            StringUtils.string2Bytes(columnFamilyName),
            familyOptions));
  }

  private int addToReport(String jobId, int index,
                          PersistentList<byte[]> diffReportEntries)
      throws IOException {
    try (ClosableIterator<byte[]>
             diffReportIterator = diffReportEntries.iterator()) {
      while (diffReportIterator.hasNext()) {

        snapDiffReportTable.put(
            codecRegistry.asRawData(jobId + DELIMITER + index),
            diffReportIterator.next());
        index++;
      }
    }
    return index;
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

  /**
   * check if the given key is in the bucket specified by tablePrefix map.
   */
  private boolean isKeyInBucket(String key, Map<String, String> tablePrefixes,
      String tableName) {
    String volumeBucketDbPrefix;
    // In case of FSO - either File/Directory table
    // the key Prefix would be volumeId/bucketId and
    // in case of non-fso - volumeName/bucketName
    if (tableName.equals(
        OmMetadataManagerImpl.DIRECTORY_TABLE) || tableName.equals(
        OmMetadataManagerImpl.FILE_TABLE)) {
      volumeBucketDbPrefix =
          tablePrefixes.get(OmMetadataManagerImpl.DIRECTORY_TABLE);
    } else {
      volumeBucketDbPrefix = tablePrefixes.get(OmMetadataManagerImpl.KEY_TABLE);
    }
    return key.startsWith(volumeBucketDbPrefix);
  }
}
