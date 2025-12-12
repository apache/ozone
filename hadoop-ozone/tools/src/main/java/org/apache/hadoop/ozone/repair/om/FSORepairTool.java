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

package org.apache.hadoop.ozone.repair.om;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Stack;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.ByteArrayCodec;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.repair.RepairTool;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Base Tool to identify and repair disconnected FSO trees across all buckets.
 * This tool logs information about reachable, pendingToDelete and orphaned files and directories in debug mode
 * and moves these orphaned files and directories to the deleted tables in repair mode.

 * If deletes are still in progress (the deleted directory table is not empty), the tool
 * reports that the tree is pendingToDelete, even though pending deletes would fix the issue.
 * If not, the tool reports them as orphaned and deletes them in repair mode.

 * Before using the tool, make sure all OMs are stopped, and that all Ratis logs have been flushed to the OM DB.
 * This can be done using `ozone admin prepare` before running the tool, and `ozone admin
 * cancelprepare` when done.

 * The tool will run a DFS from each bucket, and save all reachable directories as objectID-based keys in a
 * temporary RocksDB instance called "temp.db" in the same directory as om.db. It will also scan the
 * deletedDirectoryTable to identify objects pending deletion and store them as original keys in a pendingToDelete
 * table within the same temp.db instance.

 * It will then scan the entire file and directory tables for each bucket to classify each object:
 * - REACHABLE: Object's parent is in the reachable table (accessible from bucket root)
 * - PENDING_TO_DELETE: Object is in the pendingToDelete table (pending deletion)
 * - ORPHANED: Object is neither reachable nor pendingToDelete (orphaned, needs repair)
 * The tool is idempotent. temp.db will be automatically deleted when the tool finishes to ensure clean state.
 */
@CommandLine.Command(
    name = "fso-tree",
    description = "Identify and repair a disconnected FSO tree by marking orphaned entries for " +
        "deletion. OM should be stopped while this tool is run."
)
public class FSORepairTool extends RepairTool {
  private static final Logger LOG = LoggerFactory.getLogger(FSORepairTool.class);
  private static final String REACHABLE_TABLE = "reachable";
  private static final String PENDING_TO_DELETE_TABLE = "pendingToDelete";
  private static final byte[] EMPTY_BYTE_ARRAY = {};

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Path to OM RocksDB")
  private String omDBPath;

  @CommandLine.Option(names = {"-v", "--volume"},
      description = "Filter by volume name. Add '/' before the volume name.")
  private String volumeFilter;

  @CommandLine.Option(names = {"-b", "--bucket"},
      description = "Filter by bucket name")
  private String bucketFilter;

  @Nonnull
  @Override
  protected Component serviceToBeOffline() {
    return Component.OM;
  }

  @Override
  public void execute() throws Exception {
    try {
      Impl repairTool = new Impl();
      repairTool.run();
    } catch (Exception ex) {
      LOG.error("FSO repair failed", ex);
      throw new IllegalArgumentException("FSO repair failed: " + ex.getMessage());
    }

    if (isVerbose()) {
      info("FSO repair finished.");
    }
  }

  private class Impl {

    private final DBStore store;
    private final Table<String, OmVolumeArgs> volumeTable;
    private final Table<String, OmBucketInfo> bucketTable;
    private final Table<String, OmDirectoryInfo> directoryTable;
    private final Table<String, OmKeyInfo> fileTable;
    private final Table<String, OmKeyInfo> deletedDirectoryTable;
    private final Table<String, RepeatedOmKeyInfo> deletedTable;
    private final Table<String, SnapshotInfo> snapshotInfoTable;
    private DBStore tempDB;
    private Table<String, byte[]> reachableTable;
    private Table<String, byte[]> pendingToDeleteTable;
    private final ReportStatistics reachableStats;
    private final ReportStatistics pendingToDeleteStats;
    private final ReportStatistics orphanedStats;

    Impl() throws IOException {
      this.reachableStats = new ReportStatistics(0, 0, 0);
      this.pendingToDeleteStats = new ReportStatistics(0, 0, 0);
      this.orphanedStats = new ReportStatistics(0, 0, 0);

      this.store = getStoreFromPath(omDBPath);
      this.volumeTable = OMDBDefinition.VOLUME_TABLE_DEF.getTable(store);
      this.bucketTable = OMDBDefinition.BUCKET_TABLE_DEF.getTable(store);
      this.directoryTable = OMDBDefinition.DIRECTORY_TABLE_DEF.getTable(store);
      this.fileTable = OMDBDefinition.FILE_TABLE_DEF.getTable(store);
      this.deletedDirectoryTable = OMDBDefinition.DELETED_DIR_TABLE_DEF.getTable(store);
      this.deletedTable = OMDBDefinition.DELETED_TABLE_DEF.getTable(store);
      this.snapshotInfoTable = OMDBDefinition.SNAPSHOT_INFO_TABLE_DEF.getTable(store);
    }

    public Report run() throws Exception {
      try {
        if (bucketFilter != null && volumeFilter == null) {
          error("--bucket flag cannot be used without specifying --volume.");
          return null;
        }

        if (volumeFilter != null) {
          OmVolumeArgs volumeArgs = volumeTable.getIfExist(volumeFilter);
          if (volumeArgs == null) {
            error("Volume '" + volumeFilter + "' does not exist.");
            return null;
          }
        }

        // Iterate all volumes or a specific volume if specified
        try (TableIterator<String, ? extends Table.KeyValue<String, OmVolumeArgs>>
                 volumeIterator = volumeTable.iterator()) {
          try {
            openTempDB();
          } catch (IOException e) {
            error("Failed to open reachable database: " + e.getMessage());
            throw e;
          }
          while (volumeIterator.hasNext()) {
            Table.KeyValue<String, OmVolumeArgs> volumeEntry = volumeIterator.next();
            String volumeKey = volumeEntry.getKey();

            if (volumeFilter != null && !volumeFilter.equals(volumeKey)) {
              continue;
            }

            info("Processing volume: " + volumeKey);

            if (bucketFilter != null) {
              OmBucketInfo bucketInfo = bucketTable.getIfExist(volumeKey + "/" + bucketFilter);
              if (bucketInfo == null) {
                //Bucket does not exist in the volume
                error("Bucket '" + bucketFilter + "' does not exist in volume '" + volumeKey + "'.");
                return null;
              }

              if (bucketInfo.getBucketLayout() != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
                info("Skipping non-FSO bucket " + bucketFilter);
                continue;
              }

              processBucket(volumeEntry.getValue(), bucketInfo);
            } else {

              // Iterate all buckets in the volume.
              try (TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
                       bucketIterator = bucketTable.iterator()) {
                bucketIterator.seek(volumeKey);
                while (bucketIterator.hasNext()) {
                  Table.KeyValue<String, OmBucketInfo> bucketEntry = bucketIterator.next();
                  String bucketKey = bucketEntry.getKey();
                  OmBucketInfo bucketInfo = bucketEntry.getValue();

                  if (bucketInfo.getBucketLayout() != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
                    info("Skipping non-FSO bucket " + bucketKey);
                    continue;
                  }

                  // Stop this loop once we have seen all buckets in the current
                  // volume.
                  if (!bucketKey.startsWith(volumeKey)) {
                    break;
                  }

                  processBucket(volumeEntry.getValue(), bucketInfo);
                }
              }
            }
          }
        }
      } catch (IOException e) {
        error("An error occurred while processing" + e.getMessage());
        throw e;
      } finally {
        closeTempDB();
        store.close();
      }

      return buildReportAndLog();
    }

    private boolean checkIfSnapshotExistsForBucket(String volumeName, String bucketName) throws IOException {
      if (snapshotInfoTable == null) {
        return false;
      }

      try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>> iterator =
               snapshotInfoTable.iterator()) {
        while (iterator.hasNext()) {
          SnapshotInfo snapshotInfo = iterator.next().getValue();
          String snapshotPath = (volumeName + "/" + bucketName).replaceFirst("^/", "");
          if (snapshotInfo.getSnapshotPath().equals(snapshotPath)) {
            return true;
          }
        }
      }
      return false;
    }

    private void processBucket(OmVolumeArgs volume, OmBucketInfo bucketInfo) throws IOException {
      if (checkIfSnapshotExistsForBucket(volume.getVolume(), bucketInfo.getBucketName())) {
        info("Skipping repair for bucket '" + volume.getVolume() + "/" + bucketInfo.getBucketName() + "' " +
                "due to snapshot presence.");
        return;
      }
      info("Processing bucket: " + volume.getVolume() + "/" + bucketInfo.getBucketName());
      markReachableObjectsInBucket(volume, bucketInfo);
      markPendingToDeleteObjectsInBucket(volume, bucketInfo);
      handlePendingToDeleteAndOrphanedObjects(volume, bucketInfo);
    }

    private Report buildReportAndLog() {
      Report report = new Report.Builder()
          .setReachable(reachableStats)
          .setPendingToDelete(pendingToDeleteStats)
          .setOrphaned(orphanedStats)
          .build();

      info("\n" + report);
      return report;
    }

    private void markReachableObjectsInBucket(OmVolumeArgs volume, OmBucketInfo bucket) throws IOException {
      // Only put directories in the stack.
      // Directory keys should have the form /volumeID/bucketID/parentID/name.
      Stack<String> dirKeyStack = new Stack<>();

      // Since the tool uses parent directories to check for reachability, add
      // a reachable entry for the bucket as well.
      addReachableEntry(volume, bucket, bucket);
      // Initialize the stack with all immediate child directories of the
      // bucket, and mark them all as reachable.
      Collection<String> childDirs = getChildDirectoriesAndMarkAsReachable(volume, bucket, bucket);
      dirKeyStack.addAll(childDirs);

      while (!dirKeyStack.isEmpty()) {
        // Get one directory and process its immediate children.
        String currentDirKey = dirKeyStack.pop();
        OmDirectoryInfo currentDir = directoryTable.get(currentDirKey);
        if (currentDir == null) {
          if (isVerbose()) {
            info("Directory key" + currentDirKey + "to be processed was not found in the directory table.");
          }
          continue;
        }

        // TODO revisit this for a more memory efficient implementation,
        //  possibly making better use of RocksDB iterators.
        childDirs = getChildDirectoriesAndMarkAsReachable(volume, bucket, currentDir);
        dirKeyStack.addAll(childDirs);
      }
    }

    private void markPendingToDeleteObjectsInBucket(OmVolumeArgs volume, OmBucketInfo bucket) throws IOException {
      // Only put directories in the stack.
      // Directory keys should have the form /volumeID/bucketID/parentID/name.
      Stack<String> dirKeyStack = new Stack<>();

      // Find all deleted directories in this bucket and process their children
      String bucketPrefix = OM_KEY_PREFIX + volume.getObjectID() + OM_KEY_PREFIX + bucket.getObjectID();

      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> deletedDirIterator =
               deletedDirectoryTable.iterator()) {
        deletedDirIterator.seek(bucketPrefix);
        while (deletedDirIterator.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> deletedDirEntry = deletedDirIterator.next();
          String deletedDirKey = deletedDirEntry.getKey();

          // Only process deleted directories in this bucket
          if (!deletedDirKey.startsWith(bucketPrefix)) {
            break;
          }

          // Extract the objectID from the deleted directory entry
          OmKeyInfo deletedDirInfo = deletedDirEntry.getValue();
          long deletedObjectID = deletedDirInfo.getObjectID();

          // Build the prefix that children would have: /volID/bucketID/deletedObjectID/
          String childPrefix = OM_KEY_PREFIX + volume.getObjectID() + OM_KEY_PREFIX + bucket.getObjectID() +
              OM_KEY_PREFIX + deletedObjectID + OM_KEY_PREFIX;

          // Find all children of this deleted directory and mark as pendingToDelete
          Collection<String> childDirs = getChildDirectoriesAndMarkAsPendingToDelete(childPrefix);
          dirKeyStack.addAll(childDirs);
        }
      }

      while (!dirKeyStack.isEmpty()) {
        // Get one directory and process its immediate children.
        String currentDirKey = dirKeyStack.pop();
        OmDirectoryInfo currentDir = directoryTable.get(currentDirKey);
        if (currentDir == null) {
          if (isVerbose()) {
            info("Directory key" + currentDirKey + "to be processed was not found in the directory table.");
          }
          continue;
        }

        // For pendingToDelete directories, we need to build the prefix based on their objectID
        String childPrefix = OM_KEY_PREFIX + volume.getObjectID() + OM_KEY_PREFIX + bucket.getObjectID() +
            OM_KEY_PREFIX + currentDir.getObjectID() + OM_KEY_PREFIX;
        Collection<String> childDirs = getChildDirectoriesAndMarkAsPendingToDelete(childPrefix);
        dirKeyStack.addAll(childDirs);
      }
    }

    private void handlePendingToDeleteAndOrphanedObjects(OmVolumeArgs volume, OmBucketInfo bucket) throws IOException {
      // Check for pendingToDelete and orphaned directories in the bucket.
      String bucketPrefix = OM_KEY_PREFIX +
          volume.getObjectID() +
          OM_KEY_PREFIX +
          bucket.getObjectID();

      try (TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>> dirIterator =
               directoryTable.iterator()) {
        dirIterator.seek(bucketPrefix);
        while (dirIterator.hasNext()) {
          Table.KeyValue<String, OmDirectoryInfo> dirEntry = dirIterator.next();
          String dirKey = dirEntry.getKey();

          // Only search directories in this bucket.
          if (!dirKey.startsWith(bucketPrefix)) {
            break;
          }

          if (!isReachable(dirKey)) {
            if (!isPendingToDelete(dirKey)) {
              orphanedStats.addDir();

              info("Deleting orphaned directory " + dirKey);
              if (!isDryRun()) {
                OmDirectoryInfo dirInfo = dirEntry.getValue();
                markDirectoryForDeletion(volume.getVolume(), bucket.getBucketName(), dirKey, dirInfo);
              }
            }
          }
        }
      }

      // Check for pendingToDelete and orphaned files
      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
               fileIterator = fileTable.iterator()) {
        fileIterator.seek(bucketPrefix);
        while (fileIterator.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> fileEntry = fileIterator.next();
          String fileKey = fileEntry.getKey();
          // Only search files in this bucket.
          if (!fileKey.startsWith(bucketPrefix)) {
            break;
          }

          OmKeyInfo fileInfo = fileEntry.getValue();
          if (!isReachable(fileKey)) {
            if (!isPendingToDelete(fileKey)) {
              orphanedStats.addFile(fileInfo.getDataSize());

              info("Deleting orphaned file " + fileKey);
              if (!isDryRun()) {
                markFileForDeletion(bucket, fileKey, fileInfo);
              }
            }
          } else {
            // NOTE: We are deserializing the proto of every reachable file
            // just to log it's size. If we don't need this information we could
            // save time by skipping this step.
            reachableStats.addFile(fileInfo.getDataSize());
          }
        }
      }
    }

    protected void markFileForDeletion(OmBucketInfo bucketInfo, String fileKey, OmKeyInfo fileInfo) throws IOException {
      try (BatchOperation batch = store.initBatchOperation()) {
        fileTable.deleteWithBatch(batch, fileKey);

        RepeatedOmKeyInfo originalRepeatedKeyInfo = deletedTable.get(fileKey);
        RepeatedOmKeyInfo updatedRepeatedOmKeyInfo = OmUtils.prepareKeyForDelete(bucketInfo.getObjectID(),
            fileInfo, fileInfo.getUpdateID());
        // NOTE: The FSO code seems to write the open key entry with the whole
        // path, using the object's names instead of their ID. This would only
        // be possible when the file is deleted explicitly, and not part of a
        // directory delete. It is also not possible here if the file's parent
        // is gone. The name of the key does not matter so just use IDs.
        deletedTable.putWithBatch(batch, fileKey, updatedRepeatedOmKeyInfo);
        if (isVerbose()) {
          info("Added entry " + fileKey + " to open key table: " + updatedRepeatedOmKeyInfo);
        }
        store.commitBatchOperation(batch);
      }
    }

    protected void markDirectoryForDeletion(String volumeName, String bucketName,
        String dirKeyName, OmDirectoryInfo dirInfo) throws IOException {
      try (BatchOperation batch = store.initBatchOperation()) {
        directoryTable.deleteWithBatch(batch, dirKeyName);
        // HDDS-7592: Make directory entries in deleted dir table unique.
        String deleteDirKeyName = dirKeyName + OM_KEY_PREFIX + dirInfo.getObjectID();

        // Convert the directory to OmKeyInfo for deletion.
        OmKeyInfo dirAsKeyInfo = OMFileRequest.getOmKeyInfo(volumeName, bucketName, dirInfo, dirInfo.getName());
        deletedDirectoryTable.putWithBatch(batch, deleteDirKeyName, dirAsKeyInfo);

        store.commitBatchOperation(batch);
      }
    }

    private Collection<String> getChildDirectoriesAndMarkAsReachable(OmVolumeArgs volume, OmBucketInfo bucket,
        WithObjectID currentDir) throws IOException {

      Collection<String> childDirs = new ArrayList<>();

      try (TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>>
               dirIterator = directoryTable.iterator()) {
        String dirPrefix = buildReachableKey(volume, bucket, currentDir);
        // Start searching the directory table at the current directory's
        // prefix to get its immediate children.
        dirIterator.seek(dirPrefix);
        while (dirIterator.hasNext()) {
          Table.KeyValue<String, OmDirectoryInfo> childDirEntry = dirIterator.next();
          String childDirKey = childDirEntry.getKey();
          // Stop processing once we have seen all immediate children of this
          // directory.
          if (!childDirKey.startsWith(dirPrefix)) {
            break;
          }
          // This directory was reached by search.
          addReachableEntry(volume, bucket, childDirEntry.getValue());
          childDirs.add(childDirKey);
          reachableStats.addDir();
        }
      }

      return childDirs;
    }

    private Collection<String> getChildDirectoriesAndMarkAsPendingToDelete(String dirPrefix) throws IOException {
      Collection<String> childDirs = new ArrayList<>();

      // Find child directories and mark them as pendingToDelete
      try (TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>>
               dirIterator = directoryTable.iterator()) {
        // Start searching the directory table at the current directory's
        // prefix to get its immediate children.
        dirIterator.seek(dirPrefix);
        while (dirIterator.hasNext()) {
          Table.KeyValue<String, OmDirectoryInfo> childDirEntry = dirIterator.next();
          String childDirKey = childDirEntry.getKey();
          // Stop processing once we have seen all immediate children of this directory.
          if (!childDirKey.startsWith(dirPrefix)) {
            break;
          }

          // Ensure this is an immediate child, not a deeper descendant
          String relativePath = childDirKey.substring(dirPrefix.length());
          if (!relativePath.contains(OM_KEY_PREFIX)) {
            addPendingToDeleteEntry(childDirKey);
            childDirs.add(childDirKey);
            pendingToDeleteStats.addDir();
          }
        }
      }

      // Find child files and mark them as pendingToDelete
      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> fileIterator =
               fileTable.iterator()) {
        fileIterator.seek(dirPrefix);
        while (fileIterator.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> childFileEntry = fileIterator.next();
          String childFileKey = childFileEntry.getKey();
          // Stop if we've moved beyond children of this parent
          if (!childFileKey.startsWith(dirPrefix)) {
            break;
          }

          // Ensure this is an immediate child, not a deeper descendant
          String relativePath = childFileKey.substring(dirPrefix.length());
          if (!relativePath.contains(OM_KEY_PREFIX)) {
            addPendingToDeleteEntry(childFileKey);
            pendingToDeleteStats.addFile(childFileEntry.getValue().getDataSize());
          }
        }
      }

      return childDirs;
    }

    /**
     * Add the specified object to the reachable table, indicating it is part
     * of the connected FSO tree.
     */
    private void addReachableEntry(OmVolumeArgs volume, OmBucketInfo bucket, WithObjectID object) throws IOException {
      String reachableKey = buildReachableKey(volume, bucket, object);
      // No value is needed for this table.
      reachableTable.put(reachableKey, EMPTY_BYTE_ARRAY);
    }

    /**
     * Add the specified object to the pendingToDelete table, indicating it is part
     * of the disconnected FSO tree.
     */
    private void addPendingToDeleteEntry(String originalKey) throws IOException {
      // No value is needed for this table.
      pendingToDeleteTable.put(originalKey, EMPTY_BYTE_ARRAY);
    }

    /**
     * @param fileOrDirKey The key of a file or directory in RocksDB.
     * @return true if the entry's parent is in the reachable table.
     */
    protected boolean isReachable(String fileOrDirKey) throws IOException {
      String reachableParentKey = buildReachableParentKey(fileOrDirKey);

      return reachableTable.get(reachableParentKey) != null;
    }

    /**
     * @param fileOrDirKey The key of a file or directory in RocksDB.
     * @return true if the entry itself is in the pendingToDelete table.
     */
    protected boolean isPendingToDelete(String fileOrDirKey) throws IOException {
      return pendingToDeleteTable.get(fileOrDirKey) != null;
    }

    private void openTempDB() throws IOException {
      File tempDBFile = new File(new File(omDBPath).getParentFile(), "temp.db");
      info("Creating database with reachable and pendingToDelete tables at " + tempDBFile);
      // Delete the DB from the last run if it exists.
      if (tempDBFile.exists()) {
        FileUtils.deleteDirectory(tempDBFile);
      }

      ConfigurationSource conf = new OzoneConfiguration();
      tempDB = DBStoreBuilder.newBuilder(conf)
          .setName("temp.db")
          .setPath(tempDBFile.getParentFile().toPath())
          .addTable(REACHABLE_TABLE)
          .addTable(PENDING_TO_DELETE_TABLE)
          .build();
      reachableTable = tempDB.getTable(REACHABLE_TABLE, StringCodec.get(), ByteArrayCodec.get());
      pendingToDeleteTable = tempDB.getTable(PENDING_TO_DELETE_TABLE, StringCodec.get(), ByteArrayCodec.get());
    }

    private void closeTempDB() throws IOException {
      if (tempDB != null) {
        tempDB.close();
      }
      File tempDBFile = new File(new File(omDBPath).getParentFile(), "temp.db");
      if (tempDBFile.exists()) {
        FileUtils.deleteDirectory(tempDBFile);
      }
    }
  }

  protected static DBStore getStoreFromPath(String dbPath) throws IOException {
    File omDBFile = new File(dbPath);
    if (!omDBFile.exists() || !omDBFile.isDirectory()) {
      throw new IOException(String.format("Specified OM DB instance %s does " +
          "not exist or is not a RocksDB directory.", dbPath));
    }
    // Load RocksDB and tables needed.
    // Use the actual directory name from the provided path (e.g., "om.db", "om-db-backup")
    String dbName = omDBFile.getName();
    return DBStoreBuilder.newBuilder(new OzoneConfiguration(), OMDBDefinition.get(), dbName,
        omDBFile.getParentFile().toPath())
        .setOpenReadOnly(false)
        .setCreateCheckpointDirs(true)
        .setEnableRocksDbMetrics(true)
        .setMaxNumberOfOpenFiles(-1)
        .build();
  }

  /**
   * Build an entry in the reachable table for the current object, which
   * could be a bucket, file or directory.
   */
  private static String buildReachableKey(OmVolumeArgs volume, OmBucketInfo bucket, WithObjectID object) {
    return OM_KEY_PREFIX +
        volume.getObjectID() +
        OM_KEY_PREFIX +
        bucket.getObjectID() +
        OM_KEY_PREFIX +
        object.getObjectID();
  }

  /**
   * Build an entry in the reachable table for the current object's parent
   * object. The object could be a file or directory.
   */
  private static String buildReachableParentKey(String fileOrDirKey) {
    String[] keyParts = fileOrDirKey.split(OM_KEY_PREFIX);
    // Should be /volID/bucketID/parentID/name
    // The first part will be blank since key begins with a slash.
    Preconditions.assertTrue(keyParts.length >= 4);
    String volumeID = keyParts[1];
    String bucketID = keyParts[2];
    String parentID = keyParts[3];

    return OM_KEY_PREFIX +
        volumeID +
        OM_KEY_PREFIX +
        bucketID +
        OM_KEY_PREFIX +
        parentID;
  }

  /**
   * Define a Report to be created.
   */
  public static class Report {
    private final ReportStatistics reachable;
    private final ReportStatistics pendingToDelete;
    private final ReportStatistics orphaned;

    /**
     * Builds one report that is the aggregate of multiple others.
     */
    public Report(Report... reports) {
      reachable = new ReportStatistics();
      pendingToDelete = new ReportStatistics();
      orphaned = new ReportStatistics();

      for (Report report : reports) {
        reachable.add(report.reachable);
        pendingToDelete.add(report.pendingToDelete);
        orphaned.add(report.orphaned);
      }
    }

    private Report(Report.Builder builder) {
      this.reachable = builder.reachable;
      this.pendingToDelete = builder.pendingToDelete;
      this.orphaned = builder.orphaned;
    }

    public ReportStatistics getReachable() {
      return reachable;
    }

    public ReportStatistics getPendingToDelete() {
      return pendingToDelete;
    }

    public ReportStatistics getOrphaned() {
      return orphaned;
    }

    @Override
    public String toString() {
      return "Reachable:" + reachable + "\nUnreachable (Pending to delete):" + pendingToDelete +
          "\nUnreferenced (Orphaned):" + orphaned;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      Report report = (Report) other;

      // Useful for testing.
      System.out.println("Comparing reports\nExpect:\n" + this + "\nActual:\n" + report);

      return reachable.equals(report.reachable) && pendingToDelete.equals(report.pendingToDelete) &&
             orphaned.equals(report.orphaned);
    }

    @Override
    public int hashCode() {
      return Objects.hash(reachable, pendingToDelete, orphaned);
    }

    /**
     * Builder class for a Report.
     */
    public static final class Builder {
      private ReportStatistics reachable = new ReportStatistics();
      private ReportStatistics pendingToDelete = new ReportStatistics();
      private ReportStatistics orphaned = new ReportStatistics();

      public Builder() {
      }

      public Builder setReachable(ReportStatistics reachable) {
        this.reachable = reachable;
        return this;
      }

      public Builder setPendingToDelete(ReportStatistics pendingToDelete) {
        this.pendingToDelete = pendingToDelete;
        return this;
      }

      public Builder setOrphaned(ReportStatistics orphaned) {
        this.orphaned = orphaned;
        return this;
      }

      public Report build() {
        return new Report(this);
      }
    }
  }

  /**
   * Represents the statistics of reachable and pendingToDelete data.
   * This gives the count of dirs, files and bytes.
   */

  public static class ReportStatistics {
    private long dirs;
    private long files;
    private long bytes;

    public ReportStatistics() { }

    public ReportStatistics(long dirs, long files, long bytes) {
      this.dirs = dirs;
      this.files = files;
      this.bytes = bytes;
    }

    public void add(ReportStatistics other) {
      this.dirs += other.dirs;
      this.files += other.files;
      this.bytes += other.bytes;
    }

    public long getDirs() {
      return dirs;
    }

    public long getFiles() {
      return files;
    }

    public long getBytes() {
      return bytes;
    }

    @Override
    public String toString() {
      return "\n\tDirectories: " + dirs +
              "\n\tFiles: " + files +
              "\n\tBytes: " + bytes;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      ReportStatistics stats = (ReportStatistics) other;

      return bytes == stats.bytes && files == stats.files && dirs == stats.dirs;
    }

    @Override
    public int hashCode() {
      return Objects.hash(bytes, files, dirs);
    }

    public void addDir() {
      dirs++;
    }

    public void addFile(long size) {
      files++;
      bytes += size;
    }
  }
}
