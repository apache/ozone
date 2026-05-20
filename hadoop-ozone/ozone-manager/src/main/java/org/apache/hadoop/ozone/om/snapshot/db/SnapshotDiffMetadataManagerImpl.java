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

package org.apache.hadoop.ozone.om.snapshot.db;

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.getDBDirPath;
import static org.apache.hadoop.hdds.utils.db.cache.TableCache.CacheType.PARTIAL_CACHE;
import static org.apache.hadoop.ozone.om.snapshot.db.SnapshotDiffDBDefinition.SNAP_DIFF_FROM_SNAP_OBJECT_TABLE_DEF;
import static org.apache.hadoop.ozone.om.snapshot.db.SnapshotDiffDBDefinition.SNAP_DIFF_JOB_TABLE_DEF;
import static org.apache.hadoop.ozone.om.snapshot.db.SnapshotDiffDBDefinition.SNAP_DIFF_PURGED_JOB_TABLE_DEF;
import static org.apache.hadoop.ozone.om.snapshot.db.SnapshotDiffDBDefinition.SNAP_DIFF_REPORT_TABLE_NAME_DEF;
import static org.apache.hadoop.ozone.om.snapshot.db.SnapshotDiffDBDefinition.SNAP_DIFF_TO_SNAP_OBJECT_TABLE_DEF;
import static org.apache.hadoop.ozone.om.snapshot.db.SnapshotDiffDBDefinition.SNAP_DIFF_UNIQUE_IDS_TABLE_DEF;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.ozone.om.helpers.SnapshotDiffJob;
import org.apache.hadoop.ozone.om.snapshot.diff.helper.SnapshotDiffObjectInfo;

/**
 * Implementation of the {@link SnapshotDiffMetadataManager} interface.
 * Provides functionality for managing metadata related to snapshot difference
 * operations, including managing tables for snapshot diff jobs, reports, purged jobs,
 * and snapshot object information.
 *
 * This class handles the persistent storage of snapshot diff metadata using a database.
 * It ensures proper initialization and versioning of the metadata schema.
 */
public class SnapshotDiffMetadataManagerImpl implements SnapshotDiffMetadataManager {

  private static final String VERSION_FILE = "VERSION";
  private static final String VERSION_NUMBER = "1";

  private final DBStore dbStore;

  private final Table<String, SnapshotDiffJob> snapshotDiffJobTable;
  private final Table<String, SnapshotDiffReport.DiffReportEntry> snapshotDiffReportTable;
  private final Table<String, Long> snapshotDiffPurgedJobTable;
  private final Table<String, SnapshotDiffObjectInfo> snapshotDiffFromSnapshotObjectInfoTable;
  private final Table<String, SnapshotDiffObjectInfo> snapshotDiffToSnapshotObjectInfoTable;
  private final Table<String, Boolean> snapshotDiffUniqueObjectIdsTable;

  public SnapshotDiffMetadataManagerImpl(ConfigurationSource conf) throws IOException {
    File dbPath = getDBDirPath(SnapshotDiffDBDefinition.get(), conf).toPath()
        .resolve(SnapshotDiffDBDefinition.get().getName()).toFile();
    Path versionFilePath = dbPath.toPath().resolve(VERSION_FILE);
    boolean versionContentMatches = checkAndDeleteIfVersionMismatches(dbPath, versionFilePath);

    this.dbStore = DBStoreBuilder.newBuilder(conf, SnapshotDiffDBDefinition.get(), dbPath).build();

    this.snapshotDiffJobTable = SNAP_DIFF_JOB_TABLE_DEF.getTable(dbStore, PARTIAL_CACHE);
    this.snapshotDiffReportTable = SNAP_DIFF_REPORT_TABLE_NAME_DEF.getTable(dbStore, PARTIAL_CACHE);
    this.snapshotDiffPurgedJobTable = SNAP_DIFF_PURGED_JOB_TABLE_DEF.getTable(dbStore, PARTIAL_CACHE);
    this.snapshotDiffFromSnapshotObjectInfoTable =
        SNAP_DIFF_FROM_SNAP_OBJECT_TABLE_DEF.getTable(dbStore, PARTIAL_CACHE);
    this.snapshotDiffToSnapshotObjectInfoTable = SNAP_DIFF_TO_SNAP_OBJECT_TABLE_DEF.getTable(dbStore, PARTIAL_CACHE);
    this.snapshotDiffUniqueObjectIdsTable = SNAP_DIFF_UNIQUE_IDS_TABLE_DEF.getTable(dbStore, PARTIAL_CACHE);
    if (!versionContentMatches) {
      writeVersionFileContent(versionFilePath);
    }
  }

  /**
   * Checks if the version specified in the version file matches the expected version.
   * If the versions do not match and the database path exists, the database directory
   * is deleted.
   *
   * @param dbPath the path to the database directory that may be deleted in case of
   *               a version mismatch
   * @param versionFilePath the path to the file containing the version information
   * @return {@code true} if the version matches, {@code false} otherwise
   * @throws IOException if an I/O error occurs while reading the version file or deleting
   *                     the database directory
   */
  private boolean checkAndDeleteIfVersionMismatches(File dbPath, Path versionFilePath) throws IOException {
    boolean versionContentMatches = VERSION_NUMBER.equals(getVersionFileContent(versionFilePath));
    if (!versionContentMatches) {
      if (dbPath.exists()) {
        deleteDirectory(dbPath);
      }
    }
    return versionContentMatches;
  }

  private static String getVersionFileContent(Path versionFilePath) throws IOException {
    if (versionFilePath.toFile().exists()) {
      return StringCodec.get().fromPersistedFormat(Files.readAllBytes(versionFilePath));
    }
    return null;
  }

  private static void writeVersionFileContent(Path versionFilePath) throws IOException {
    Files.write(versionFilePath, StringCodec.get().toPersistedFormat(VERSION_NUMBER));
  }

  @Override
  public Table<String, SnapshotDiffJob> getSnapshotDiffJobTable() {
    return snapshotDiffJobTable;
  }

  @Override
  public Table<String, SnapshotDiffReport.DiffReportEntry> getSnapshotDiffReportTable() {
    return snapshotDiffReportTable;
  }

  @Override
  public Table<String, Long> getSnapshotDiffPurgedJobTable() {
    return snapshotDiffPurgedJobTable;
  }

  @Override
  public Table<String, SnapshotDiffObjectInfo> getSnapshotDiffFromSnapshotObjectInfoTable() {
    return snapshotDiffFromSnapshotObjectInfoTable;
  }

  @Override
  public Table<String, SnapshotDiffObjectInfo> getSnapshotDiffToSnapshotObjectInfoTable() {
    return snapshotDiffToSnapshotObjectInfoTable;
  }

  @Override
  public Table<String, Boolean> getSnapshotDiffUniqueObjectIdsTable() {
    return snapshotDiffUniqueObjectIdsTable;
  }

  @Override
  public void close() throws Exception {
    dbStore.close();
  }
}
