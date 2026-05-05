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

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.OptionsUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Managed {@link RocksDB}.
 */
public class ManagedRocksDB extends ManagedObject<RocksDB> {
  public static final Class<RocksDB> ORIGINAL_CLASS = RocksDB.class;
  public static final int NOT_FOUND = RocksDB.NOT_FOUND;

  static final Logger LOG = LoggerFactory.getLogger(ManagedRocksDB.class);

  ManagedRocksDB(RocksDB original) {
    super(original);
  }

  public static ManagedRocksDB openReadOnly(
      final ManagedOptions options,
      final String path)
      throws RocksDBException {
    return new ManagedRocksDB(RocksDB.openReadOnly(options, path));
  }

  public static ManagedRocksDB openReadOnly(
      final ManagedDBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.openReadOnly(options, path,
            columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  public static ManagedRocksDB openReadOnly(
      final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.openReadOnly(path, columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  public static ManagedRocksDB open(
      final DBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.open(options, path,
            columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  /**
   * Open a RocksDB option with the latest options. Other than {@link ManagedConfigOptions} and
   * path, the other options and handles should be empty since it will be populated with
   * loadLatestOptions. Nevertheless, the caller is still responsible in cleaning up / closing the resources.
   * @param configOptions Config options.
   * @param options DBOptions. This would be modified based on the latest options.
   * @param path DB path.
   * @param columnFamilyDescriptors Column family descriptors. These would be modified based on the latest options.
   * @param columnFamilyHandles Column family handles of the underlying column family
   * @return A RocksDB instance.
   * @throws RocksDBException thrown if error happens in underlying native library.
   */
  public static ManagedRocksDB openWithLatestOptions(
      final ManagedConfigOptions configOptions,
      final DBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    // Preserve all the previous DB options
    OptionsUtil.loadLatestOptions(configOptions, path, options, columnFamilyDescriptors);
    return new ManagedRocksDB(
        RocksDB.open(options, path, columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  /**
   * Delete liveMetaDataFile from rocks db using RocksDB#deleteFile Api.
   * This function makes the RocksDB#deleteFile Api synchronized by waiting
   * for the deletes to happen.
   * @param fileToBeDeleted File to be deleted.
   * @throws RocksDatabaseException if the underlying db throws an exception
   *                                or the file is not deleted within a time limit.
   */
  public void deleteFile(LiveFileMetaData fileToBeDeleted) throws RocksDatabaseException {
    String sstFileName = fileToBeDeleted.fileName();
    File file = new File(fileToBeDeleted.path(), fileToBeDeleted.fileName());
    try {
      get().deleteFile(sstFileName);
    } catch (RocksDBException e) {
      throw new RocksDatabaseException("Failed to delete " + file, e);
    }
    ManagedRocksObjectUtils.waitForFileDelete(file, Duration.ofSeconds(60));
  }

  public static Map<String, LiveFileMetaData> getLiveMetadataForSSTFiles(RocksDB db) {
    return db.getLiveFilesMetaData().stream().collect(
            Collectors.toMap(liveFileMetaData -> FilenameUtils.getBaseName(liveFileMetaData.fileName()),
                liveFileMetaData -> liveFileMetaData));
  }

  public Map<String, LiveFileMetaData> getLiveMetadataForSSTFiles() {
    return getLiveMetadataForSSTFiles(this.get());
  }
}
