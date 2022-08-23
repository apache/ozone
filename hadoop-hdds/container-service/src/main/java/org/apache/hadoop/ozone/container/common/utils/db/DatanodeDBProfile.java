/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.utils.db;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.managed.ManagedBlockBasedTableConfig;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedLRUCache;

import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_METADATA_ROCKSDB_CACHE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_METADATA_ROCKSDB_CACHE_SIZE_DEFAULT;

/**
 * The class manages DBProfiles for Datanodes. Since ColumnFamilyOptions need to
 * be shared across containers the options are maintained in the profile itself.
 */
public abstract class DatanodeDBProfile {

  /**
   * Returns DBOptions to be used for rocksDB in datanodes.
   */
  public abstract ManagedDBOptions getDBOptions();

  /**
   * Returns ColumnFamilyOptions to be used for rocksDB column families in
   * datanodes.
   */
  public abstract ManagedColumnFamilyOptions getColumnFamilyOptions(
      ConfigurationSource config);

  /**
   * Returns DatanodeDBProfile for corresponding storage type.
   */
  public static DatanodeDBProfile getProfile(DBProfile dbProfile) {
    switch (dbProfile) {
    case SSD:
      return new SSD();
    case DISK:
      return new Disk();
    default:
      throw new IllegalArgumentException(
          "DatanodeDBProfile does not exist for " + dbProfile);
    }
  }

  /**
   * DBProfile for SSD datanode disks.
   */
  public static class SSD extends DatanodeDBProfile {
    private static final StorageBasedProfile SSD_STORAGE_BASED_PROFILE =
        new StorageBasedProfile(DBProfile.SSD);

    @Override
    public ManagedDBOptions getDBOptions() {
      return SSD_STORAGE_BASED_PROFILE.getDBOptions();
    }

    @Override
    public ManagedColumnFamilyOptions getColumnFamilyOptions(
        ConfigurationSource config) {
      return SSD_STORAGE_BASED_PROFILE.getColumnFamilyOptions(config);
    }
  }

  /**
   * DBProfile for HDD datanode disks.
   */
  public static class Disk extends DatanodeDBProfile {
    private static final StorageBasedProfile DISK_STORAGE_BASED_PROFILE =
        new StorageBasedProfile(DBProfile.DISK);

    @Override
    public ManagedDBOptions getDBOptions() {
      return DISK_STORAGE_BASED_PROFILE.getDBOptions();
    }

    @Override
    public ManagedColumnFamilyOptions getColumnFamilyOptions(
        ConfigurationSource config) {
      return DISK_STORAGE_BASED_PROFILE.getColumnFamilyOptions(config);
    }
  }

  /**
   * Base profile for datanode storage disks.
   */
  private static final class StorageBasedProfile {
    private final DBProfile baseProfile;

    private StorageBasedProfile(DBProfile profile) {
      baseProfile = profile;
    }

    private ManagedDBOptions getDBOptions() {
      return baseProfile.getDBOptions();
    }

    private ManagedColumnFamilyOptions getColumnFamilyOptions(
        ConfigurationSource config) {
      ManagedColumnFamilyOptions cfOptions =
          baseProfile.getColumnFamilyOptions();
      return cfOptions.closeAndSetTableFormatConfig(
          getBlockBasedTableConfig(config));
    }

    private ManagedBlockBasedTableConfig getBlockBasedTableConfig(
        ConfigurationSource config) {
      ManagedBlockBasedTableConfig blockBasedTableConfig =
          baseProfile.getBlockBasedTableConfig();
      if (config == null) {
        return blockBasedTableConfig;
      }

      long cacheSize = (long) config
          .getStorageSize(HDDS_DATANODE_METADATA_ROCKSDB_CACHE_SIZE,
              HDDS_DATANODE_METADATA_ROCKSDB_CACHE_SIZE_DEFAULT,
              StorageUnit.BYTES);
      blockBasedTableConfig.closeAndSetBlockCache(
          new ManagedLRUCache(cacheSize));
      return blockBasedTableConfig;
    }
  }
}
