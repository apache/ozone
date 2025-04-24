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

package org.apache.hadoop.hdds.utils.db;

import java.math.BigDecimal;
import java.nio.file.Path;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.db.managed.ManagedBlockBasedTableConfig;
import org.apache.hadoop.hdds.utils.db.managed.ManagedBloomFilter;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedLRUCache;
import org.rocksdb.CompactionStyle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User visible configs based RocksDB tuning page. Documentation for Options.
 * <p>
 * https://github.com/facebook/rocksdb/blob/master/include/rocksdb/options.h
 * <p>
 * Most tuning parameters are based on this URL.
 * <p>
 * https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning
 */
public enum DBProfile {
  //TODO : Add more profiles like TEST etc.
  SSD {
    @Override
    public String toString() {
      return "SSD";
    }

    @Override
    public ManagedDBOptions getDBOptions(Path optionsPath) {
      ManagedDBOptions option = null;
      try {
        if (optionsPath != null) {
          option = DBConfigFromFile.readDBOptionsFromFile(optionsPath);
        }
      } catch (RocksDBException ex) {
        LOG.error("Unable to read RocksDB DBOptions from {}, exception {}", optionsPath, ex);
      }
      if (option != null) {
        LOG.info("Using RocksDB DBOptions from {}.ini file", optionsPath);
      } else {
        final int maxBackgroundCompactions = 4;
        final int maxBackgroundFlushes = 2;
        final long bytesPerSync = toLong(StorageUnit.MB.toBytes(1.00));
        final boolean createIfMissing = true;
        final boolean createMissingColumnFamilies = true;
        ManagedDBOptions dbOptions = new ManagedDBOptions();
        dbOptions
            .setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
            .setMaxBackgroundCompactions(maxBackgroundCompactions)
            .setMaxBackgroundFlushes(maxBackgroundFlushes)
            .setBytesPerSync(bytesPerSync)
            .setCreateIfMissing(createIfMissing)
            .setCreateMissingColumnFamilies(createMissingColumnFamilies);
        return dbOptions;
      }
      return option;
    }

    @Override
    public ManagedColumnFamilyOptions getColumnFamilyOptions(Path optionsPath, String cfName) {
      ManagedColumnFamilyOptions option = null;
      if (optionsPath == null) {
        LOG.debug("RocksDB path is null");
        return null;
      }
      try {
        option = DBConfigFromFile.readCFOptionsFromFile(optionsPath, cfName);
      } catch (RocksDBException ex) {
        LOG.error("Unable to read RocksDB CFOptions from {}, exception {}", optionsPath, ex);
      }

      if (option != null) {
        LOG.info("Using RocksDB CFOptions from {}.ini file", optionsPath);
        // TODO HDDS-12695: RocksDB 7.x doesn't read TableConfigs from files, remove this setting once upgraded
        option.setTableFormatConfig(createDefaultBlockBasedTableConfig());
        return option;
      }

      final long writeBufferSize = toLong(StorageUnit.MB.toBytes(128));
      ManagedColumnFamilyOptions managedColumnFamilyOptions =
          new ManagedColumnFamilyOptions();
      managedColumnFamilyOptions.setLevelCompactionDynamicLevelBytes(true)
          .setWriteBufferSize(writeBufferSize)
          .setTableFormatConfig(createDefaultBlockBasedTableConfig());
      return managedColumnFamilyOptions;
    }

    @Override
    public ManagedBlockBasedTableConfig getBlockBasedTableConfig(Path optionsPath, String cfName) {
      // TODO HDDS-12695: RocksDB 7.x doesn't read TableConfigs from files,
      //  so this can be commented out once upgrade happens.
      /*
      ManagedBlockBasedTableConfig option = new ManagedBlockBasedTableConfig();
      if (dbPath != null && dbPath.toFile().exists() && StringUtil.isNotBlank(cfName)) {
        try {
          ManagedColumnFamilyOptions cfOption = DBConfigFromFile.readCFOptionsFromFile(dbPath, cfName);
          if (cfOption != null) {
            option.setAllProperties((BlockBasedTableConfig) cfOption.tableFormatConfig());
            LOG.info("Using RocksDB BlockBasedTableConfig from {}.ini file", dbPath);
            return option;
          }
        } catch (IOException ex) {
          option.close();
          LOG.error("Unable to read RocksDB BlockBasedTableConfig from {}, exception {}", dbPath, ex);
        }
      }*/
      return createDefaultBlockBasedTableConfig();
    }

    private ManagedBlockBasedTableConfig createDefaultBlockBasedTableConfig() {
      final long blockCacheSize = toLong(StorageUnit.MB.toBytes(256.00));

      // Set the Default block size to 16KB
      final long blockSize = toLong(StorageUnit.KB.toBytes(16));

      ManagedBlockBasedTableConfig config = new ManagedBlockBasedTableConfig();
      config.setBlockCache(new ManagedLRUCache(blockCacheSize))
          .setBlockSize(blockSize)
          .setPinL0FilterAndIndexBlocksInCache(true)
          .setFilterPolicy(new ManagedBloomFilter());
      return config;
    }

  },
  DISK {
    @Override
    public String toString() {
      return "DISK";
    }

    @Override
    public ManagedDBOptions getDBOptions(Path optionsPath) {
      final long readAheadSize = toLong(StorageUnit.MB.toBytes(4.00));
      ManagedDBOptions dbOptions = SSD.getDBOptions(optionsPath);
      dbOptions.setCompactionReadaheadSize(readAheadSize);
      return dbOptions;
    }

    @Override
    public ManagedColumnFamilyOptions getColumnFamilyOptions(Path optionsPath, String cfName) {
      ManagedColumnFamilyOptions cfOptions = SSD.getColumnFamilyOptions(optionsPath, cfName);
      cfOptions.setCompactionStyle(CompactionStyle.LEVEL);
      return cfOptions;
    }

    @Override
    public ManagedBlockBasedTableConfig getBlockBasedTableConfig(Path optionsPath, String cfName) {
      return SSD.getBlockBasedTableConfig(optionsPath, cfName);
    }
  },
  TEST {
    @Override
    public String toString() {
      return "TEST";
    }

    @Override
    public ManagedDBOptions getDBOptions(Path optionsPath) {
      return SSD.getDBOptions(optionsPath);
    }

    @Override
    public ManagedColumnFamilyOptions getColumnFamilyOptions(Path optionsPath, String cfName) {
      ManagedColumnFamilyOptions cfOptions = SSD.getColumnFamilyOptions(optionsPath, cfName);
      cfOptions.setCompactionStyle(CompactionStyle.LEVEL);
      cfOptions.setDisableAutoCompactions(true);
      return cfOptions;
    }

    @Override
    public ManagedBlockBasedTableConfig getBlockBasedTableConfig(Path optionsPath, String cfName) {
      return SSD.getBlockBasedTableConfig(optionsPath, cfName);
    }
  };

  public static long toLong(double value) {
    BigDecimal temp = BigDecimal.valueOf(value);
    return temp.longValue();
  }

  private static final Logger LOG = LoggerFactory.getLogger(DBProfile.class);

  public abstract ManagedDBOptions getDBOptions(Path optionsPath);

  public abstract ManagedColumnFamilyOptions getColumnFamilyOptions(Path optionsPath, String cfName);

  public abstract ManagedBlockBasedTableConfig getBlockBasedTableConfig(Path optionsPath, String cfName);
}
