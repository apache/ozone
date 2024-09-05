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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.utils.db;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedBlockBasedTableConfig;
import org.apache.hadoop.hdds.utils.db.managed.ManagedBloomFilter;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedLRUCache;
import org.eclipse.jetty.util.StringUtil;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;

import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;


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
    public ManagedDBOptions getDBOptions(String dbName) {
      ManagedDBOptions option = null;
      if (StringUtil.isNotBlank(dbName)) {
        try {
          option = DBConfigFromFile.readDBOptionsFromFile(dbName);
          if (option != null) {
            LOG.info("Using RocksDB DBOptions from {}.ini file", dbName);
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
        } catch (IOException ex) {
          LOG.error("Unable to read RocksDB DBOptions from {}, exception {}", dbName, ex);
        }
      }
      return option;
    }
    @Override
    public Map<String, ManagedColumnFamilyOptions> getColumnFamilyOptions(String dbName) {
      Map<String, ManagedColumnFamilyOptions> options = null;
      if (StringUtil.isNotBlank(dbName)) {
        try {
          options = DBConfigFromFile.readCFOptionsFromFile(dbName);
          if (MapUtils.isNotEmpty(options)) {
            LOG.info("Using RocksDB CFOptions from {}.ini file", dbName);
          } else {
            final long writeBufferSize = toLong(StorageUnit.MB.toBytes(128));
            ManagedColumnFamilyOptions managedColumnFamilyOptions =
                new ManagedColumnFamilyOptions();
            managedColumnFamilyOptions.setLevelCompactionDynamicLevelBytes(true)
                .setWriteBufferSize(writeBufferSize)
                .setTableFormatConfig(createDefaultBlockBasedTableConfig());
            return Collections.singletonMap(StringUtils.bytes2String(DEFAULT_COLUMN_FAMILY),
                managedColumnFamilyOptions);
          }
        } catch (IOException ex) {
          LOG.error("Unable to read RocksDB CFOptions from {}, exception {}", dbName, ex);
        }
      }
      return options;
    }


    @Override
    public ManagedColumnFamilyOptions getColumnFamilyOptions(String dbName, String cfName) {
      ManagedColumnFamilyOptions option = null;
      if (StringUtil.isNotBlank(dbName)) {
        try {
          option = DBConfigFromFile.readCFOptionsFromFile(dbName, cfName);
          if (option != null) {
            LOG.info("Using RocksDB CFOptions from {}.ini file", dbName);
          } else {
            final long writeBufferSize = toLong(StorageUnit.MB.toBytes(128));
            ManagedColumnFamilyOptions managedColumnFamilyOptions =
                new ManagedColumnFamilyOptions();
            managedColumnFamilyOptions.setLevelCompactionDynamicLevelBytes(true)
                .setWriteBufferSize(writeBufferSize)
                .setTableFormatConfig(createDefaultBlockBasedTableConfig());
            return managedColumnFamilyOptions;
          }
        } catch (IOException ex) {
          LOG.error("Unable to read RocksDB CFOptions from {}, exception {}", dbName, ex);
        }
      }
      return option;
    }

    @Override
    public ManagedBlockBasedTableConfig getBlockBasedTableConfig(String dbName, String cfName) {
      ManagedBlockBasedTableConfig option = new ManagedBlockBasedTableConfig();
      if (StringUtil.isNotBlank(dbName) && StringUtil.isNotBlank(cfName)) {
        try {
          ManagedColumnFamilyOptions cfOption = DBConfigFromFile.readCFOptionsFromFile(dbName, cfName);
          if (cfOption != null) {
            option.setAllProperties((BlockBasedTableConfig) cfOption.tableFormatConfig());
            LOG.info("Using RocksDB BlockBasedTableConfig from {}.ini file", dbName);
            return option;
          }
        } catch (IOException ex) {
          LOG.error("Unable to read RocksDB BlockBasedTableConfig from {}, exception {}", dbName, ex);
        }
      }
      return createDefaultBlockBasedTableConfig();
    }

    private ManagedBlockBasedTableConfig createDefaultBlockBasedTableConfig() {
      final long blockCacheSize = toLong(StorageUnit.MB.toBytes(256.00));
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
    public ManagedDBOptions getDBOptions(String dbName) {
      final long readAheadSize = toLong(StorageUnit.MB.toBytes(4.00));
      ManagedDBOptions dbOptions = SSD.getDBOptions(dbName);
      dbOptions.setCompactionReadaheadSize(readAheadSize);
      return dbOptions;
    }

    @Override
    public Map<String, ManagedColumnFamilyOptions> getColumnFamilyOptions(String dbName) {
      return SSD.getColumnFamilyOptions(dbName);
    }

    @Override
    public ManagedColumnFamilyOptions getColumnFamilyOptions(String dbName, String cfName) {
      ManagedColumnFamilyOptions cfOptions = SSD.getColumnFamilyOptions(dbName, cfName);
      cfOptions.setCompactionStyle(CompactionStyle.LEVEL);
      return cfOptions;
    }

    @Override
    public ManagedBlockBasedTableConfig getBlockBasedTableConfig(String dbName, String cfName) {
      return SSD.getBlockBasedTableConfig(dbName, cfName);
    }
  },
  TEST {
    @Override
    public String toString() {
      return "TEST";
    }

    @Override
    public ManagedDBOptions getDBOptions(String dbName) {
      return SSD.getDBOptions(dbName);
    }

    @Override
    public Map<String, ManagedColumnFamilyOptions> getColumnFamilyOptions(String dbName) {
      return SSD.getColumnFamilyOptions(dbName);
    }

    @Override
    public ManagedColumnFamilyOptions getColumnFamilyOptions(String dbName, String cfName) {
      ManagedColumnFamilyOptions cfOptions = SSD.getColumnFamilyOptions(dbName, cfName);
      cfOptions.setCompactionStyle(CompactionStyle.LEVEL);
      cfOptions.setDisableAutoCompactions(true);
      return cfOptions;
    }

    @Override
    public ManagedBlockBasedTableConfig getBlockBasedTableConfig(String dbName, String cfName) {
      return SSD.getBlockBasedTableConfig(dbName, cfName);
    }
  };

  public static long toLong(double value) {
    BigDecimal temp = BigDecimal.valueOf(value);
    return temp.longValue();
  }

  private static final Logger LOG = LoggerFactory.getLogger(DBProfile.class);

  public abstract ManagedDBOptions getDBOptions(String dbName);

  public abstract Map<String, ManagedColumnFamilyOptions> getColumnFamilyOptions(String dbName);

  public abstract ManagedColumnFamilyOptions getColumnFamilyOptions(String dbName, String cfName);

  public abstract ManagedBlockBasedTableConfig getBlockBasedTableConfig(String dbName, String cfName);
}
