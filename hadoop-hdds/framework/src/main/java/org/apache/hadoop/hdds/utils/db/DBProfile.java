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
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.db.managed.ManagedBlockBasedTableConfig;
import org.apache.hadoop.hdds.utils.db.managed.ManagedBloomFilter;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedLRUCache;
import org.rocksdb.CompactionStyle;

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
    public ManagedDBOptions getDBOptions() {
      ManagedDBOptions dbOptions = new ManagedDBOptions();
      dbOptions
          .setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
          .setMaxBackgroundCompactions(4)
          .setMaxBackgroundFlushes(2)
          .setBytesPerSync(toLong(StorageUnit.MB.toBytes(1.00)))
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
      return dbOptions;
    }

    @Override
    public ManagedColumnFamilyOptions getColumnFamilyOptions() {
      ManagedColumnFamilyOptions managedColumnFamilyOptions =
          new ManagedColumnFamilyOptions();
      managedColumnFamilyOptions.setLevelCompactionDynamicLevelBytes(true)
          .setWriteBufferSize(toLong(StorageUnit.MB.toBytes(128)))
          .setTableFormatConfig(createDefaultBlockBasedTableConfig());
      return managedColumnFamilyOptions;
    }

    @Override
    public ManagedBlockBasedTableConfig getBlockBasedTableConfig() {
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
    public ManagedDBOptions getDBOptions() {
      final long readAheadSize = toLong(StorageUnit.MB.toBytes(4.00));
      ManagedDBOptions dbOptions = SSD.getDBOptions();
      dbOptions.setCompactionReadaheadSize(readAheadSize);
      return dbOptions;
    }

    @Override
    public ManagedColumnFamilyOptions getColumnFamilyOptions() {
      ManagedColumnFamilyOptions cfOptions = SSD.getColumnFamilyOptions();
      cfOptions.setCompactionStyle(CompactionStyle.LEVEL);
      return cfOptions;
    }

    @Override
    public ManagedBlockBasedTableConfig getBlockBasedTableConfig() {
      return SSD.getBlockBasedTableConfig();
    }
  },
  TEST {
    @Override
    public String toString() {
      return "TEST";
    }

    @Override
    public ManagedDBOptions getDBOptions() {
      return SSD.getDBOptions();
    }

    @Override
    public ManagedColumnFamilyOptions getColumnFamilyOptions() {
      ManagedColumnFamilyOptions cfOptions = SSD.getColumnFamilyOptions();
      cfOptions.setCompactionStyle(CompactionStyle.LEVEL);
      cfOptions.setDisableAutoCompactions(true);
      return cfOptions;
    }

    @Override
    public ManagedBlockBasedTableConfig getBlockBasedTableConfig() {
      return SSD.getBlockBasedTableConfig();
    }
  };

  public static long toLong(double value) {
    BigDecimal temp = BigDecimal.valueOf(value);
    return temp.longValue();
  }

  public abstract ManagedDBOptions getDBOptions();

  public abstract ManagedColumnFamilyOptions getColumnFamilyOptions();

  public abstract ManagedBlockBasedTableConfig getBlockBasedTableConfig();
}
