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

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.db.managed.ManagedBlockBasedTableConfig;
import org.apache.hadoop.hdds.utils.db.managed.ManagedBloomFilter;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedLRUCache;
import org.rocksdb.CompactionStyle;

import java.math.BigDecimal;

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
    public ManagedColumnFamilyOptions getColumnFamilyOptions() {
      // Write Buffer Size -- set to 128 MB
      final long writeBufferSize = toLong(StorageUnit.MB.toBytes(128));

      ManagedColumnFamilyOptions managedColumnFamilyOptions =
          new ManagedColumnFamilyOptions();

      managedColumnFamilyOptions.setLevelCompactionDynamicLevelBytes(true)
          .setWriteBufferSize(writeBufferSize)
          .setTableFormatConfig(getBlockBasedTableConfig());

      return managedColumnFamilyOptions;
    }

    @Override
    public ManagedDBOptions getDBOptions() {
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

    @Override
    public ManagedBlockBasedTableConfig getBlockBasedTableConfig() {
      // Set BlockCacheSize to 256 MB. This should not be an issue for HADOOP.
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
  };

  public static long toLong(double value) {
    BigDecimal temp = BigDecimal.valueOf(value);
    return temp.longValue();
  }

  public abstract ManagedDBOptions getDBOptions();

  public abstract ManagedColumnFamilyOptions getColumnFamilyOptions();

  public abstract ManagedBlockBasedTableConfig getBlockBasedTableConfig();
}
