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

package org.apache.hadoop.hdds.utils;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import com.google.common.annotations.VisibleForTesting;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_TYPE_LEVELDB;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_TYPE_ROCKSDB;

import org.iq80.leveldb.Options;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for metadata store.
 */
public class MetadataStoreBuilder {

  @VisibleForTesting
  static final Logger LOG = LoggerFactory.getLogger(MetadataStoreBuilder.class);
  private File dbFile;
  private long cacheSize;
  private boolean createIfMissing = true;
  private Optional<ConfigurationSource> optionalConf = Optional.empty();
  private String dbType;
  @VisibleForTesting
  public static final Map<ConfigurationSource, org.rocksdb.Options>
      CACHED_OPTS = new ConcurrentHashMap<>();
  @VisibleForTesting
  public static final OzoneConfiguration DEFAULT_CONF =
      new OzoneConfiguration();

  public static MetadataStoreBuilder newBuilder() {
    return new MetadataStoreBuilder();
  }

  public MetadataStoreBuilder setDbFile(File dbPath) {
    this.dbFile = dbPath;
    return this;
  }

  public MetadataStoreBuilder setCacheSize(long cache) {
    this.cacheSize = cache;
    return this;
  }

  public MetadataStoreBuilder setCreateIfMissing(boolean doCreate) {
    this.createIfMissing = doCreate;
    return this;
  }

  public MetadataStoreBuilder setConf(ConfigurationSource configuration) {
    this.optionalConf = Optional.of(configuration);
    return this;
  }

  /**
   * Set the container DB Type.
   * @param type
   * @return MetadataStoreBuilder
   */
  public MetadataStoreBuilder setDBType(String type) {
    this.dbType = type;
    return this;
  }

  public MetadataStore build() throws IOException {
    if (dbFile == null) {
      throw new IllegalArgumentException("Failed to build metadata store, "
          + "dbFile is required but not found");
    }

    // Build db store based on configuration
    final ConfigurationSource conf = optionalConf.orElse(DEFAULT_CONF);

    if (dbType == null) {
      dbType = CONTAINER_DB_TYPE_ROCKSDB;
      LOG.debug("dbType is null, using dbType {}.", dbType);
    } else {
      LOG.debug("Using dbType {} for metastore", dbType);
    }
    if (CONTAINER_DB_TYPE_LEVELDB.equals(dbType)) {
      Options options = new Options();
      options.createIfMissing(createIfMissing);
      if (cacheSize > 0) {
        options.cacheSize(cacheSize);
      }
      return new LevelDBStore(dbFile, options);
    } else if (CONTAINER_DB_TYPE_ROCKSDB.equals(dbType)) {
      org.rocksdb.Options opts;
      // Used cached options if config object passed down is the same
      if (CACHED_OPTS.containsKey(conf)) {
        opts = CACHED_OPTS.get(conf);
      } else {
        opts = new org.rocksdb.Options();
        if (cacheSize > 0) {
          BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
          tableConfig.setBlockCacheSize(cacheSize);
          opts.setTableFormatConfig(tableConfig);
        }

        String rocksDbStat = conf.getTrimmed(
            OZONE_METADATA_STORE_ROCKSDB_STATISTICS,
            OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT);

        if (!rocksDbStat.equals(OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF)) {
          Statistics statistics = new Statistics();
          statistics.setStatsLevel(StatsLevel.valueOf(rocksDbStat));
          opts = opts.setStatistics(statistics);
        }
      }
      opts.setCreateIfMissing(createIfMissing);
      CACHED_OPTS.put(conf, opts);
      return new RocksDBStore(dbFile, opts);
    }
    
    throw new IllegalArgumentException("Invalid Container DB type. Expecting "
        + CONTAINER_DB_TYPE_LEVELDB + " or "
        + CONTAINER_DB_TYPE_ROCKSDB + ", but met " + dbType);
  }
}
