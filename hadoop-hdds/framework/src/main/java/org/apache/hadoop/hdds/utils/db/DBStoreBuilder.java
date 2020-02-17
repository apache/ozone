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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import com.google.common.base.Preconditions;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF;
import org.eclipse.jetty.util.StringUtil;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DBStore Builder.
 */
public final class DBStoreBuilder {

  private static final Logger LOG =
      LoggerFactory.getLogger(DBStoreBuilder.class);

  public static final Logger ROCKS_DB_LOGGER =
      LoggerFactory.getLogger(RocksDB.class);

  private static final String DEFAULT_COLUMN_FAMILY_NAME =
      StringUtils.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY);

  // DB PKIProfile used by ROCKDB instances.
  public static final DBProfile HDDS_DEFAULT_DB_PROFILE = DBProfile.DISK;

  private Set<TableConfig> tables;
  private DBProfile dbProfile;
  private DBOptions rocksDBOption;
  private String dbname;
  private Path dbPath;
  private List<String> tableNames;
  private Configuration configuration;
  private CodecRegistry registry;
  private String rocksDbStat;
  private RocksDBConfiguration rocksDBConfiguration;

  private DBStoreBuilder(OzoneConfiguration configuration) {
    this(configuration, configuration.getObject(RocksDBConfiguration.class));
  }
  private DBStoreBuilder(OzoneConfiguration configuration,
      RocksDBConfiguration rocksDBConfiguration) {
    tables = new HashSet<>();
    tableNames = new LinkedList<>();
    this.configuration = configuration;
    this.registry = new CodecRegistry();
    this.rocksDbStat = configuration.getTrimmed(
        OZONE_METADATA_STORE_ROCKSDB_STATISTICS,
        OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT);
    this.rocksDBConfiguration = rocksDBConfiguration;
  }


  public static DBStoreBuilder newBuilder(OzoneConfiguration configuration) {
    return new DBStoreBuilder(configuration);
  }

  public static DBStoreBuilder newBuilder(OzoneConfiguration configuration,
      RocksDBConfiguration rocksDBConfiguration) {
    return new DBStoreBuilder(configuration, rocksDBConfiguration);
  }

  public DBStoreBuilder setProfile(DBProfile profile) {
    dbProfile = profile;
    return this;
  }

  public DBStoreBuilder setName(String name) {
    dbname = name;
    return this;
  }

  public DBStoreBuilder addTable(String tableName) {
    tableNames.add(tableName);
    return this;
  }

  public <T> DBStoreBuilder addCodec(Class<T> type, Codec<T> codec) {
    registry.addCodec(type, codec);
    return this;
  }

  public DBStoreBuilder addTable(String tableName, ColumnFamilyOptions option)
      throws IOException {
    LOG.debug("using custom profile for table: {}", tableName);
    return addTableDefinition(tableName, option);
  }

  private DBStoreBuilder addTableDefinition(String tableName,
      ColumnFamilyOptions option) throws IOException {
    TableConfig tableConfig = new TableConfig(tableName, option);
    if (!tables.add(tableConfig)) {
      String message = "Unable to add the table: " + tableName +
          ".  Please check if this table name is already in use.";
      LOG.error(message);
      throw new IOException(message);
    }
    return this;
  }

  public DBStoreBuilder setDBOption(DBOptions option) {
    rocksDBOption = option;
    return this;
  }

  public DBStoreBuilder setPath(Path path) {
    Preconditions.checkNotNull(path);
    dbPath = path;
    return this;
  }

  /**
   * Builds a DBStore instance and returns that.
   *
   * @return DBStore
   */
  public DBStore build() throws IOException {
    if(StringUtil.isBlank(dbname) || (dbPath == null)) {
      LOG.error("Required Parameter missing.");
      throw new IOException("Required parameter is missing. Please make sure "
          + "sure Path and DB name is provided.");
    }
    processDBProfile();
    processTables();
    DBOptions options = getDbProfile();

    WriteOptions writeOptions = new WriteOptions();
    writeOptions.setSync(rocksDBConfiguration.getSyncOption());


    File dbFile = getDBFile();
    if (!dbFile.getParentFile().exists()) {
      throw new IOException("The DB destination directory should exist.");
    }
    return new RDBStore(dbFile, options, writeOptions, tables, registry);
  }

  /**
   * if the DBProfile is not set, we will default to using default from the
   * config file.
   */
  private void processDBProfile() {
    if (dbProfile == null) {
      dbProfile = this.configuration.getEnum(HDDS_DB_PROFILE,
          HDDS_DEFAULT_DB_PROFILE);
    }
    LOG.debug("default profile:{}", dbProfile);
  }

  private void processTables() throws IOException {
    List<String> list = new ArrayList<>(tableNames);
    list.add(DEFAULT_COLUMN_FAMILY_NAME);
    for (String name : list) {
      LOG.debug("using default profile for table:{}", name);
      addTableDefinition(name, dbProfile.getColumnFamilyOptions());
    }
  }

  private DBOptions getDbProfile() {
    if (rocksDBOption != null) {
      return rocksDBOption;
    }
    DBOptions option = null;
    if (StringUtil.isNotBlank(dbname)) {
      List<ColumnFamilyDescriptor> columnFamilyDescriptors = new LinkedList<>();

      for (TableConfig tc : tables) {
        columnFamilyDescriptors.add(tc.getDescriptor());
      }

      if (columnFamilyDescriptors.size() > 0) {
        try {
          option = DBConfigFromFile.readFromFile(dbname,
              columnFamilyDescriptors);
          if(option != null) {
            LOG.info("Using Configs from {}.ini file", dbname);
          }
        } catch (IOException ex) {
          LOG.info("Unable to read RocksDB config from {}", dbname, ex);
        }
      }
    }

    if (option == null) {
      LOG.debug("Using default options: {}", dbProfile);
      option = dbProfile.getDBOptions();
    }

    if (rocksDBConfiguration.isRocksdbLoggingEnabled()) {
      org.rocksdb.Logger logger = new org.rocksdb.Logger(option) {
        @Override
        protected void log(InfoLogLevel infoLogLevel, String s) {
          ROCKS_DB_LOGGER.info(s);
        }
      };
      InfoLogLevel level = InfoLogLevel.valueOf(rocksDBConfiguration
          .getRocksdbLogLevel() + "_LEVEL");
      logger.setInfoLogLevel(level);
      option.setLogger(logger);
    }

    if (!rocksDbStat.equals(OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF)) {
      Statistics statistics = new Statistics();
      statistics.setStatsLevel(StatsLevel.valueOf(rocksDbStat));
      option = option.setStatistics(statistics);
    }
    return option;
  }

  private File getDBFile() throws IOException {
    if (dbPath == null) {
      LOG.error("DB path is required.");
      throw new IOException("A Path to for DB file is needed.");
    }

    if (StringUtil.isBlank(dbname)) {
      LOG.error("DBName is a required.");
      throw new IOException("A valid DB name is required.");
    }
    return Paths.get(dbPath.toString(), dbname).toFile();
  }

}
