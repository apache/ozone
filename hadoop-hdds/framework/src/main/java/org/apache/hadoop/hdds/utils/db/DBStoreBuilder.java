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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;

import com.google.common.base.Preconditions;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;
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

  // the DBOptions used if the caller does not specify.
  private final DBOptions defaultDBOptions;
  // The DBOptions specified by the caller.
  private DBOptions rocksDBOption;
  // The column family options that will be used for any column families
  // added by name only (without specifying options).
  private ColumnFamilyOptions defaultCfOptions;
  private String dbname;
  private Path dbPath;
  // Maps added column family names to the column family options they were
  // added with. Value will be null if the column family was not added with
  // any options. On build, this will be replaced with defaultCfOptions.
  private Map<String, ColumnFamilyOptions> cfOptions;
  private ConfigurationSource configuration;
  private CodecRegistry registry;
  private String rocksDbStat;
  private RocksDBConfiguration rocksDBConfiguration;
  // Flag to indicate if the RocksDB should be opened readonly.
  private boolean openReadOnly = false;

  /**
   * Create DBStoreBuilder from a generic DBDefinition.
   */
  public static DBStore createDBStore(ConfigurationSource configuration,
      DBDefinition definition) throws IOException {
    return newBuilder(configuration, definition).build();
  }

  public static DBStoreBuilder newBuilder(ConfigurationSource configuration,
      DBDefinition definition) {

    DBStoreBuilder builder = newBuilder(configuration);
    builder.applyDBDefinition(definition);

    return builder;
  }

  public static DBStoreBuilder newBuilder(ConfigurationSource configuration) {
    return newBuilder(configuration,
        configuration.getObject(RocksDBConfiguration.class));
  }

  public static DBStoreBuilder newBuilder(ConfigurationSource configuration,
      RocksDBConfiguration rocksDBConfiguration) {
    return new DBStoreBuilder(configuration, rocksDBConfiguration);
  }

  private DBStoreBuilder(ConfigurationSource configuration,
      RocksDBConfiguration rocksDBConfiguration) {
    cfOptions = new HashMap<>();
    this.configuration = configuration;
    this.registry = new CodecRegistry();
    this.rocksDbStat = configuration.getTrimmed(
        OZONE_METADATA_STORE_ROCKSDB_STATISTICS,
        OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT);
    this.rocksDBConfiguration = rocksDBConfiguration;

    // Get default DBOptions and ColumnFamilyOptions from the default DB
    // profile.
    DBProfile dbProfile = this.configuration.getEnum(HDDS_DB_PROFILE,
          HDDS_DEFAULT_DB_PROFILE);
    LOG.debug("Default DB profile:{}", dbProfile);

    defaultDBOptions = dbProfile.getDBOptions();
    setDefaultCFOptions(dbProfile.getColumnFamilyOptions());
  }

  private void applyDBDefinition(DBDefinition definition) {
    // Set metadata dirs.
    File metadataDir = definition.getDBLocation(configuration);

    if (metadataDir == null) {
      LOG.warn("{} is not configured. We recommend adding this setting. " +
              "Falling back to {} instead.",
          definition.getLocationConfigKey(),
          HddsConfigKeys.OZONE_METADATA_DIRS);
      metadataDir = getOzoneMetaDirPath(configuration);
    }

    setName(definition.getName());
    setPath(Paths.get(metadataDir.getPath()));

    // Add column family names and codecs.
    for (DBColumnFamilyDefinition columnFamily :
        definition.getColumnFamilies()) {

      addTable(columnFamily.getName());
      addCodec(columnFamily.getKeyType(), columnFamily.getKeyCodec());
      addCodec(columnFamily.getValueType(), columnFamily.getValueCodec());
    }
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
          + "Path and DB name is provided.");
    }

    Set<TableConfig> tableConfigs = makeTableConfigs();

    if (rocksDBOption == null) {
      rocksDBOption = getDefaultDBOptions(tableConfigs);
    }

    WriteOptions writeOptions = new WriteOptions();
    writeOptions.setSync(rocksDBConfiguration.getSyncOption());

    File dbFile = getDBFile();
    if (!dbFile.getParentFile().exists()) {
      throw new IOException("The DB destination directory should exist.");
    }

    return new RDBStore(dbFile, rocksDBOption, writeOptions, tableConfigs,
        registry, openReadOnly);
  }

  public DBStoreBuilder setName(String name) {
    dbname = name;
    return this;
  }

  public DBStoreBuilder addTable(String tableName) {
    return addTable(tableName, null);
  }

  public DBStoreBuilder addTable(String tableName,
      ColumnFamilyOptions options) {
    cfOptions.put(tableName, options);
    return this;
  }

  public <T> DBStoreBuilder addCodec(Class<T> type, Codec<T> codec) {
    registry.addCodec(type, codec);
    return this;
  }

  public DBStoreBuilder setDBOptions(DBOptions option) {
    rocksDBOption = option;
    return this;
  }

  public DBStoreBuilder setDefaultCFOptions(
      ColumnFamilyOptions options) {
    defaultCfOptions = options;
    return this;
  }

  public DBStoreBuilder setPath(Path path) {
    Preconditions.checkNotNull(path);
    dbPath = path;
    return this;
  }

  public DBStoreBuilder setOpenReadOnly(boolean readOnly) {
    this.openReadOnly = readOnly;
    return this;
  }

  /**
   * Set the {@link DBOptions} and default {@link ColumnFamilyOptions} based
   * on {@code prof}.
   */
  public DBStoreBuilder setProfile(DBProfile prof) {
    setDBOptions(prof.getDBOptions());
    setDefaultCFOptions(prof.getColumnFamilyOptions());
    return this;
  }

  /**
   * Converts column families and their corresponding options that have been
   * registered with the builder to a set of {@link TableConfig} objects.
   * Column families with no options specified will have the default column
   * family options for this builder applied.
   */
  private Set<TableConfig> makeTableConfigs() {
    Set<TableConfig> tableConfigs = new HashSet<>();

    // If default column family was not added, add it with the default options.
    cfOptions.putIfAbsent(DEFAULT_COLUMN_FAMILY_NAME,
        defaultCfOptions);

    for (Map.Entry<String, ColumnFamilyOptions> entry:
        cfOptions.entrySet()) {
      String name = entry.getKey();
      ColumnFamilyOptions options = entry.getValue();

      if (options == null) {
        LOG.debug("using default column family options for table: {}", name);
        tableConfigs.add(new TableConfig(name, defaultCfOptions));
      } else {
        tableConfigs.add(new TableConfig(name, options));
      }
    }

    return tableConfigs;
  }

  /**
   * Attempts to get RocksDB {@link DBOptions} from an ini config file. If
   * that file does not exist, the value of {@code defaultDBOptions}  is used
   * instead.
   * After an {@link DBOptions} is chosen, it will have the logging level
   * specified by builder's {@link RocksDBConfiguration} applied to it. It
   * will also have statistics added if they are not turned off in the
   * builder's {@link ConfigurationSource}.
   *
   * @param tableConfigs Configurations for each column family, used when
   * reading DB options from the ini file.
   *
   * @return The {@link DBOptions} that should be used as the default value
   * for this builder if one is not specified by the caller.
   */
  private DBOptions getDefaultDBOptions(Collection<TableConfig> tableConfigs) {
    DBOptions dbOptions = getDBOptionsFromFile(tableConfigs);

    if (dbOptions == null) {
      dbOptions = defaultDBOptions;
      LOG.debug("Using RocksDB DBOptions from default profile.");
    }

    // Apply logging settings.
    if (rocksDBConfiguration.isRocksdbLoggingEnabled()) {
      org.rocksdb.Logger logger = new org.rocksdb.Logger(dbOptions) {
        @Override
        protected void log(InfoLogLevel infoLogLevel, String s) {
          ROCKS_DB_LOGGER.info(s);
        }
      };
      InfoLogLevel level = InfoLogLevel.valueOf(rocksDBConfiguration
          .getRocksdbLogLevel() + "_LEVEL");
      logger.setInfoLogLevel(level);
      dbOptions.setLogger(logger);
    }

    // Create statistics.
    if (!rocksDbStat.equals(OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF)) {
      Statistics statistics = new Statistics();
      statistics.setStatsLevel(StatsLevel.valueOf(rocksDbStat));
      dbOptions = dbOptions.setStatistics(statistics);
    }

    return dbOptions;
  }

  /**
   * Attempts to construct a {@link DBOptions} object from the configuration
   * directory with name equal to {@code database name}.ini, where {@code
   * database name} is the property set by
   * {@link DBStoreBuilder#setName(String)}.
   */
  private DBOptions getDBOptionsFromFile(Collection<TableConfig> tableConfigs) {
    DBOptions option = null;

    List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

    if (StringUtil.isNotBlank(dbname)) {
      for (TableConfig tc : tableConfigs) {
        columnFamilyDescriptors.add(tc.getDescriptor());
      }

      if (columnFamilyDescriptors.size() > 0) {
        try {
          option = DBConfigFromFile.readFromFile(dbname,
              columnFamilyDescriptors);
          if(option != null) {
            LOG.info("Using RocksDB DBOptions from {}.ini file", dbname);
          }
        } catch (IOException ex) {
          LOG.info("Unable to read RocksDB DBOptions from {}", dbname, ex);
        }
      }
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
