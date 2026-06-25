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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_CF_WRITE_BUFFER_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_CF_WRITE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_DELTA_UPDATE_DATA_SIZE_MAX_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_DELTA_UPDATE_DATA_SIZE_MAX_LIMIT_DEFAULT;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedLogger;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedStatistics;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.RocksDBException;
import org.rocksdb.StatsLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DBStore Builder.
 */
public final class DBStoreBuilder {
  private static final Logger LOG =
      LoggerFactory.getLogger(DBStoreBuilder.class);

  public static final Logger ROCKS_DB_LOGGER =
      LoggerFactory.getLogger(ManagedRocksDB.ORIGINAL_CLASS);

  public static final String DEFAULT_COLUMN_FAMILY_NAME =
      org.apache.hadoop.hdds.StringUtils.bytes2String(DEFAULT_COLUMN_FAMILY);

  // DB PKIProfile used by ROCKDB instances.
  public static final DBProfile HDDS_DEFAULT_DB_PROFILE = DBProfile.DISK;

  // The DBOptions specified by the caller.
  private ManagedDBOptions rocksDBOption;
  // The column family options that will be used for any column families
  // added by name only (without specifying options).
  private ManagedColumnFamilyOptions defaultCfOptions;
  // Initialize the Statistics instance if ROCKSDB_STATISTICS enabled
  private ManagedStatistics statistics;

  private String dbname;
  private Path dbPath;
  private Path optionsPath;
  private String dbJmxBeanNameName;
  // Maps added column family names to the column family options they were
  // added with. Value will be null if the column family was not added with
  // any options. On build, this will be replaced with defaultCfOptions.
  private Map<String, ManagedColumnFamilyOptions> cfOptions;
  private ConfigurationSource configuration;
  private String rocksDbStat;
  // RocksDB column family write buffer size
  private long rocksDbCfWriteBufferSize;
  private RocksDBConfiguration rocksDBConfiguration;
  // Flag to indicate if the RocksDB should be opened readonly.
  private boolean openReadOnly = false;
  private final DBProfile defaultCfProfile;
  private boolean enableCompactionDag;
  private boolean createCheckpointDirs = true;
  private boolean enableRocksDbMetrics = true;
  // this is to track the total size of dbUpdates data since sequence
  // number in request to avoid increase in heap memory.
  private long maxDbUpdatesSizeThreshold;
  private Integer maxNumberOfOpenFiles = null;
  private Function<Boolean, UncheckedAutoCloseable> differLockSupplier;

  /**
   * Create DBStoreBuilder from a generic DBDefinition.
   */
  public static DBStore createDBStore(ConfigurationSource configuration, DBDefinition definition)
      throws RocksDatabaseException {
    return newBuilder(configuration, definition, null, null).build();
  }

  public static DBStoreBuilder newBuilder(ConfigurationSource conf, DBDefinition definition, File dbDir) {
    return newBuilder(conf, definition, dbDir.getName(), dbDir.getParentFile().toPath());
  }

  public static DBStoreBuilder newBuilder(ConfigurationSource conf, DBDefinition definition,
      String name, Path metadataDir) {
    return newBuilder(conf).apply(definition, name, metadataDir);
  }

  public static DBStoreBuilder newBuilder(ConfigurationSource configuration) {
    return new DBStoreBuilder(configuration,
        configuration.getObject(RocksDBConfiguration.class));
  }

  private DBStoreBuilder(ConfigurationSource configuration,
      RocksDBConfiguration rocksDBConfiguration) {
    cfOptions = new HashMap<>();
    this.configuration = configuration;
    this.rocksDbStat = configuration.getTrimmed(
        OZONE_METADATA_STORE_ROCKSDB_STATISTICS,
        OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT);
    this.rocksDbCfWriteBufferSize = (long) configuration.getStorageSize(
        OZONE_METADATA_STORE_ROCKSDB_CF_WRITE_BUFFER_SIZE,
        OZONE_METADATA_STORE_ROCKSDB_CF_WRITE_BUFFER_SIZE_DEFAULT,
        StorageUnit.BYTES);
    this.rocksDBConfiguration = rocksDBConfiguration;

    // Get default DBOptions and ColumnFamilyOptions from the default DB
    // profile.
    defaultCfProfile = this.configuration.getEnum(HDDS_DB_PROFILE,
        HDDS_DEFAULT_DB_PROFILE);
    LOG.debug("Default DB profile:{}", defaultCfProfile);

    this.maxDbUpdatesSizeThreshold = (long) configuration.getStorageSize(
        OZONE_OM_DELTA_UPDATE_DATA_SIZE_MAX_LIMIT,
        OZONE_OM_DELTA_UPDATE_DATA_SIZE_MAX_LIMIT_DEFAULT, StorageUnit.BYTES);
    this.differLockSupplier = null;
  }

  public static File getDBDirPath(DBDefinition definition,
                                  ConfigurationSource configuration) {
    // Set metadata dirs.
    File metadataDir = definition.getDBLocation(configuration);

    if (metadataDir == null) {
      LOG.warn("{} is not configured. We recommend adding this setting. " +
              "Falling back to {} instead.",
          definition.getLocationConfigKey(),
          HddsConfigKeys.OZONE_METADATA_DIRS);
      metadataDir = getOzoneMetaDirPath(configuration);
    }
    return metadataDir;
  }

  private DBStoreBuilder apply(DBDefinition definition, String name, Path metadataDir) {
    if (name == null) {
      name = definition.getName();
    }
    setName(name);

    // Set metadata dirs.
    if (metadataDir == null) {
      metadataDir = getDBDirPath(definition, configuration).toPath();
    }
    setPath(metadataDir);
    setOptionsPath(definition.getOptionsPath(configuration));

    // Add column family names and codecs.
    for (DBColumnFamilyDefinition<?, ?> columnFamily : definition.getColumnFamilies()) {
      ManagedColumnFamilyOptions cfOptionsFromFile = getCfOptionsFromFile(columnFamily.getName());
      if (cfOptionsFromFile == null) {
        cfOptionsFromFile = columnFamily.getCfOptions();
      }
      addTable(columnFamily.getName(), cfOptionsFromFile);
    }
    return this;
  }

  private void setDBOptionsProps(ManagedDBOptions dbOptions) {
    if (maxNumberOfOpenFiles != null) {
      dbOptions.setMaxOpenFiles(maxNumberOfOpenFiles);
    }
    if (!rocksDbStat.equals(OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF)) {
      statistics = new ManagedStatistics();
      statistics.setStatsLevel(StatsLevel.valueOf(rocksDbStat));
      dbOptions.setStatistics(statistics);
    }
  }

  /**
   * Builds a DBStore instance and returns that.
   *
   * @return DBStore
   */
  public RDBStore build() throws RocksDatabaseException {
    if (StringUtils.isBlank(dbname) || (dbPath == null)) {
      LOG.error("Required Parameter missing.");
      throw new RocksDatabaseException("Required parameter is missing. Please make sure "
          + "Path and DB name is provided.");
    }

    Set<TableConfig> tableConfigs = makeTableConfigs();
    ManagedWriteOptions writeOptions = null;
    try {
      if (rocksDBOption == null) {
        rocksDBOption = getDefaultDBOptions();
      }
      setDBOptionsProps(rocksDBOption);
      File dbFile = getDBFile();
      if (!dbFile.getParentFile().exists()) {
        throw new RocksDatabaseException("The DB destination directory should exist.");
      }
      writeOptions = new ManagedWriteOptions();
      writeOptions.setSync(rocksDBConfiguration.getSyncOption());

      return new RDBStore(dbFile, rocksDBOption, statistics, writeOptions, tableConfigs,
          openReadOnly, dbJmxBeanNameName, enableCompactionDag, differLockSupplier,
          maxDbUpdatesSizeThreshold, createCheckpointDirs, configuration,
          enableRocksDbMetrics);
    } catch (Exception ex) {
      try {
        if (writeOptions != null) {
          writeOptions.close();
        }
      } catch (Exception suppressed) {
        ex.addSuppressed(suppressed);
      }
      throw ex;
    } finally {
      tableConfigs.forEach(TableConfig::close);
    }
  }

  public DBStoreBuilder setName(String name) {
    dbname = name;
    return this;
  }

  public DBStoreBuilder setDBJmxBeanNameName(String name) {
    dbJmxBeanNameName = name;
    return this;
  }

  public DBStoreBuilder addTable(String tableName) {
    return addTable(tableName, null);
  }

  public DBStoreBuilder addTable(String tableName,
      ManagedColumnFamilyOptions options) {
    cfOptions.put(tableName, options);
    return this;
  }

  public DBStoreBuilder setDBOptions(ManagedDBOptions option) {
    rocksDBOption = option;
    return this;
  }

  public DBStoreBuilder setDefaultCFOptions(
      ManagedColumnFamilyOptions options) {
    defaultCfOptions = options;
    return this;
  }

  public DBStoreBuilder setPath(Path path) {
    Objects.requireNonNull(path, "path == null");
    dbPath = path;
    return this;
  }

  public DBStoreBuilder setOptionsPath(Path optionsPath) {
    Objects.requireNonNull(optionsPath, "optionsPath == null");
    this.optionsPath = optionsPath;
    return this;
  }

  public DBStoreBuilder setOpenReadOnly(boolean readOnly) {
    this.openReadOnly = readOnly;
    return this;
  }

  public DBStoreBuilder setEnableCompactionDag(boolean compactionDagEnable,
      Function<Boolean, UncheckedAutoCloseable> lockSupplier) {
    this.enableCompactionDag = compactionDagEnable;
    this.differLockSupplier = lockSupplier;
    return this;
  }

  public DBStoreBuilder setCreateCheckpointDirs(boolean createCheckpointDirs) {
    this.createCheckpointDirs = createCheckpointDirs;
    return this;
  }

  public DBStoreBuilder setEnableRocksDbMetrics(boolean enableRocksDbMetrics) {
    this.enableRocksDbMetrics = enableRocksDbMetrics;
    return this;
  }

  /**
   * Set the {@link ManagedDBOptions} and default
   * {@link ManagedColumnFamilyOptions} based on {@code prof}.
   */
  public DBStoreBuilder setProfile(DBProfile prof) {
    setDBOptions(prof.getDBOptions());
    setDefaultCFOptions(prof.getColumnFamilyOptions());
    return this;
  }

  public DBStoreBuilder setMaxNumberOfOpenFiles(Integer maxNumberOfOpenFiles) {
    this.maxNumberOfOpenFiles = maxNumberOfOpenFiles;
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
    ManagedColumnFamilyOptions usedCFOptions = getFromFileOrDefault(DEFAULT_COLUMN_FAMILY_NAME);
    usedCFOptions.setWriteBufferSize(rocksDbCfWriteBufferSize);
    cfOptions.putIfAbsent(DEFAULT_COLUMN_FAMILY_NAME, usedCFOptions);

    for (Map.Entry<String, ManagedColumnFamilyOptions> entry :
        cfOptions.entrySet()) {
      String name = entry.getKey();
      ManagedColumnFamilyOptions options = entry.getValue();

      if (options == null) {
        LOG.debug("using default column family options for table: {}", name);
        ManagedColumnFamilyOptions fromFileOrDefault = getFromFileOrDefault(name);
        fromFileOrDefault.setWriteBufferSize(rocksDbCfWriteBufferSize);
        tableConfigs.add(new TableConfig(name, fromFileOrDefault));
      } else {
        tableConfigs.add(new TableConfig(name, options));
      }
    }

    return tableConfigs;
  }

  private ManagedColumnFamilyOptions getDefaultCfOptions() {
    return Optional.ofNullable(defaultCfOptions)
        .orElseGet(defaultCfProfile::getColumnFamilyOptions);
  }

  private ManagedColumnFamilyOptions getFromFileOrDefault(String cfName) {
    ManagedColumnFamilyOptions cfOptionsFromFile = getCfOptionsFromFile(cfName);
    return cfOptionsFromFile != null
        ? cfOptionsFromFile : getDefaultCfOptions();
  }

  /**
   * Pass true to disable auto compaction for Column Family by default.
   * Sets Disable auto compaction flag for Default Column Family option
   *
   * @param defaultCFAutoCompaction
   */
  public DBStoreBuilder disableDefaultCFAutoCompaction(boolean defaultCFAutoCompaction) {
    ManagedColumnFamilyOptions defaultCFOptions = getFromFileOrDefault(DEFAULT_COLUMN_FAMILY_NAME);
    defaultCFOptions.setDisableAutoCompactions(defaultCFAutoCompaction);
    setDefaultCFOptions(defaultCFOptions);
    return this;
  }

  /**
   * Attempts to get RocksDB {@link ManagedDBOptions} from an ini config
   * file. If that file does not exist, the value of {@code defaultDBOptions}
   * is used instead.
   * After an {@link ManagedDBOptions} is chosen, it will have the logging level
   * specified by builder's {@link RocksDBConfiguration} applied to it. It
   * will also have statistics added if they are not turned off in the
   * builder's {@link ConfigurationSource}.
   *
   * @return The {@link ManagedDBOptions} that should be used as the default
   * value for this builder if one is not specified by the caller.
   */
  private ManagedDBOptions getDefaultDBOptions() {
    ManagedDBOptions dbOptions = null;
    try {
      Path configuredPath = optionsPath != null ? optionsPath : dbPath;
      dbOptions = DBConfigFromFile.readDBOptionsFromFile(configuredPath);
    } catch (RocksDBException e) {
      LOG.error("Error trying to use dbOptions from file: {}", optionsPath);
    }
    if (dbOptions == null) {
      dbOptions = defaultCfProfile.getDBOptions();
    }

    // Apply logging settings.
    if (rocksDBConfiguration.isRocksdbLoggingEnabled()) {
      ManagedLogger logger = new ManagedLogger(dbOptions, (infoLogLevel, s) -> ROCKS_DB_LOGGER.info(s));
      InfoLogLevel level = InfoLogLevel.valueOf(rocksDBConfiguration
          .getRocksdbLogLevel() + "_LEVEL");
      logger.setInfoLogLevel(level);
      dbOptions.setLogger(logger);
    }

    // RocksDB log settings.
    dbOptions.setMaxLogFileSize(rocksDBConfiguration.getMaxLogFileSize());
    dbOptions.setKeepLogFileNum(rocksDBConfiguration.getKeepLogFileNum());

    // Apply WAL settings.
    dbOptions.setWalTtlSeconds(rocksDBConfiguration.getWalTTL());
    dbOptions.setWalSizeLimitMB(rocksDBConfiguration.getWalSizeLimit());

    return dbOptions;
  }

  /**
   * Attempts to get RocksDB {@link ManagedColumnFamilyOptions} from an ini config
   * file. If that file does not exist, the value of {@code getColumnFamilyOptions}
   * is used instead.
   *
   * @return The {@link ManagedColumnFamilyOptions} that should be used as the default
   * value for this builder if one is not specified by the caller.
   */
  public ManagedColumnFamilyOptions getCfOptionsFromFile(String cfName) {
    if (Objects.isNull(defaultCfProfile)) {
      throw new RuntimeException();
    }
    Path configuredPath = optionsPath != null ? optionsPath : dbPath;
    ManagedColumnFamilyOptions cfOptionsFromFile = null;
    try {
      cfOptionsFromFile = DBConfigFromFile.readCFOptionsFromFile(configuredPath, cfName);
    } catch (RocksDBException e) {
      LOG.error("Error while trying to read ColumnFamilyOptions from file: {}", configuredPath);
    }
    return cfOptionsFromFile;
  }

  private File getDBFile() throws RocksDatabaseException {
    if (dbPath == null) {
      LOG.error("DB path is required.");
      throw new RocksDatabaseException("A Path to for DB file is needed.");
    }

    if (StringUtils.isBlank(dbname)) {
      LOG.error("DBName is a required.");
      throw new RocksDatabaseException("A valid DB name is required.");
    }
    return Paths.get(dbPath.toString(), dbname).toFile();
  }
}
