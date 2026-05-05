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

import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedConfigOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.OptionsUtil;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Class that controls the standard config options of RocksDB.
 * <p>
 * Important : Some of the functions in this file are magic functions designed
 * for the use of OZONE developers only. Due to that this information is
 * documented in this files only and is *not* intended for end user consumption.
 * Please do not use this information to tune your production environments.
 * Please remember the SpiderMan principal; with great power comes great
 * responsibility.
 */
public final class DBConfigFromFile {
  private static final Logger LOG =
      LoggerFactory.getLogger(DBConfigFromFile.class);

  public static final String CONFIG_DIR = "OZONE_CONF_DIR";

  private DBConfigFromFile() {
  }

  public static File getConfigLocation() {
    String path = System.getenv(CONFIG_DIR);

    // Make testing easy.
    // If there is No Env. defined, let us try to read the JVM property
    if (StringUtils.isBlank(path)) {
      path = System.getProperty(CONFIG_DIR);
    }

    if (StringUtils.isBlank(path)) {
      LOG.debug("Unable to find the configuration directory. "
          + "Please make sure that " + CONFIG_DIR + " is setup correctly.");
      return null;
    }

    return new File(path);
  }

  /**
   * This class establishes a magic pattern where we look for DBFile.ini as the
   * options for RocksDB.
   *
   * @param dbFileName - The DBFile Name. For example, OzoneManager.db
   * @return Name of the DB File options
   */
  public static String getOptionsFileNameFromDB(String dbFileName) {
    Objects.requireNonNull(dbFileName, "dbFileName == null");
    return dbFileName.isEmpty() ? "" : dbFileName + ".ini";
  }

  /**
   * One of the Magic functions designed for the use of Ozone Developers *ONLY*.
   * This function takes the name of DB file and looks up the a .ini file that
   * follows the ROCKSDB config format and uses that file for DBOptions and
   * Column family Options. The Format for this file is specified by RockDB.
   * <p>
   * Here is a sample config from RocksDB sample Repo.
   * <p>
   * https://github.com/facebook/rocksdb/blob/master/examples
   * /rocksdb_option_file_example.ini
   * <p>
   * We look for a specific pattern, say OzoneManager.db will have its configs
   * specified in OzoneManager.db.ini. This option is used only by the
   * performance testing group to allow tuning of all parameters freely.
   * <p>
   * For the end users we offer a set of Predefined options that is easy to use
   * and the user does not need to become an expert in RockDB config.
   * <p>
   * This code assumes the .ini file is placed in the same directory as normal
   * config files. That is in $OZONE_DIR/etc/hadoop. For example, if we want to
   * control OzoneManager.db configs from a file, we need to create a file
   * called OzoneManager.db.ini and place that file in $OZONE_DIR/etc/hadoop.
   *
   * @param dbPath - The DB File Name, for example, OzoneManager.db.
   * @return DBOptions, Options to be used for opening/creating the DB.
   */
  public static ManagedDBOptions readDBOptionsFromFile(Path dbPath) throws RocksDBException {
    Path generatedDBPath = generateDBPath(dbPath);
    if (generatedDBPath.toString().isEmpty()) {
      return null;
    }
    if (!generatedDBPath.toFile().exists()) {
      LOG.warn("Error trying to read generated rocksDB file: {}, file does not exists.", generatedDBPath);
      return null;
    }
    List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
    try {
      //TODO: Add Documentation on how to support RocksDB Mem Env.
      ManagedDBOptions options = new ManagedDBOptions();
      try (ManagedConfigOptions configOptions = new ManagedConfigOptions()) {
        OptionsUtil.loadOptionsFromFile(configOptions, generatedDBPath.toString(), options, descriptors);
      } catch (RocksDBException rdEx) {
        options.close();
        throw new RocksDBException("There was an error opening rocksDB Options file: " + rdEx.getMessage());
      }
      return options;
    } finally {
      //readDBOptions will be freed once the store using it is closed, but the descriptors need to be closed.
      closeDescriptors(descriptors);
    }
  }

  public static ManagedColumnFamilyOptions readCFOptionsFromFile(Path optionsPath, String cfName)
      throws RocksDBException {
    Path generatedDBPath = generateDBPath(optionsPath);
    if (generatedDBPath.toString().isEmpty()) {
      return null;
    }
    if (!generatedDBPath.toFile().exists()) {
      LOG.warn("Error trying to read column family options from file: {}, file does not exists.", generatedDBPath);
      return null;
    }
    List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
    String defaultColumnFamilyString = StringUtils.toEncodedString(DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8);
    String validatedCfName = StringUtils.isEmpty(cfName) ? defaultColumnFamilyString : cfName;
    ManagedColumnFamilyOptions resultCfOptions = null;
    try (ManagedConfigOptions ignored = new ManagedConfigOptions(); ManagedDBOptions options = new ManagedDBOptions()) {
      OptionsUtil.loadOptionsFromFile(ignored, generatedDBPath.toString(), options, descriptors);
      ColumnFamilyDescriptor descriptor = descriptors.stream()
          .filter(desc -> StringUtils.toEncodedString(desc.getName(), StandardCharsets.UTF_8).equals(validatedCfName))
          .findAny().orElse(null);
      if (descriptor != null) {
        resultCfOptions = new ManagedColumnFamilyOptions(descriptor.getOptions());
      }
    } finally {
      closeDescriptors(descriptors);
    }
    return resultCfOptions;
  }

  private static void closeDescriptors(List<ColumnFamilyDescriptor> descriptors) {
    //note that close() is an idempotent operation here so calling it multiple times won't cause issues.
    descriptors.forEach(descriptor -> descriptor.getOptions().close());
  }

  /**
   * Tries looking up possible options for the DB. If the specified dbPath exists it uses it.
   * If not then it tries reading it from the default config location and also tries appending +.ini to the file.
   *
   * @param path
   * @return
   * @throws RocksDBException
   */
  private static Path generateDBPath(Path path) {
    String dbPath = path == null ? "" : path.toString();
    if (dbPath.isEmpty()) {
      return Paths.get("");
    }
    if (path.toFile().exists()) {
      LOG.debug("RocksDB path found: {}, opening db from it.", path);
      return path;
    } else {
      LOG.debug("RocksDB path: {} not found, attempting to use fallback", path);
      File configLocation = getConfigLocation();
      if (configLocation != null &&
          StringUtils.isNotBlank(configLocation.toString())) {
        Path fallbackPath = Paths.get(configLocation.toString(),
            getOptionsFileNameFromDB(path.toString()));
        LOG.debug("Fallback path found: {}", path);
        return fallbackPath;
      }
    }
    LOG.info("No RocksDB path found");
    return Paths.get("");
  }
}
