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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedConfigOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.eclipse.jetty.util.StringUtil;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.OptionsUtil;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.toIOException;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

/**
 * A Class that controls the standard config options of RocksDB.
 * <p>
 * Instructions:
 * <p>
 * You can use different ini files to customize the rocksDB configuration
 * of corresponding services (such as OM, SCM, DN). For example, you can create a file
 * such as om.db.ini to configure OM's rocksDB,
 * scm.db.ini to configure SCM's rocksDB, and container.db.ini to configure DN's rocksDB
 *
 */
public final class DBConfigFromFile {
  private static final Logger LOG =
      LoggerFactory.getLogger(DBConfigFromFile.class);

  public static final String CONFIG_DIR = "OZONE_CONF_DIR";

  private DBConfigFromFile() {
  }

  public static File getConfigLocation() throws IOException {
    String path = System.getenv(CONFIG_DIR);

    // Make testing easy.
    // If there is No Env. defined, let us try to read the JVM property
    if (StringUtil.isBlank(path)) {
      path = System.getProperty(CONFIG_DIR);
    }

    if (StringUtil.isBlank(path)) {
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
    Preconditions.checkNotNull(dbFileName);
    return dbFileName + ".ini";
  }

  /**
   * This function takes the name of DB file and looks up the a .ini file
   * that follows the ROCKSDB config format and uses that file for DBOptions
   * and Column family Options. The Format for this file is specified by RockDB.
   * We allow user tuning of all parameters freely.
   * <p>
   * Here is a sample config from RocksDB sample Repo.
   * <p>
   * https://github.com/facebook/rocksdb/blob/master/examples
   * /rocksdb_option_file_example.ini
   * <p>
   * This code assumes the .ini file is placed in the same directory as normal config files.
   * That is in $OZONE_DIR/etc/hadoop. For example, if we want to control OzoneManager.db
   * configs from a file,we need to create a file called om.db.ini and place that file
   * in $OZONE_DIR/etc/hadoop.
   *
   * @param dbFileName - The DB File Name, for example, OzoneManager.db.
   * @return Pair<ManagedDBOptions, Map<String, ManagedColumnFamilyOptions>>
   * where the first element is the DBOptions and the second element is the map of CFOptions
   * ,to be used for opening/creating the DB.
   * @throws IOException
   */
  private static Pair<ManagedDBOptions, Map<String, ManagedColumnFamilyOptions>> readOptionsFromFile(String dbFileName)
      throws IOException {
    List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
    Preconditions.checkNotNull(dbFileName);
    Preconditions.checkNotNull(cfDescs);

    ManagedDBOptions options = null;
    File configLocation = getConfigLocation();
    if (configLocation != null &&
        StringUtil.isNotBlank(configLocation.toString())) {
      Path optionsFile = Paths.get(configLocation.toString(),
          getOptionsFileNameFromDB(dbFileName));
      if (optionsFile.toFile().exists()) {
        options = new ManagedDBOptions();
        try {
          OptionsUtil.loadOptionsFromFile(new ManagedConfigOptions(), optionsFile.toString(),
              options, cfDescs);
        } catch (RocksDBException rdEx) {
          throw toIOException("Unable to find/open Options file.", rdEx);
        }
      }
    }
    return Pair.of(options, cfDescs.stream().collect(Collectors.toMap(
        cfDesc -> StringUtils.bytes2String(cfDesc.getName()),
        cfDesc -> new ManagedColumnFamilyOptions(cfDesc.getOptions())))
    );
  }

  public static ManagedDBOptions readDBOptionsFromFile(String dbFileName) throws IOException {
    return readOptionsFromFile(dbFileName).getLeft();
  }

  public static Map<String, ManagedColumnFamilyOptions> readCFOptionsFromFile(String dbFileName) throws IOException {
    return readOptionsFromFile(dbFileName).getRight();
  }

  public static ManagedColumnFamilyOptions readCFOptionsFromFile(String dbFileName, String cfName) throws IOException {
    Map<String, ManagedColumnFamilyOptions> columnFamilyOptionsMap = readCFOptionsFromFile(dbFileName);
    if (Objects.isNull(cfName)) {
      cfName = StringUtils.bytes2String(DEFAULT_COLUMN_FAMILY);
    }
    return columnFamilyOptionsMap.get(cfName);
  }



}
