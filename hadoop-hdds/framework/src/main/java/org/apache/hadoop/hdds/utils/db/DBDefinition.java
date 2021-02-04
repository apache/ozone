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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

/**
 * Simple interface to provide information to create a DBStore..
 */
public interface DBDefinition {

  Logger LOG = LoggerFactory.getLogger(DBDefinition.class);

  /**
   * Logical name of the DB.
   */
  String getName();

  /**
   * Configuration key defines the location of the DB.
   */
  String getLocationConfigKey();

  /**
   * @param conf The configuration for the DB.
   * @return The parent directory for this definition's .db file.
   */
  default File getDBLocation(ConfigurationSource conf) {
    return ServerUtils.getDirectoryFromConfig(conf,
            getLocationConfigKey(), getName());
  }

  /**
   * @return The column families present in the DB.
   */
  DBColumnFamilyDefinition[] getColumnFamilies();

  /**
   * Get the key type class for the given table.
   * @param table table name
   * @return the class of key type of the given table wrapped in an
   * {@link Optional}
   */
  default Optional<Class> getKeyType(String table) {
    return Arrays.stream(getColumnFamilies()).filter(cf -> cf.getName().equals(
        table)).map((Function<DBColumnFamilyDefinition, Class>)
        DBColumnFamilyDefinition::getKeyType).findAny();
  }

  /**
   * Get the value type class for the given table.
   * @param table table name
   * @return the class of value type of the given table wrapped in an
   * {@link Optional}
   */
  default Optional<Class> getValueType(String table) {
    return Arrays.stream(getColumnFamilies()).filter(cf -> cf.getName().equals(
        table)).map((Function<DBColumnFamilyDefinition, Class>)
        DBColumnFamilyDefinition::getValueType).findAny();
  }
}
