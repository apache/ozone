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
import java.nio.file.Paths;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;

import static org.apache.hadoop.hdds.server.ServerUtils.getDirectoryFromConfig;
import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple interface to provide a db store.
 */
public interface DBDefinition {

  Logger LOG = LoggerFactory.getLogger(DBDefinition.class);

  String getName();

  String getLocationConfigKey();

  /**
   * Create a new DB store instance based on the configuration.
   */
  DBColumnFamilyDefinition[] getColumnFamilies();

  default void registerTables(DBStoreBuilder builder) {
    for (DBColumnFamilyDefinition columnTableDefinition : getColumnFamilies()) {
      columnTableDefinition.registerTable(builder);
    }
  }

  default DBStoreBuilder createDBStoreBuilder(
      OzoneConfiguration configuration) {

    File metadataDir = getDirectoryFromConfig(configuration,
        ScmConfigKeys.OZONE_SCM_DB_DIRS, getName());

    if (metadataDir != null) {

      LOG.warn("{} is not configured. We recommend adding this setting. " +
              "Falling back to {} instead.",
          ScmConfigKeys.OZONE_SCM_DB_DIRS, HddsConfigKeys.OZONE_METADATA_DIRS);
      metadataDir = getOzoneMetaDirPath(configuration);
    }

    DBStoreBuilder builder = DBStoreBuilder.newBuilder(configuration)
        .setName(getName())
        .setPath(Paths.get(metadataDir.getPath()));
    return builder;
  }

  default DBStore createDBStore(OzoneConfiguration configuration)
      throws IOException {
    DBStoreBuilder builder = createDBStoreBuilder(configuration);
    registerTables(builder);
    return builder.build();
  }
}
