/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.metadata;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;

import java.io.File;

/**
 * This class serves as an intermediate format for all possible database
 * layouts for datanodes.
 */
public abstract class AbstractDatanodeDBDefinition implements DBDefinition {

  private File dbDir;

  /**
   * @param dbPath The absolute path to the .db file corresponding to this
   * {@link DBDefinition}.
   */
  protected AbstractDatanodeDBDefinition(String dbPath) {
    this.dbDir = new File(dbPath);
  }

  @Override
  public String getName() {
    return dbDir.getName();
  }

  @Override
  public File getDBLocation(ConfigurationSource conf) {
    return dbDir.getParentFile();
  }

  @Override
  public String getLocationConfigKey() {
    throw new UnsupportedOperationException(
            "No location config key available for datanode databases.");
  }

  @Override
  public DBColumnFamilyDefinition[] getColumnFamilies() {
    return new DBColumnFamilyDefinition[] {getBlockDataColumnFamily(),
        getMetadataColumnFamily(), getDeletedBlocksColumnFamily()};
  }

  public abstract DBColumnFamilyDefinition<String, BlockData>
      getBlockDataColumnFamily();

  public abstract DBColumnFamilyDefinition<String, Long>
      getMetadataColumnFamily();

  public abstract DBColumnFamilyDefinition<String, ChunkInfoList>
      getDeletedBlocksColumnFamily();
}
