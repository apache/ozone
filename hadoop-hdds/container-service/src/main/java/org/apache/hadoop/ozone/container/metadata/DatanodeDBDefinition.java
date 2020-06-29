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

import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import java.io.File;

/**
 * Class defines the structure and types of the dn-container.db.
 */
public class DatanodeDBDefinition implements DBDefinition {

  // TODO : Determine the correct types for these tables.
  public static final DBColumnFamilyDefinition<Long, Long>
          BLOCK_DATA =
          new DBColumnFamilyDefinition<>(
                  "block_data",
                  Long.class,
                  new LongCodec(),
                  Long.class,
                  new LongCodec());

  public static final DBColumnFamilyDefinition<Long, Long>
          METADATA =
          new DBColumnFamilyDefinition<>(
          "metadata",
          Long.class,
          new LongCodec(),
          Long.class,
          new LongCodec());

  private File dbDir;

  public DatanodeDBDefinition(String dbPath) {
    this.dbDir = new File(dbPath);
  }

  @Override
  public String getName() {
    return dbDir.getName();
  }

  @Override
  public String getLocationConfigKey() {
    return dbDir.getParentFile().getParentFile().getAbsolutePath();
  }

  @Override
  public DBColumnFamilyDefinition[] getColumnFamilies() {
    return new DBColumnFamilyDefinition[] {BLOCK_DATA, METADATA};
  }
}
