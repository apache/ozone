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
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;

/**
 * This class defines the new RocksDB structure for datanodes, where the block data and metadata
 * are put in their own separate column families. In the old format, everything was placed in teh
 * default column family.
 */
public class DatanodeTwoTableDBDefinition extends AbstractDatanodeDBDefinition {

  public static final DBColumnFamilyDefinition<String, BlockData>
          BLOCK_DATA =
          new DBColumnFamilyDefinition<>(
                  "block_data",
                  String.class,
                  new StringCodec(),
                  BlockData.class,
                  new BlockDataCodec());

  public static final DBColumnFamilyDefinition<String, Long>
          METADATA =
          new DBColumnFamilyDefinition<>(
          "metadata",
          String.class,
          new StringCodec(),
          Long.class,
          new LongCodec());

  protected DatanodeTwoTableDBDefinition(String dbPath) {
    super(dbPath);
  }

  @Override
  public DBColumnFamilyDefinition<String, BlockData> getBlockDataColumnFamily() {
    return BLOCK_DATA;
  }

  @Override
  public DBColumnFamilyDefinition<String, Long> getMetadataColumnFamily() {
    return METADATA;
  }
}
