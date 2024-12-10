package org.apache.hadoop.ozone.container.metadata;

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

import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.OzoneConsts;

import java.util.Map;

/**
 * Class for defining the schema for master volume in a datanode.
 */
public final class WitnessedContainerDBDefinition extends DBDefinition.WithMap {

  private static final String CONTAINER_IDS_TABLE_NAME = "containerIds";

  public static final DBColumnFamilyDefinition<Long, String>
      CONTAINER_IDS_TABLE = new DBColumnFamilyDefinition<>(
      CONTAINER_IDS_TABLE_NAME,
      LongCodec.get(),
      StringCodec.get());

  private static final Map<String, DBColumnFamilyDefinition<?, ?>>
      COLUMN_FAMILIES = DBColumnFamilyDefinition.newUnmodifiableMap(
      CONTAINER_IDS_TABLE);

  private static final WitnessedContainerDBDefinition INSTANCE = new WitnessedContainerDBDefinition();

  public static WitnessedContainerDBDefinition get() {
    return INSTANCE;
  }

  private WitnessedContainerDBDefinition() {
    super(COLUMN_FAMILIES);
  }

  @Override
  public String getName() {
    return OzoneConsts.WITNESSED_CONTAINER_DB_NAME;
  }

  @Override
  public String getLocationConfigKey() {
    return ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR;
  }

  public DBColumnFamilyDefinition<Long, String> getContainerIdsTable() {
    return CONTAINER_IDS_TABLE;
  }
}
