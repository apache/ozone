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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.Proto2EnumCodec;
import org.apache.hadoop.ozone.OzoneConsts;

import java.util.Map;

/**
 * Class for defining the schema for master volume in a datanode.
 */
public final class MasterVolumeDBDefinition extends DBDefinition.WithMap {

  private static final String CONTAINER_IDS_TABLE_NAME = "containerIds";

  public static final DBColumnFamilyDefinition<Long, State>
      CONTAINER_IDS_TABLE = new DBColumnFamilyDefinition<>(
      CONTAINER_IDS_TABLE_NAME,
      LongCodec.get(),
      Proto2EnumCodec.get(State.OPEN));

  private static final Map<String, DBColumnFamilyDefinition<?, ?>>
      COLUMN_FAMILIES = DBColumnFamilyDefinition.newUnmodifiableMap(
      CONTAINER_IDS_TABLE);

  private static final MasterVolumeDBDefinition INSTANCE = new MasterVolumeDBDefinition();

  public static MasterVolumeDBDefinition get() {
    return INSTANCE;
  }

  private MasterVolumeDBDefinition() {
    super(COLUMN_FAMILIES);
  }

  @Override
  public String getName() {
    return OzoneConsts.CONTAINER_META_DB_NAME;
  }

  @Override
  public String getLocationConfigKey() {
    return ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR;
  }

  public DBColumnFamilyDefinition<Long, State> getContainerIdsTable() {
    return CONTAINER_IDS_TABLE;
  }
}