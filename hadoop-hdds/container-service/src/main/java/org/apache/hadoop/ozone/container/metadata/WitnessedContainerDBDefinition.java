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

package org.apache.hadoop.ozone.container.metadata;

import java.util.Map;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class for defining the schema for master volume in a datanode.
 */
public final class WitnessedContainerDBDefinition extends DBDefinition.WithMap {

  private static final String CONTAINER_CREATE_INFO_TABLE_NAME = "ContainerCreateInfoTable";

  public static final DBColumnFamilyDefinition<ContainerID, ContainerCreateInfo>
      CONTAINER_CREATE_INFO_TABLE_DEF = new DBColumnFamilyDefinition<>(
      CONTAINER_CREATE_INFO_TABLE_NAME,
      ContainerID.getCodec(),
      ContainerCreateInfo.getCodec());

  private static final Map<String, DBColumnFamilyDefinition<?, ?>>
      COLUMN_FAMILIES = DBColumnFamilyDefinition.newUnmodifiableMap(
      CONTAINER_CREATE_INFO_TABLE_DEF);

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

  DBColumnFamilyDefinition<ContainerID, ContainerCreateInfo> getContainerCreateInfoTableDef() {
    return CONTAINER_CREATE_INFO_TABLE_DEF;
  }
}
