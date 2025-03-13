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

package org.apache.hadoop.ozone.containerlog.parser;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;

/**
 * Manages container-datanode store.
 */

public class ContainerDatanodeStore {
  private static final String CONTAINER_TABLE_NAME = "ContainerLogTable";
  private static final DBColumnFamilyDefinition<Long, List<ContainerInfo>> CONTAINER_LOG_TABLE_COLUMN_FAMILY
      = new DBColumnFamilyDefinition<>(
          CONTAINER_TABLE_NAME,
          LongCodec.get(),
          new GenericInfoCodec<ContainerInfo>()
  );
  private static final String DATANODE_TABLE_NAME = "DatanodeContainerLogTable";
  private static final DBColumnFamilyDefinition<String, List<DatanodeContainerInfo>>
      DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY
      = new DBColumnFamilyDefinition<>(
          DATANODE_TABLE_NAME,
          StringCodec.get(),
          new GenericInfoCodec<DatanodeContainerInfo>()
  );
  private DBStore containerDbStore = null;
  private ContainerLogTable<Long, List<ContainerInfo>> containerLogTable = null;
  private DatanodeContainerLogTable<String, List<DatanodeContainerInfo>> datanodeContainerLogTable = null;
  private DBStore dbStore = null;

  private DBStore openDb(File dbPath) {

    File dbFile = new File(dbPath, "ContainerDatanodeLogStore.db");

    try {

      ConfigurationSource conf = new OzoneConfiguration();
      DBStoreBuilder dbStoreBuilder = DBStoreBuilder.newBuilder(conf);
      dbStoreBuilder.setName("ContainerDatanodeLogStore.db");
      dbStoreBuilder.setPath(dbFile.toPath());

      dbStoreBuilder.addTable(CONTAINER_LOG_TABLE_COLUMN_FAMILY.getName());
      dbStoreBuilder.addTable(DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY.getName());
      dbStoreBuilder.addCodec(CONTAINER_LOG_TABLE_COLUMN_FAMILY.getKeyType(),
          CONTAINER_LOG_TABLE_COLUMN_FAMILY.getKeyCodec());
      dbStoreBuilder.addCodec(CONTAINER_LOG_TABLE_COLUMN_FAMILY.getValueType(),
          CONTAINER_LOG_TABLE_COLUMN_FAMILY.getValueCodec());
      dbStoreBuilder.addCodec(DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY.getKeyType(),
          DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY.getKeyCodec());
      dbStoreBuilder.addCodec(DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY.getValueType(),
          DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY.getValueCodec());

      dbStore = dbStoreBuilder.build();

      containerLogTable = new ContainerLogTable<>(dbStore.getTable(CONTAINER_LOG_TABLE_COLUMN_FAMILY.getName(),
          Long.class, new GenericInfoCodec<ContainerInfo>().getTypeClass()));
      datanodeContainerLogTable = new DatanodeContainerLogTable<>(
          dbStore.getTable(DATANODE_CONTAINER_LOG_TABLE_COLUMN_FAMILY.getName(),
          String.class, new GenericInfoCodec<DatanodeContainerInfo>().getTypeClass()));

      return dbStore;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public void initialize(File dbFile) {

    containerDbStore = openDb(dbFile);
  }

  public void close() throws IOException {
    if (containerDbStore != null) {
      containerDbStore.close();
    }
  }
}
