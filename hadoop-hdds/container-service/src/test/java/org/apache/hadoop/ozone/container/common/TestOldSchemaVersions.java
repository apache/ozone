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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.metadata.NoData;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_CHUNK;
import static org.junit.Assert.*;

/**
 * Tests reading of containers written with DB schema version 1, which will
 * have different column family layout than newer schema versions.
 * These tests only need to read data from these containers, since any new
 * containers will be created using the latest schema version.
 */
public class TestOldSchemaVersions {
  public static final String CONTAINER_FILE_NAME = "schema-v1.container";
  public static final String DB_NAME = "schema-v1-dn-container.db";

  private OzoneConfiguration conf;
  private KeyValueContainerData kvData;
  private KeyValueContainer container;

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();

    // Convert container file to ContainerData.
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader
            .getResource(CONTAINER_FILE_NAME).getFile());
    kvData = (KeyValueContainerData) ContainerDataYaml.readContainerFile(file);
    ContainerUtils.verifyChecksum(kvData);

    container = new KeyValueContainer(kvData, conf);
  }

  @Test
  public void testSchemaVersion1() {
    // test kvdata has schema version null
    assertNull(kvData.getSchemaVersion());
  }

  @Test
  public void testKeyRead() throws Exception {
    // Read values from guide.
    try(ReferenceCountedDB db = BlockUtils.getDB(container.getContainerData(),
            conf)) {
      // Test metadata values.
      Table<String, Long> metadataTable = db.getStore().getMetadataTable();

      // Test block data values.
      Table<String, BlockData> blockDataTable =
              db.getStore().getBlockDataTable();

      // Test deleted blocks values.
      Table<Long, NoData> deletedBlocksTable =
              db.getStore().getDeletedBlocksTable();


    }
  }
}
