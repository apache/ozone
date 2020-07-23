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
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.metadata.NoData;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.UUID;

import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_CHUNK;
import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.getChunkLayOutVersion;
import static org.junit.Assert.*;

/**
 * Tests reading of containers written with DB schema version 1, which will
 * have different column family layout than newer schema versions.
 * These tests only need to read data from these containers, since any new
 * containers will be created using the latest schema version.
 */
public class TestOldSchemaVersions {
  private static final long CONTAINER_ID = 123;
  private static final String CONTAINER_FILE_NAME = CONTAINER_ID + ".container";
  private static final String DB_NAME = CONTAINER_ID + "-dn-container.db";

  private OzoneConfiguration conf;
  private KeyValueContainerData kvData;
  private KeyValueContainer container;

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();

    ChunkLayOutVersion clVersion =
          ChunkLayOutVersion.getChunkLayOutVersion(1);
    // TODO : set this with a legit value (max container size).
    long size = 0;
    String pipelineID = UUID.randomUUID().toString();
    String nodeID = UUID.randomUUID().toString();

    kvData = new KeyValueContainerData(CONTAINER_ID, clVersion, size,
            pipelineID, nodeID);

    String containerFilePath =
            getClass().getClassLoader().getResource(CONTAINER_FILE_NAME)
            .getFile();
    kvData.setMetadataPath(new File(containerFilePath).getParent());

    // Convert container file to ContainerData.
//    ClassLoader classLoader = getClass().getClassLoader();
//    File file = new File(classLoader
//            .getResource(CONTAINER_FILE_NAME).getFile());
//    kvData = (KeyValueContainerData) ContainerDataYaml.readContainerFile(file);
//    ContainerUtils.verifyChecksum(kvData);
//
//    container = new KeyValueContainer(kvData, conf);
  }

  @Test
  public void testSchemaVersion() {
    // test kvdata has schema version null
    assertNull(kvData.getSchemaVersion());
  }

  @Test
  public void testRead() throws Exception {
    // kvData should have no values set, so this call will fill in its
    // information from the .db file.
    Yaml yaml = ContainerDataYaml.getYamlForContainerType(
            kvData.getContainerType());
    kvData.computeAndSetChecksum(yaml);
    KeyValueContainerUtil.parseKVContainerData(kvData, conf);

    try (ReferenceCountedDB db = BlockUtils.getDB(kvData,
            conf)) {

      // TODO : set constants instead of magic numbers.
      assertEquals("1", kvData.getSchemaVersion());
      assertEquals(4, kvData.getKeyCount());
      assertEquals(600, kvData.getBytesUsed());
      assertEquals(2, kvData.getNumPendingDeletionBlocks());

      // Test deleted blocks values.
      assertEquals(2, getDeletedBlocksCount(db));
    }
  }

  @Test
  public void testDelete() {

  }

  private int getDeletedBlocksCount(ReferenceCountedDB db) throws IOException {
    return db.getStore().getDeletedBlocksTable()
            .getRangeKVs(null, 100,
                    new MetadataKeyFilters.KeyPrefixFilter()).size();
  }
}
