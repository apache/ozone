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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

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

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

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

    // Copy data to the temporary folder so it can be safely modified.
    File tempMetadataDir = constructTempResources();
    kvData.setMetadataPath(tempMetadataDir.getAbsolutePath());
    File dbFile =
            tempMetadataDir.listFiles((dir, name) -> name.equals(DB_NAME))[0];
    kvData.setDbFile(dbFile);

    Yaml yaml = ContainerDataYaml.getYamlForContainerType(
            kvData.getContainerType());
    kvData.computeAndSetChecksum(yaml);
  }

  /**
   * Copies test resources from the classpath to a temporary folder where
   * they can be modified.
   * @return The new temporary metadata folder.
   */
  private File constructTempResources() throws IOException {
    File tempMetadataDir =
            tempFolder.newFolder(OzoneConsts.CONTAINER_META_PATH);

    ClassLoader loader = getClass().getClassLoader();
    File containerFile = new File(loader.getResource(CONTAINER_FILE_NAME)
            .getFile());
    File dbDir = new File(loader.getResource(DB_NAME).getFile());

    FileUtils.copyDirectoryToDirectory(dbDir, tempMetadataDir);
    FileUtils.copyFileToDirectory(containerFile, tempMetadataDir);

    return tempMetadataDir;
  }

  @Test
  public void testSchemaVersion() {
    // test kvdata has schema version null
    assertNull(kvData.getSchemaVersion());
  }


  /**
   * Tests reading of a container that was written in schema version 1, when
   * the container has metadata keys present.
   * The {@link KeyValueContainerUtil} will read these values to fill in a
   * {@link KeyValueContainerData} object.
   * @throws Exception
   */
  @Test
  public void testReadWithMetadata() throws Exception {
    checkContainerData();
  }

  /**
   * Tests reading of a container that was written in schema version 1, when
   * the container has no metadata keys present.
   * The {@link KeyValueContainerUtil} will scan the blocks in the database
   * to fill these metadata values in the database and a
   * {@link KeyValueContainerData} object.
   * @throws Exception
   */
  @Test
  public void testReadWithoutMetadata() throws Exception {
//    try (ReferenceCountedDB db = BlockUtils.getDB(kvData, conf)) {
  }

  /**
   * Tests reading blocks marked for deletion from a container written in
   * schema version 1.
   */
  @Test
  public void testDelete() {

  }

  private void checkContainerData() throws IOException {
    KeyValueContainerUtil.parseKVContainerData(kvData, conf);

    // TODO : set constants instead of magic numbers.
    assertEquals(OzoneConsts.SCHEMA_V1, kvData.getSchemaVersion());
    assertEquals(2, kvData.getKeyCount());
    assertEquals(600, kvData.getBytesUsed());
    assertEquals(2, kvData.getNumPendingDeletionBlocks());

    try (ReferenceCountedDB db = BlockUtils.getDB(kvData, conf)) {
      // Test deleted blocks values. Returns 9 currently.
      assertEquals(2, getDeletedBlocksCount(db));
    }
  }

  private int getDeletedBlocksCount(ReferenceCountedDB db) throws IOException {
    return db.getStore().getDeletedBlocksTable()
            .getRangeKVs(null, 100,
                    new MetadataKeyFilters.KeyPrefixFilter()).size();
  }
}
