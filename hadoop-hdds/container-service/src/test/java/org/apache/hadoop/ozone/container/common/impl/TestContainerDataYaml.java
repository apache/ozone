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

package org.apache.hadoop.ozone.container.common.impl;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.junit.Assert;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.UUID;

import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.FILE_PER_CHUNK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * This class tests create/read .container files.
 */
@RunWith(Parameterized.class)
public class TestContainerDataYaml {

  private long testContainerID = 1234;

  private static String testRoot = new FileSystemTestHelper().getTestRootDir();

  private static final long MAXSIZE = (long) StorageUnit.GB.toBytes(5);
  private static final Instant SCAN_TIME = Instant.now();

  private static final String VOLUME_OWNER = "hdfs";
  private static final String CONTAINER_DB_TYPE = "RocksDB";

  private final ContainerLayoutVersion layout;
  private OzoneConfiguration conf = new OzoneConfiguration();
    
  public TestContainerDataYaml(ContainerLayoutVersion layout) {
    this.layout = layout;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ContainerLayoutTestInfo.containerLayoutParameters();
  }

  /**
   * Creates a .container file. cleanup() should be called at the end of the
   * test when container file is created.
   */
  private File createContainerFile(long containerID, int replicaIndex)
      throws IOException {
    new File(testRoot).mkdirs();

    String containerPath = containerID + ".container";

    KeyValueContainerData keyValueContainerData = new KeyValueContainerData(
        containerID, layout, MAXSIZE,
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    keyValueContainerData.setContainerDBType(CONTAINER_DB_TYPE);
    keyValueContainerData.setMetadataPath(testRoot);
    keyValueContainerData.setChunksPath(testRoot);
    keyValueContainerData.updateDataScanTime(SCAN_TIME);
    keyValueContainerData.setSchemaVersion(
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion());
    keyValueContainerData.setReplicaIndex(replicaIndex);

    File containerFile = new File(testRoot, containerPath);

    // Create .container file with ContainerData
    ContainerDataYaml.createContainerFile(ContainerProtos.ContainerType
        .KeyValueContainer, keyValueContainerData, containerFile);

    //Check .container file exists or not.
    assertTrue(containerFile.exists());

    return containerFile;
  }

  private void cleanup() {
    FileUtil.fullyDelete(new File(testRoot));
  }

  @Test
  public void testCreateContainerFile() throws IOException {
    long containerID = testContainerID++;

    File containerFile = createContainerFile(containerID, 7);

    // Read from .container file, and verify data.
    KeyValueContainerData kvData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertEquals(containerID, kvData.getContainerID());
    assertEquals(ContainerProtos.ContainerType.KeyValueContainer, kvData
        .getContainerType());
    assertEquals(CONTAINER_DB_TYPE, kvData.getContainerDBType());
    assertEquals(containerFile.getParent(), kvData.getMetadataPath());
    assertEquals(containerFile.getParent(), kvData.getChunksPath());
    assertEquals(ContainerProtos.ContainerDataProto.State.OPEN, kvData
        .getState());
    assertEquals(layout, kvData.getLayoutVersion());
    assertEquals(0, kvData.getMetadata().size());
    assertEquals(MAXSIZE, kvData.getMaxSize());
    assertEquals(MAXSIZE, kvData.getMaxSize());
    assertTrue(kvData.lastDataScanTime().isPresent());
    assertEquals(SCAN_TIME.toEpochMilli(),
                 kvData.lastDataScanTime().get().toEpochMilli());
    assertEquals(SCAN_TIME.toEpochMilli(),
                 kvData.getDataScanTimestamp().longValue());
    assertEquals(VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(),
        kvData.getSchemaVersion());
    assertEquals(7, kvData.getReplicaIndex());

    // Update ContainerData.
    kvData.addMetadata(OzoneConsts.VOLUME, VOLUME_OWNER);
    kvData.addMetadata(OzoneConsts.OWNER, OzoneConsts.OZONE);
    kvData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);


    ContainerDataYaml.createContainerFile(ContainerProtos.ContainerType
            .KeyValueContainer, kvData, containerFile);

    // Reading newly updated data from .container file
    kvData =  (KeyValueContainerData) ContainerDataYaml.readContainerFile(
        containerFile);

    // verify data.
    assertEquals(containerID, kvData.getContainerID());
    assertEquals(ContainerProtos.ContainerType.KeyValueContainer, kvData
        .getContainerType());
    assertEquals(CONTAINER_DB_TYPE, kvData.getContainerDBType());
    assertEquals(containerFile.getParent(), kvData.getMetadataPath());
    assertEquals(containerFile.getParent(), kvData.getChunksPath());
    assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED, kvData
        .getState());
    assertEquals(layout, kvData.getLayoutVersion());
    assertEquals(2, kvData.getMetadata().size());
    assertEquals(VOLUME_OWNER, kvData.getMetadata().get(OzoneConsts.VOLUME));
    assertEquals(OzoneConsts.OZONE,
        kvData.getMetadata().get(OzoneConsts.OWNER));
    assertEquals(MAXSIZE, kvData.getMaxSize());
    assertTrue(kvData.lastDataScanTime().isPresent());
    assertEquals(SCAN_TIME.toEpochMilli(),
                 kvData.lastDataScanTime().get().toEpochMilli());
    assertEquals(SCAN_TIME.toEpochMilli(),
                 kvData.getDataScanTimestamp().longValue());
  }

  @Test
  public void testCreateContainerFileWithoutReplicaIndex() throws IOException {
    long containerID = testContainerID++;

    File containerFile = createContainerFile(containerID, 0);

    final String content =
        FileUtils.readFileToString(containerFile, Charset.defaultCharset());

    Assert.assertFalse("ReplicaIndex shouldn't be persisted if zero",
        content.contains("replicaIndex"));
    cleanup();
  }


  @Test
  public void testIncorrectContainerFile() throws IOException {
    try {
      String containerFile = "incorrect.container";
      //Get file from resources folder
      ClassLoader classLoader = getClass().getClassLoader();
      File file = new File(classLoader.getResource(containerFile).getFile());
      KeyValueContainerData kvData = (KeyValueContainerData) ContainerDataYaml
          .readContainerFile(file);
      fail("testIncorrectContainerFile failed");
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("No enum constant", ex);
    }
  }


  @Test
  public void testCheckBackWardCompatibilityOfContainerFile() throws
      IOException {
    // This test is for if we upgrade, and then .container files added by new
    // server will have new fields added to .container file, after a while we
    // decided to rollback. Then older ozone can read .container files
    // created or not.

    try {
      String containerFile = "additionalfields.container";
      //Get file from resources folder
      ClassLoader classLoader = getClass().getClassLoader();
      File file = new File(classLoader.getResource(containerFile).getFile());
      KeyValueContainerData kvData = (KeyValueContainerData) ContainerDataYaml
          .readContainerFile(file);
      ContainerUtils.verifyChecksum(kvData, conf);

      //Checking the Container file data is consistent or not
      assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED, kvData
          .getState());
      assertEquals(CONTAINER_DB_TYPE, kvData.getContainerDBType());
      assertEquals(ContainerProtos.ContainerType.KeyValueContainer, kvData
          .getContainerType());
      assertEquals(9223372036854775807L, kvData.getContainerID());
      assertEquals("/hdds/current/aed-fg4-hji-jkl/containerDir0/1", kvData
          .getChunksPath());
      assertEquals("/hdds/current/aed-fg4-hji-jkl/containerDir0/1", kvData
          .getMetadataPath());
      assertEquals(FILE_PER_CHUNK, kvData.getLayoutVersion());
      assertEquals(2, kvData.getMetadata().size());

    } catch (Exception ex) {
      ex.printStackTrace();
      fail("testCheckBackWardCompatibilityOfContainerFile failed");
    }
  }

  /**
   * Test to verify {@link ContainerUtils#verifyChecksum(ContainerData)}.
   */
  @Test
  public void testChecksumInContainerFile() throws IOException {
    long containerID = testContainerID++;

    File containerFile = createContainerFile(containerID, 0);

    // Read from .container file, and verify data.
    KeyValueContainerData kvData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    ContainerUtils.verifyChecksum(kvData, conf);

    cleanup();
  }

  /**
   * Test to verify {@link ContainerUtils#verifyChecksum(ContainerData)}.
   */
  @Test
  public void testChecksumInContainerFileWithReplicaIndex() throws IOException {
    long containerID = testContainerID++;

    File containerFile = createContainerFile(containerID, 10);

    // Read from .container file, and verify data.
    KeyValueContainerData kvData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    ContainerUtils.verifyChecksum(kvData, conf);

    cleanup();
  }

  private KeyValueContainerData getKeyValueContainerData() throws IOException {
    String containerFile = "incorrect.checksum.container";
    //Get file from resources folder
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(containerFile).getFile());
    return (KeyValueContainerData) ContainerDataYaml.readContainerFile(file);
  }

  /**
   * Test to verify incorrect checksum is detected.
   */
  @Test
  public void testIncorrectChecksum() {
    try {
      KeyValueContainerData kvData = getKeyValueContainerData();
      ContainerUtils.verifyChecksum(kvData, conf);
      fail("testIncorrectChecksum failed");
    } catch (Exception ex) {
      GenericTestUtils.assertExceptionContains("Container checksum error for " +
          "ContainerID:", ex);
    }
  }

  /**
   * Test to verify disabled checksum with incorrect checksum.
   */
  @Test
  public void testDisabledChecksum() throws IOException {
    KeyValueContainerData kvData = getKeyValueContainerData();
    conf.setBoolean(HddsConfigKeys.
                    HDDS_CONTAINER_CHECKSUM_VERIFICATION_ENABLED, false);
    ContainerUtils.verifyChecksum(kvData, conf);
  }
}
