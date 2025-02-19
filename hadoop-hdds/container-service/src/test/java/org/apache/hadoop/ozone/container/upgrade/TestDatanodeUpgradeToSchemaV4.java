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

package org.apache.hadoop.ozone.container.upgrade;

import static org.apache.hadoop.ozone.OzoneConsts.CHUNKS_PATH;
import static org.apache.hadoop.ozone.OzoneConsts.METADATA_PATH;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V4;
import static org.apache.hadoop.ozone.OzoneConsts.STORAGE_DIR_CHUNKS;
import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.ScmTestMock;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests upgrading a single datanode from container Schema V4.
 */
public class TestDatanodeUpgradeToSchemaV4 {
  @TempDir
  private File tempFolder;

  private DatanodeStateMachine dsm;
  private OzoneConfiguration conf;
  private static final String CLUSTER_ID = "clusterID";

  private RPC.Server scmRpcServer;
  private InetSocketAddress address;

  private void initTests(Boolean enable) throws Exception {
    boolean schemaV3Enabled = enable;
    conf = new OzoneConfiguration();
    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED, schemaV3Enabled);
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, true);
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_RANDOM_PORT, true);
    setup();
  }

  private void setup() throws Exception {
    address = SCMTestUtils.getReuseableAddress();
    conf.setSocketAddr(ScmConfigKeys.OZONE_SCM_NAMES, address);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempFolder.toString());
  }

  @AfterEach
  public void teardown() throws Exception {
    if (scmRpcServer != null) {
      scmRpcServer.stop();
    }

    if (dsm != null) {
      dsm.close();
    }
  }

  public static Stream<Arguments> parameters() {
    return Stream.of(
        arguments(true, false),
        arguments(true, true),
        arguments(false, false),
        arguments(false, true)
    );
  }

  /**
   * a. new container will be schema V2/V3 before DATANODE_SCHEMA_V4 is finalized,
   *    depending on whether V3 is enabled or not.
   * b. new container will be schema V2/V4 after DATANODE_SCHEMA_V4 is finalized,
   *    depending on whether V3 is enabled or not.
   */
  @ParameterizedTest(name = "schema V3 enabled :{0}, SchemaV4 finalized: {1}")
  @MethodSource("parameters")
  public void testContainerSchemaV4(boolean schemaV3Enabled, boolean finalize) throws Exception {
    initTests(schemaV3Enabled);
    // start DN and SCM
    ScmTestMock scmTestMock = new ScmTestMock(CLUSTER_ID);
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf, scmTestMock, address, 10);
    UpgradeTestHelper.addHddsVolume(conf, tempFolder.toPath());
    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder.toPath(), dsm, address,
        HDDSLayoutFeature.HBASE_SUPPORT.layoutVersion());
    ContainerDispatcher dispatcher = dsm.getContainer().getDispatcher();
    dispatcher.setClusterId(CLUSTER_ID);
    if (finalize) {
      dsm.finalizeUpgrade();
    }

    final Pipeline pipeline = MockPipeline.createPipeline(
        Collections.singletonList(dsm.getDatanodeDetails()));
    // Create a container to write data.
    final long containerID1 = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    UpgradeTestHelper.putBlock(dispatcher, containerID1, pipeline);
    UpgradeTestHelper.closeContainer(dispatcher, containerID1, pipeline);
    KeyValueContainer container = (KeyValueContainer)
        dsm.getContainer().getContainerSet().getContainer(containerID1);
    File yamlFile = container.getContainerFile();
    String content =
        FileUtils.readFileToString(yamlFile, Charset.defaultCharset());
    System.out.println(content);
    if (finalize) {
      if (schemaV3Enabled) {
        assertThat(content).doesNotContain(METADATA_PATH);
        assertThat(content).doesNotContain(CHUNKS_PATH);
      } else {
        assertThat(content).contains(METADATA_PATH);
        assertThat(content).contains(CHUNKS_PATH);
      }
    } else {
      assertThat(content).contains(METADATA_PATH);
      assertThat(content).contains(CHUNKS_PATH);
    }
    assertEquals(yamlFile.getParentFile().getParentFile().toPath().resolve(STORAGE_DIR_CHUNKS).toString(),
        container.getContainerData().getChunksPath());
    assertEquals(yamlFile.getParentFile().getAbsolutePath(), container.getContainerData().getMetadataPath());
    File containerDir = new File(container.getContainerData().getContainerPath());
    assertTrue(containerDir.exists() && containerDir.isDirectory());
    FileTime creationTime1 = (FileTime) Files.getAttribute(containerDir.toPath(), "creationTime");

    // export the container
    File folderToExport = Files.createFile(
        tempFolder.toPath().resolve("export-testContainerSchemaV4.tar")).toFile();
    TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);

    //export the container
    try (FileOutputStream fos = new FileOutputStream(folderToExport)) {
      container.exportContainerData(fos, packer);
    }

    //delete the original one
    KeyValueContainerUtil.removeContainer(container.getContainerData(), conf);
    container.delete();
    assertFalse(new File(container.getContainerData().getContainerPath()).exists());
    if (schemaV3Enabled) {
      assertTrue(container.getContainerData().getDbFile().exists());
    }

    //create a new one
    KeyValueContainerData oldContainerData = container.getContainerData();
    KeyValueContainerData newContainerData =
        new KeyValueContainerData(containerID1,
            oldContainerData.getLayoutVersion(),
            oldContainerData.getMaxSize(), pipeline.getId().getId().toString(),
            dsm.getDatanodeDetails().getUuidString());
    newContainerData.setSchemaVersion(oldContainerData.getSchemaVersion());
    KeyValueContainer newContainer = new KeyValueContainer(newContainerData, conf);
    newContainer.populatePathFields(scmTestMock.getClusterId(), oldContainerData.getVolume());

    // verify yaml file checksum
    try (FileInputStream fis = new FileInputStream(folderToExport)) {
      byte[] containerDescriptorYaml = packer.unpackContainerDescriptor(fis);
      KeyValueContainerData data = (KeyValueContainerData) ContainerDataYaml
          .readContainer(containerDescriptorYaml);
      ContainerUtils.verifyChecksum(data, conf);
    }

    // sleep 1s to make sure creationTime will change
    Thread.sleep(1000);
    try (FileInputStream fis = new FileInputStream(folderToExport)) {
      newContainer.importContainerData(fis, packer);
    }

    assertEquals(newContainerData.getContainerDBType(), oldContainerData.getContainerDBType());
    assertEquals(newContainerData.getState(), oldContainerData.getState());
    assertEquals(newContainerData.getBlockCount(), oldContainerData.getBlockCount());
    assertEquals(newContainerData.getLayoutVersion(), oldContainerData.getLayoutVersion());
    assertEquals(newContainerData.getMaxSize(), oldContainerData.getMaxSize());
    assertEquals(newContainerData.getBytesUsed(), oldContainerData.getBytesUsed());
    assertEquals(newContainerData.getMetadataPath(), oldContainerData.getMetadataPath());
    assertEquals(newContainerData.getChunksPath(), oldContainerData.getChunksPath());
    assertEquals(newContainerData.getContainerPath(), oldContainerData.getContainerPath());
    assertTrue(new File(newContainerData.getContainerPath()).exists());
    assertTrue(new File(newContainerData.getChunksPath()).exists());
    assertTrue(new File(newContainerData.getMetadataPath()).exists());
    if (schemaV3Enabled) {
      assertTrue(newContainerData.getDbFile().exists());
      assertEquals(newContainerData.getDbFile(), oldContainerData.getDbFile());
    }
    yamlFile = newContainer.getContainerFile();
    content = FileUtils.readFileToString(yamlFile, Charset.defaultCharset());
    System.out.println(content);
    if (finalize) {
      if (schemaV3Enabled) {
        assertThat(content).doesNotContain(METADATA_PATH);
        assertThat(content).doesNotContain(CHUNKS_PATH);
      } else {
        assertThat(content).contains(METADATA_PATH);
        assertThat(content).contains(CHUNKS_PATH);
      }
    } else {
      assertThat(content).contains(METADATA_PATH);
      assertThat(content).contains(CHUNKS_PATH);
    }
    assertEquals(yamlFile.getParentFile().getParentFile().toPath().resolve(STORAGE_DIR_CHUNKS).toString(),
        newContainer.getContainerData().getChunksPath());
    assertEquals(yamlFile.getParentFile().getAbsolutePath(), newContainer.getContainerData().getMetadataPath());
    FileTime creationTime2 = (FileTime) Files.getAttribute(
        Paths.get(newContainer.getContainerData().getContainerPath()), "creationTime");
    assertNotEquals(creationTime1.toInstant(), creationTime2.toInstant());
  }

  /**
   * Test container created before finalization, still be accessible.
   * V3 container will be automatically migrated to V4 if there is any container yaml file update on disk.
   */
  @ParameterizedTest(name = "schema V3 enabled :{0}, export container before finalization: {1}")
  @MethodSource("parameters")
  public void testContainerBeforeFinalization(
      boolean schemaV3Enabled, boolean exportBeforeFinalization) throws Exception {
    initTests(schemaV3Enabled);
    // start DN and SCM
    ScmTestMock scmTestMock = new ScmTestMock(CLUSTER_ID);
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf, scmTestMock, address, 10);
    UpgradeTestHelper.addHddsVolume(conf, tempFolder.toPath());
    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder.toPath(), dsm, address,
        HDDSLayoutFeature.HBASE_SUPPORT.layoutVersion());
    ContainerDispatcher dispatcher = dsm.getContainer().getDispatcher();
    dispatcher.setClusterId(CLUSTER_ID);

    // create container
    final Pipeline pipeline = MockPipeline.createPipeline(
        Collections.singletonList(dsm.getDatanodeDetails()));
    // Create a container to write data.
    final long containerID1 = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    UpgradeTestHelper.putBlock(dispatcher, containerID1, pipeline);
    UpgradeTestHelper.closeContainer(dispatcher, containerID1, pipeline);
    KeyValueContainer container = (KeyValueContainer)
        dsm.getContainer().getContainerSet().getContainer(containerID1);
    File yamlFile = container.getContainerFile();
    String content =
        FileUtils.readFileToString(yamlFile, Charset.defaultCharset());
    System.out.println(content);
    // yaml file contains chunkPath and metadataPath
    assertThat(content).contains(METADATA_PATH);
    assertThat(content).contains(CHUNKS_PATH);
    assertEquals(yamlFile.getParentFile().getParentFile().toPath().resolve(STORAGE_DIR_CHUNKS).toString(),
        container.getContainerData().getChunksPath());
    assertEquals(yamlFile.getParentFile().getAbsolutePath(), container.getContainerData().getMetadataPath());
    File containerDir = new File(container.getContainerData().getContainerPath());
    assertTrue(containerDir.exists() && containerDir.isDirectory());
    FileTime creationTime1 = (FileTime) Files.getAttribute(containerDir.toPath(), "creationTime");

    File folderToExport = Files.createFile(
        tempFolder.toPath().resolve("export-testContainerBeforeFinalization.tar")).toFile();
    TarContainerPacker packer = new TarContainerPacker(NO_COMPRESSION);
    if (exportBeforeFinalization) {
      //export the container
      try (FileOutputStream fos = new FileOutputStream(folderToExport)) {
        container.exportContainerData(fos, packer);
      }
    }

    dsm.finalizeUpgrade();

    if (!exportBeforeFinalization) {
      //export the container
      try (FileOutputStream fos = new FileOutputStream(folderToExport)) {
        container.exportContainerData(fos, packer);
      }
    }

    //delete the original one
    KeyValueContainerUtil.removeContainer(container.getContainerData(), conf);
    container.delete();
    assertFalse(new File(container.getContainerData().getContainerPath()).exists());
    if (schemaV3Enabled) {
      assertTrue(container.getContainerData().getDbFile().exists());
    }

    //create a new one
    KeyValueContainerData oldContainerData = container.getContainerData();
    KeyValueContainerData newContainerData =
        new KeyValueContainerData(containerID1,
            oldContainerData.getLayoutVersion(),
            oldContainerData.getMaxSize(), pipeline.getId().getId().toString(),
            dsm.getDatanodeDetails().getUuidString());
    newContainerData.setSchemaVersion(oldContainerData.getSchemaVersion());
    KeyValueContainer newContainer = new KeyValueContainer(newContainerData, conf);
    newContainer.populatePathFields(scmTestMock.getClusterId(), oldContainerData.getVolume());

    // verify yaml file checksum
    try (FileInputStream fis = new FileInputStream(folderToExport)) {
      byte[] containerDescriptorYaml = packer.unpackContainerDescriptor(fis);
      KeyValueContainerData data = (KeyValueContainerData) ContainerDataYaml
          .readContainer(containerDescriptorYaml);
      ContainerUtils.verifyChecksum(data, conf);
    }

    // sleep 1s to make sure creationTime will change
    Thread.sleep(1000);
    try (FileInputStream fis = new FileInputStream(folderToExport)) {
      newContainer.importContainerData(fis, packer);
    }

    assertEquals(newContainerData.getContainerDBType(), oldContainerData.getContainerDBType());
    assertEquals(newContainerData.getState(), oldContainerData.getState());
    assertEquals(newContainerData.getBlockCount(), oldContainerData.getBlockCount());
    assertEquals(newContainerData.getLayoutVersion(), oldContainerData.getLayoutVersion());
    assertEquals(newContainerData.getMaxSize(), oldContainerData.getMaxSize());
    assertEquals(newContainerData.getBytesUsed(), oldContainerData.getBytesUsed());
    assertEquals(newContainerData.getMetadataPath(), oldContainerData.getMetadataPath());
    assertEquals(newContainerData.getChunksPath(), oldContainerData.getChunksPath());
    assertEquals(newContainerData.getContainerPath(), oldContainerData.getContainerPath());
    assertTrue(new File(newContainerData.getContainerPath()).exists());
    assertTrue(new File(newContainerData.getChunksPath()).exists());
    assertTrue(new File(newContainerData.getMetadataPath()).exists());
    if (schemaV3Enabled) {
      assertTrue(newContainerData.getDbFile().exists());
      assertEquals(newContainerData.getDbFile(), oldContainerData.getDbFile());
    }
    yamlFile = newContainer.getContainerFile();
    content = FileUtils.readFileToString(yamlFile, Charset.defaultCharset());
    System.out.println(content);
    if (schemaV3Enabled) {
      assertThat(content).doesNotContain(METADATA_PATH);
      assertThat(content).doesNotContain(CHUNKS_PATH);
      // V3 migrate to V4 automatically
      assertTrue(newContainer.getContainerData().hasSchema(SCHEMA_V4));
    } else {
      assertThat(content).contains(METADATA_PATH);
      assertThat(content).contains(CHUNKS_PATH);
      assertTrue(newContainer.getContainerData().hasSchema(SCHEMA_V2));
    }

    assertEquals(yamlFile.getParentFile().getParentFile().toPath().resolve(STORAGE_DIR_CHUNKS).toString(),
        newContainer.getContainerData().getChunksPath());
    assertEquals(yamlFile.getParentFile().getAbsolutePath(), newContainer.getContainerData().getMetadataPath());
    FileTime creationTime2 = (FileTime) Files.getAttribute(
        Paths.get(newContainer.getContainerData().getContainerPath()), "creationTime");
    assertNotEquals(creationTime1.toInstant(), creationTime2.toInstant());
  }
}
