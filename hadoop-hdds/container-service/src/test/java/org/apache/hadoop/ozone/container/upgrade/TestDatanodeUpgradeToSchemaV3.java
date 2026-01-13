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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.ScmTestMock;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.volume.DbVolume;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests upgrading a single datanode from container Schema V2 to Schema V3.
 */
public class TestDatanodeUpgradeToSchemaV3 {
  @TempDir
  private Path tempFolder;

  private DatanodeStateMachine dsm;
  private OzoneConfiguration conf;
  private static final String CLUSTER_ID = "clusterID";

  private RPC.Server scmRpcServer;
  private InetSocketAddress address;

  private void initTests(Boolean enable) throws Exception {
    boolean schemaV3Enabled = enable;
    conf = new OzoneConfiguration();
    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED,
        schemaV3Enabled);
    conf.setBoolean(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, true);
    conf.setBoolean(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_RANDOM_PORT, true);
    setup();
  }

  private void setup() throws Exception {
    address = SCMTestUtils.getReuseableAddress();
    conf.setSocketAddr(ScmConfigKeys.OZONE_SCM_NAMES, address);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempFolder.toString());
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

  /**
   * Test RocksDB is created on data volume, not matter Schema V3 is
   * enabled or not.
   * If Schema V3 is enabled, RocksDB will be loaded.
   */
  @ParameterizedTest(name = "schema V3 enabled :{0}")
  @ValueSource(booleans = {true, false})
  public void testDBOnHddsVolume(boolean schemaV3Enabled) throws Exception {
    initTests(schemaV3Enabled);
    // start DN and SCM
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        new ScmTestMock(CLUSTER_ID), address, 10);
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);

    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion());
    HddsVolume dataVolume = (HddsVolume) dsm.getContainer().getVolumeSet()
        .getVolumesList().get(0);
    assertNull(dataVolume.getDbVolume());
    assertFalse(dataVolume.isDbLoaded());

    dsm.finalizeUpgrade();
    // RocksDB is created during upgrade
    File dbFile = new File(dataVolume.getStorageDir().getAbsolutePath() + "/" +
        dataVolume.getClusterID() + "/" + dataVolume.getStorageID());
    assertTrue(dbFile.exists());

    // RocksDB loaded when SchemaV3 is enabled
    if (VersionedDatanodeFeatures.SchemaV3.isFinalizedAndEnabled(conf)) {
      assertTrue(dataVolume.getDbParentDir().getAbsolutePath()
          .startsWith(dataVolume.getStorageDir().toString()));
    } else {
      // RocksDB is not loaded when SchemaV3 is disabled.
      assertFalse(dataVolume.isDbLoaded());
    }
  }

  /**
   * Test RocksDB is created on DB volume when configured, not matter
   * Schema V3 is enabled or not.
   * If Schema V3 is enabled, RocksDB will be loaded.
   */
  @ParameterizedTest(name = "schema V3 enabled :{0}")
  @ValueSource(booleans = {false, true})
  public void testDBOnDbVolume(boolean schemaV3Enabled) throws Exception {
    initTests(schemaV3Enabled);
    // start DN and SCM
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        new ScmTestMock(CLUSTER_ID), address, 10);
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    UpgradeTestHelper.addDbVolume(conf, tempFolder);

    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion());
    HddsVolume dataVolume = (HddsVolume) dsm.getContainer().getVolumeSet()
        .getVolumesList().get(0);
    assertNull(dataVolume.getDbParentDir());

    dsm.finalizeUpgrade();
    // RocksDB is created during upgrade
    DbVolume dbVolume = (DbVolume) dsm.getContainer().getDbVolumeSet()
        .getVolumesList().get(0);
    assertEquals(dbVolume, dataVolume.getDbVolume());
    assertThat(dbVolume.getHddsVolumeIDs()).contains(dataVolume.getStorageID());
    File dbFile = new File(dbVolume.getStorageDir().getAbsolutePath() + "/" +
        dbVolume.getClusterID() + "/" + dataVolume.getStorageID());
    assertTrue(dbFile.exists());

    // RocksDB loaded when SchemaV3 is enabled
    if (VersionedDatanodeFeatures.SchemaV3.isFinalizedAndEnabled(conf)) {
      assertTrue(dataVolume.getDbParentDir().getAbsolutePath()
          .startsWith(dbVolume.getStorageDir().toString()));
    } else {
      // RocksDB is not loaded when SchemaV3 is disabled.
      assertFalse(dataVolume.isDbLoaded());
    }
  }

  /**
   * Test RocksDB in created in Finalize action for an existing hddsVolume.
   * This mimics the real cluster upgrade situation.
   */
  @ParameterizedTest(name = "schema V3 enabled :{0}")
  @ValueSource(booleans = {false, true})
  public void testDBCreatedInFinalize(boolean schemaV3Enabled)
      throws Exception {
    initTests(schemaV3Enabled);
    // start DN and SCM
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        new ScmTestMock(CLUSTER_ID), address, 10);
    // add one HddsVolume
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);

    // Set layout version.
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString(),
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion());
    layoutStorage.initialize();
    dsm = new DatanodeStateMachine(
        ContainerTestUtils.createDatanodeDetails(), conf);
    HddsVolume dataVolume = (
        HddsVolume) dsm.getContainer().getVolumeSet().getVolumesList().get(0);
    // Format HddsVolume to mimic the real cluster upgrade situation
    dataVolume.format(CLUSTER_ID);
    File idDir = new File(dataVolume.getStorageDir(), CLUSTER_ID);
    if (!idDir.mkdir()) {
      fail("Failed to create id directory");
    }

    assertNull(dataVolume.getDbParentDir());

    // Restart DN and finalize upgrade
    dsm = UpgradeTestHelper.restartDatanode(conf, dsm, false, tempFolder, address,
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion(), true);
    dsm.finalizeUpgrade();

    // RocksDB is created by upgrade action
    dataVolume = ((HddsVolume) dsm.getContainer().getVolumeSet()
        .getVolumesList().get(0));
    assertNotNull(dataVolume.getDbParentDir());
    if (VersionedDatanodeFeatures.SchemaV3.isFinalizedAndEnabled(conf)) {
      assertTrue(dataVolume.isDbLoaded());
    } else {
      assertFalse(dataVolume.isDbLoaded());
    }
  }

  /**
   * Test finalize twice won't recreate any RocksDB for HddsVolume.
   */
  @ParameterizedTest(name = "schema V3 enabled :{0}")
  @ValueSource(booleans = {false, true})
  public void testFinalizeTwice(boolean schemaV3Enabled) throws Exception {
    initTests(schemaV3Enabled);
    // start DN and SCM
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        new ScmTestMock(CLUSTER_ID), address, 10);
    // add one HddsVolume and two DbVolume
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    UpgradeTestHelper.addDbVolume(conf, tempFolder);
    UpgradeTestHelper.addDbVolume(conf, tempFolder);

    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion());
    dsm.finalizeUpgrade();

    DbVolume dbVolume = ((HddsVolume) dsm.getContainer().getVolumeSet()
        .getVolumesList().get(0)).getDbVolume();
    assertNotNull(dbVolume);

    dsm.finalizeUpgrade();
    // DB Dir should be the same.
    assertEquals(dbVolume, ((HddsVolume) dsm.getContainer()
        .getVolumeSet().getVolumesList().get(0)).getDbVolume());
  }

  /**
   * For a finalized cluster, add a new HddsVolume.
   */
  @ParameterizedTest(name = "schema V3 enabled :{0}")
  @ValueSource(booleans = {false, true})
  public void testAddHddsVolumeAfterFinalize(boolean schemaV3Enabled)
      throws Exception {
    initTests(schemaV3Enabled);
    // start DN and SCM
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        new ScmTestMock(CLUSTER_ID), address, 10);
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);

    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion());
    dsm.finalizeUpgrade();

    // Add a new HddsVolume. It should have DB created after DN restart.
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    dsm = UpgradeTestHelper.restartDatanode(conf, dsm, false, tempFolder, address,
        HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(),
        false);
    for (StorageVolume vol:
        dsm.getContainer().getVolumeSet().getVolumesList()) {
      HddsVolume hddsVolume = (HddsVolume) vol;
      if (VersionedDatanodeFeatures.SchemaV3.isFinalizedAndEnabled(conf)) {
        assertTrue(hddsVolume.isDbLoaded());
        assertTrue(hddsVolume.getDbParentDir().getAbsolutePath()
            .startsWith(hddsVolume.getStorageDir().getAbsolutePath()));
      } else {
        assertFalse(hddsVolume.isDbLoaded());
      }
    }
  }

  /**
   * For a finalized cluster, add a new DbVolume.
   */
  @ParameterizedTest(name = "schema V3 enabled :{0}")
  @ValueSource(booleans = {false, true})
  public void testAddDbVolumeAfterFinalize(boolean schemaV3Enabled)
      throws Exception {
    initTests(schemaV3Enabled);
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        new ScmTestMock(CLUSTER_ID), address, 10);
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);

    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion());
    HddsVolume hddsVolume = (HddsVolume) dsm.getContainer().getVolumeSet()
        .getVolumesList().get(0);
    assertNull(hddsVolume.getDbParentDir());
    dsm.finalizeUpgrade();
    // DB is created during upgrade
    File dbDir = hddsVolume.getDbParentDir();
    assertTrue(dbDir.getAbsolutePath().startsWith(
        hddsVolume.getStorageDir().getAbsolutePath()));

    // Add a new DbVolume
    UpgradeTestHelper.addDbVolume(conf, tempFolder);
    dsm = UpgradeTestHelper.restartDatanode(conf, dsm, false, tempFolder, address,
        HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(),
        false);

    // HddsVolume should still use the rocksDB under it's volume
    DbVolume dbVolume = (DbVolume) dsm.getContainer().getDbVolumeSet()
        .getVolumesList().get(0);
    assertEquals(0, dbVolume.getHddsVolumeIDs().size());

    if (VersionedDatanodeFeatures.SchemaV3.isFinalizedAndEnabled(conf)) {
      hddsVolume = (HddsVolume) dsm.getContainer().getVolumeSet()
          .getVolumesList().get(0);
      assertEquals(dbDir, hddsVolume.getDbParentDir());
      assertTrue(hddsVolume.isDbLoaded());
    }
  }

  /**
   * For a finalized cluster, add a new DbVolume and a new HddsVolume.
   */
  @ParameterizedTest(name = "schema V3 enabled :{0}")
  @ValueSource(booleans = {false, true})
  public void testAddDbAndHddsVolumeAfterFinalize(boolean schemaV3Enabled)
      throws Exception {
    initTests(schemaV3Enabled);
    // start DN and SCM
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        new ScmTestMock(CLUSTER_ID), address, 10);
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);

    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion());
    dsm.finalizeUpgrade();

    UpgradeTestHelper.addDbVolume(conf, tempFolder);
    File newDataVolume = UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    dsm = UpgradeTestHelper.restartDatanode(conf, dsm, false, tempFolder, address,
        HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(),
        false);

    DbVolume dbVolume = (DbVolume) dsm.getContainer().getDbVolumeSet()
        .getVolumesList().get(0);

    for (StorageVolume vol:
        dsm.getContainer().getVolumeSet().getVolumesList()) {
      HddsVolume hddsVolume = (HddsVolume) vol;
      File dbFile;
      if (hddsVolume.getStorageDir().getAbsolutePath().startsWith(
          newDataVolume.getAbsolutePath())) {
        if (VersionedDatanodeFeatures.SchemaV3.isFinalizedAndEnabled(conf)) {
          assertEquals(dbVolume, hddsVolume.getDbVolume());
        }
        // RocksDB of newly added HddsVolume is created on the newly added
        // DbVolume
        dbFile = new File(dbVolume.getStorageDir() + "/" +
            hddsVolume.getClusterID() + "/" + hddsVolume.getStorageID());
      } else {
        assertNull(hddsVolume.getDbVolume());
        dbFile = new File(hddsVolume.getStorageDir() + "/" +
            hddsVolume.getClusterID() + "/" + hddsVolume.getStorageID());
      }
      if (VersionedDatanodeFeatures.SchemaV3.isFinalizedAndEnabled(conf)) {
        assertTrue(hddsVolume.isDbLoaded());
        assertTrue(hddsVolume.getDbParentDir().exists());
        assertTrue(dbFile.exists());
        assertEquals(dbFile, hddsVolume.getDbParentDir());
      }
    }
  }

  /**
   * Test data write after finalization.
   */
  @ParameterizedTest(name = "schema V3 enabled :{0}")
  @ValueSource(booleans = {false, true})
  public void testWriteWithV3Enabled(boolean schemaV3Enabled) throws Exception {
    initTests(schemaV3Enabled);
    testWrite(false, OzoneConsts.SCHEMA_V2);
  }

  /**
   * Test data write after finalization.
   */
  @ParameterizedTest(name = "schema V3 enabled :{0}")
  @ValueSource(booleans = {false, true})
  public void testWriteWithV3Disabled(boolean schemaV3Enabled)
      throws Exception {
    initTests(schemaV3Enabled);
    testWrite(true, OzoneConsts.SCHEMA_V3);
  }

  public void testWrite(boolean enable, String expectedVersion)
      throws Exception {
    // start DN and SCM
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        new ScmTestMock(CLUSTER_ID), address, 10);
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    // Disable Schema V3
    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED, false);
    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion());
    ContainerDispatcher dispatcher = dsm.getContainer().getDispatcher();
    dsm.finalizeUpgrade();

    final Pipeline pipeline = MockPipeline.createPipeline(
        Collections.singletonList(dsm.getDatanodeDetails()));
    // Create a container to write data.
    final long containerID1 = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    UpgradeTestHelper.putBlock(dispatcher, containerID1, pipeline);
    UpgradeTestHelper.closeContainer(dispatcher, containerID1, pipeline);
    KeyValueContainer container = (KeyValueContainer)
        dsm.getContainer().getContainerSet().getContainer(containerID1);
    // When SchemaV3 is disabled, new data should be saved as SchemaV2.
    assertEquals(OzoneConsts.SCHEMA_V2,
        container.getContainerData().getSchemaVersion());

    // Set SchemaV3 enable status
    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED,
        enable);
    dsm = UpgradeTestHelper.restartDatanode(conf, dsm, false, tempFolder, address,
        HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(),
        false);
    dispatcher = dsm.getContainer().getDispatcher();

    // Write new data
    final long containerID2 = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    UpgradeTestHelper.putBlock(dispatcher, containerID2, pipeline);
    UpgradeTestHelper.closeContainer(dispatcher, containerID2, pipeline);
    container = (KeyValueContainer)
        dsm.getContainer().getContainerSet().getContainer(containerID2);
    // If SchemaV3 is enabled, new data should be saved as SchemaV3
    // If SchemaV3 is still disabled, new data should still be saved as SchemaV2
    assertEquals(expectedVersion,
        container.getContainerData().getSchemaVersion());
  }

  /**
   * Test data read during and after finalization.
   */
  @ParameterizedTest(name = "schema V3 enabled :{0}")
  @ValueSource(booleans = {false, true})
  public void testReadsDuringFinalize(boolean schemaV3Enabled)
      throws Exception {
    initTests(schemaV3Enabled);
    // start DN and SCM
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        new ScmTestMock(CLUSTER_ID), address, 10);
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion());
    ContainerDispatcher dispatcher = dsm.getContainer().getDispatcher();
    final Pipeline pipeline = MockPipeline.createPipeline(
        Collections.singletonList(dsm.getDatanodeDetails()));

    // Add data to read.
    final long containerID = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk =
        UpgradeTestHelper.putBlock(dispatcher, containerID, pipeline);
    UpgradeTestHelper.closeContainer(dispatcher, containerID, pipeline);

    // Create thread to keep reading during finalization.
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future<Void> readFuture = executor.submit(() -> {
      // Layout version check should be thread safe.
      while (!dsm.getLayoutVersionManager()
          .isAllowed(HDDSLayoutFeature.DATANODE_SCHEMA_V3)) {
        UpgradeTestHelper.readChunk(dispatcher, writeChunk, pipeline);
      }
      // Make sure we can read after finalizing too.
      UpgradeTestHelper.readChunk(dispatcher, writeChunk, pipeline);
      return null;
    });

    dsm.finalizeUpgrade();
    // If there was a failure reading during the upgrade, the exception will
    // be thrown here.
    readFuture.get();
  }

  /**
   * Test finalization failure.
   */
  @ParameterizedTest(name = "schema V3 enabled :{0}")
  @ValueSource(booleans = {false, true})
  public void testFinalizeFailure(boolean schemaV3Enabled) throws Exception {
    initTests(schemaV3Enabled);
    // start DN and SCM
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        new ScmTestMock(CLUSTER_ID), address, 10);
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    // Let HddsVolume be formatted to mimic the real cluster upgrade
    // Set layout version.
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString(),
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion());
    layoutStorage.initialize();
    dsm = new DatanodeStateMachine(
        ContainerTestUtils.createDatanodeDetails(), conf);
    HddsVolume dataVolume = (
        HddsVolume) dsm.getContainer().getVolumeSet().getVolumesList().get(0);
    // Format HddsVolume to mimic the real cluster upgrade situation
    dataVolume.format(CLUSTER_ID);
    File idDir = new File(dataVolume.getStorageDir(), CLUSTER_ID);
    if (!idDir.mkdir()) {
      fail("Failed to create id directory");
    }
    assertNull(dataVolume.getDbParentDir());

    // Restart DN
    dsm = UpgradeTestHelper.restartDatanode(conf, dsm, false, tempFolder, address,
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion(), true);
    ContainerDispatcher dispatcher = dsm.getContainer().getDispatcher();

    // Write some data.
    final Pipeline pipeline = MockPipeline.createPipeline(
        Collections.singletonList(dsm.getDatanodeDetails()));
    final long containerID = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk =
        UpgradeTestHelper.putBlock(dispatcher, containerID, pipeline);
    UpgradeTestHelper.closeContainer(dispatcher, containerID, pipeline);
    KeyValueContainer container = (KeyValueContainer)
        dsm.getContainer().getContainerSet().getContainer(containerID);
    assertEquals(OzoneConsts.SCHEMA_V2,
        container.getContainerData().getSchemaVersion());


    HddsVolume volume = mock(HddsVolume.class);
    doThrow(new IOException("Failed to init DB")).when(volume).
        createDbStore(any());
    Map volumeMap = new HashMap<String, StorageVolume>();
    volumeMap.put(dataVolume.getStorageID(), volume);
    dsm.getContainer().getVolumeSet().setVolumeMapForTesting(volumeMap);

    // Finalize will fail because of DB creation failure
    try {
      dsm.finalizeUpgrade();
    } catch (Exception e) {
      // Currently there will be retry if finalization failed.
      // Let's assume retry is terminated by user.
    }

    // Check that old data is readable
    container = (KeyValueContainer)
        dsm.getContainer().getContainerSet().getContainer(containerID);
    assertEquals(OzoneConsts.SCHEMA_V2,
        container.getContainerData().getSchemaVersion());
    UpgradeTestHelper.readChunk(dispatcher, writeChunk, pipeline);

    // SchemaV3 is not finalized, so still ERASURE_CODED_STORAGE_SUPPORT
    dsm = UpgradeTestHelper.restartDatanode(conf, dsm, false, tempFolder, address,
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion(), true);
    dispatcher = dsm.getContainer().getDispatcher();

    // Old data is readable after DN restart
    container = (KeyValueContainer)
        dsm.getContainer().getContainerSet().getContainer(containerID);
    assertEquals(OzoneConsts.SCHEMA_V2,
        container.getContainerData().getSchemaVersion());
    UpgradeTestHelper.readChunk(dispatcher, writeChunk, pipeline);
  }
}
