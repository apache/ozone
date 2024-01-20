/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.upgrade;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.ScmTestMock;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.states.endpoint.VersionEndpointTask;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.DbVolume;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

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

  private Random random;

  private void initTests(Boolean enable) throws Exception {
    boolean schemaV3Enabled = enable;
    conf = new OzoneConfiguration();
    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED,
        schemaV3Enabled);
    conf.setBoolean(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_DATASTREAM_ENABLED, true);
    conf.setBoolean(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_DATASTREAM_RANDOM_PORT, true);
    setup();
  }

  private void setup() throws Exception {
    random = new Random();

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
    startScmServer();
    addHddsVolume();

    startPreFinalizedDatanode();
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
    startScmServer();
    addHddsVolume();
    addDbVolume();

    startPreFinalizedDatanode();
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
    startScmServer();
    // add one HddsVolume
    addHddsVolume();

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
    restartDatanode(
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
    startScmServer();
    // add one HddsVolume and two DbVolume
    addHddsVolume();
    addDbVolume();
    addDbVolume();

    startPreFinalizedDatanode();
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
    startScmServer();
    addHddsVolume();

    startPreFinalizedDatanode();
    dsm.finalizeUpgrade();

    // Add a new HddsVolume. It should have DB created after DN restart.
    addHddsVolume();
    restartDatanode(HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(),
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
    startScmServer();
    addHddsVolume();

    startPreFinalizedDatanode();
    HddsVolume hddsVolume = (HddsVolume) dsm.getContainer().getVolumeSet()
        .getVolumesList().get(0);
    assertNull(hddsVolume.getDbParentDir());
    dsm.finalizeUpgrade();
    // DB is created during upgrade
    File dbDir = hddsVolume.getDbParentDir();
    assertTrue(dbDir.getAbsolutePath().startsWith(
        hddsVolume.getStorageDir().getAbsolutePath()));

    // Add a new DbVolume
    addDbVolume();
    restartDatanode(HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(),
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
    startScmServer();
    addHddsVolume();

    startPreFinalizedDatanode();
    dsm.finalizeUpgrade();

    addDbVolume();
    File newDataVolume = addHddsVolume();
    restartDatanode(HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(),
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
    startScmServer();
    addHddsVolume();
    // Disable Schema V3
    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED, false);
    startPreFinalizedDatanode();
    dsm.finalizeUpgrade();

    final Pipeline pipeline = getPipeline();
    // Create a container to write data.
    final long containerID1 = addContainer(pipeline);
    putBlock(containerID1, pipeline);
    closeContainer(containerID1, pipeline);
    KeyValueContainer container = (KeyValueContainer)
        dsm.getContainer().getContainerSet().getContainer(containerID1);
    // When SchemaV3 is disabled, new data should be saved as SchemaV2.
    assertEquals(OzoneConsts.SCHEMA_V2,
        container.getContainerData().getSchemaVersion());

    // Set SchemaV3 enable status
    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED,
        enable);
    restartDatanode(HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(),
        false);

    // Write new data
    final long containerID2 = addContainer(pipeline);
    putBlock(containerID2, pipeline);
    closeContainer(containerID2, pipeline);
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
    startScmServer();
    addHddsVolume();
    startPreFinalizedDatanode();
    final Pipeline pipeline = getPipeline();

    // Add data to read.
    final long containerID = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk = putBlock(containerID,
        pipeline);
    closeContainer(containerID, pipeline);

    // Create thread to keep reading during finalization.
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future<Void> readFuture = executor.submit(() -> {
      // Layout version check should be thread safe.
      while (!dsm.getLayoutVersionManager()
          .isAllowed(HDDSLayoutFeature.DATANODE_SCHEMA_V3)) {
        readChunk(writeChunk, pipeline);
      }
      // Make sure we can read after finalizing too.
      readChunk(writeChunk, pipeline);
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
    startScmServer();
    addHddsVolume();
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
    restartDatanode(
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion(), true);

    // Write some data.
    final Pipeline pipeline = getPipeline();
    final long containerID = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk = putBlock(containerID,
        pipeline);
    closeContainer(containerID, pipeline);
    KeyValueContainer container = (KeyValueContainer)
        dsm.getContainer().getContainerSet().getContainer(containerID);
    assertEquals(OzoneConsts.SCHEMA_V2,
        container.getContainerData().getSchemaVersion());


    HddsVolume volume = mock(HddsVolume.class);
    doThrow(new IOException("Failed to init DB")).when(volume).
        createDbStore(any());
    Map volumeMap = new HashMap<String, StorageVolume>();
    volumeMap.put(dataVolume.getStorageID(), volume);
    dsm.getContainer().getVolumeSet().setVolumeMap(volumeMap);

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
    readChunk(writeChunk, pipeline);

    // SchemaV3 is not finalized, so still ERASURE_CODED_STORAGE_SUPPORT
    restartDatanode(
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion(), true);

    // Old data is readable after DN restart
    container = (KeyValueContainer)
        dsm.getContainer().getContainerSet().getContainer(containerID);
    assertEquals(OzoneConsts.SCHEMA_V2,
        container.getContainerData().getSchemaVersion());
    readChunk(writeChunk, pipeline);
  }

  public void checkContainerPathID(long containerID, String expectedID) {
    KeyValueContainerData data =
        (KeyValueContainerData) dsm.getContainer().getContainerSet()
            .getContainer(containerID).getContainerData();
    assertThat(data.getChunksPath()).contains(expectedID);
    assertThat(data.getMetadataPath()).contains(expectedID);
  }

  public List<File> getHddsSubdirs(File volume) {
    File[] subdirsArray = getHddsRoot(volume).listFiles(File::isDirectory);
    assertNotNull(subdirsArray);
    return Arrays.asList(subdirsArray);
  }

  public File getHddsRoot(File volume) {
    return new File(HddsVolumeUtil.getHddsRoot(volume.getAbsolutePath()));
  }

  /**
   * Starts the datanode with the fore layout version, and calls the version
   * endpoint task to get cluster ID and SCM ID.
   *
   * The daemon for the datanode state machine is not started in this test.
   * This greatly speeds up execution time.
   * It means we do not have heartbeat functionality or pre-finalize
   * upgrade actions, but neither of those things are needed for these tests.
   */
  public void startPreFinalizedDatanode() throws Exception {
    // Set layout version.
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempFolder.toString());
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString(),
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion());
    layoutStorage.initialize();

    // Build and start the datanode.
    DatanodeDetails dd = ContainerTestUtils.createDatanodeDetails();
    DatanodeStateMachine newDsm = new DatanodeStateMachine(dd, conf);
    int actualMlv = newDsm.getLayoutVersionManager().getMetadataLayoutVersion();
    assertEquals(
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion(),
        actualMlv);
    if (dsm != null) {
      dsm.close();
    }
    dsm = newDsm;

    callVersionEndpointTask();
  }

  public void restartDatanode(int expectedMlv, boolean exactMatch)
      throws Exception {
    // Stop existing datanode.
    DatanodeDetails dd = dsm.getDatanodeDetails();
    dsm.close();

    // Start new datanode with the same configuration.
    dsm = new DatanodeStateMachine(dd, conf);
    int mlv = dsm.getLayoutVersionManager().getMetadataLayoutVersion();
    if (exactMatch) {
      assertEquals(expectedMlv, mlv);
    } else {
      assertThat(expectedMlv).isLessThanOrEqualTo(mlv);
    }

    callVersionEndpointTask();
  }

  /**
   * Get the cluster ID and SCM ID from SCM to the datanode.
   */
  public void callVersionEndpointTask() throws Exception {
    try (EndpointStateMachine esm = ContainerTestUtils.createEndpoint(conf,
        address, 1000)) {
      VersionEndpointTask vet = new VersionEndpointTask(esm, conf,
          dsm.getContainer());
      esm.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      vet.call();
    }
  }

  public String startScmServer() throws IOException {
    String scmID = UUID.randomUUID().toString();
    ScmTestMock scmServerImpl = new ScmTestMock(CLUSTER_ID, scmID);
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        scmServerImpl, address, 10);
    return scmID;
  }

  /// CONTAINER OPERATIONS ///
  public void readChunk(ContainerProtos.WriteChunkRequestProto writeChunk,
      Pipeline pipeline)  throws Exception {
    ContainerProtos.ContainerCommandRequestProto readChunkRequest =
        ContainerTestHelper.getReadChunkRequest(pipeline, writeChunk);

    dispatchRequest(readChunkRequest);
  }

  public ContainerProtos.WriteChunkRequestProto putBlock(long containerID,
      Pipeline pipeline) throws Exception {
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        getWriteChunk(containerID, pipeline);
    dispatchRequest(writeChunkRequest);

    ContainerProtos.ContainerCommandRequestProto putBlockRequest =
        ContainerTestHelper.getPutBlockRequest(pipeline,
            writeChunkRequest.getWriteChunk());
    dispatchRequest(putBlockRequest);

    return writeChunkRequest.getWriteChunk();
  }

  public ContainerProtos.ContainerCommandRequestProto getWriteChunk(
      long containerID, Pipeline pipeline) throws Exception {
    return ContainerTestHelper.getWriteChunkRequest(pipeline,
            ContainerTestHelper.getTestBlockID(containerID), 100);
  }

  public Pipeline getPipeline() {
    return MockPipeline.createPipeline(
        Collections.singletonList(dsm.getDatanodeDetails()));
  }

  public long addContainer(Pipeline pipeline)
      throws Exception {
    long containerID = random.nextInt(Integer.MAX_VALUE);
    ContainerProtos.ContainerCommandRequestProto createContainerRequest =
        ContainerTestHelper.getCreateContainerRequest(containerID, pipeline);
    dispatchRequest(createContainerRequest);

    return containerID;
  }

  public void deleteContainer(long containerID, Pipeline pipeline)
      throws Exception {
    ContainerProtos.ContainerCommandRequestProto deleteContainerRequest =
        ContainerTestHelper.getDeleteContainer(pipeline, containerID, true);
    dispatchRequest(deleteContainerRequest);
  }

  public void closeContainer(long containerID, Pipeline pipeline)
      throws Exception {
    closeContainer(containerID, pipeline, ContainerProtos.Result.SUCCESS);
  }

  public void closeContainer(long containerID, Pipeline pipeline,
      ContainerProtos.Result expectedResult) throws Exception {
    ContainerProtos.ContainerCommandRequestProto closeContainerRequest =
        ContainerTestHelper.getCloseContainer(pipeline, containerID);
    dispatchRequest(closeContainerRequest, expectedResult);
  }

  public void dispatchRequest(
      ContainerProtos.ContainerCommandRequestProto request) {
    dispatchRequest(request, ContainerProtos.Result.SUCCESS);
  }

  public void dispatchRequest(
      ContainerProtos.ContainerCommandRequestProto request,
      ContainerProtos.Result expectedResult) {
    ContainerProtos.ContainerCommandResponseProto response =
        dsm.getContainer().getDispatcher().dispatch(request, null);
    assertEquals(expectedResult, response.getResult());
  }

  /// VOLUME OPERATIONS ///

  /**
   * Append a datanode volume to the existing volumes in the configuration.
   * @return The root directory for the new volume.
   */
  public File addHddsVolume() throws IOException {

    File vol = Files.createDirectory(tempFolder.resolve(UUID.randomUUID()
        .toString())).toFile();
    String[] existingVolumes =
        conf.getStrings(ScmConfigKeys.HDDS_DATANODE_DIR_KEY);
    List<String> allVolumes = new ArrayList<>();
    if (existingVolumes != null) {
      allVolumes.addAll(Arrays.asList(existingVolumes));
    }

    allVolumes.add(vol.getAbsolutePath());
    conf.setStrings(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        allVolumes.toArray(new String[0]));

    return vol;
  }

  /**
   * Append a db volume to the existing volumes in the configuration.
   * @return The root directory for the new volume.
   */
  public File addDbVolume() throws Exception {
    File vol = Files.createDirectory(tempFolder.resolve(UUID.randomUUID()
        .toString())).toFile();
    String[] existingVolumes =
        conf.getStrings(OzoneConfigKeys.HDDS_DATANODE_CONTAINER_DB_DIR);
    List<String> allVolumes = new ArrayList<>();
    if (existingVolumes != null) {
      allVolumes.addAll(Arrays.asList(existingVolumes));
    }

    allVolumes.add(vol.getAbsolutePath());
    conf.setStrings(OzoneConfigKeys.HDDS_DATANODE_CONTAINER_DB_DIR,
        allVolumes.toArray(new String[0]));

    return vol;
  }
}
