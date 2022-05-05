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
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.replication.ContainerReplicationSource;
import org.apache.hadoop.ozone.container.replication.DownloadAndImportReplicator;
import org.apache.hadoop.ozone.container.replication.OnDemandContainerReplicationSource;
import org.apache.hadoop.ozone.container.replication.SimpleContainerDownloader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Tests upgrading a single datanode from container Schema V2 to Schema V3.
 */
@RunWith(Parameterized.class)
public class TestDatanodeUpgradeToSchemaV3 {
  @Rule
  public TemporaryFolder tempFolder;

  private DatanodeStateMachine dsm;
  private final OzoneConfiguration conf;
  private static final String CLUSTER_ID = "clusterID";
  private final boolean schemaV3Enabled;

  private RPC.Server scmRpcServer;
  private InetSocketAddress address;
  private ScmTestMock scmServerImpl;

  private Random random;

  @Parameterized.Parameters(name = "{index}: hdds.datanode.container.schema.v3.enabled={0}")
  public static Collection<Object[]> getSchemaFiles() {
    Collection<Object[]> parameters = new ArrayList<>();
    parameters.add(new Boolean[]{false});
    parameters.add(new Boolean[]{true});
    return parameters;
  }

  public TestDatanodeUpgradeToSchemaV3(boolean enabled) {
    this.schemaV3Enabled = enabled;
    conf = new OzoneConfiguration();
    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED,
        schemaV3Enabled);
  }

  @Before
  public void setup() throws Exception {
    tempFolder = new TemporaryFolder();
    tempFolder.create();
    random = new Random();

    address = SCMTestUtils.getReuseableAddress();
    conf.setSocketAddr(ScmConfigKeys.OZONE_SCM_NAMES, address);
  }

  @After
  public void teardown() throws Exception {
    if (scmRpcServer != null) {
      scmRpcServer.stop();
    }

    if (dsm != null) {
      dsm.close();
    }
  }

  @Test
  public void testDBOnHddsVolume() throws Exception {
    // start DN and SCM
    startScmServer();
    addHddsVolume();

    startPreFinalizedDatanode();
    HddsVolume volume = (HddsVolume) dsm.getContainer().getVolumeSet()
        .getVolumesList().get(0);
    Assert.assertNull(volume.getDbVolume());
    Assert.assertNull(volume.getDbParentDir());

    dsm.finalizeUpgrade();
    // HddsVolume has DB Dir set with no DbVolume
    Assert.assertNull(volume.getDbVolume());
    Assert.assertNotNull(volume.getDbParentDir());
  }

  @Test
  public void testDBOnDbVolume() throws Exception {
    // start DN and SCM
    startScmServer();
    File vol = addHddsVolume();
    addDbVolume();

    startPreFinalizedDatanode();
    HddsVolume dataVolume = (HddsVolume) dsm.getContainer().getVolumeSet()
        .getVolumesList().get(0);
    Assert.assertNull(dataVolume.getDbVolume());
    Assert.assertNull(dataVolume.getDbParentDir());

    dsm.finalizeUpgrade();
    DbVolume dbVolume = (DbVolume) dsm.getContainer().getDbVolumeSet()
        .getVolumesList().get(0);
    Assert.assertEquals(1, dbVolume.getHddsVolumeIDs().size());

    // RocksDB for dataVolume is created on dbVolume
    Assert.assertEquals(dbVolume, dataVolume.getDbVolume());
    Assert.assertTrue(
        dbVolume.getHddsVolumeIDs().contains(vol.getName()));
    Assert.assertNotNull(dataVolume.getDbParentDir().getAbsolutePath()
        .startsWith(dbVolume.getStorageDir().toString()));
  }

  @Test
  public void testFinalizeTwice() throws Exception {
    // start DN and SCM
    startScmServer();
    // add one HddsVolume and two DbVolume
    addHddsVolume();
    addDbVolume();
    addDbVolume();

    startPreFinalizedDatanode();
    dsm.finalizeUpgrade();

    File dbDir = ((HddsVolume) dsm.getContainer().getVolumeSet()
        .getVolumesList().get(0)).getDbParentDir();
    Assert.assertNotNull(dbDir);

    dsm.finalizeUpgrade();
    // DB Dir should be the same.
    Assert.assertEquals(dbDir, ((HddsVolume) dsm.getContainer().getVolumeSet()
       .getVolumesList().get(0)).getDbParentDir());
  }

  @Test
  public void testAddHddsVolumeAfterFinalize() throws Exception {
    // start DN and SCM
    startScmServer();
    addHddsVolume();

    startPreFinalizedDatanode();
    dsm.finalizeUpgrade();
    restartDatanode(HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(), true);

    // All HddsVolume should have RocksDB created.
    for(StorageVolume vol: dsm.getContainer().getVolumeSet().getVolumesList()) {
      HddsVolume hddsVolume = (HddsVolume) vol;
      File dbDir = hddsVolume.getDbParentDir();
      Assert.assertNotNull(dbDir);
      Assert.assertTrue(dbDir.getAbsolutePath().startsWith(
          hddsVolume.getStorageDir().getAbsolutePath()));
    }

    addHddsVolume();
    // New HddsVolume should have RocksDB created too.
    restartDatanode(HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(), true);
    for(StorageVolume vol: dsm.getContainer().getVolumeSet().getVolumesList()) {
      HddsVolume hddsVolume = (HddsVolume) vol;
      Assert.assertNotNull(hddsVolume.getDbParentDir());
      Assert.assertTrue(hddsVolume.getDbParentDir().getAbsolutePath()
          .startsWith(hddsVolume.getStorageDir().getAbsolutePath()));
    }
  }

  @Test
  public void testAddDbVolumeAfterFinalize() throws Exception {
    // start DN and SCM
    startScmServer();
    addHddsVolume();

    startPreFinalizedDatanode();
    dsm.finalizeUpgrade();
    restartDatanode(HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(), true);

    // All HddsVolume should have RocksDB created
    for(StorageVolume vol: dsm.getContainer().getVolumeSet().getVolumesList()) {
      HddsVolume hddsVolume = (HddsVolume) vol;
      File dbDir = hddsVolume.getDbParentDir();
      Assert.assertNotNull(dbDir);
      Assert.assertTrue(dbDir.getAbsolutePath().startsWith(
          hddsVolume.getStorageDir().getAbsolutePath()));
    }

    File dbVolume = addDbVolume();
    File newHddsVolume = addHddsVolume();
    restartDatanode(HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(), true);
    for(StorageVolume vol:dsm.getContainer().getVolumeSet().getVolumesList()) {
      HddsVolume hddsVolume = (HddsVolume) vol;
      if (hddsVolume.getStorageDir() == newHddsVolume) {
        Assert.assertTrue(hddsVolume.getDbParentDir().getAbsolutePath()
            .startsWith(dbVolume.getAbsolutePath()));
      } else {
        File dbDir = hddsVolume.getDbParentDir();
        Assert.assertNotNull(dbDir);
        Assert.assertTrue(dbDir.getAbsolutePath().startsWith(
            hddsVolume.getStorageDir().getAbsolutePath()));
      }
    }
  }

  @Test
  public void testWriteBeforeAndAfterFinalizeWithV3Disabled() throws Exception {
    testWriteBeforeAndAfterFinalize(false, OzoneConsts.SCHEMA_V2);
  }

  @Test
  public void testWriteBeforeAndAfterFinalizeWithV3Enabled() throws Exception{
    testWriteBeforeAndAfterFinalize(true, OzoneConsts.SCHEMA_V3);
  }

  private void testWriteBeforeAndAfterFinalize(boolean schemaV3Enabled,
      String expectedSchemaVersion) throws Exception{
    // start DN and SCM
    startScmServer();
    addHddsVolume();
    // Disable Schema V3
    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED,
        schemaV3Enabled);
    startPreFinalizedDatanode();
    final Pipeline pipeline = getPipeline();

    // Pre-export a container to continuously import and delete.
    final long exportContainerID = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto exportWriteChunk =
        putBlock(exportContainerID, pipeline);
    closeContainer(exportContainerID, pipeline);
    File exportedContainerFile = exportContainer(exportContainerID);
    deleteContainer(exportContainerID, pipeline);

    // Export another container to import while pre-finalized and read
    // finalized.
    final long exportContainerID2 = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto exportWriteChunk2 =
        putBlock(exportContainerID2, pipeline);
    closeContainer(exportContainerID2, pipeline);
    File exportedContainerFile2 = exportContainer(exportContainerID2);
    deleteContainer(exportContainerID2, pipeline);

    // Make sure we can import and read a container pre-finalized.
    importContainer(exportContainerID2, exportedContainerFile2);
    readChunk(exportWriteChunk2, pipeline);
    KeyValueContainer container = (KeyValueContainer)
        dsm.getContainer().getContainerSet().getContainer(exportContainerID2);
    Assert.assertEquals(OzoneConsts.SCHEMA_V2,
        container.getContainerData().getSchemaVersion());

    // Create thread to keep importing containers during the upgrade.
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future<Void> importFuture = executor.submit(() -> {
      // Layout version check should be thread safe.
      while (!dsm.getLayoutVersionManager()
          .isAllowed(HDDSLayoutFeature.DATANODE_SCHEMA_V3)) {
        importContainer(exportContainerID, exportedContainerFile);
        readChunk(exportWriteChunk, pipeline);
        deleteContainer(exportContainerID, pipeline);
      }
      // Make sure we can import after finalizing too.
      importContainer(exportContainerID, exportedContainerFile);
      readChunk(exportWriteChunk, pipeline);

      return null;
    });

    dsm.finalizeUpgrade();
    // Wait the last importContainer to finish
    importFuture.get();
    container = (KeyValueContainer)
        dsm.getContainer().getContainerSet().getContainer(exportContainerID);
    // According to Schema V3 is enabled or not, the imported container after
    // finalization should have the expected schema version.
    Assert.assertEquals(expectedSchemaVersion,
        container.getContainerData().getSchemaVersion());

    // Make sure we can read the container that was imported while
    // pre-finalized after finalizing.
    readChunk(exportWriteChunk2, pipeline);
  }

  @Test
  public void testWriteBeforeAndAfterSchemaV3Enabled() throws Exception {
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
    Assert.assertEquals(OzoneConsts.SCHEMA_V2,
        container.getContainerData().getSchemaVersion());

    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED, true);
    restartDatanode(HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(), true);

    final long containerID2 = addContainer(pipeline);
    putBlock(containerID2, pipeline);
    closeContainer(containerID2, pipeline);
    container = (KeyValueContainer)
        dsm.getContainer().getContainerSet().getContainer(containerID1);
    Assert.assertEquals(OzoneConsts.SCHEMA_V3,
        container.getContainerData().getSchemaVersion());
  }

  @Test
  public void testReadsDuringFinalization() throws Exception {
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
          .isAllowed(HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)) {
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

  @Test
  public void testDbConfigureAndSchemaV3Together() throws Exception {
    conf.setBoolean(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED, false);
    startScmServer();
    addHddsVolume();
    startPreFinalizedDatanode();

    HddsVolume hddsVolume = (HddsVolume) dsm.getContainer().getVolumeSet()
        .getVolumesList().get(0);
    File dbDir = hddsVolume.getDbParentDir();
    Assert.assertTrue(dbDir.getAbsolutePath().startsWith(
        hddsVolume.getStorageDir().getAbsolutePath()));

    addDbVolume();
    restartDatanode(HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion(), true);
    hddsVolume = (HddsVolume) dsm.getContainer().getVolumeSet()
        .getVolumesList().get(0);
    dbDir = hddsVolume.getDbParentDir();
    // HddsVolume should still use the rocksDB under it's volume
    Assert.assertTrue(dbDir.getAbsolutePath().startsWith(
        hddsVolume.getStorageDir().getAbsolutePath()));
    DbVolume dbVolume = (DbVolume) dsm.getContainer().getDbVolumeSet()
        .getVolumesList().get(0);
    Assert.assertEquals(0, dbVolume.getHddsVolumeIDs().size());
  }

  public void checkContainerPathID(long containerID, String expectedID) {
    KeyValueContainerData data =
        (KeyValueContainerData) dsm.getContainer().getContainerSet()
            .getContainer(containerID).getContainerData();
    Assert.assertTrue(data.getChunksPath().contains(expectedID));
    Assert.assertTrue(data.getMetadataPath().contains(expectedID));
  }

  public List<File> getHddsSubdirs(File volume) {
    File[] subdirsArray = getHddsRoot(volume).listFiles(File::isDirectory);
    Assert.assertNotNull(subdirsArray);
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
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempFolder.getRoot().getAbsolutePath());
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString(),
        HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT.layoutVersion());
    layoutStorage.initialize();

    // Build and start the datanode.
    DatanodeDetails dd = ContainerTestUtils.createDatanodeDetails();
    DatanodeStateMachine newDsm = new DatanodeStateMachine(dd,
        conf, null, null, null);
    int actualMlv = newDsm.getLayoutVersionManager().getMetadataLayoutVersion();
    Assert.assertEquals(
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
    dsm = new DatanodeStateMachine(dd,
        conf, null, null, null);
    int mlv = dsm.getLayoutVersionManager().getMetadataLayoutVersion();
    if (exactMatch) {
      Assert.assertEquals(expectedMlv, mlv);
    } else {
      Assert.assertTrue("Expected minimum mlv(" + expectedMlv
          + ") is smaller than mlv(" + mlv + ").", expectedMlv <= mlv);
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

  public String startScmServer() throws Exception {
    String scmID = UUID.randomUUID().toString();
    scmServerImpl = new ScmTestMock(CLUSTER_ID, scmID);
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
            ContainerTestHelper.getTestBlockID(containerID), 100, null);
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

  /**
   * Exports the specified container to a temporary file and returns the file.
   */
  public File exportContainer(long containerId) throws Exception {
    final ContainerReplicationSource replicationSource =
        new OnDemandContainerReplicationSource(
            dsm.getContainer().getController());

    replicationSource.prepare(containerId);

    File destination = tempFolder.newFile();
    try (FileOutputStream fos = new FileOutputStream(destination)) {
      replicationSource.copyData(containerId, fos);
    }
    return destination;
  }

  /**
   * Imports the container found in {@code source} to the datanode with the ID
   * {@code containerID}.
   */
  public void importContainer(long containerID, File source) throws Exception {
    DownloadAndImportReplicator replicator =
        new DownloadAndImportReplicator(dsm.getContainer().getContainerSet(),
            dsm.getContainer().getController(),
            new SimpleContainerDownloader(conf, null),
            new TarContainerPacker());

    File tempFile = tempFolder.newFile();
    Files.copy(source.toPath(), tempFile.toPath(),
        StandardCopyOption.REPLACE_EXISTING);
    replicator.importContainer(containerID, tempFile.toPath());
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
    Assert.assertEquals(expectedResult, response.getResult());
  }

  /// VOLUME OPERATIONS ///

  /**
   * Append a datanode volume to the existing volumes in the configuration.
   * @return The root directory for the new volume.
   */
  public File addHddsVolume() throws Exception {
    File vol = tempFolder.newFolder(UUID.randomUUID().toString());
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
    File vol = tempFolder.newFolder(UUID.randomUUID().toString());
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

  /**
   * Renames the specified volume directory so it will appear as failed to
   * the datanode.
   */
  public void failVolume(File volume) {
    File failedVolume = getFailedVolume(volume);
    Assert.assertTrue(volume.renameTo(failedVolume));
  }

  /**
   * Convert the specified volume from its failed name back to its original
   * name. The File passed should be the original volume path, not the one it
   * was renamed to to fail it.
   */
  public void restoreVolume(File volume) {
    File failedVolume = getFailedVolume(volume);
    Assert.assertTrue(failedVolume.renameTo(volume));
  }

  /**
   * @return The file name that will be used to rename a volume to fail it.
   */
  public File getFailedVolume(File volume) {
    return new File(volume.getParent(), volume.getName() + "-failed");
  }

  /**
   * Checks whether the datanode thinks the volume has failed.
   * This could be outdated information if the volume was restored already
   * and the datanode has not been restarted since then.
   */
  public boolean dnThinksVolumeFailed(File volume) {
    return dsm.getContainer().getVolumeSet().getFailedVolumesList().stream()
        .anyMatch(v ->
            getHddsRoot(v.getStorageDir()).equals(getHddsRoot(volume)));
  }
}
