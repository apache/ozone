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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.ScmTestMock;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.states.endpoint.VersionEndpointTask;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.replication.ContainerImporter;
import org.apache.hadoop.ozone.container.replication.ContainerReplicationSource;
import org.apache.hadoop.ozone.container.replication.OnDemandContainerReplicationSource;
import org.apache.ozone.test.LambdaTestUtils;
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
import java.nio.file.Path;
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

import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;

/**
 * Tests upgrading a single datanode from pre-SCM HA volume format that used
 * SCM ID to the post-SCM HA volume format using cluster ID. If SCM HA was
 * already being used before the upgrade, there should be no changes.
 */
@RunWith(Parameterized.class)
public class TestDatanodeUpgradeToScmHA {
  @Rule
  public TemporaryFolder tempFolder;

  private DatanodeStateMachine dsm;
  private final OzoneConfiguration conf;
  private static final String CLUSTER_ID = "clusterID";
  private final boolean scmHAAlreadyEnabled;

  private RPC.Server scmRpcServer;
  private InetSocketAddress address;
  private ScmTestMock scmServerImpl;

  private Random random;

  @Parameterized.Parameters(name = "{index}: scmHAAlreadyEnabled={0}")
  public static Collection<Object[]> getSchemaFiles() {
    Collection<Object[]> parameters = new ArrayList<>();
    parameters.add(new Boolean[]{false});
    parameters.add(new Boolean[]{true});
    return parameters;
  }

  public TestDatanodeUpgradeToScmHA(boolean scmHAAlreadyEnabled) {
    this.scmHAAlreadyEnabled = scmHAAlreadyEnabled;
    conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, scmHAAlreadyEnabled);
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
  public void testReadsDuringFinalization() throws Exception {
    // start DN and SCM
    startScmServer();
    addVolume();
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
          .isAllowed(HDDSLayoutFeature.SCM_HA)) {
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
  public void testImportContainer() throws Exception {
    // start DN and SCM
    startScmServer();
    addVolume();
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

    // Now SCM and enough other DNs finalize to enable SCM HA. This DN is
    // restarted with SCM HA config and gets a different SCM ID.
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    changeScmID();

    restartDatanode(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion(), true);

    // Make sure the existing container can be read.
    readChunk(exportWriteChunk2, pipeline);

    // Create thread to keep importing containers during the upgrade.
    // Since the datanode's MLV is behind SCM's, container creation is not
    // allowed. We will keep importing and deleting the same container since
    // we cannot create new ones to import here.
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future<Void> importFuture = executor.submit(() -> {
      // Layout version check should be thread safe.
      while (!dsm.getLayoutVersionManager()
          .isAllowed(HDDSLayoutFeature.SCM_HA)) {
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
    // If there was a failure importing during the upgrade, the exception will
    // be thrown here.
    importFuture.get();

    // Make sure we can read the container that was imported while
    // pre-finalized after finalizing.
    readChunk(exportWriteChunk2, pipeline);
  }

  @Test
  public void testFailedVolumeDuringFinalization() throws Exception {
    /// SETUP ///

    String originalScmID = startScmServer();
    File volume = addVolume();
    startPreFinalizedDatanode();
    final Pipeline pipeline = getPipeline();

    /// PRE-FINALIZED: Write and Read from formatted volume ///

    Assert.assertEquals(1,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    Assert.assertEquals(0,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());

    // Add container with data, make sure it can be read and written.
    final long containerID = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk = putBlock(containerID,
        pipeline);
    readChunk(writeChunk, pipeline);

    checkPreFinalizedVolumePathID(volume, originalScmID, CLUSTER_ID);
    checkContainerPathID(containerID, originalScmID, CLUSTER_ID);

    // FINALIZE: With failed volume ///

    failVolume(volume);
    // Since volume is failed, container should be marked unhealthy.
    // Finalization should proceed anyways.
    closeContainer(containerID, pipeline,
        ContainerProtos.Result.CONTAINER_FILES_CREATE_ERROR);
    State containerState = dsm.getContainer().getContainerSet()
        .getContainer(containerID).getContainerState();
    Assert.assertEquals(State.UNHEALTHY, containerState);
    dsm.finalizeUpgrade();
    LambdaTestUtils.await(2000, 500,
        () -> dsm.getLayoutVersionManager()
            .isAllowed(HDDSLayoutFeature.SCM_HA));

    /// FINALIZED: Volume marked failed but gets restored on disk ///

    // Check that volume is marked failed during finalization.
    Assert.assertEquals(0,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    Assert.assertEquals(1,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());

    // Since the volume was out during the upgrade, it should maintain its
    // original format.
    checkPreFinalizedVolumePathID(volume, originalScmID, CLUSTER_ID);
    checkContainerPathID(containerID, originalScmID, CLUSTER_ID);

    // Now that we are done finalizing, restore the volume.
    restoreVolume(volume);
    // After restoring the failed volume, its containers are readable again.
    // However, since it is marked as failed no containers can be created or
    // imported to it.
    // This should log a warning about reading from an unhealthy container
    // but otherwise proceed successfully.
    readChunk(writeChunk, pipeline);

    /// FINALIZED: Restart datanode to upgrade the failed volume ///

    restartDatanode(HDDSLayoutFeature.SCM_HA.layoutVersion(), false);

    Assert.assertEquals(1,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    Assert.assertEquals(0,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());

    checkFinalizedVolumePathID(volume, originalScmID, CLUSTER_ID);
    checkContainerPathID(containerID, originalScmID, CLUSTER_ID);

    // Read container from before upgrade. The upgrade required it to be closed.
    readChunk(writeChunk, pipeline);
    // Write and read container after upgrade.
    long newContainerID = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto newWriteChunk =
        putBlock(newContainerID, pipeline);
    readChunk(newWriteChunk, pipeline);
    // The new container should use cluster ID in its path.
    // The volume it is placed on is up to the implementation.
    checkContainerPathID(newContainerID, CLUSTER_ID);
  }

  @Test
  public void testFormattingNewVolumes() throws Exception {
    /// SETUP ///

    String originalScmID = startScmServer();
    File preFinVolume1 = addVolume();
    startPreFinalizedDatanode();
    final Pipeline pipeline = getPipeline();

    /// PRE-FINALIZED: Write and Read from formatted volume ///

    Assert.assertEquals(1,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    Assert.assertEquals(0,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());

    // Add container with data, make sure it can be read and written.
    final long containerID = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk = putBlock(containerID,
        pipeline);
    readChunk(writeChunk, pipeline);

    checkPreFinalizedVolumePathID(preFinVolume1, originalScmID, CLUSTER_ID);
    checkContainerPathID(containerID, originalScmID, CLUSTER_ID);

    /// PRE-FINALIZED: Restart with SCM HA enabled and new SCM ID ///

    // Now SCM and enough other DNs finalize to enable SCM HA. This DN is
    // restarted with SCM HA config and gets a different SCM ID.
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    changeScmID();
    // A new volume is added that must be formatted.
    File preFinVolume2 = addVolume();

    restartDatanode(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion(), true);

    Assert.assertEquals(2,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    Assert.assertEquals(0,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());

    // Because DN mlv would be behind SCM mlv, only reads are allowed.
    readChunk(writeChunk, pipeline);

    // On restart, there should have been no changes to the paths already used.
    checkPreFinalizedVolumePathID(preFinVolume1, originalScmID, CLUSTER_ID);
    checkContainerPathID(containerID, originalScmID, CLUSTER_ID);
    // No new containers can be created on this volume since SCM MLV is ahead
    // of DN MLV at this point.
    // cluster ID should always be used for the new volume since SCM HA is now
    // enabled.
    checkVolumePathID(preFinVolume2, CLUSTER_ID);

    /// FINALIZE ///

    closeContainer(containerID, pipeline);
    dsm.finalizeUpgrade();
    LambdaTestUtils.await(2000, 500,
        () -> dsm.getLayoutVersionManager()
            .isAllowed(HDDSLayoutFeature.SCM_HA));

    /// FINALIZED: Add a new volume and check its formatting ///

    // Add a new volume that should be formatted with cluster ID only, since
    // DN has finalized.
    File finVolume = addVolume();
    // Yet another SCM ID is received this time, but it should not matter.
    changeScmID();

    restartDatanode(HDDSLayoutFeature.SCM_HA.layoutVersion(), false);

    Assert.assertEquals(3,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    Assert.assertEquals(0,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());

    checkFinalizedVolumePathID(preFinVolume1, originalScmID, CLUSTER_ID);
    checkVolumePathID(preFinVolume2, CLUSTER_ID);
    checkContainerPathID(containerID, originalScmID, CLUSTER_ID);
    // New volume should have been formatted with cluster ID only, since the
    // datanode is finalized.
    checkVolumePathID(finVolume, CLUSTER_ID);

    /// FINALIZED: Read old data and write + read new data ///

    // Read container from before upgrade. The upgrade required it to be closed.
    readChunk(writeChunk, pipeline);
    // Write and read container after upgrade.
    long newContainerID = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto newWriteChunk =
        putBlock(newContainerID, pipeline);
    readChunk(newWriteChunk, pipeline);
    // The new container should use cluster ID in its path.
    // The volume it is placed on is up to the implementation.
    checkContainerPathID(newContainerID, CLUSTER_ID);
  }

  /// CHECKS FOR TESTING ///

  public void checkContainerPathID(long containerID, String scmID,
      String clusterID) {
    if (scmHAAlreadyEnabled) {
      checkContainerPathID(containerID, clusterID);
    } else {
      checkContainerPathID(containerID, scmID);
    }
  }

  public void checkContainerPathID(long containerID, String expectedID) {
    KeyValueContainerData data =
        (KeyValueContainerData) dsm.getContainer().getContainerSet()
            .getContainer(containerID).getContainerData();
    Assert.assertTrue(data.getChunksPath().contains(expectedID));
    Assert.assertTrue(data.getMetadataPath().contains(expectedID));
  }

  public void checkFinalizedVolumePathID(File volume, String scmID,
      String clusterID) throws Exception {

    if (scmHAAlreadyEnabled)  {
      checkVolumePathID(volume, clusterID);
    } else {
      List<File> subdirs = getHddsSubdirs(volume);
      File hddsRoot = getHddsRoot(volume);

      // Volume should have SCM ID and cluster ID directory, where cluster ID
      // is a symlink to SCM ID.
      Assert.assertEquals(2, subdirs.size());

      File scmIDDir = new File(hddsRoot, scmID);
      Assert.assertTrue(subdirs.contains(scmIDDir));

      File clusterIDDir = new File(hddsRoot, CLUSTER_ID);
      Assert.assertTrue(subdirs.contains(clusterIDDir));
      Assert.assertTrue(Files.isSymbolicLink(clusterIDDir.toPath()));
      Path symlinkTarget = Files.readSymbolicLink(clusterIDDir.toPath());
      Assert.assertEquals(scmID, symlinkTarget.toString());
    }
  }

  public void checkPreFinalizedVolumePathID(File volume, String scmID,
      String clusterID) {

    if (scmHAAlreadyEnabled) {
      checkVolumePathID(volume, clusterID);
    } else {
      checkVolumePathID(volume, scmID);
    }

  }

  public void checkVolumePathID(File volume, String expectedID) {
    List<File> subdirs;
    File hddsRoot;
    if (dnThinksVolumeFailed(volume)) {
      // If the volume is failed, read from the failed location it was
      // moved to.
      subdirs = getHddsSubdirs(getFailedVolume(volume));
      hddsRoot = getHddsRoot(getFailedVolume(volume));
    } else {
      subdirs = getHddsSubdirs(volume);
      hddsRoot = getHddsRoot(volume);
    }

    // Volume should only have the specified ID directory.
    Assert.assertEquals(1, subdirs.size());
    File idDir = new File(hddsRoot, expectedID);
    Assert.assertTrue(subdirs.contains(idDir));
  }

  public List<File> getHddsSubdirs(File volume) {
    File[] subdirsArray = getHddsRoot(volume).listFiles(File::isDirectory);
    Assert.assertNotNull(subdirsArray);
    return Arrays.asList(subdirsArray);
  }

  public File getHddsRoot(File volume) {
    return new File(HddsVolumeUtil.getHddsRoot(volume.getAbsolutePath()));
  }

  /// CLUSTER OPERATIONS ///

  /**
   * Starts the datanode with the first layout version, and calls the version
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
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    layoutStorage.initialize();

    // Build and start the datanode.
    DatanodeDetails dd = ContainerTestUtils.createDatanodeDetails();
    DatanodeStateMachine newDsm = new DatanodeStateMachine(dd, conf);
    int actualMlv = newDsm.getLayoutVersionManager().getMetadataLayoutVersion();
    Assert.assertEquals(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion(),
        actualMlv);
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

  /**
   * Updates the SCM ID on the SCM server. Datanode will not be aware of this
   * until {@link this#callVersionEndpointTask} is called.
   * @return the new scm ID.
   */
  public String changeScmID() {
    String scmID = UUID.randomUUID().toString();
    scmServerImpl.setScmId(scmID);
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
      replicationSource.copyData(containerId, fos, NO_COMPRESSION);
    }
    return destination;
  }

  /**
   * Imports the container found in {@code source} to the datanode with the ID
   * {@code containerID}.
   */
  public void importContainer(long containerID, File source) throws Exception {
    ContainerImporter replicator =
        new ContainerImporter(dsm.getConf(),
            dsm.getContainer().getContainerSet(),
            dsm.getContainer().getController(),
            dsm.getContainer().getVolumeSet());

    File tempFile = tempFolder.newFile(
        ContainerUtils.getContainerTarName(containerID));
    Files.copy(source.toPath(), tempFile.toPath(),
        StandardCopyOption.REPLACE_EXISTING);
    replicator.importContainer(containerID, tempFile.toPath(), null,
        NO_COMPRESSION);
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
  public File addVolume() throws Exception {
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
