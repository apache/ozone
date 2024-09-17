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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.ScmTestMock;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.replication.ContainerImporter;
import org.apache.hadoop.ozone.container.replication.ContainerReplicationSource;
import org.apache.hadoop.ozone.container.replication.OnDemandContainerReplicationSource;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hadoop.ozone.container.replication.CopyContainerCompression.NO_COMPRESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests upgrading a single datanode from pre-SCM HA volume format that used
 * SCM ID to the post-SCM HA volume format using cluster ID. If SCM HA was
 * already being used before the upgrade, there should be no changes.
 */
public class TestDatanodeUpgradeToScmHA {
  @TempDir
  private Path tempFolder;

  private DatanodeStateMachine dsm;
  private ContainerDispatcher dispatcher;
  private OzoneConfiguration conf;
  private static final String CLUSTER_ID = "clusterID";
  private boolean scmHAAlreadyEnabled;

  private RPC.Server scmRpcServer;
  private InetSocketAddress address;
  private ScmTestMock scmServerImpl;

  private void setScmHAEnabled(boolean enableSCMHA)
      throws Exception {
    this.scmHAAlreadyEnabled = enableSCMHA;
    conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, scmHAAlreadyEnabled);
    setup();
  }

  private void setup() throws Exception {
    address = SCMTestUtils.getReuseableAddress();
    conf.setSocketAddr(ScmConfigKeys.OZONE_SCM_NAMES, address);
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

  @ParameterizedTest(name = "{index}: scmHAAlreadyEnabled={0}")
  @ValueSource(booleans = {true, false})
  public void testReadsDuringFinalization(boolean enableSCMHA)
      throws Exception {
    setScmHAEnabled(enableSCMHA);
    // start DN and SCM
    startScmServer();
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    dispatcher = dsm.getContainer().getDispatcher();
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
          .isAllowed(HDDSLayoutFeature.SCM_HA)) {
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

  @ParameterizedTest(name = "{index}: scmHAAlreadyEnabled={0}")
  @ValueSource(booleans = {true, false})
  public void testImportContainer(boolean enableSCMHA) throws Exception {
    setScmHAEnabled(enableSCMHA);
    // start DN and SCM
    startScmServer();
    UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    dispatcher = dsm.getContainer().getDispatcher();
    final Pipeline pipeline = MockPipeline.createPipeline(
        Collections.singletonList(dsm.getDatanodeDetails()));

    // Pre-export a container to continuously import and delete.
    final long exportContainerID = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    ContainerProtos.WriteChunkRequestProto exportWriteChunk =
        UpgradeTestHelper.putBlock(dispatcher, exportContainerID, pipeline);
    UpgradeTestHelper.closeContainer(dispatcher, exportContainerID, pipeline);
    File exportedContainerFile = exportContainer(exportContainerID);
    UpgradeTestHelper.deleteContainer(dispatcher, exportContainerID, pipeline);

    // Export another container to import while pre-finalized and read
    // finalized.
    final long exportContainerID2 = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    ContainerProtos.WriteChunkRequestProto exportWriteChunk2 =
        UpgradeTestHelper.putBlock(dispatcher, exportContainerID2, pipeline);
    UpgradeTestHelper.closeContainer(dispatcher, exportContainerID2, pipeline);
    File exportedContainerFile2 = exportContainer(exportContainerID2);
    UpgradeTestHelper.deleteContainer(dispatcher, exportContainerID2, pipeline);

    // Make sure we can import and read a container pre-finalized.
    importContainer(exportContainerID2, exportedContainerFile2);
    UpgradeTestHelper.readChunk(dispatcher, exportWriteChunk2, pipeline);

    // Now SCM and enough other DNs finalize to enable SCM HA. This DN is
    // restarted with SCM HA config and gets a different SCM ID.
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    changeScmID();

    dsm = UpgradeTestHelper.restartDatanode(conf, dsm, true, tempFolder, address,
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion(), true);
    dispatcher = dsm.getContainer().getDispatcher();

    // Make sure the existing container can be read.
    UpgradeTestHelper.readChunk(dispatcher, exportWriteChunk2, pipeline);

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
        UpgradeTestHelper.readChunk(dispatcher, exportWriteChunk, pipeline);
        UpgradeTestHelper.deleteContainer(dispatcher, exportContainerID, pipeline);
      }
      // Make sure we can import after finalizing too.
      importContainer(exportContainerID, exportedContainerFile);
      UpgradeTestHelper.readChunk(dispatcher, exportWriteChunk, pipeline);
      return null;
    });

    dsm.finalizeUpgrade();
    // If there was a failure importing during the upgrade, the exception will
    // be thrown here.
    importFuture.get();

    // Make sure we can read the container that was imported while
    // pre-finalized after finalizing.
    UpgradeTestHelper.readChunk(dispatcher, exportWriteChunk2, pipeline);
  }

  @ParameterizedTest(name = "{index}: scmHAAlreadyEnabled={0}")
  @ValueSource(booleans = {true, false})
  public void testFailedVolumeDuringFinalization(boolean enableSCMHA)
      throws Exception {
    setScmHAEnabled(enableSCMHA);
    /// SETUP ///

    startScmServer();
    String originalScmID = scmServerImpl.getScmId();
    File volume = UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    dispatcher = dsm.getContainer().getDispatcher();
    final Pipeline pipeline = MockPipeline.createPipeline(
        Collections.singletonList(dsm.getDatanodeDetails()));

    /// PRE-FINALIZED: Write and Read from formatted volume ///

    assertEquals(1,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    assertEquals(0,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());

    // Add container with data, make sure it can be read and written.
    final long containerID = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk =
        UpgradeTestHelper.putBlock(dispatcher, containerID, pipeline);
    UpgradeTestHelper.readChunk(dispatcher, writeChunk, pipeline);

    checkPreFinalizedVolumePathID(volume, originalScmID, CLUSTER_ID);
    checkContainerPathID(containerID, originalScmID, CLUSTER_ID);

    // FINALIZE: With failed volume ///

    failVolume(volume);
    // Since volume is failed, container should be marked unhealthy.
    // Finalization should proceed anyways.
    UpgradeTestHelper.closeContainer(dispatcher, containerID, pipeline,
        ContainerProtos.Result.CONTAINER_FILES_CREATE_ERROR);
    State containerState = dsm.getContainer().getContainerSet()
        .getContainer(containerID).getContainerState();
    assertEquals(State.UNHEALTHY, containerState);
    dsm.finalizeUpgrade();
    LambdaTestUtils.await(2000, 500,
        () -> dsm.getLayoutVersionManager()
            .isAllowed(HDDSLayoutFeature.SCM_HA));

    /// FINALIZED: Volume marked failed but gets restored on disk ///

    // Check that volume is marked failed during finalization.
    assertEquals(0,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    assertEquals(1,
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
    UpgradeTestHelper.readChunk(dispatcher, writeChunk, pipeline);

    /// FINALIZED: Restart datanode to upgrade the failed volume ///

    dsm = UpgradeTestHelper.restartDatanode(conf, dsm, true, tempFolder, address,
        HDDSLayoutFeature.SCM_HA.layoutVersion(), false);
    dispatcher = dsm.getContainer().getDispatcher();

    assertEquals(1,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    assertEquals(0,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());

    checkFinalizedVolumePathID(volume, originalScmID, CLUSTER_ID);
    checkContainerPathID(containerID, originalScmID, CLUSTER_ID);

    // Read container from before upgrade. The upgrade required it to be closed.
    UpgradeTestHelper.readChunk(dispatcher, writeChunk, pipeline);
    // Write and read container after upgrade.
    long newContainerID = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    ContainerProtos.WriteChunkRequestProto newWriteChunk =
        UpgradeTestHelper.putBlock(dispatcher, newContainerID, pipeline);
    UpgradeTestHelper.readChunk(dispatcher, newWriteChunk, pipeline);
    // The new container should use cluster ID in its path.
    // The volume it is placed on is up to the implementation.
    checkContainerPathID(newContainerID, CLUSTER_ID);
  }

  @ParameterizedTest(name = "{index}: scmHAAlreadyEnabled={0}")
  @ValueSource(booleans = {true, false})
  public void testFormattingNewVolumes(boolean enableSCMHA) throws Exception {
    setScmHAEnabled(enableSCMHA);
    /// SETUP ///

    startScmServer();
    String originalScmID = scmServerImpl.getScmId();
    File preFinVolume1 = UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    dsm = UpgradeTestHelper.startPreFinalizedDatanode(conf, tempFolder, dsm, address,
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    dispatcher = dsm.getContainer().getDispatcher();
    final Pipeline pipeline = MockPipeline.createPipeline(
        Collections.singletonList(dsm.getDatanodeDetails()));

    /// PRE-FINALIZED: Write and Read from formatted volume ///

    assertEquals(1,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    assertEquals(0,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());

    // Add container with data, make sure it can be read and written.
    final long containerID = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk =
        UpgradeTestHelper.putBlock(dispatcher, containerID, pipeline);
    UpgradeTestHelper.readChunk(dispatcher, writeChunk, pipeline);

    checkPreFinalizedVolumePathID(preFinVolume1, originalScmID, CLUSTER_ID);
    checkContainerPathID(containerID, originalScmID, CLUSTER_ID);

    /// PRE-FINALIZED: Restart with SCM HA enabled and new SCM ID ///

    // Now SCM and enough other DNs finalize to enable SCM HA. This DN is
    // restarted with SCM HA config and gets a different SCM ID.
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    changeScmID();
    // A new volume is added that must be formatted.
    File preFinVolume2 = UpgradeTestHelper.addHddsVolume(conf, tempFolder);

    dsm = UpgradeTestHelper.restartDatanode(conf, dsm, true, tempFolder, address,
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion(), true);
    dispatcher = dsm.getContainer().getDispatcher();

    assertEquals(2,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    assertEquals(0,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());

    // Because DN mlv would be behind SCM mlv, only reads are allowed.
    UpgradeTestHelper.readChunk(dispatcher, writeChunk, pipeline);

    // On restart, there should have been no changes to the paths already used.
    checkPreFinalizedVolumePathID(preFinVolume1, originalScmID, CLUSTER_ID);
    checkContainerPathID(containerID, originalScmID, CLUSTER_ID);
    // No new containers can be created on this volume since SCM MLV is ahead
    // of DN MLV at this point.
    // cluster ID should always be used for the new volume since SCM HA is now
    // enabled.
    checkVolumePathID(preFinVolume2, CLUSTER_ID);

    /// FINALIZE ///

    UpgradeTestHelper.closeContainer(dispatcher, containerID, pipeline);
    dsm.finalizeUpgrade();
    LambdaTestUtils.await(2000, 500,
        () -> dsm.getLayoutVersionManager()
            .isAllowed(HDDSLayoutFeature.SCM_HA));

    /// FINALIZED: Add a new volume and check its formatting ///

    // Add a new volume that should be formatted with cluster ID only, since
    // DN has finalized.
    File finVolume = UpgradeTestHelper.addHddsVolume(conf, tempFolder);
    // Yet another SCM ID is received this time, but it should not matter.
    changeScmID();

    dsm = UpgradeTestHelper.restartDatanode(conf, dsm, true, tempFolder, address,
        HDDSLayoutFeature.SCM_HA.layoutVersion(), false);
    dispatcher = dsm.getContainer().getDispatcher();

    assertEquals(3,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    assertEquals(0,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());

    checkFinalizedVolumePathID(preFinVolume1, originalScmID, CLUSTER_ID);
    checkVolumePathID(preFinVolume2, CLUSTER_ID);
    checkContainerPathID(containerID, originalScmID, CLUSTER_ID);
    // New volume should have been formatted with cluster ID only, since the
    // datanode is finalized.
    checkVolumePathID(finVolume, CLUSTER_ID);

    /// FINALIZED: Read old data and write + read new data ///

    // Read container from before upgrade. The upgrade required it to be closed.
    UpgradeTestHelper.readChunk(dispatcher, writeChunk, pipeline);
    // Write and read container after upgrade.
    long newContainerID = UpgradeTestHelper.addContainer(dispatcher, pipeline);
    ContainerProtos.WriteChunkRequestProto newWriteChunk =
        UpgradeTestHelper.putBlock(dispatcher, newContainerID, pipeline);
    UpgradeTestHelper.readChunk(dispatcher, newWriteChunk, pipeline);
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
    assertThat(data.getChunksPath()).contains(expectedID);
    assertThat(data.getMetadataPath()).contains(expectedID);
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
      assertEquals(2, subdirs.size());

      File scmIDDir = new File(hddsRoot, scmID);
      assertThat(subdirs).contains(scmIDDir);

      File clusterIDDir = new File(hddsRoot, CLUSTER_ID);
      assertThat(subdirs).contains(clusterIDDir);
      assertTrue(Files.isSymbolicLink(clusterIDDir.toPath()));
      Path symlinkTarget = Files.readSymbolicLink(clusterIDDir.toPath());
      assertEquals(scmID, symlinkTarget.toString());
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
    assertEquals(1, subdirs.size());
    File idDir = new File(hddsRoot, expectedID);
    assertThat(subdirs).contains(idDir);
  }

  public List<File> getHddsSubdirs(File volume) {
    File[] subdirsArray = getHddsRoot(volume).listFiles(File::isDirectory);
    assertNotNull(subdirsArray);
    return Arrays.asList(subdirsArray);
  }

  public File getHddsRoot(File volume) {
    return new File(HddsVolumeUtil.getHddsRoot(volume.getAbsolutePath()));
  }

  /// CLUSTER OPERATIONS ///

  private void startScmServer() throws Exception {
    scmServerImpl = new ScmTestMock(CLUSTER_ID);
    scmRpcServer = SCMTestUtils.startScmRpcServer(conf,
        scmServerImpl, address, 10);
  }

  /**
   * Updates the SCM ID on the SCM server. Datanode will not be aware of this
   * until {@link UpgradeTestHelper#callVersionEndpointTask} is called.
   * @return the new scm ID.
   */
  private String changeScmID() {
    String scmID = UUID.randomUUID().toString();
    scmServerImpl.setScmId(scmID);
    return scmID;
  }

  /// CONTAINER OPERATIONS ///

  /**
   * Exports the specified container to a temporary file and returns the file.
   */
  private File exportContainer(long containerId) throws Exception {
    final ContainerReplicationSource replicationSource =
        new OnDemandContainerReplicationSource(
            dsm.getContainer().getController());

    replicationSource.prepare(containerId);

    File destination =
        Files.createFile(tempFolder.resolve("destFile" + containerId)).toFile();
    try (FileOutputStream fos = new FileOutputStream(destination)) {
      replicationSource.copyData(containerId, fos, NO_COMPRESSION);
    }
    return destination;
  }

  /**
   * Imports the container found in {@code source} to the datanode with the ID
   * {@code containerID}.
   */
  private void importContainer(long containerID, File source) throws Exception {
    ContainerImporter replicator =
        new ContainerImporter(dsm.getConf(),
            dsm.getContainer().getContainerSet(),
            dsm.getContainer().getController(),
            dsm.getContainer().getVolumeSet());

    File tempFile = Files.createFile(
            tempFolder.resolve(ContainerUtils.getContainerTarName(containerID)))
        .toFile();
    Files.copy(source.toPath(), tempFile.toPath(),
        StandardCopyOption.REPLACE_EXISTING);
    replicator.importContainer(containerID, tempFile.toPath(), null,
        NO_COMPRESSION);
  }

  /// VOLUME OPERATIONS ///

  /**
   * Renames the specified volume directory so it will appear as failed to
   * the datanode.
   */
  public void failVolume(File volume) {
    File failedVolume = getFailedVolume(volume);
    assertTrue(volume.renameTo(failedVolume));
  }

  /**
   * Convert the specified volume from its failed name back to its original
   * name. The File passed should be the original volume path, not the one it
   * was renamed to to fail it.
   */
  public void restoreVolume(File volume) {
    File failedVolume = getFailedVolume(volume);
    assertTrue(failedVolume.renameTo(volume));
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
