package org.apache.hadoop.ozone.container.upgrade;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
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
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.states.endpoint.VersionEndpointTask;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Tests upgrading a single datanode from pre-SCM HA volume format that used
 * SCM ID to the post-SCM HA volume format using cluster ID.
 */
public class TestDatanodeUpgradeToScmHA {
  @Rule
  public TemporaryFolder tempFolder;

  // The daemon for the datanode state machine will not be started in this test.
  // This greatly speeds up execution time, and the only  thing we lose is
  // any actions that would need to be run in pre-finalize, but we do not
  // have any such actions for SCM HA.
  private DatanodeStateMachine dsm;
  private OzoneConfiguration conf;
  private RPC.Server scmRpcServer;

  private static final String CLUSTER_ID = "clusterID";

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    tempFolder = new TemporaryFolder();
    tempFolder.create();
  }

  @After
  public void teardown() {
    if (scmRpcServer != null) {
      scmRpcServer.stop();
    }
  }

  // TODO:
  //  - Test container import
  //  - Test with SCM HA already enabled
  @Test
  public void testChaoticUpgrade() throws Exception {
    /// SETUP ///

    File volume = addVolume();
    final String originalScmID =
        startDnWithScm(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    final Pipeline pipeline = getPipeline();

    /// PRE-FINALIZED: Write and Read ///

    // Add container with data, make sure it can be read and written.
    final long containerID = 123;
    addContainer(containerID, pipeline);
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        getWriteChunk(containerID, pipeline);
    putBlock(writeChunkRequest, pipeline);
    readChunk(writeChunkRequest.getWriteChunk(), pipeline);
    // In pre-finalize, the volume and container should only be formatted
    // with SCM ID.
    checkVolumePathID(volume, originalScmID);
    checkContainerPathID(containerID, originalScmID);

    /// PRE-FINALIZED: SCM finalizes and SCM HA is enabled ///

    // Now simulate SCMs finishing finalization and SCM HA being enabled.
    // Even though the DN is pre-finalized, SCM may have finalized itself and
    // 3 other datanodes, indicating it has finished finalization while this
    // datanode lags. As a result this datanode is restarted with SCM HA
    // on while pre-finalized, although it should not do anything with this
    // information.
    // DN restarts but gets an ID from a different SCM.
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    restartDnWithNewScm(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    // Because DN mlv would be behind SCM mlv, only reads are allowed.
    readChunk(writeChunkRequest.getWriteChunk(), pipeline);
    // On restart, there should have been no changes to the paths used.
    checkVolumePathID(volume, originalScmID);
    checkContainerPathID(containerID, originalScmID);

    /// PRE-FINALIZED: Do finalization while the one volume is failed ///

    // Close container in preparation for upgrade. SCM would normally handle
    // this, but there is no real SCM in this unit test.
    dsm.getContainer().getContainerSet().getContainer(containerID).close();
    // Move the volume to fail it.
    File failedVolume = new File(volume.getParent(), "fail");
    Assert.assertTrue(volume.renameTo(failedVolume));
    dsm.finalizeUpgrade();
    LambdaTestUtils.await(2000, 500,
        () -> dsm.getLayoutVersionManager()
            .isAllowed(HDDSLayoutFeature.SCM_HA));

    /// FINALIZED: Volume failed, but container can still be read ///

    // Check that volume is marked failed during finalization.
    Assert.assertEquals(1,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());
    Assert.assertEquals(0,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    // Now that we are done finalizing, restore the volume.
    Assert.assertTrue(failedVolume.renameTo(volume));
    // After restoring the failed volume, its containers are readable again.
    // No new containers can be created on it due to its failed status.
    readChunk(writeChunkRequest.getWriteChunk(), pipeline);
    // Since the volume was out during the upgrade, it should maintain its
    // original format.
    checkVolumePathID(volume, originalScmID);
    checkContainerPathID(containerID, originalScmID);

    /// FINALIZED: Add a new volume and check its formatting ///

    // Add a new volume that should be formatted with cluster ID only, since
    // DN has finalized
    File newVolume = addVolume();
    // Restart the datanode. It should upgrade the volume that was down
    // during finalization.
    // Yet another SCM ID is received this time, but it should not matter.
    restartDnWithNewScm(HDDSLayoutFeature.SCM_HA.layoutVersion());
    // New and old volume should be fully functional.
    Assert.assertEquals(2,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    Assert.assertEquals(0,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());
    // New volume should have been formatted with cluster ID only, since the
    // datanode is finalized.
    checkVolumePathID(newVolume, CLUSTER_ID);
    // After upgrade, the old volume should have a cluster ID symlink.
    checkVolumePathID(volume, originalScmID, CLUSTER_ID);
    checkContainerPathID(containerID, originalScmID);

    /// FINALIZED: Read old data and write + read new data ///

    // Read container from before upgrade. The upgrade required it to be closed.
    readChunk(writeChunkRequest.getWriteChunk(), pipeline);
    // Write and read container after upgrade.
    long newContainerID = containerID + 1;
    addContainer(newContainerID, pipeline);
    ContainerProtos.ContainerCommandRequestProto newWriteChunkRequest =
        getWriteChunk(newContainerID, pipeline);
    putBlock(newWriteChunkRequest, pipeline);
    readChunk(newWriteChunkRequest.getWriteChunk(), pipeline);
    // The new container should use cluster ID in its path.
    // The volume it is placed on is up to the implementation.
    checkContainerPathID(newContainerID, CLUSTER_ID);
  }

  public void checkContainerPathID(long containerID, String expectedID) {
    KeyValueContainerData data =
        (KeyValueContainerData) dsm.getContainer().getContainerSet()
            .getContainer(containerID).getContainerData();
    Assert.assertTrue(data.getChunksPath().contains(expectedID));
    Assert.assertTrue(data.getMetadataPath().contains(expectedID));
  }

  public void checkVolumePathID(File volume, String expectedID) {
    File hddsRoot =
        new File(HddsVolumeUtil.getHddsRoot(volume.getAbsolutePath()));
    File[] subdirsArray = hddsRoot.listFiles(File::isDirectory);
    Assert.assertNotNull(subdirsArray);
    List<File> subdirs = Arrays.asList(subdirsArray);

    // Volume should only have the specified ID directory.
    Assert.assertEquals(1, subdirs.size());
    File idDir = new File(hddsRoot, expectedID);
    Assert.assertTrue(subdirs.contains(idDir));
  }

  public void checkVolumePathID(File volume, String scmID, String clusterID)
      throws Exception {
    File hddsRoot =
        new File(HddsVolumeUtil.getHddsRoot(volume.getAbsolutePath()));
    File[] subdirsArray = hddsRoot.listFiles(File::isDirectory);
    Assert.assertNotNull(subdirsArray);
    List<File> subdirs = Arrays.asList(subdirsArray);

    // Volume should have SCM ID and cluster ID directory, where cluster ID
    // is a symlink to SCM ID.
    Assert.assertEquals(2, subdirs.size());

    File scmIDDir = new File(hddsRoot, scmID);
    Assert.assertTrue(subdirs.contains(scmIDDir));

    File clusterIDDir = new File(hddsRoot, clusterID);
    Assert.assertTrue(subdirs.contains(clusterIDDir));
    Assert.assertTrue(Files.isSymbolicLink(clusterIDDir.toPath()));
    Path symlinkTarget = Files.readSymbolicLink(clusterIDDir.toPath());
    Assert.assertEquals(scmID, symlinkTarget.toString());
  }

  public String startDnWithScm(int mlv) throws Exception {
    InetSocketAddress address = SCMTestUtils.getReuseableAddress();
    conf.set(ScmConfigKeys.OZONE_SCM_NAMES, address.getHostName());

    // Set layout version.
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempFolder.getRoot().getAbsolutePath());
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString(), mlv);
    layoutStorage.initialize();

    // Build and start the datanode.
    DatanodeDetails dd = ContainerTestUtils.createDatanodeDetails();
    DatanodeStateMachine newDsm = new DatanodeStateMachine(dd,
        conf, null, null,
        null);
    int actualMlv = newDsm.getLayoutVersionManager().getMetadataLayoutVersion();
    Assert.assertEquals(mlv, actualMlv);

    dsm = newDsm;
    String scmID = UUID.randomUUID().toString();
    startScmServer(scmID);
    return scmID;
  }

  public String restartDnWithNewScm(int expectedMlv)
      throws Exception {
    // Stop existing datanode.
    DatanodeDetails dd = dsm.getDatanodeDetails();
    dsm.stopDaemon();

    // Start new datanode with the same configuration.
    dsm = new DatanodeStateMachine(dd,
        conf, null, null,
        null);
    int mlv = dsm.getLayoutVersionManager().getMetadataLayoutVersion();
    Assert.assertEquals(expectedMlv, mlv);

    String scmID = UUID.randomUUID().toString();
    startScmServer(scmID);
    return scmID;
  }

  public void startScmServer(String scmID)
      throws Exception {
    if(scmRpcServer != null) {
      scmRpcServer.stop();
    }

    Collection<InetSocketAddress> addresses =
        HddsUtils.getSCMAddressForDatanodes(conf);
    Assert.assertEquals(1, addresses.size());
    InetSocketAddress address = new ArrayList<>(addresses).get(0);
    ScmTestMock scmServerImpl = new ScmTestMock(CLUSTER_ID, scmID);
    scmRpcServer = SCMTestUtils.startScmRpcServer(SCMTestUtils.getConf(),
        scmServerImpl, address, 10);

    EndpointStateMachine esm = ContainerTestUtils.createEndpoint(conf,
        address, 1000);
    VersionEndpointTask vet = new VersionEndpointTask(esm, conf,
        dsm.getContainer());
    esm.setState(EndpointStateMachine.EndPointStates.GETVERSION);
    vet.call();
  }

  public void dispatchRequest(
      ContainerProtos.ContainerCommandRequestProto request) {
    ContainerProtos.ContainerCommandResponseProto response =
        dsm.getContainer().getDispatcher().dispatch(request, null);
    Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
  }

  public void readChunk(ContainerProtos.WriteChunkRequestProto writeChunk,
      Pipeline pipeline)  throws Exception {
    ContainerProtos.ContainerCommandRequestProto readChunkRequest =
        ContainerTestHelper.getReadChunkRequest(pipeline, writeChunk);

    dispatchRequest(readChunkRequest);
  }

  public void putBlock(ContainerProtos.ContainerCommandRequestProto writeChunk,
      Pipeline pipeline) throws Exception {
    ContainerProtos.ContainerCommandRequestProto putBlockRequest =
        ContainerTestHelper.getPutBlockRequest(pipeline,
            writeChunk.getWriteChunk());

    dispatchRequest(writeChunk);
    dispatchRequest(putBlockRequest);
  }

  public void addContainer(long containerID, Pipeline pipeline)
      throws Exception {
    ContainerProtos.ContainerCommandRequestProto createContainerRequest =
        ContainerTestHelper.getCreateContainerRequest(containerID, pipeline);

    dispatchRequest(createContainerRequest);
  }


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

  public ContainerProtos.ContainerCommandRequestProto getWriteChunk(
      long containerID, Pipeline pipeline) throws Exception {
    return ContainerTestHelper.getWriteChunkRequest(pipeline,
            ContainerTestHelper.getTestBlockID(containerID), 100, null);
  }

  public Pipeline getPipeline() {
    return MockPipeline.createPipeline(
        Collections.singletonList(dsm.getDatanodeDetails()));
  }
}
