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
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.ScmTestMock;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.states.endpoint.VersionEndpointTask;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestDatanodeUpgradeToScmHA {
  @Rule
  public TemporaryFolder tempFolder;

  private DatanodeStateMachine dsm;
  private OzoneConfiguration conf;
  private RPC.Server scmRpcServer;

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    tempFolder = new TemporaryFolder();
    tempFolder.create();
  }

  @After
  public void teardown() throws Exception {
    if (dsm != null) {
      dsm.stopDaemon();
    }

    if (scmRpcServer != null) {
      scmRpcServer.stop();
    }
  }

  @Test
  public void test() throws Exception {
    File volume = addVolume();
    dsm = buildPreFinalizedDatanode(conf);
    dsm.startDaemon();
    startScmServer("clusterID", "scmID");

    final long containerID = 123;
    final Pipeline pipeline = getPipeline();

    // Add container with data.
    addContainer(containerID, pipeline);
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        getWriteChunk(containerID, pipeline);
    putBlock(writeChunkRequest, pipeline);
    readChunk(writeChunkRequest.getWriteChunk(), pipeline);

    // scm ha with upgrades
    // In all places where we write container, can also import container.

    /** failed volume on finalize + format pre-finalized + different scm ids preF
     * Call vet
     * Write data with container
     * check scm id format
     * restart dsm
     * call vet with different scm id
     * check original scm id in container format
     * read container
     *
     * Remove volume
     * Finalize
     * Check that volume is added to failed volume set or something.
     * Restart dsm with volume.
     *
     * Check volume in old format.
     * call VET.
     * Volume should be formatted with cluster ID.
     * read container
     * write container
     * should be formatted with cluster ID
     * read container
     */

    /** use while finalizing
     * write data with container
     * finalize on one thread
     * read on one thread until done finalizing.
     * import container on one thread until done finalizing.
     * read all containers
     */

    // scm ha before upgrades
    // same tests but with scm ha on in the config.
  }

  @Test
  public void testChaoticUpgrade() throws Exception {
    File volume = addVolume();
    dsm = buildPreFinalizedDatanode(conf);
    dsm.startDaemon();
    startScmServer("clusterID", "scmID");

    final long containerID = 123;
    final Pipeline pipeline = getPipeline();

    // Add container with data, make sure it can be read and written.
    addContainer(containerID, pipeline);
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        getWriteChunk(containerID, pipeline);
    putBlock(writeChunkRequest, pipeline);
    readChunk(writeChunkRequest.getWriteChunk(), pipeline);

    // TODO: Check container uses scm ID only.
    // TODO: Check volume uses scm ID only.

    // Now simulate SCMs finishing finalization and SCM HA beging enabled.
    // DN restarts but gets an ID from a different SCM.
    restartDsm(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    scmRpcServer.stop();
    startScmServer("clusterID", "scmID2");

    // Because DN mlv would be behind SCM mlv, only reads are allowed.
    readChunk(writeChunkRequest.getWriteChunk(), pipeline);
    // Close container in preparation for upgrade. SCM would normally handle
    // this, but there is no real SCM in this unit test.
    dsm.getContainer().getContainerSet().getContainer(containerID).close();

    // TODO: Check container and volume have old SCM ID.

    // Move the volume to fail it.
    File failedVolume = new File(volume.getParent(), "baz");
    Assert.assertTrue(volume.renameTo(failedVolume));
    dsm.finalizeUpgrade();
    LambdaTestUtils.await(2000, 500,
        () -> dsm.getLayoutVersionManager().isAllowed(HDDSLayoutFeature.SCM_HA));
    // Check that volume is marked failed.
    Assert.assertEquals(1,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());
    Assert.assertEquals(0,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    // Now that we are done finalizing, unfail the volume.
    Assert.assertTrue(failedVolume.renameTo(volume));
    // After restoring the failed volume, its containers are readable again.
    // No new containers can be created on it, however.
    readChunk(writeChunkRequest.getWriteChunk(), pipeline);

    // Add a new volume that should be formatted with cluster ID only, since
    // DN has finalized
    addVolume();
    // Restart the datanode. It should upgrade the volume that was down
    // during finalization.
    restartDsm(HDDSLayoutFeature.SCM_HA.layoutVersion());
    scmRpcServer.stop();
    // Yet another SCM ID is received this time, but it should not matter.
    startScmServer("clusterID", "scmID3");

    // New and old volume should be fully functional.
    Assert.assertEquals(2,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    Assert.assertEquals(0,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());

    // TODO: Check format of new volume.
    // TODO: Check old container has old SCM ID.
    // TODO: Check old volume has old SCM ID and cluster ID link.

    // Read container from before upgrade.
    readChunk(writeChunkRequest.getWriteChunk(), pipeline);

    // Write then read container after upgrade.
    long newContainerID = 1234;
    addContainer(newContainerID, pipeline);
    ContainerProtos.ContainerCommandRequestProto newWriteChunkRequest =
        getWriteChunk(newContainerID, pipeline);
    putBlock(newWriteChunkRequest, pipeline);
    readChunk(newWriteChunkRequest.getWriteChunk(), pipeline);

    // TODO: Check container has cluster ID.
  }

  public void restartDsm(int expectedMlv) throws Exception {
    // Assume conf remains the same.
    DatanodeDetails dd = dsm.getDatanodeDetails();
    dsm.stopDaemon();
    dsm = new DatanodeStateMachine(dd,
        conf, null, null,
        null);
    int mlv =
        dsm.getLayoutVersionManager().getMetadataLayoutVersion();
    Assert.assertEquals(expectedMlv, mlv);

    dsm.startDaemon();
  }

  public void checkVolumeFormatScmID(KeyValueContainer container,
      String scmID) {
    String chunksPath = container.getContainerData().getChunksPath();

  }

  public void startScmServer(String clusterID, String scmID) throws Exception {
    InetSocketAddress address = SCMTestUtils.getReuseableAddress();
    ScmTestMock scmServerImpl = new ScmTestMock(clusterID, scmID);
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

  public void addContainer(long containerID, Pipeline pipeline) throws Exception {
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

  public DatanodeStateMachine buildPreFinalizedDatanode(OzoneConfiguration conf) throws Exception {
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempFolder.getRoot().getAbsolutePath());
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString(),
        HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    layoutStorage.initialize();

    DatanodeDetails dd = ContainerTestUtils.createDatanodeDetails();

    DatanodeStateMachine dsm = new DatanodeStateMachine(dd,
        conf, null, null,
        null);
    int mlv =
        dsm.getLayoutVersionManager().getMetadataLayoutVersion();
    Assert.assertEquals(0, mlv);

    return dsm;
  }
}
