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
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.replication.ContainerReplicationSource;
import org.apache.hadoop.ozone.container.replication.DownloadAndImportReplicator;
import org.apache.hadoop.ozone.container.replication.OnDemandContainerReplicationSource;
import org.apache.hadoop.ozone.container.replication.SimpleContainerDownloader;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.xml.bind.ValidationEvent;
import java.io.File;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Tests upgrading a single datanode from pre-SCM HA volume format that used
 * SCM ID to the post-SCM HA volume format using cluster ID.
 */
@RunWith(Parameterized.class)
public class TestDatanodeUpgradeToScmHA {
  @Rule
  public TemporaryFolder tempFolder;

  // The daemon for the datanode state machine is not started in this test.
  // This greatly speeds up execution time, and the only thing we lose is
  // any actions that would need to be run in pre-finalize, but we do not
  // have any such actions for SCM HA.
  private DatanodeStateMachine dsm;
  private OzoneConfiguration conf;
  private RPC.Server scmRpcServer;

  private static final String CLUSTER_ID = "clusterID";

  private Map<String, CreationInfo> volumeCreationInfo;
  private Map<Long, CreationInfo> containerCreationInfo;
  private String currentScmID;
  private final boolean scmHAAlreadyEnabled;

  private InetSocketAddress address;

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

    volumeCreationInfo = new HashMap<>();
    containerCreationInfo = new HashMap<>();
    currentScmID = null;
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
  public void testBusyUpgrade() throws Exception {
    // start DN and SCM
    startScmServer();
    addVolume();
    startDatanode(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    final Pipeline pipeline = getPipeline();

    // Add data to read.
    final long containerID = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk = putBlock(containerID,
        pipeline);
    dsm.getContainer().getContainerSet().getContainer(containerID).close();

    ExecutorService executor = Executors.newFixedThreadPool(2);

    // Create thread to keep reading during the upgrade.
    Future<Void> readFuture = executor.submit(() -> {
      try {
        // Layout version check should be thread safe.
        while(!dsm.getLayoutVersionManager()
            .isAllowed(HDDSLayoutFeature.SCM_HA)) {
          readChunk(writeChunk, pipeline);
        }
        // Make sure we can read after upgrade too.
        readChunk(writeChunk, pipeline);
      } catch(Exception ex) {
        Assert.fail(ex.getMessage());
      }
      return null;
    });

    dsm.finalizeUpgrade();
    // If there was a failure reading during the upgrade, the exception will
    // be thrown here.
    readFuture.get();
  }

  @Test
  public void testImportContainer() {
    // Create container normally.
    // Export it to temp location
    // For each import: modify

    // DownloadAndImportReplicator#importContainer
    // ONDemandcontainerReplicationSource#copyData

  }

  @Test
  public void testChaoticUpgrade() throws Exception {
    /// SETUP ///

    startScmServer();
    File volume = addVolume();
    startDatanode(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    final Pipeline pipeline = getPipeline();

    // Pre-export a container for import testing.
    final long exportContainerID = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto exportWriteChunk =
        putBlock(exportContainerID, pipeline);
    dsm.getContainer().getContainerSet().getContainer(exportContainerID).close();
    File exportedContainerFile = exportContainer(exportContainerID);
    deleteContainer(exportContainerID, pipeline);

    /// PRE-FINALIZED: Write and Read ///

    // Add container with data, make sure it can be read and written.
    final long containerID = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto writeChunk = putBlock(containerID,
        pipeline);
    readChunk(writeChunk, pipeline);

    checkVolumePathID(volume);
    checkContainerPathID(containerID);

    importContainer(exportContainerID, exportedContainerFile);
    readChunk(exportWriteChunk, pipeline);
    deleteContainer(exportContainerID, pipeline);

    /// PRE-FINALIZED: SCM finalizes and SCM HA is enabled ///

    // Now simulate SCMs finishing finalization and SCM HA being enabled.
    // Even though the DN is pre-finalized, SCM may have finalized itself and
    // 3 other datanodes, indicating it has finished finalization while this
    // datanode lags. As a result this datanode is restarted with SCM HA
    // on while pre-finalized, although it should not do anything with this
    // information.
    // DN restarts but gets an ID from a different SCM.
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    startScmServer();
    restartDatanode(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    // Because DN mlv would be behind SCM mlv, only reads are allowed.
    readChunk(writeChunk, pipeline);

    // On restart, there should have been no changes to the paths used.
    checkVolumePathID(volume);
    checkContainerPathID(containerID);

    importContainer(exportContainerID, exportedContainerFile);
    readChunk(exportWriteChunk, pipeline);
    deleteContainer(exportContainerID, pipeline);

    /// PRE-FINALIZED: Do finalization while the one volume is failed ///

    // Close container in preparation for upgrade. SCM would normally handle
    // this, but there is no real SCM in this unit test.
    dsm.getContainer().getContainerSet().getContainer(containerID).close();
    // Fail the volume during the upgrade.
    failVolume(volume);
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

    // Since the volume was out during the upgrade, it should maintain its
    // original format.
    checkVolumePathID(volume);
    checkContainerPathID(containerID);

    // Now that we are done finalizing, restore the volume.
    restoreVolume(volume);
    // After restoring the failed volume, its containers are readable again.
    // No new containers can be created on it due to its failed status.
    readChunk(writeChunk, pipeline);

    /// FINALIZED: Add a new volume and check its formatting ///

    // Add a new volume that should be formatted with cluster ID only, since
    // DN has finalized.
    File newVolume = addVolume();
    // Restart the datanode. It should upgrade the volume that was down
    // during finalization.
    // Yet another SCM ID is received this time, but it should not matter.
    startScmServer();
    restartDatanode(HDDSLayoutFeature.SCM_HA.layoutVersion());
    // New and old volume should be fully functional.
    Assert.assertEquals(2,
        dsm.getContainer().getVolumeSet().getVolumesList().size());
    Assert.assertEquals(0,
        dsm.getContainer().getVolumeSet().getFailedVolumesList().size());

    checkVolumePathID(volume);
    checkContainerPathID(containerID);
    // New volume should have been formatted with cluster ID only, since the
    // datanode is finalized.
    checkVolumePathID(newVolume);

    /// FINALIZED: Read old data and write + read new data ///

    importContainer(exportContainerID, exportedContainerFile);
    readChunk(exportWriteChunk, pipeline);
    deleteContainer(exportContainerID, pipeline);

    // Read container from before upgrade. The upgrade required it to be closed.
    readChunk(writeChunk, pipeline);
    // Write and read container after upgrade.
    long newContainerID = addContainer(pipeline);
    ContainerProtos.WriteChunkRequestProto newWriteChunk =
        putBlock(newContainerID, pipeline);
    readChunk(newWriteChunk, pipeline);
    // The new container should use cluster ID in its path.
    // The volume it is placed on is up to the implementation.
    checkContainerPathID(containerID);
  }

  public File exportContainer(long containerId) throws Exception {
    final ContainerReplicationSource replicationSource =
        new OnDemandContainerReplicationSource(dsm.getContainer().getController());

    replicationSource.prepare(containerId);

    File destination = tempFolder.newFile();
    try (FileOutputStream fos = new FileOutputStream(destination)) {
      replicationSource.copyData(containerId, fos);
    }
    return destination;
  }

  public void importContainer(long containerID, File source) throws Exception {
    DownloadAndImportReplicator replicator =
        new DownloadAndImportReplicator(dsm.getContainer().getContainerSet(),
            dsm.getContainer().getController(),
            new SimpleContainerDownloader(conf, null),
            new TarContainerPacker());

    // Import will delete the source container. We want to keep reusing it,
    // so give import a copy instead.
    File tempFile = tempFolder.newFile();
    Files.copy(source.toPath(), tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    replicator.importContainer(containerID, tempFile.toPath());
  }

  public void checkContainerPathID(long containerID) {
    CreationInfo creationInfo =  containerCreationInfo.get(containerID);
    String expectedID;

    if (scmHAAlreadyEnabled || creationInfo.createdAfterScmHA()) {
      expectedID = CLUSTER_ID;
    } else {
      expectedID = creationInfo.getScmID();
    }

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

  public void checkVolumePathID(File volume) throws Exception {
    CreationInfo creationInfo =
        volumeCreationInfo.get(volume.getAbsolutePath());
    Assert.assertNotNull(creationInfo);

    if (!scmHAAlreadyEnabled && !creationInfo.createdAfterScmHA() &&
      dsm.getLayoutVersionManager().isAllowed(HDDSLayoutFeature.SCM_HA) &&
          !isFailed(volume)) {

      List<File> subdirs = getHddsSubdirs(volume);
      File hddsRoot = getHddsRoot(volume);

      // Volume should have SCM ID and cluster ID directory, where cluster ID
      // is a symlink to SCM ID.
      Assert.assertEquals(2, subdirs.size());

      File scmIDDir = new File(hddsRoot, creationInfo.getScmID());
      Assert.assertTrue(subdirs.contains(scmIDDir));

      File clusterIDDir = new File(hddsRoot, CLUSTER_ID);
      Assert.assertTrue(subdirs.contains(clusterIDDir));
      Assert.assertTrue(Files.isSymbolicLink(clusterIDDir.toPath()));
      Path symlinkTarget = Files.readSymbolicLink(clusterIDDir.toPath());
      Assert.assertEquals(creationInfo.getScmID(), symlinkTarget.toString());
    } else {
      String expectedID;
      if (scmHAAlreadyEnabled || creationInfo.createdAfterScmHA()) {
        expectedID = CLUSTER_ID;
      } else {
        expectedID = creationInfo.getScmID();
      }

      List<File> subdirs;
      File hddsRoot;
      if (isFailed(volume)) {
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
  }

  public boolean isFailed(File volume) {
    return dsm.getContainer().getVolumeSet().getFailedVolumesList().stream()
        .anyMatch(v ->
            getHddsRoot(v.getStorageDir()).equals(getHddsRoot(volume)));
  }

  public void startDatanode(int mlv) throws Exception {
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

    EndpointStateMachine esm = ContainerTestUtils.createEndpoint(conf,
        address, 1000);
    VersionEndpointTask vet = new VersionEndpointTask(esm, conf,
        dsm.getContainer());
    esm.setState(EndpointStateMachine.EndPointStates.GETVERSION);
    vet.call();
  }

  public void restartDatanode(int expectedMlv)
      throws Exception {
    // Stop existing datanode.
    DatanodeDetails dd = dsm.getDatanodeDetails();
    dsm.close();

    // Start new datanode with the same configuration.
    dsm = new DatanodeStateMachine(dd,
        conf, null, null,
        null);
    int mlv = dsm.getLayoutVersionManager().getMetadataLayoutVersion();
    Assert.assertEquals(expectedMlv, mlv);

    EndpointStateMachine esm = ContainerTestUtils.createEndpoint(conf,
        address, 1000);
    VersionEndpointTask vet = new VersionEndpointTask(esm, conf,
        dsm.getContainer());
    esm.setState(EndpointStateMachine.EndPointStates.GETVERSION);
    vet.call();
  }

  public void startScmServer() throws Exception {
    if(scmRpcServer != null) {
      scmRpcServer.stop();
    }

    currentScmID = UUID.randomUUID().toString();
    ScmTestMock scmServerImpl = new ScmTestMock(CLUSTER_ID, currentScmID);
    scmRpcServer = SCMTestUtils.startScmRpcServer(SCMTestUtils.getConf(),
        scmServerImpl, address, 10);
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
    long containerID = Math.abs(random.nextLong());
    ContainerProtos.ContainerCommandRequestProto createContainerRequest =
        ContainerTestHelper.getCreateContainerRequest(containerID, pipeline);
    dispatchRequest(createContainerRequest);

    containerCreationInfo.put(containerID, new CreationInfo());
    return containerID;
  }

  public void deleteContainer(long containerID, Pipeline pipeline)
      throws Exception {
    ContainerProtos.ContainerCommandRequestProto deleteContainerRequest =
        ContainerTestHelper.getDeleteContainer(pipeline, containerID, true);
    dispatchRequest(deleteContainerRequest);

//    containerCreationInfo.put(containerID, new CreationInfo());
//    return containerID;
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

    volumeCreationInfo.put(vol.getAbsolutePath(), new CreationInfo());
    return vol;
  }

  public void failVolume(File volume) {
    // Rename the volume to invalidate its path and fail it.
    File failedVolume = getFailedVolume(volume);
    Assert.assertTrue(volume.renameTo(failedVolume));
  }

  public void restoreVolume(File volume) {
    File failedVolume = getFailedVolume(volume);
    Assert.assertTrue(failedVolume.renameTo(volume));
  }

  public File getFailedVolume(File volume) {
    return new File(volume.getParent(), volume.getName() + "-failed");
  }

  /**
   * Maintains information about the state of the datanode when a volume or
   * container is created. This allows us to test that an existing volume or
   * container's state is correct at each stage of finalization.
   */
  public class CreationInfo {
    private final String scmID;
    private final int layoutVersionCreatedIn;

    public CreationInfo() {
      // Pull necessary information from the enclosing class.
      this.scmID = currentScmID;
      if (dsm == null) {
        this.layoutVersionCreatedIn =
            HDDSLayoutFeature.INITIAL_VERSION.layoutVersion();
      } else {
        this.layoutVersionCreatedIn =
            dsm.getLayoutVersionManager().getMetadataLayoutVersion();
      }
    }

    public String getScmID() {
      return scmID;
    }

    public boolean createdAfterScmHA() {
      return layoutVersionCreatedIn >= HDDSLayoutFeature.SCM_HA.layoutVersion();
    }
  }
}
