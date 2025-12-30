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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests FinalizeBlock.
 */
public class TestFinalizeBlock {

  private OzoneClient client;
  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private ObjectStore objectStore;
  private static String volumeName = UUID.randomUUID().toString();
  private static String bucketName = UUID.randomUUID().toString();

  private void setup(boolean enableSchemaV3) throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CONTAINER_SIZE, "1GB");
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        0, StorageUnit.MB);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setBoolean(CONTAINER_SCHEMA_V3_ENABLED, enableSchemaV3);

    DatanodeConfiguration datanodeConfiguration = conf.getObject(
        DatanodeConfiguration.class);
    datanodeConfiguration.setBlockDeletionInterval(Duration.ofMillis(100));
    conf.setFromObject(datanodeConfiguration);
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    scmConfig.setBlockDeletionInterval(Duration.ofMillis(100));
    conf.setFromObject(scmConfig);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(ONE, 30000);

    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      try {
        cluster.shutdown();
      } catch (Exception e) {
        // do nothing.
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testFinalizeBlock(boolean enableSchemaV3) throws Exception {
    setup(enableSchemaV3);
    String keyName = UUID.randomUUID().toString();
    // create key
    createKey(keyName);

    ContainerID containerId = cluster.getStorageContainerManager()
        .getContainerManager().getContainers().get(0).containerID();

    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName).setKeyName(keyName).setDataSize(0)
        .build();
    List<OmKeyLocationInfoGroup> omKeyLocationInfoGroupList =
        cluster.getOzoneManager().lookupKey(keyArgs).getKeyLocationVersions();

    ContainerInfo container = cluster.getStorageContainerManager()
        .getContainerManager().getContainer(containerId);
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager().getPipeline(container.getPipelineID());

    XceiverClientManager xceiverClientManager = new XceiverClientManager(conf);
    XceiverClientSpi xceiverClient =
        xceiverClientManager.acquireClient(pipeline);

    // Before finalize block WRITE chunk on the same block should pass through
    ContainerProtos.ContainerCommandRequestProto request =
        ContainerTestHelper.getWriteChunkRequest(pipeline, (
            new BlockID(containerId.getId(), omKeyLocationInfoGroupList.get(0)
                .getLocationList().get(0).getLocalID())), 100);
    xceiverClient.sendCommand(request);

    // Before finalize block PUT block on the same block should pass through
    request = ContainerTestHelper.getPutBlockRequest(request);
    xceiverClient.sendCommand(request);

    // Now Finalize Block
    request = getFinalizeBlockRequest(omKeyLocationInfoGroupList, container);
    ContainerProtos.ContainerCommandResponseProto response =
        xceiverClient.sendCommand(request);

    assertEquals(response.getFinalizeBlock().getBlockData().getBlockID().getLocalID(),
        omKeyLocationInfoGroupList.get(0).getLocationList().get(0).getLocalID());

    assertEquals(1, ((KeyValueContainerData)getContainerfromDN(cluster.getHddsDatanodes().get(0),
        containerId.getId()).getContainerData()).getFinalizedBlockSet().size());

    testRejectPutAndWriteChunkAfterFinalizeBlock(containerId, pipeline, xceiverClient, omKeyLocationInfoGroupList);
    testFinalizeBlockReloadAfterDNRestart(containerId);
    testFinalizeBlockClearAfterCloseContainer(containerId);
  }

  private void testFinalizeBlockReloadAfterDNRestart(ContainerID containerId) {
    try {
      cluster.restartHddsDatanode(0, true);
    } catch (Exception e) {
      fail("Fail to restart Datanode");
    }

    // After restart DN, finalizeBlock should be loaded into memory
    assertEquals(1, ((KeyValueContainerData)getContainerfromDN(cluster.getHddsDatanodes().get(0),
            containerId.getId()).getContainerData()).getFinalizedBlockSet().size());
  }

  private void testFinalizeBlockClearAfterCloseContainer(ContainerID containerId)
      throws InterruptedException, TimeoutException {
    OzoneTestUtils.closeAllContainers(cluster.getStorageContainerManager().getEventQueue(),
        cluster.getStorageContainerManager());

    // Finalize Block should be cleared from container data.
    GenericTestUtils.waitFor(() -> (
            (KeyValueContainerData)getContainerfromDN(cluster.getHddsDatanodes().get(0),
                containerId.getId()).getContainerData()).getFinalizedBlockSet().isEmpty(),
        100, 10 * 1000);
    try {
      // Restart DataNode
      cluster.restartHddsDatanode(0, true);
    } catch (Exception e) {
      fail("Fail to restart Datanode");
    }

    // After DN restart also there should not be any finalizeBlock
    assertTrue(((KeyValueContainerData)getContainerfromDN(
        cluster.getHddsDatanodes().get(0),
        containerId.getId()).getContainerData())
        .getFinalizedBlockSet().isEmpty());
  }

  private void testRejectPutAndWriteChunkAfterFinalizeBlock(ContainerID containerId, Pipeline pipeline,
      XceiverClientSpi xceiverClient, List<OmKeyLocationInfoGroup> omKeyLocationInfoGroupList)
      throws IOException {
    // Try doing WRITE chunk on the already finalized block
    ContainerProtos.ContainerCommandRequestProto request =
        ContainerTestHelper.getWriteChunkRequest(pipeline,
            (new BlockID(containerId.getId(), omKeyLocationInfoGroupList.get(0)
                .getLocationList().get(0).getLocalID())), 100);

    try {
      xceiverClient.sendCommand(request);
      fail("Write chunk should fail.");
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage()
          .contains("Block already finalized"));
    }

    // Try doing PUT block on the already finalized block
    request = ContainerTestHelper.getPutBlockRequest(request);
    try {
      xceiverClient.sendCommand(request);
      fail("Put block should fail.");
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage()
          .contains("Block already finalized"));
    }
  }

  @Nonnull
  private ContainerProtos.ContainerCommandRequestProto getFinalizeBlockRequest(
      List<OmKeyLocationInfoGroup> omKeyLocationInfoGroupList, ContainerInfo container) {
    String uuidString = cluster.getHddsDatanodes().get(0).getDatanodeDetails().getUuidString();
    long localID = omKeyLocationInfoGroupList.get(0).getLocationList().get(0).getLocalID();

    return ContainerTestHelper.getFinalizeBlockRequest(localID, container, uuidString);
  }

  /**
   * create a key with specified name.
   * @param keyName
   * @throws IOException
   */
  private void createKey(String keyName) throws IOException {
    OzoneOutputStream key = objectStore.getVolume(volumeName)
        .getBucket(bucketName)
        .createKey(keyName, 1024, ReplicationType.RATIS,
            ReplicationFactor.ONE, new HashMap<>());
    key.write("test".getBytes(UTF_8));
    key.close();
  }

  /**
   * Return the container for the given containerID from the given DN.
   */
  private Container getContainerfromDN(HddsDatanodeService hddsDatanodeService,
      long containerID) {
    return hddsDatanodeService.getDatanodeStateMachine().getContainer()
        .getContainerSet().getContainer(containerID);
  }
}
