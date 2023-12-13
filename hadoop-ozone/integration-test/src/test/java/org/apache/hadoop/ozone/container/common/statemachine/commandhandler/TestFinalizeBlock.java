/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.hadoop.hdds.HddsConfigKeys;
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
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.JUnit5AwareTimeout;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests FinalizeBlock.
 */
public class TestFinalizeBlock {

  private OzoneClient client;
  /**
    * Set a timeout for each test.
    */
  @Rule
  public TestRule timeout = new JUnit5AwareTimeout(Timeout.seconds(300));
  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private ObjectStore objectStore;
  private static String volumeName = UUID.randomUUID().toString();
  private static String bucketName = UUID.randomUUID().toString();

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CONTAINER_SIZE, "1GB");
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        0, StorageUnit.MB);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        3, TimeUnit.SECONDS);

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

  @After
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

  @Test
  public void testFinalizeBlock()
      throws IOException {
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


    final ContainerProtos.ContainerCommandRequestProto.Builder builder =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.FinalizeBlock)
            .setContainerID(container.getContainerID())
            .setDatanodeUuid(cluster.getHddsDatanodes()
            .get(0).getDatanodeDetails().getUuidString());

    final ContainerProtos.DatanodeBlockID blockId =
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(container.getContainerID()).setLocalID(
             omKeyLocationInfoGroupList.get(0)
            .getLocationList().get(0).getLocalID())
            .setBlockCommitSequenceId(0).build();

    builder.setFinalizeBlock(ContainerProtos.FinalizeBlockRequestProto
        .newBuilder().setBlockID(blockId).build());

    XceiverClientManager xceiverClientManager = new XceiverClientManager(conf);
    XceiverClientSpi xceiverClient =
        xceiverClientManager.acquireClient(pipeline);

    ContainerProtos.ContainerCommandRequestProto request = builder.build();
    ContainerProtos.ContainerCommandResponseProto response =
        xceiverClient.sendCommand(request);

    Assert.assertTrue(response.getFinalizeBlock()
        .getBlockData().getBlockID().getLocalID()
        == omKeyLocationInfoGroupList.get(0)
        .getLocationList().get(0).getLocalID());

    Assert.assertTrue(((KeyValueContainerData)getContainerfromDN(
        cluster.getHddsDatanodes().get(0),
        containerId.getId()).getContainerData())
        .getFinalizedBlockSet().size() == 1);
  }

  @Test
  public void testFinalizeBlockReloadAfterDNRestart()
      throws IOException {
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

    final ContainerProtos.ContainerCommandRequestProto.Builder builder =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.FinalizeBlock)
            .setContainerID(container.getContainerID())
            .setDatanodeUuid(cluster.getHddsDatanodes()
             .get(0).getDatanodeDetails().getUuidString());

    final ContainerProtos.DatanodeBlockID blockId =
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(container.getContainerID()).setLocalID(
            omKeyLocationInfoGroupList.get(0)
            .getLocationList().get(0).getLocalID())
            .setBlockCommitSequenceId(0).build();

    builder.setFinalizeBlock(ContainerProtos.FinalizeBlockRequestProto
        .newBuilder().setBlockID(blockId).build());

    XceiverClientManager xceiverClientManager = new XceiverClientManager(conf);
    XceiverClientSpi xceiverClient =
        xceiverClientManager.acquireClient(pipeline);

    ContainerProtos.ContainerCommandRequestProto request = builder.build();
    ContainerProtos.ContainerCommandResponseProto response =
        xceiverClient.sendCommand(request);

    Assert.assertTrue(response.getFinalizeBlock()
        .getBlockData().getBlockID().getLocalID()
        == omKeyLocationInfoGroupList.get(0)
        .getLocationList().get(0).getLocalID());
    Assert.assertTrue(((KeyValueContainerData)
        getContainerfromDN(cluster.getHddsDatanodes().get(0),
        containerId.getId()).getContainerData())
        .getFinalizedBlockSet().size() == 1);

    try {
      cluster.restartHddsDatanode(0, false);
    } catch (Exception e) {
    }

    // After restart DN, finalizeBlock should be loaded into memory
    Assert.assertTrue(((KeyValueContainerData)
        getContainerfromDN(cluster.getHddsDatanodes().get(0),
        containerId.getId()).getContainerData())
        .getFinalizedBlockSet().size() == 1);
  }

  @Test
  public void testFinalizeBlockClearAfterCloseContainer()
      throws IOException {
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


    final ContainerProtos.ContainerCommandRequestProto.Builder builder =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.FinalizeBlock)
            .setContainerID(container.getContainerID())
            .setDatanodeUuid(cluster.getHddsDatanodes()
            .get(0).getDatanodeDetails().getUuidString());

    final ContainerProtos.DatanodeBlockID blockId =
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(container.getContainerID()).setLocalID(
                omKeyLocationInfoGroupList.get(0)
            .getLocationList().get(0).getLocalID())
            .setBlockCommitSequenceId(0).build();

    builder.setFinalizeBlock(ContainerProtos.FinalizeBlockRequestProto
        .newBuilder().setBlockID(blockId).build());

    XceiverClientManager xceiverClientManager = new XceiverClientManager(conf);
    XceiverClientSpi xceiverClient =
        xceiverClientManager.acquireClient(pipeline);
    ContainerProtos.ContainerCommandRequestProto request = builder.build();

    ContainerProtos.ContainerCommandResponseProto response =
        xceiverClient.sendCommand(request);
    Assert.assertTrue(response.getFinalizeBlock()
        .getBlockData().getBlockID().getLocalID()
        == omKeyLocationInfoGroupList.get(0)
        .getLocationList().get(0).getLocalID());

    Assert.assertTrue(((KeyValueContainerData)
        getContainerfromDN(cluster.getHddsDatanodes().get(0),
        containerId.getId()).getContainerData())
        .getFinalizedBlockSet().size() == 1);

    OzoneTestUtils.closeAllContainers(cluster
            .getStorageContainerManager().getEventQueue(),
        cluster.getStorageContainerManager());

    try {
      // Finalize Block should be cleared from container data.
      GenericTestUtils.waitFor(() ->
              ((KeyValueContainerData)
                  getContainerfromDN(cluster.getHddsDatanodes().get(0),
              containerId.getId()).getContainerData())
                  .getFinalizedBlockSet().size() == 0,
          500, 5 * 1000);

      // Restart DataNode
      cluster.restartHddsDatanode(0, true);
    } catch (Exception e) {
    }

    // After DN restart also there should not be any finalizeBlock
    Assert.assertTrue(((KeyValueContainerData)getContainerfromDN(
        cluster.getHddsDatanodes().get(0),
        containerId.getId()).getContainerData())
        .getFinalizedBlockSet().size() == 0);
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
