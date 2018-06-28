/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfo;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class TestCloseContainerByPipeline {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static OzoneClient client;
  private static ObjectStore objectStore;


  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true and
   * OZONE_HANDLER_TYPE_KEY = "distributed"
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_HANDLER_TYPE_KEY,
        OzoneConsts.OZONE_HANDLER_DISTRIBUTED);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getClient(conf);
    objectStore = client.getObjectStore();
    objectStore.createVolume("test");
    objectStore.getVolume("test").createBucket("test");
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testCloseContainerViaStandaAlone()
      throws IOException, TimeoutException, InterruptedException {

    OzoneOutputStream key = objectStore.getVolume("test").getBucket("test")
        .createKey("standalone", 1024, ReplicationType.STAND_ALONE,
            ReplicationFactor.ONE);
    key.write("standalone".getBytes());
    key.close();

    //get the name of a valid container
    KsmKeyArgs keyArgs =
        new KsmKeyArgs.Builder().setVolumeName("test").setBucketName("test")
            .setType(HddsProtos.ReplicationType.STAND_ALONE)
            .setFactor(HddsProtos.ReplicationFactor.ONE).setDataSize(1024)
            .setKeyName("standalone").build();

    KsmKeyLocationInfo ksmKeyLocationInfo =
        cluster.getKeySpaceManager().lookupKey(keyArgs).getKeyLocationVersions()
            .get(0).getBlocksLatestVersionOnly().get(0);

    long containerID = ksmKeyLocationInfo.getContainerID();
    List<DatanodeDetails> datanodes =
        cluster.getStorageContainerManager().getContainerInfo(containerID)
            .getPipeline().getMachines();
    Assert.assertTrue(datanodes.size() == 1);

    DatanodeDetails datanodeDetails = datanodes.get(0);
    Assert
        .assertFalse(isContainerClosed(cluster, containerID, datanodeDetails));

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(OzoneContainer.LOG);
    //send the order to close the container
    cluster.getStorageContainerManager().getScmNodeManager()
        .addDatanodeCommand(datanodeDetails.getUuid(),
            new CloseContainerCommand(containerID,
                HddsProtos.ReplicationType.STAND_ALONE));

    GenericTestUtils
        .waitFor(() -> isContainerClosed(cluster, containerID, datanodeDetails),
            500, 5 * 1000);

    //double check if it's really closed (waitFor also throws an exception)
    Assert.assertTrue(isContainerClosed(cluster, containerID, datanodeDetails));
    Assert.assertTrue(logCapturer.getOutput().contains(
        "submitting CloseContainer request over STAND_ALONE server for"
            + " container " + containerID));
    // Make sure it was really closed via StandAlone not Ratis server
    Assert.assertFalse((logCapturer.getOutput().contains(
        "submitting CloseContainer request over RATIS server for container "
            + containerID)));
  }

  @Test
  public void testCloseContainerViaRatis() throws IOException,
      TimeoutException, InterruptedException {

    OzoneOutputStream key = objectStore.getVolume("test").getBucket("test")
        .createKey("ratis", 1024, ReplicationType.RATIS,
            ReplicationFactor.THREE);
    key.write("ratis".getBytes());
    key.close();

    //get the name of a valid container
    KsmKeyArgs keyArgs = new KsmKeyArgs.Builder().setVolumeName("test").
        setBucketName("test").setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.THREE).setDataSize(1024)
        .setKeyName("ratis").build();

    KsmKeyLocationInfo ksmKeyLocationInfo =
        cluster.getKeySpaceManager().lookupKey(keyArgs).getKeyLocationVersions()
            .get(0).getBlocksLatestVersionOnly().get(0);

    long containerID = ksmKeyLocationInfo.getContainerID();
    List<DatanodeDetails> datanodes =
        cluster.getStorageContainerManager().getContainerInfo(containerID)
            .getPipeline().getMachines();
    Assert.assertTrue(datanodes.size() == 3);

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(OzoneContainer.LOG);

    for (DatanodeDetails details : datanodes) {
      Assert.assertFalse(isContainerClosed(cluster, containerID, details));
      //send the order to close the container
      cluster.getStorageContainerManager().getScmNodeManager()
          .addDatanodeCommand(details.getUuid(),
              new CloseContainerCommand(containerID,
                  HddsProtos.ReplicationType.RATIS));
  }

    for (DatanodeDetails datanodeDetails : datanodes) {
      GenericTestUtils.waitFor(
          () -> isContainerClosed(cluster, containerID, datanodeDetails), 500,
          15 * 1000);
      //double check if it's really closed (waitFor also throws an exception)
      Assert.assertTrue(isContainerClosed(cluster, containerID, datanodeDetails));
    }
    Assert.assertFalse(logCapturer.getOutput().contains(
        "submitting CloseContainer request over STAND_ALONE "
            + "server for container " + containerID));
    // Make sure it was really closed via StandAlone not Ratis server
    Assert.assertTrue((logCapturer.getOutput().contains(
        "submitting CloseContainer request over RATIS server for container "
            + containerID)));
  }

  private Boolean isContainerClosed(MiniOzoneCluster cluster, long containerID,
      DatanodeDetails datanode) {
    ContainerData containerData;
    try {
      for (HddsDatanodeService datanodeService : cluster.getHddsDatanodes())
        if (datanode.equals(datanodeService.getDatanodeDetails())) {
          containerData =
              datanodeService.getDatanodeStateMachine().getContainer()
                  .getContainerSet().getContainer(containerID).getContainerData();
          if (!containerData.isOpen()) {
            // make sure the closeContainerHandler on the Datanode is invoked
            Assert.assertTrue(
                datanodeService.getDatanodeStateMachine().getCommandDispatcher()
                    .getCloseContainerHandler().getInvocationCount() > 0);
            return true;
          }
        }
    } catch (StorageContainerException e) {
      throw new AssertionError(e);
    }
    return false;
  }
}
