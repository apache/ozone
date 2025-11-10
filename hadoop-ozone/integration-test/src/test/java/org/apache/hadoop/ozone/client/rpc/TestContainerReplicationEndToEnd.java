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

package org.apache.hadoop.ozone.client.rpc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * Tests delete key operation with a slow follower in the datanode
 * pipeline.
 */
public class TestContainerReplicationEndToEnd {

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static String volumeName;
  private static String bucketName;
  private static XceiverClientManager xceiverClientManager;
  private static long containerReportInterval;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    containerReportInterval = 2000;

    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL,
        containerReportInterval, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        5 * containerReportInterval, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL,
        10 * containerReportInterval, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, 1000,
        TimeUnit.SECONDS);
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setFollowerSlownessTimeout(Duration.ofSeconds(1000));
    ratisServerConfig.setNoLeaderTimeout(Duration.ofSeconds(1000));
    conf.setFromObject(ratisServerConfig);
    ReplicationManagerConfiguration replicationConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    replicationConf.setInterval(Duration.ofMillis(containerReportInterval));
    conf.setFromObject(replicationConf);
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 2);
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, 6);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 200, TimeUnit.MILLISECONDS);

    conf.setQuietMode(false);
    cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(4)
            .build();
    cluster.waitForClusterToBeReady();
    cluster.getStorageContainerManager().getReplicationManager().start();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    xceiverClientManager = new XceiverClientManager(conf);
    volumeName = "testcontainerstatemachinefailures";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (xceiverClientManager != null) {
      xceiverClientManager.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * The test simulates end to end container replication.
   */
  @Test
  public void testContainerReplication() throws Exception {
    String keyName = "testContainerReplication";
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey(keyName, 0,
                ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
                    ReplicationFactor.THREE), new HashMap<>());
    byte[] testData = "ratis".getBytes(UTF_8);
    // First write and flush creates a container in the datanode
    key.write(testData);
    key.flush();

    KeyOutputStream groupOutputStream = (KeyOutputStream) key.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    long containerID = omKeyLocationInfo.getContainerID();
    PipelineID pipelineID =
        cluster.getStorageContainerManager().getContainerManager()
            .getContainer(ContainerID.valueOf(containerID)).getPipelineID();
    Pipeline pipeline =
        cluster.getStorageContainerManager().getPipelineManager()
            .getPipeline(pipelineID);
    key.close();

    HddsProtos.LifeCycleState containerState =
        cluster.getStorageContainerManager().getContainerManager()
            .getContainer(ContainerID.valueOf(containerID)).getState();
    LoggerFactory.getLogger(TestContainerReplicationEndToEnd.class).info(
        "Current Container State is {}",  containerState);
    if ((containerState != HddsProtos.LifeCycleState.CLOSING) &&
        (containerState != HddsProtos.LifeCycleState.CLOSED)) {
      cluster.getStorageContainerManager().getContainerManager()
          .updateContainerState(ContainerID.valueOf(containerID),
              HddsProtos.LifeCycleEvent.FINALIZE);
    }
    // wait for container to move to OPEN state in SCM
    Thread.sleep(2 * containerReportInterval);
    DatanodeDetails oldReplicaNode = pipeline.getFirstNode();
    // now move the container to the closed on the datanode.
    ContainerProtos.ContainerCommandRequestProto.Builder request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    request.setCmdType(ContainerProtos.Type.CloseContainer);
    request.setContainerID(containerID);
    request.setCloseContainer(
        ContainerProtos.CloseContainerRequestProto.getDefaultInstance());
    XceiverClientSpi xceiverClient =
        xceiverClientManager.acquireClient(pipeline);
    try {
      xceiverClient.sendCommand(request.build());
    } finally {
      xceiverClientManager.releaseClient(xceiverClient, false);
    }
    // wait for container to move to closed state in SCM
    Thread.sleep(2 * containerReportInterval);
    assertSame(
        cluster.getStorageContainerManager().getContainerInfo(containerID)
            .getState(), HddsProtos.LifeCycleState.CLOSED);
    // shutdown the replica node
    cluster.shutdownHddsDatanode(oldReplicaNode);
    // now the container is under replicated and will be moved to a different dn
    HddsDatanodeService dnService = null;

    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      Predicate<DatanodeDetails> p =
          i -> i.getUuid().equals(dn.getDatanodeDetails().getUuid());
      if (!pipeline.getNodes().stream().anyMatch(p)) {
        dnService = dn;
      }
    }

    assertNotNull(dnService);
    final HddsDatanodeService newReplicaNode = dnService;
    // wait for the container to get replicated
    GenericTestUtils.waitFor(() -> {
      return newReplicaNode.getDatanodeStateMachine().getContainer()
          .getContainerSet().getContainer(containerID) != null;
    }, 500, 100000);
    assertThat(newReplicaNode.getDatanodeStateMachine().getContainer()
        .getContainerSet().getContainer(containerID).getContainerData()
        .getBlockCommitSequenceId())
        .isGreaterThan(0);
    // wait for SCM to update the replica Map
    Thread.sleep(5 * containerReportInterval);
    // now shutdown the other two dns of the original pipeline and try reading
    // the key again
    for (DatanodeDetails dn : pipeline.getNodes()) {
      cluster.shutdownHddsDatanode(dn);
    }
    // This will try to read the data from the dn to which the container got
    // replicated after the container got closed.
    TestHelper
        .validateData(keyName, testData, objectStore, volumeName, bucketName);
  }
}
