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
package org.apache.hadoop.ozone.client.rpc;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.ozoneimpl.TestOzoneContainer;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.test.GenericTestUtils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.ratis.grpc.server.GrpcLogAppender;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test to verify pipeline is closed on readStateMachine failure.
 */
@Ignore("see HDDS-3294")
public class TestContainerStateMachineFailureOnRead {
  private MiniOzoneCluster cluster;
  private ObjectStore objectStore;
  private String volumeName;
  private String bucketName;
  private OzoneConfiguration conf;

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    String path = GenericTestUtils
        .getTempPath(TestContainerStateMachineFailures.class.getSimpleName());
    File baseDir = new File(path);
    baseDir.mkdirs();

    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 1200, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, 1000,
        TimeUnit.SECONDS);
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setFollowerSlownessTimeout(Duration.ofSeconds(1000));
    ratisServerConfig.setNoLeaderTimeout(Duration.ofSeconds(1000));
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(3));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(3));
    conf.setFromObject(raftClientConfig);


    RatisClientConfig ratisClientConfig =
        conf.getObject(RatisClientConfig.class);
    ratisClientConfig.setWriteRequestTimeout(Duration.ofSeconds(30));
    ratisClientConfig.setWatchRequestTimeout(Duration.ofSeconds(30));
    conf.setFromObject(ratisClientConfig);

    conf.setQuietMode(false);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setHbInterval(200)
        .build();
    cluster.waitForClusterToBeReady();
    OzoneClient client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    volumeName = "testcontainerstatemachinefailures";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
    Logger.getLogger(GrpcLogAppender.class).setLevel(Level.WARN);
  }

  @After
  public void teardown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  @SuppressWarnings("squid:S3655")
  public void testReadStateMachineFailureClosesPipeline() throws Exception {
    // Stop one follower datanode
    List<Pipeline> pipelines =
        cluster.getStorageContainerManager().getPipelineManager().getPipelines(
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);
    Assert.assertEquals(1, pipelines.size());
    Pipeline ratisPipeline = pipelines.iterator().next();

    Optional<HddsDatanodeService> dnToStop =
        cluster.getHddsDatanodes().stream().filter(
            s -> {
              try {
                return ContainerTestHelper.isRatisFollower(s, ratisPipeline);
              } catch (Exception e) {
                e.printStackTrace();
                return false;
              }
            }).findFirst();

    Assert.assertTrue(dnToStop.isPresent());
    cluster.shutdownHddsDatanode(dnToStop.get().getDatanodeDetails());
    // Verify healthy pipeline before creating key
    XceiverClientRatis xceiverClientRatis =
        XceiverClientRatis.newXceiverClientRatis(ratisPipeline, conf);
    xceiverClientRatis.connect();
    TestOzoneContainer.createContainerForTesting(xceiverClientRatis, 100L);

    OmKeyLocationInfo omKeyLocationInfo;
    OzoneOutputStream key = objectStore.getVolume(volumeName)
        .getBucket(bucketName)
        .createKey("ratis", 1024, ReplicationType.RATIS,
            ReplicationFactor.THREE, new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis".getBytes(UTF_8));
    key.flush();
    
    // get the name of a valid container
    KeyOutputStream groupOutputStream = (KeyOutputStream) key.getOutputStream();

    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    omKeyLocationInfo = locationInfoList.get(0);
    key.close();
    groupOutputStream.close();

    Optional<HddsDatanodeService> leaderDn =
        cluster.getHddsDatanodes().stream().filter(dn -> {
          try {
            return ContainerTestHelper.isRatisLeader(dn, ratisPipeline);
          } catch (Exception e) {
            e.printStackTrace();
            return false;
          }
        }).findFirst();

    Assert.assertTrue(leaderDn.isPresent());
    // delete the container dir from leader
    FileUtil.fullyDelete(new File(
        leaderDn.get().getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID()).getContainerData()
            .getContainerPath()));
    // Start the stopped datanode
    // Do not wait on restart since on stop will take long time due to
    // stale interval timeout for the test
    cluster.restartHddsDatanode(dnToStop.get().getDatanodeDetails(), false);
    cluster.waitForClusterToBeReady();
    Thread.sleep(10000);

    try {
      Pipeline pipeline = cluster.getStorageContainerManager()
          .getPipelineManager().getPipeline(pipelines.get(0).getId());
      Assert.assertEquals("Pipeline " + pipeline.getId()
              + "should be in CLOSED state",
          Pipeline.PipelineState.CLOSED,
          pipeline.getPipelineState());
    } catch (PipelineNotFoundException e) {
      // do nothing
    }
  }
}
