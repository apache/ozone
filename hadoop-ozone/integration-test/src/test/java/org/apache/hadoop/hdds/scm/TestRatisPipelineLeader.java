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
package org.apache.hadoop.hdds.scm;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.ratis.grpc.client.GrpcClientProtocolService;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Test pipeline leader information is correctly used.
 */
@Ignore("HDDS-3265")
public class TestRatisPipelineLeader {
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(TestRatisPipelineLeader.class);

  @BeforeClass
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, "100ms");
    cluster = MiniOzoneCluster
        .newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 120000)
  public void testLeaderIdUsedOnFirstCall() throws Exception {
    List<Pipeline> pipelines = cluster.getStorageContainerManager()
        .getPipelineManager().getPipelines(HddsProtos.ReplicationType.RATIS,
            3);
    Assert.assertFalse(pipelines.isEmpty());
    Pipeline ratisPipeline = pipelines.iterator().next();
    Assert.assertTrue(ratisPipeline.isHealthy());
    // Verify correct leader info populated
    GenericTestUtils.waitFor(() -> {
      try {
        return verifyLeaderInfo(ratisPipeline);
      } catch (Exception e) {
        LOG.error("Failed verifying the leader info.", e);
        Assert.fail("Failed verifying the leader info.");
        return false;
      }
    }, 200, 20000);
    // Verify client connects to Leader without NotLeaderException
    XceiverClientRatis xceiverClientRatis =
        XceiverClientRatis.newXceiverClientRatis(ratisPipeline, conf);
    Logger.getLogger(GrpcClientProtocolService.class).setLevel(Level.DEBUG);
    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(GrpcClientProtocolService.LOG);
    xceiverClientRatis.connect();
    ContainerProtocolCalls.createContainer(xceiverClientRatis, 1L, null);
    logCapturer.stopCapturing();
    Assert.assertFalse("Client should connect to pipeline leader on first try.",
        logCapturer.getOutput().contains(
            "org.apache.ratis.protocol.NotLeaderException"));
  }

  @Test(timeout = 120000)
  public void testLeaderIdAfterLeaderChange() throws Exception {
    List<Pipeline> pipelines = cluster.getStorageContainerManager()
        .getPipelineManager().getPipelines(HddsProtos.ReplicationType.RATIS,
            3);
    Assert.assertFalse(pipelines.isEmpty());
    Pipeline ratisPipeline = pipelines.iterator().next();
    Assert.assertTrue(ratisPipeline.isHealthy());
    Optional<HddsDatanodeService> dnToStop =
        cluster.getHddsDatanodes().stream().filter(s ->
            !s.getDatanodeStateMachine().getDatanodeDetails().getUuid().equals(
                ratisPipeline.getLeaderId())).findAny();
    Assert.assertTrue(dnToStop.isPresent());
    dnToStop.get().stop();
    // wait long enough based on leader election min timeout
    Thread.sleep(4000 * conf.getTimeDuration(
        DFS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY,
        5, TimeUnit.SECONDS));
    GenericTestUtils.waitFor(() -> {
      try {
        return verifyLeaderInfo(ratisPipeline);
      } catch (Exception e) {
        LOG.error("Failed verifying the leader info.", e);
        Assert.fail("Failed getting leader info.");
        return false;
      }
    }, 200, 20000);
  }

  private boolean verifyLeaderInfo(Pipeline ratisPipeline) throws Exception {
    Optional<HddsDatanodeService> hddsDatanodeService =
        cluster.getHddsDatanodes().stream().filter(s ->
            s.getDatanodeStateMachine().getDatanodeDetails().getUuid()
                .equals(ratisPipeline.getLeaderId())).findFirst();
    Assert.assertTrue(hddsDatanodeService.isPresent());

    XceiverServerRatis serverRatis =
        (XceiverServerRatis) hddsDatanodeService.get()
            .getDatanodeStateMachine().getContainer().getWriteChannel();

    GroupInfoRequest groupInfoRequest = new GroupInfoRequest(
        ClientId.randomId(), serverRatis.getServer().getId(),
        RaftGroupId.valueOf(ratisPipeline.getId().getId()), 100);
    GroupInfoReply reply =
        serverRatis.getServer().getGroupInfo(groupInfoRequest);
    return reply.getRoleInfoProto().hasLeaderInfo() &&
        ratisPipeline.getLeaderId().toString().equals(
            reply.getRoleInfoProto().getSelf().getId().toStringUtf8());
  }
}
