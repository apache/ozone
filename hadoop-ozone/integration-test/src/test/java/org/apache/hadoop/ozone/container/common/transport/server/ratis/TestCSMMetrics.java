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
package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.*;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.transport.server
    .XceiverServerSpi;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import org.apache.ratis.RatisHelper;
import org.apache.ratis.rpc.RpcType;
import static org.apache.ratis.rpc.SupportedRpcType.GRPC;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.util.CheckedBiConsumer;

import java.util.function.BiConsumer;

import org.junit.Test;
import org.junit.Assert;

/**
 * This class tests the metrics of ContainerStateMachine.
 */
public class TestCSMMetrics {
  static final String TEST_DIR
      = GenericTestUtils.getTestDir("dfs").getAbsolutePath() + File.separator;

  @FunctionalInterface
  interface CheckedBiFunction<LEFT, RIGHT, OUT, THROWABLE extends Throwable> {
    OUT apply(LEFT left, RIGHT right) throws THROWABLE;
  }

  @Test
  public void testContainerStateMachineMetrics() throws Exception {
    runContainerStateMachineMetrics(1,
        (pipeline, conf) -> RatisTestHelper.initRatisConf(GRPC, conf),
        XceiverClientRatis::newXceiverClientRatis,
        TestCSMMetrics::newXceiverServerRatis,
        (dn, p) -> initXceiverServerRatis(GRPC, dn, p));
  }

  static void runContainerStateMachineMetrics(
      int numDatanodes,
      BiConsumer<Pipeline, OzoneConfiguration> initConf,
      TestCSMMetrics.CheckedBiFunction<Pipeline, OzoneConfiguration,
          XceiverClientSpi, IOException> createClient,
      TestCSMMetrics.CheckedBiFunction<DatanodeDetails, OzoneConfiguration,
          XceiverServerSpi, IOException> createServer,
      CheckedBiConsumer<DatanodeDetails, Pipeline, IOException> initServer)
      throws Exception {
    final List<XceiverServerSpi> servers = new ArrayList<>();
    XceiverClientSpi client = null;
    String containerName = OzoneUtils.getRequestID();
    try {
      final Pipeline pipeline = ContainerTestHelper.createPipeline(
          numDatanodes);
      final OzoneConfiguration conf = new OzoneConfiguration();
      initConf.accept(pipeline, conf);

      for (DatanodeDetails dn : pipeline.getMachines()) {
        final XceiverServerSpi s = createServer.apply(dn, conf);
        servers.add(s);
        s.start();
        initServer.accept(dn, pipeline);
      }

      client = createClient.apply(pipeline, conf);
      client.connect();

      // Before Read Chunk/Write Chunk
      MetricsRecordBuilder metric = getMetrics(CSMMetrics.SOURCE_NAME);
      assertCounter("NumWriteStateMachineOps", 0L, metric);
      assertCounter("NumReadStateMachineOps", 0L, metric);
      assertCounter("NumApplyTransactionOps", 0L, metric);

      // Write Chunk
      BlockID blockID = ContainerTestHelper.getTestBlockID(ContainerTestHelper.
          getTestContainerID());
      ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
          ContainerTestHelper.getWriteChunkRequest(
              pipeline, blockID, 1024);
      ContainerCommandResponseProto response =
          client.sendCommand(writeChunkRequest);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      metric = getMetrics(CSMMetrics.SOURCE_NAME);
      assertCounter("NumWriteStateMachineOps", 1L, metric);
      assertCounter("NumApplyTransactionOps", 1L, metric);

      //Read Chunk
      ContainerProtos.ContainerCommandRequestProto readChunkRequest =
          ContainerTestHelper.getReadChunkRequest(pipeline, writeChunkRequest
              .getWriteChunk());
      response = client.sendCommand(readChunkRequest);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      metric = getMetrics(CSMMetrics.SOURCE_NAME);
      assertCounter("NumReadStateMachineOps", 1L, metric);
      assertCounter("NumApplyTransactionOps", 1L, metric);
    } finally {
      if (client != null) {
        client.close();
      }
      servers.stream().forEach(XceiverServerSpi::stop);
    }
  }

  static XceiverServerRatis newXceiverServerRatis(
      DatanodeDetails dn, OzoneConfiguration conf) throws IOException {
    conf.setInt(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT,
        dn.getPort(DatanodeDetails.Port.Name.RATIS).getValue());
    final String dir = TEST_DIR + dn.getUuid();
    conf.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, dir);

    final ContainerDispatcher dispatcher = new TestContainerDispatcher();
    return XceiverServerRatis.newXceiverServerRatis(dn, conf, dispatcher);
  }

  static void initXceiverServerRatis(
      RpcType rpc, DatanodeDetails dd, Pipeline pipeline) throws IOException {
    final RaftPeer p = RatisHelper.toRaftPeer(dd);
    final RaftClient client = RatisHelper.newRaftClient(rpc, p);
    RaftGroupId groupId = pipeline.getId().getRaftGroupID();
    client.reinitialize(RatisHelper.newRaftGroup(groupId,
        pipeline.getMachines()), p.getId());
  }

  private static class TestContainerDispatcher implements ContainerDispatcher {
    /**
     * Dispatches commands to container layer.
     *
     * @param msg - Command Request
     * @return Command Response
     */
    @Override
    public ContainerCommandResponseProto dispatch(
        ContainerCommandRequestProto msg) {
      return ContainerTestHelper.getCreateContainerResponse(msg);
    }

    @Override
    public void init() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public Handler getHandler(ContainerProtos.ContainerType containerType) {
      return null;
    }

    @Override
    public void setScmId(String scmId) {

    }
  }
}
