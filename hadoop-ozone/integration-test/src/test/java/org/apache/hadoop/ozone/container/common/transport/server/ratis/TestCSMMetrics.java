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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.apache.ozone.test.MetricsAsserts.assertCounter;
import static org.apache.ozone.test.MetricsAsserts.getDoubleGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.apache.ratis.rpc.SupportedRpcType.GRPC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class tests the metrics of ContainerStateMachine.
 */
public class TestCSMMetrics {
  @TempDir
  private static Path testDir;

  @BeforeAll
  public static void setup() {
    ExitUtils.disableSystemExit();
  }

  @Test
  public void testContainerStateMachineMetrics() throws Exception {
    runContainerStateMachineMetrics(1,
        (pipeline, conf) -> RatisTestHelper.initRatisConf(GRPC, conf),
        XceiverClientRatis::newXceiverClientRatis,
        TestCSMMetrics::newXceiverServerRatis,
        (dn, p) -> RatisTestHelper.initXceiverServerRatis(GRPC, dn, p));
  }

  static void runContainerStateMachineMetrics(
      int numDatanodes,
      BiConsumer<Pipeline, OzoneConfiguration> initConf,
      CheckedBiFunction<Pipeline, OzoneConfiguration,
                XceiverClientSpi, IOException> createClient,
      CheckedBiFunction<DatanodeDetails, OzoneConfiguration,
          XceiverServerSpi, IOException> createServer,
      CheckedBiConsumer<DatanodeDetails, Pipeline, IOException> initServer)
      throws Exception {
    final List<XceiverServerSpi> servers = new ArrayList<>();
    XceiverClientSpi client = null;
    try {
      final Pipeline pipeline = MockPipeline.createPipeline(numDatanodes);
      final OzoneConfiguration conf = new OzoneConfiguration();
      initConf.accept(pipeline, conf);

      for (DatanodeDetails dn : pipeline.getNodes()) {
        final XceiverServerSpi s = createServer.apply(dn, conf);
        servers.add(s);
        s.start();
        initServer.accept(dn, pipeline);
      }

      client = createClient.apply(pipeline, conf);
      client.connect();

      // Before Read Chunk/Write Chunk
      MetricsRecordBuilder metric = getMetrics(CSMMetrics.SOURCE_NAME +
          RaftGroupId.valueOf(pipeline.getId().getId()));
      assertCounter("NumWriteStateMachineOps", 0L, metric);
      assertCounter("NumReadStateMachineOps", 0L, metric);
      assertCounter("NumApplyTransactionOps", 0L, metric);
      assertCounter("NumBytesWrittenCount", 0L, metric);
      assertCounter("NumBytesCommittedCount", 0L, metric);
      assertCounter("NumStartTransactionVerifyFailures", 0L, metric);
      assertCounter("NumContainerNotOpenVerifyFailures", 0L, metric);
      assertCounter("WriteChunkMsNumOps", 0L, metric);
      double applyTransactionLatency = getDoubleGauge(
          "ApplyTransactionNsAvgTime", metric);
      assertEquals(0.0, applyTransactionLatency, 0.0);
      double writeStateMachineLatency = getDoubleGauge(
          "WriteStateMachineDataNsAvgTime", metric);
      assertEquals(0.0, writeStateMachineLatency, 0.0);

      // Write Chunk
      BlockID blockID = ContainerTestHelper.getTestBlockID(ContainerTestHelper.
          getTestContainerID());
      ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
          ContainerTestHelper.getWriteChunkRequest(
              pipeline, blockID, 1024);
      ContainerCommandResponseProto response =
          client.sendCommand(writeChunkRequest);
      assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      metric = getMetrics(CSMMetrics.SOURCE_NAME +
          RaftGroupId.valueOf(pipeline.getId().getId()));
      assertCounter("NumWriteStateMachineOps", 1L, metric);
      assertCounter("NumBytesWrittenCount", 1024L, metric);
      assertCounter("NumApplyTransactionOps", 1L, metric);
      assertCounter("NumBytesCommittedCount", 1024L, metric);
      assertCounter("NumStartTransactionVerifyFailures", 0L, metric);
      assertCounter("NumContainerNotOpenVerifyFailures", 0L, metric);
      assertCounter("WriteChunkMsNumOps", 1L, metric);

      applyTransactionLatency = getDoubleGauge(
          "ApplyTransactionNsAvgTime", metric);
      assertThat(applyTransactionLatency).isGreaterThan(0.0);
      writeStateMachineLatency = getDoubleGauge(
          "WriteStateMachineDataNsAvgTime", metric);
      assertThat(writeStateMachineLatency).isGreaterThan(0.0);


      //Read Chunk
      ContainerProtos.ContainerCommandRequestProto readChunkRequest =
          ContainerTestHelper.getReadChunkRequest(pipeline, writeChunkRequest
              .getWriteChunk());
      response = client.sendCommand(readChunkRequest);
      assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      metric = getMetrics(CSMMetrics.SOURCE_NAME +
          RaftGroupId.valueOf(pipeline.getId().getId()));
      assertCounter("NumQueryStateMachineOps", 1L, metric);
      assertCounter("NumApplyTransactionOps", 1L, metric);

    } finally {
      if (client != null) {
        client.close();
      }
      servers.forEach(XceiverServerSpi::stop);
    }
  }

  static XceiverServerRatis newXceiverServerRatis(
      DatanodeDetails dn, OzoneConfiguration conf) throws IOException {
    conf.setInt(OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_PORT,
        dn.getRatisPort().getValue());
    final String dir = testDir.resolve(dn.getUuidString()).toString();
    conf.set(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, dir);

    final ContainerDispatcher dispatcher = new TestContainerDispatcher();
    return XceiverServerRatis.newXceiverServerRatis(null, dn, conf, dispatcher,
        new ContainerController(newContainerSet(), Maps.newHashMap()),
        null, null);
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
        ContainerCommandRequestProto msg,
        DispatcherContext context) {
      return ContainerTestHelper.getCreateContainerResponse(msg);
    }

    @Override
    public void validateContainerCommand(
        ContainerCommandRequestProto msg) {
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
    public void setClusterId(String scmId) {

    }

    @Override
    public void buildMissingContainerSetAndValidate(
        Map<Long, Long> container2BCSIDMap) {
    }
  }
}
