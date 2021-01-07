/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.metrics;

import java.io.File;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.impl.TestHddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerGrpc;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.test.GenericTestUtils;

import com.google.common.collect.Maps;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertQuantileGauges;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

/**
 * Test for metrics published by storage containers.
 */
public class TestContainerMetrics {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = new Timeout(300000);

  @Test
  public void testContainerMetrics() throws Exception {
    XceiverServerGrpc server = null;
    XceiverClientGrpc client = null;
    long containerID = ContainerTestHelper.getTestContainerID();
    String path = GenericTestUtils.getRandomizedTempPath();

    try {
      final int interval = 1;
      Pipeline pipeline = MockPipeline
          .createSingleNodePipeline();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          pipeline.getFirstNode()
              .getPort(DatanodeDetails.Port.Name.STANDALONE).getValue());
      conf.setInt(DFSConfigKeysLegacy.DFS_METRICS_PERCENTILES_INTERVALS_KEY,
          interval);

      DatanodeDetails datanodeDetails = randomDatanodeDetails();
      conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, path);
      VolumeSet volumeSet = new MutableVolumeSet(
          datanodeDetails.getUuidString(), conf);
      ContainerSet containerSet = new ContainerSet();
      DatanodeStateMachine stateMachine = Mockito.mock(
          DatanodeStateMachine.class);
      StateContext context = Mockito.mock(StateContext.class);
      Mockito.when(stateMachine.getDatanodeDetails())
          .thenReturn(datanodeDetails);
      Mockito.when(context.getParent()).thenReturn(stateMachine);
      ContainerMetrics metrics = ContainerMetrics.create(conf);
      Map<ContainerProtos.ContainerType, Handler> handlers = Maps.newHashMap();
      for (ContainerProtos.ContainerType containerType :
          ContainerProtos.ContainerType.values()) {
        handlers.put(containerType,
            Handler.getHandlerForContainerType(containerType, conf,
                context.getParent().getDatanodeDetails().getUuidString(),
                containerSet, volumeSet, metrics,
                TestHddsDispatcher.NO_OP_ICR_SENDER));
      }
      HddsDispatcher dispatcher = new HddsDispatcher(conf, containerSet,
          volumeSet, handlers, context, metrics, null);
      dispatcher.setScmId(UUID.randomUUID().toString());

      server = new XceiverServerGrpc(datanodeDetails, conf, dispatcher, null);
      client = new XceiverClientGrpc(pipeline, conf);

      server.start();
      client.connect();

      // Create container
      ContainerCommandRequestProto request = ContainerTestHelper
          .getCreateContainerRequest(containerID, pipeline);
      ContainerCommandResponseProto response = client.sendCommand(request);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      // Write Chunk
      BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
      ContainerTestHelper.getWriteChunkRequest(
          pipeline, blockID, 1024, null);
      ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
          ContainerTestHelper.getWriteChunkRequest(
              pipeline, blockID, 1024, null);
      response = client.sendCommand(writeChunkRequest);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      //Read Chunk
      ContainerProtos.ContainerCommandRequestProto readChunkRequest =
          ContainerTestHelper.getReadChunkRequest(pipeline, writeChunkRequest
              .getWriteChunk());
      response = client.sendCommand(readChunkRequest);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());

      MetricsRecordBuilder containerMetrics = getMetrics(
          "StorageContainerMetrics");
      assertCounter("NumOps", 3L, containerMetrics);
      assertCounter("numCreateContainer", 1L, containerMetrics);
      assertCounter("numWriteChunk", 1L, containerMetrics);
      assertCounter("numReadChunk", 1L, containerMetrics);
      assertCounter("bytesWriteChunk", 1024L, containerMetrics);
      assertCounter("bytesReadChunk", 1024L, containerMetrics);

      String sec = interval + "s";
      Thread.sleep((interval + 1) * 1000);
      assertQuantileGauges("WriteChunkNanos" + sec, containerMetrics);

      // Check VolumeIOStats metrics
      HddsVolume hddsVolume = volumeSet.getVolumesList().get(0);
      MetricsRecordBuilder volumeIOMetrics =
          getMetrics(hddsVolume.getVolumeIOStats().getMetricsSourceName());
      assertCounter("ReadBytes", 1024L, volumeIOMetrics);
      assertCounter("ReadOpCount", 1L, volumeIOMetrics);
      assertCounter("WriteBytes", 1024L, volumeIOMetrics);
      assertCounter("WriteOpCount", 1L, volumeIOMetrics);

    } finally {
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.stop();
      }
      // clean up volume dir
      File file = new File(path);
      if(file.exists()) {
        FileUtil.fullyDelete(file);
      }
    }
  }
}