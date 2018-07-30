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

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertQuantileGauges;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServer;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.XceiverClient;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

/**
 * Test for metrics published by storage containers.
 */
public class TestContainerMetrics {

  @Test
  public void testContainerMetrics() throws Exception {
    XceiverServer server = null;
    XceiverClient client = null;
    long containerID = ContainerTestHelper.getTestContainerID();
    String path = GenericTestUtils.getRandomizedTempPath();

    try {
      final int interval = 1;
      Pipeline pipeline = ContainerTestHelper
          .createSingleNodePipeline();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          pipeline.getLeader()
              .getPort(DatanodeDetails.Port.Name.STANDALONE).getValue());
      conf.setInt(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY,
          interval);

      DatanodeDetails datanodeDetails = TestUtils.randomDatanodeDetails();
      conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, path);
      VolumeSet volumeSet = new VolumeSet(
          datanodeDetails.getUuidString(), conf);
      ContainerSet containerSet = new ContainerSet();
      HddsDispatcher dispatcher = new HddsDispatcher(conf, containerSet,
          volumeSet, null);
      dispatcher.setScmId(UUID.randomUUID().toString());

      server = new XceiverServer(datanodeDetails, conf, dispatcher);
      client = new XceiverClient(pipeline, conf);

      server.start();
      client.connect();

      // Create container
      ContainerCommandRequestProto request = ContainerTestHelper
          .getCreateContainerRequest(containerID, pipeline);
      ContainerCommandResponseProto response = client.sendCommand(request);
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      // Write Chunk
      BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
      ContainerTestHelper.getWriteChunkRequest(
          pipeline, blockID, 1024);
      ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
          ContainerTestHelper.getWriteChunkRequest(
              pipeline, blockID, 1024);
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