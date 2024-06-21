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
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerGrpc;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.ozone.test.GenericTestUtils;

import com.google.common.collect.Maps;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.ozone.test.MetricsAsserts.assertCounter;
import static org.apache.ozone.test.MetricsAsserts.assertQuantileGauges;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ratis.util.function.CheckedBiConsumer;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.xml.crypto.Data;

/**
 * Test for metrics published by storage containers.
 */
@Timeout(300)
public class TestContainerMetrics {
  @TempDir
  private Path tempDir;
  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static final int DFS_METRICS_PERCENTILES_INTERVALS = 1;

  @Test
  public void testContainerMetrics() throws Exception {
    DatanodeDetails dd = randomDatanodeDetails();
    String path = GenericTestUtils.getRandomizedTempPath();
    MutableVolumeSet volumeSet = createVolumeSet(dd, path);
    HddsDispatcher hddsDispatcher = createDispatcher(dd, CONF, volumeSet);
    runTestClientServer(volumeSet, (pipeline, conf) -> conf
            .setInt(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
                pipeline.getFirstNode()
                    .getPort(DatanodeDetails.Port.Name.STANDALONE).getValue()),
        XceiverClientGrpc::new,
        (dn, conf) -> new XceiverServerGrpc(dd, conf,
            hddsDispatcher, null), (dn, p) -> {
        });
  }

  private MutableVolumeSet createVolumeSet(DatanodeDetails dn, String path) throws IOException {
    CONF.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, path);
    CONF.set(OzoneConfigKeys.OZONE_METADATA_DIRS, path);
    return new MutableVolumeSet(
        dn.getUuidString(), CONF,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);
  }

  private HddsDispatcher createDispatcher(DatanodeDetails dd,
                                          OzoneConfiguration conf,
                                          VolumeSet volumeSet) {
    conf.setInt(DFSConfigKeysLegacy.DFS_METRICS_PERCENTILES_INTERVALS_KEY,
        DFS_METRICS_PERCENTILES_INTERVALS);
    ContainerSet containerSet = new ContainerSet(1000);
    StateContext context = ContainerTestUtils.getMockContext(
        dd, conf);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    Map<ContainerProtos.ContainerType, Handler> handlers = Maps.newHashMap();
    for (ContainerProtos.ContainerType containerType :
        ContainerProtos.ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(containerType, conf,
              context.getParent().getDatanodeDetails().getUuidString(),
              containerSet, volumeSet, metrics,
              c -> { }));
    }
    HddsDispatcher dispatcher = new HddsDispatcher(conf, containerSet,
        volumeSet, handlers, context, metrics, null);
    StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())
        .forEach(hddsVolume -> hddsVolume.setDbParentDir(tempDir.toFile()));
    dispatcher.setClusterId(UUID.randomUUID().toString());
    return dispatcher;
  }

  static void runTestClientServer(
      MutableVolumeSet volumeSet,
      CheckedBiConsumer<Pipeline, OzoneConfiguration, IOException> initConf,
      CheckedBiFunction<Pipeline, OzoneConfiguration, XceiverClientSpi,
          IOException> createClient,
      CheckedBiFunction<DatanodeDetails, OzoneConfiguration, XceiverServerSpi,
          IOException> createServer,
      CheckedBiConsumer<DatanodeDetails, Pipeline, IOException> initServer)
      throws Exception {
    final List<XceiverServerSpi> servers = new ArrayList<>();
    XceiverClientSpi client = null;
    long containerID = ContainerTestHelper.getTestContainerID();

    try {
      final Pipeline pipeline =
          MockPipeline.createSingleNodePipeline();
      initConf.accept(pipeline, CONF);

      for (DatanodeDetails dn : pipeline.getNodes()) {
        final XceiverServerSpi s = createServer.apply(dn, CONF);
        servers.add(s);
        s.start();
        initServer.accept(dn, pipeline);
      }

      client = createClient.apply(pipeline, CONF);
      client.connect();

      // Write Chunk
      BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
      final ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
          ContainerTestHelper.getWriteChunkRequest(
              pipeline, blockID, 1024);
      ContainerCommandResponseProto response = client.sendCommand(writeChunkRequest);
      assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      //Read Chunk
      final ContainerProtos.ContainerCommandRequestProto readChunkRequest =
          ContainerTestHelper.getReadChunkRequest(pipeline, writeChunkRequest
              .getWriteChunk());
      response = client.sendCommand(readChunkRequest);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());

      MetricsRecordBuilder containerMetrics = getMetrics(
          "StorageContainerMetrics");
      assertCounter("NumOps", 3L, containerMetrics);
      assertCounter("numCreateContainer", 1L, containerMetrics);
      assertCounter("numWriteChunk", 1L, containerMetrics);
      assertCounter("numReadChunk", 1L, containerMetrics);
      assertCounter("bytesWriteChunk", 1024L, containerMetrics);
      assertCounter("bytesReadChunk", 1024L, containerMetrics);

      String sec = DFS_METRICS_PERCENTILES_INTERVALS + "s";
      Thread.sleep((DFS_METRICS_PERCENTILES_INTERVALS + 1) * 1000);
      assertQuantileGauges("WriteChunkNanos" + sec, containerMetrics);

      // Check VolumeIOStats metrics
      List<HddsVolume> volumes =
          StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList());
      HddsVolume hddsVolume = volumes.get(0);
      MetricsRecordBuilder volumeIOMetrics =
          getMetrics(hddsVolume.getVolumeIOStats().getMetricsSourceName());
      assertCounter("ReadBytes", 1024L, volumeIOMetrics);
      assertCounter("ReadOpCount", 1L, volumeIOMetrics);
      assertCounter("WriteBytes", 1024L, volumeIOMetrics);
      assertCounter("WriteOpCount", 1L, volumeIOMetrics);

    } finally {
      ContainerMetrics.remove();
      volumeSet.shutdown();
      if (client != null) {
        client.close();
      }
      servers.forEach(XceiverServerSpi::stop);
      // clean up volume dir
      File file = new File("");
      if (file.exists()) {
        FileUtil.fullyDelete(file);
      }
    }
  }

}
