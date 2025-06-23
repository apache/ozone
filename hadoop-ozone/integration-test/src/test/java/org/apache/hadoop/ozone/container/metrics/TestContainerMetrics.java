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

package org.apache.hadoop.ozone.container.metrics;

import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.apache.ozone.test.MetricsAsserts.assertCounter;
import static org.apache.ozone.test.MetricsAsserts.assertQuantileGauges;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.apache.ratis.rpc.SupportedRpcType.GRPC;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerGrpc;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.apache.ratis.util.function.CheckedConsumer;
import org.apache.ratis.util.function.CheckedFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for metrics published by storage containers.
 */
public class TestContainerMetrics {
  @TempDir
  private static Path testDir;
  @TempDir
  private Path tempDir;
  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static final int DFS_METRICS_PERCENTILES_INTERVALS = 1;
  private static VolumeChoosingPolicy volumeChoosingPolicy;

  @BeforeAll
  public static void setup() {
    DefaultMetricsSystem.setMiniClusterMode(true);
    CONF.setInt(HddsConfigKeys.HDDS_METRICS_PERCENTILES_INTERVALS_KEY,
        DFS_METRICS_PERCENTILES_INTERVALS);
    CONF.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, false);
    CONF.set(OzoneConfigKeys.OZONE_METADATA_DIRS, testDir.toString());
    volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(CONF);
  }

  @AfterEach
  public void cleanUp() throws IOException {
    FileUtils.deleteQuietly(new File(CONF.get(ScmConfigKeys.HDDS_DATANODE_DIR_KEY)));
    FileUtils.deleteQuietly(CONF.get(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR) == null ?
        null : new File(CONF.get(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR)));
  }

  @Test
  public void testContainerMetrics() throws Exception {
    runTestClientServer(pipeline -> CONF
            .setInt(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
                pipeline.getFirstNode()
                    .getStandalonePort().getValue()),
        pipeline -> new XceiverClientGrpc(pipeline, CONF),
        (dn, volumeSet) -> new XceiverServerGrpc(dn, CONF,
            createDispatcher(dn, volumeSet), null), (dn, p) -> {
        });
  }

  @Test
  public void testContainerMetricsRatis() throws Exception {
    runTestClientServer(
        pipeline -> RatisTestHelper.initRatisConf(GRPC, CONF),
        pipeline -> XceiverClientRatis.newXceiverClientRatis(pipeline, CONF),
        this::newXceiverServerRatis, (dn, p) ->
            RatisTestHelper.initXceiverServerRatis(GRPC, dn, p));
  }

  private static MutableVolumeSet createVolumeSet(DatanodeDetails dn, String path) throws IOException {
    CONF.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, path);
    return new MutableVolumeSet(
        dn.getUuidString(), CONF,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);
  }

  private HddsDispatcher createDispatcher(DatanodeDetails dd, VolumeSet volumeSet) {
    ContainerSet containerSet = newContainerSet();
    StateContext context = ContainerTestUtils.getMockContext(
        dd, CONF);
    ContainerMetrics metrics = ContainerMetrics.create(CONF);
    Map<ContainerProtos.ContainerType, Handler> handlers = Maps.newHashMap();
    for (ContainerProtos.ContainerType containerType :
        ContainerProtos.ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(containerType, CONF,
              context.getParent().getDatanodeDetails().getUuidString(),
              containerSet, volumeSet, volumeChoosingPolicy, metrics,
              c -> { }, new ContainerChecksumTreeManager(CONF)));
    }
    HddsDispatcher dispatcher = new HddsDispatcher(CONF, containerSet,
        volumeSet, handlers, context, metrics, null);
    StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())
        .forEach(hddsVolume -> hddsVolume.setDbParentDir(tempDir.toFile()));
    dispatcher.setClusterId(UUID.randomUUID().toString());
    return dispatcher;
  }

  static void runTestClientServer(
      CheckedConsumer<Pipeline, IOException> initConf,
      CheckedFunction<Pipeline, XceiverClientSpi,
                IOException> createClient,
      CheckedBiFunction<DatanodeDetails, MutableVolumeSet, XceiverServerSpi,
          IOException> createServer,
      CheckedBiConsumer<DatanodeDetails, Pipeline, IOException> initServer)
      throws Exception {
    XceiverServerSpi server = null;
    XceiverClientSpi client = null;
    long containerID = ContainerTestHelper.getTestContainerID();
    MutableVolumeSet volumeSet = null;

    try {
      final Pipeline pipeline =
          MockPipeline.createSingleNodePipeline();
      initConf.accept(pipeline);

      DatanodeDetails dn = pipeline.getFirstNode();
      volumeSet = createVolumeSet(dn, testDir.resolve(dn.getUuidString()).toString());
      server = createServer.apply(dn, volumeSet);
      server.start();
      initServer.accept(dn, pipeline);

      client = createClient.apply(pipeline);
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
      if (volumeSet != null) {
        volumeSet.shutdown();
      }
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.stop();
      }
    }
  }

  private XceiverServerSpi newXceiverServerRatis(DatanodeDetails dn, MutableVolumeSet volumeSet)
      throws IOException {
    CONF.setInt(OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_PORT,
        dn.getRatisPort().getValue());
    final String dir = testDir.resolve(dn.getUuidString()).toString();
    CONF.set(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, dir);
    final ContainerDispatcher dispatcher = createDispatcher(dn,
        volumeSet);
    return XceiverServerRatis.newXceiverServerRatis(null, dn, CONF, dispatcher,
        new ContainerController(newContainerSet(), Maps.newHashMap()),
        null, null);
  }
}
