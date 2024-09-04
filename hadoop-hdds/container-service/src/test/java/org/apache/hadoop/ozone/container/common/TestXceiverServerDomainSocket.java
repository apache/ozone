/*
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
package org.apache.hadoop.ozone.container.common;

import com.google.common.collect.Maps;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.DomainSocketFactory;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerDomainSocket;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the XceiverServerDomainSocket class.
 */
public class TestXceiverServerDomainSocket {
  // Add "-Djava.library.path=${native_lib_path}" to intellij run configuration to run it locally
  // Dynamically set the java.library.path in java code doesn't affect the library loading
  @Test
  @Ignore
  public void test() {
    // enable short-circuit read
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OzoneClientConfig.OZONE_READ_SHORT_CIRCUIT, true);
    OzoneClientConfig ozoneClientConfig = conf.getObject(OzoneClientConfig.class);
    ozoneClientConfig.setShortCircuitReadDisableInterval(1);
    conf.setFromObject(ozoneClientConfig);
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, "/Users/sammi/ozone_dn_socket");

    // create DomainSocketFactory
    DomainSocketFactory domainSocketFactory = DomainSocketFactory.getInstance(conf);
    assertTrue(conf.getBoolean(OzoneClientConfig.OZONE_READ_SHORT_CIRCUIT,
        OzoneClientConfig.OZONE_READ_SHORT_CIRCUIT_DEFAULT));
    assertTrue(domainSocketFactory.isServiceEnabled());
    assertTrue(domainSocketFactory.isServiceReady());

    XceiverServerDomainSocket serverDomainSocket = null;
    String path = GenericTestUtils.getRandomizedTempPath();
    System.out.println("Randomized temp path " + path);
    try {
      ThreadPoolExecutor readExecutors = new ThreadPoolExecutor(1, 1,
          60, TimeUnit.SECONDS,
          new LinkedBlockingQueue<>());
      ContainerMetrics metrics = ContainerMetrics.create(conf);
      serverDomainSocket = new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
          conf, null, readExecutors, metrics, domainSocketFactory);
      serverDomainSocket.setContainerDispatcher(
          createDispatcherAndPrepareData(conf, path, serverDomainSocket, metrics));
      serverDomainSocket.start();
      Thread.sleep(600 * 1000);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      if (domainSocketFactory != null) {
        domainSocketFactory.close();
      }
      if (serverDomainSocket != null) {
        serverDomainSocket.stop();
      }
      try {
        FileUtils.deleteDirectory(new File(path));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private ContainerDispatcher createDispatcherAndPrepareData(OzoneConfiguration conf, String path,
      XceiverServerDomainSocket domainSocketServer, ContainerMetrics metrics) throws IOException {
    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, path);
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, path);
    VolumeSet volumeSet = new MutableVolumeSet(datanodeDetails.getUuidString(), conf,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);
    String cID = UUID.randomUUID().toString();
    HddsVolume dataVolume = (HddsVolume) volumeSet.getVolumesList().get(0);
    dataVolume.format(cID);
    dataVolume.setDbParentDir(Paths.get(path).toFile());
    assertTrue(dataVolume.getDbParentDir() != null);
    ContainerSet containerSet = new ContainerSet(1000);

    // create HddsDispatcher
    StateContext context = ContainerTestUtils.getMockContext(datanodeDetails, conf);
    Map<ContainerProtos.ContainerType, Handler> handlers = Maps.newHashMap();
    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getReadDomainSocketChannel()).thenReturn(domainSocketServer);
    for (ContainerProtos.ContainerType containerType :
        ContainerProtos.ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(containerType, conf,
              context.getParent().getDatanodeDetails().getUuidString(),
              containerSet, volumeSet, metrics,
              c -> { }, ozoneContainer));
    }
    HddsDispatcher dispatcher = new HddsDispatcher(conf, containerSet, volumeSet, handlers, context, metrics, null);
    dispatcher.setClusterId(cID);
    // create container
    long value = 1L;
    String pipelineID = UUID.randomUUID().toString();
    final ContainerProtos.ContainerCommandRequestProto createContainer =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CreateContainer)
            .setDatanodeUuid(datanodeDetails.getUuidString()).setCreateContainer(
                ContainerProtos.CreateContainerRequestProto.newBuilder()
                    .setContainerType(ContainerProtos.ContainerType.KeyValueContainer).build())
            .setContainerID(value).setPipelineID(pipelineID)
            .build();
    dispatcher.dispatch(createContainer, null);

    // write chunk
    long id = 1;
    int chunkSize = 1024 * 1024;
    byte[] rawData = RandomStringUtils.randomAscii(chunkSize).getBytes(StandardCharsets.UTF_8);
    Checksum checksum = new Checksum(ContainerProtos.ChecksumType.CRC32, chunkSize);
    ContainerProtos.ChecksumData checksumProtobuf = checksum.computeChecksum(rawData).getProtoBufMessage();
    ContainerProtos.DatanodeBlockID blockId = ContainerProtos.DatanodeBlockID.newBuilder()
        .setContainerID(id).setLocalID(id).setBlockCommitSequenceId(id).build();
    ContainerProtos.BlockData.Builder blockData = ContainerProtos.BlockData.newBuilder().setBlockID(blockId);
    ContainerProtos.ChunkInfo.Builder chunkInfo = ContainerProtos.ChunkInfo.newBuilder()
        .setChunkName("chunk_" + value).setOffset(0).setLen(chunkSize).setChecksumData(checksumProtobuf);
    blockData.addChunks(chunkInfo);
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    ContainerProtos.WriteChunkRequestProto.Builder writeChunk =
        ContainerProtos.WriteChunkRequestProto.newBuilder()
            .setBlockID(blockId).setChunkData(chunkInfo)
            .setData(ChunkBuffer.wrap(ByteBuffer.wrap(rawData)).toByteString());

    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.WriteChunk)
            .setContainerID(blockId.getContainerID())
            .setWriteChunk(writeChunk)
            .setDatanodeUuid(pipeline.getFirstNode().getUuidString()).build();
    dispatcher.dispatch(writeChunkRequest, null);

    ContainerProtos.PutBlockRequestProto.Builder putBlock = ContainerProtos.PutBlockRequestProto
        .newBuilder().setBlockData(blockData);
    ContainerProtos.ContainerCommandRequestProto putBlockRequest =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.PutBlock)
            .setContainerID(blockId.getContainerID())
            .setDatanodeUuid(datanodeDetails.getUuidString())
            .setPutBlock(putBlock)
            .build();

    dispatcher.dispatch(putBlockRequest, null);
    return dispatcher;
  }
}
