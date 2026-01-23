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

package org.apache.hadoop.ozone.container.common.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.fs.MockSpaceUsagePersistence.inMemory;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.hdds.scm.protocolPB.ContainerCommandResponseBuilders.getContainerCommandResponse;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.COMMIT_STAGE;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.MockSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProtoOrBuilder;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.security.token.TokenVerifier;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.report.IncrementalReportSender;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext.Op;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext.WriteChunkStage;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test-cases to verify the functionality of HddsDispatcher.
 */
public class TestHddsDispatcher {
  @TempDir
  private Path tempDir;

  private static final Logger LOG = LoggerFactory.getLogger(
      TestHddsDispatcher.class);
  @TempDir
  private File testDir;

  private static VolumeChoosingPolicy volumeChoosingPolicy;

  public static final IncrementalReportSender<Container> NO_OP_ICR_SENDER =
      c -> {
      };

  @BeforeAll
  public static void init() {
    volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(new OzoneConfiguration());
  }

  /**
   * Tests that close container action is sent when a container is full. First two containers are created. Then we
   * write to one of them to confirm normal writes are successful. Then we increase the used space of both containers
   * such that they're close to full, and write to both of them simultaneously. The expectation is that close
   * container action should be added for both of them and two immediate heartbeats should be sent. Next, we write
   * again to the first container. This time the close container action should be queued but immediate heartbeat
   * should not be sent because of throttling. This confirms that the throttling is per container.
   * @param layout
   * @throws IOException
   */
  @ContainerLayoutTestInfo.ContainerTest
  public void testContainerCloseActionWhenFull(
      ContainerLayoutVersion layout) throws IOException {

    String testDirPath = testDir.getPath();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, testDirPath);
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, testDirPath);
    DatanodeDetails dd = randomDatanodeDetails();
    MutableVolumeSet volumeSet = new MutableVolumeSet(dd.getUuidString(), "test", conf,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);
    volumeSet.getVolumesList().forEach(e -> e.setState(StorageVolume.VolumeState.NORMAL));
    volumeSet.startAllVolume();

    try {
      UUID scmId = UUID.randomUUID();
      ContainerSet containerSet = newContainerSet();
      StateContext context = ContainerTestUtils.getMockContext(dd, conf);
      // create both containers
      KeyValueContainerData containerData = new KeyValueContainerData(1L,
          layout,
          (long) StorageUnit.GB.toBytes(1), UUID.randomUUID().toString(),
          dd.getUuidString());
      KeyValueContainerData containerData2 = new KeyValueContainerData(2L,
          layout, (long) StorageUnit.GB.toBytes(1), UUID.randomUUID().toString(), dd.getUuidString());
      Container container = new KeyValueContainer(containerData, conf);
      Container container2 = new KeyValueContainer(containerData2, conf);
      StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())
          .forEach(hddsVolume -> hddsVolume.setDbParentDir(tempDir.toFile()));
      container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(),
          scmId.toString());
      container2.create(volumeSet, new RoundRobinVolumeChoosingPolicy(),
          scmId.toString());
      containerSet.addContainer(container);
      containerSet.addContainer(container2);
      ContainerMetrics metrics = ContainerMetrics.create(conf);
      Map<ContainerType, Handler> handlers = Maps.newHashMap();
      for (ContainerType containerType : ContainerType.values()) {
        handlers.put(containerType, Handler.getHandlerForContainerType(containerType, conf,
            context.getParent().getDatanodeDetails().getUuidString(),
            containerSet, volumeSet, volumeChoosingPolicy, metrics, NO_OP_ICR_SENDER,
            new ContainerChecksumTreeManager(conf)));
      }
      // write successfully to first container
      HddsDispatcher hddsDispatcher = new HddsDispatcher(
          conf, containerSet, volumeSet, handlers, context, metrics, null);
      hddsDispatcher.setClusterId(scmId.toString());
      ContainerCommandResponseProto responseOne = hddsDispatcher
          .dispatch(getWriteChunkRequest(dd.getUuidString(), 1L, 1L), null);
      assertEquals(ContainerProtos.Result.SUCCESS,
          responseOne.getResult());
      verify(context, times(0))
          .addContainerActionIfAbsent(any(ContainerAction.class));
      // increment used space of both containers
      containerData.getStatistics().setBlockBytesForTesting(Double.valueOf(
          StorageUnit.MB.toBytes(950)).longValue());
      containerData2.getStatistics().setBlockBytesForTesting(Double.valueOf(
          StorageUnit.MB.toBytes(950)).longValue());
      ContainerCommandResponseProto responseTwo = hddsDispatcher
          .dispatch(getWriteChunkRequest(dd.getUuidString(), 1L, 2L), null);
      ContainerCommandResponseProto responseThree = hddsDispatcher
          .dispatch(getWriteChunkRequest(dd.getUuidString(), 2L, 1L), null);
      assertEquals(ContainerProtos.Result.SUCCESS,
          responseTwo.getResult());
      assertEquals(ContainerProtos.Result.SUCCESS, responseThree.getResult());
      // container action should be added for both containers
      verify(context, times(2))
          .addContainerActionIfAbsent(any(ContainerAction.class));
      DatanodeStateMachine stateMachine = context.getParent();
      // immediate heartbeat should be triggered for both the containers
      verify(stateMachine, times(2)).triggerHeartbeat();

      // if we write again to container 1, the container action should get added but heartbeat should not get triggered
      // again because of throttling
      hddsDispatcher.dispatch(getWriteChunkRequest(dd.getUuidString(), 1L, 3L), null);
      verify(context, times(3)).addContainerActionIfAbsent(any(ContainerAction.class));
      verify(stateMachine, times(2)).triggerHeartbeat(); // was called twice before
    } finally {
      volumeSet.shutdown();
      ContainerMetrics.remove();
    }
  }

  @Test
  public void testSmallFileChecksum() throws IOException {
    String testDirPath = testDir.getPath();
    try {
      UUID scmId = UUID.randomUUID();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(HDDS_DATANODE_DIR_KEY, testDirPath);
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, testDirPath);
      DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
      dnConf.setChunkDataValidationCheck(true);
      conf.setFromObject(dnConf);
      DatanodeDetails dd = randomDatanodeDetails();
      HddsDispatcher hddsDispatcher = createDispatcher(dd, scmId, conf);

      ContainerCommandResponseProto smallFileResponse =
          hddsDispatcher.dispatch(newPutSmallFile(1L, 1L), null);

      assertEquals(ContainerProtos.Result.SUCCESS, smallFileResponse.getResult());
    } finally {
      ContainerMetrics.remove();
    }
  }

  @Test
  public void testWriteChunkChecksum() throws IOException {
    String testDirPath = testDir.getPath();
    try {
      UUID scmId = UUID.randomUUID();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(HDDS_DATANODE_DIR_KEY, testDirPath);
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, testDirPath);
      DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
      dnConf.setChunkDataValidationCheck(true);
      conf.setFromObject(dnConf);
      DatanodeDetails dd = randomDatanodeDetails();
      HddsDispatcher hddsDispatcher = createDispatcher(dd, scmId, conf);
      //Send a few WriteChunkRequests
      ContainerCommandResponseProto response;
      ContainerCommandRequestProto writeChunkRequest0 = getWriteChunkRequest0(dd.getUuidString(), 1L, 1L, 0);
      hddsDispatcher.dispatch(writeChunkRequest0, null);
      hddsDispatcher.dispatch(getWriteChunkRequest0(dd.getUuidString(), 1L, 1L, 1), null);
      response = hddsDispatcher.dispatch(getWriteChunkRequest0(dd.getUuidString(), 1L, 1L, 2), null);

      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      // Send Read Chunk request for written chunk.
      response =
          hddsDispatcher.dispatch(getReadChunkRequest(writeChunkRequest0), null);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());

      ByteString responseData = BufferUtils.concatByteStrings(
          response.getReadChunk().getDataBuffers().getBuffersList());
      assertEquals(writeChunkRequest0.getWriteChunk().getData(),
          responseData);

      // Test checksum on Read:
      final DispatcherContext context = DispatcherContext
          .newBuilder(DispatcherContext.Op.READ_STATE_MACHINE_DATA)
          .build();
      response =
          hddsDispatcher.dispatch(getReadChunkRequest(writeChunkRequest0), context);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    } finally {
      ContainerMetrics.remove();
    }
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testContainerCloseActionWhenVolumeFull(
      ContainerLayoutVersion layoutVersion) throws Exception {
    String testDirPath = testDir.getPath();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setStorageSize(DatanodeConfiguration.HDDS_DATANODE_VOLUME_MIN_FREE_SPACE,
        100.0, StorageUnit.BYTES);
    DatanodeDetails dd = randomDatanodeDetails();

    HddsVolume.Builder volumeBuilder =
        new HddsVolume.Builder(testDirPath).datanodeUuid(dd.getUuidString())
            .conf(conf).usageCheckFactory(MockSpaceUsageCheckFactory.NONE).clusterID("test");
    // state of cluster : available (160) > 100  ,datanode volume
    // utilisation threshold not yet reached. container creates are successful.
    AtomicLong usedSpace = new AtomicLong(340);
    SpaceUsageSource spaceUsage = MockSpaceUsageSource.of(500, usedSpace);

    SpaceUsageCheckFactory factory = MockSpaceUsageCheckFactory.of(
        spaceUsage, Duration.ZERO, inMemory(new AtomicLong(0)));
    volumeBuilder.usageCheckFactory(factory);
    MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);
    when(volumeSet.getVolumesList())
        .thenReturn(Collections.singletonList(volumeBuilder.build()));
    volumeSet.getVolumesList().get(0).setState(StorageVolume.VolumeState.NORMAL);
    volumeSet.getVolumesList().get(0).start();
    try {
      UUID scmId = UUID.randomUUID();
      ContainerSet containerSet = newContainerSet();
      StateContext context = ContainerTestUtils.getMockContext(dd, conf);
      DatanodeStateMachine stateMachine = context.getParent();
      // create a 50 byte container
      // available (160) > 100 (min free space) + 50 (container size)
      KeyValueContainerData containerData = new KeyValueContainerData(1L,
          layoutVersion,
          50, UUID.randomUUID().toString(),
          dd.getUuidString());
      Container container = new KeyValueContainer(containerData, conf);
      StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())
          .forEach(hddsVolume -> hddsVolume.setDbParentDir(tempDir.toFile()));
      container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(),
          scmId.toString());
      containerSet.addContainer(container);
      ContainerMetrics metrics = ContainerMetrics.create(conf);
      Map<ContainerType, Handler> handlers = Maps.newHashMap();
      for (ContainerType containerType : ContainerType.values()) {
        handlers.put(containerType,
            Handler.getHandlerForContainerType(containerType, conf,
                context.getParent().getDatanodeDetails().getUuidString(),
                containerSet, volumeSet, volumeChoosingPolicy, metrics, NO_OP_ICR_SENDER,
                new ContainerChecksumTreeManager(conf)));
      }
      HddsDispatcher hddsDispatcher = new HddsDispatcher(
          conf, containerSet, volumeSet, handlers, context, metrics, null);
      hddsDispatcher.setClusterId(scmId.toString());
      containerData.getVolume().incrementUsedSpace(60);
      usedSpace.addAndGet(60);
      ContainerCommandResponseProto response = hddsDispatcher
          .dispatch(getWriteChunkRequest(dd.getUuidString(), 1L, 1L), null);
      assertEquals(ContainerProtos.Result.DISK_OUT_OF_SPACE, response.getResult());
      verify(context, times(1))
          .addContainerActionIfAbsent(any(ContainerAction.class));
      // verify that immediate heartbeat is triggered
      verify(stateMachine, times(1)).triggerHeartbeat();
      // the volume has reached the min free space boundary but this time the heartbeat should not be triggered because
      // of throttling
      hddsDispatcher.dispatch(getWriteChunkRequest(dd.getUuidString(), 1L, 2L), null);
      verify(context, times(2)).addContainerActionIfAbsent(any(ContainerAction.class));
      verify(stateMachine, times(1)).triggerHeartbeat(); // was called once before

      // try creating another container now as the volume used has crossed
      // threshold

      KeyValueContainerData containerData2 = new KeyValueContainerData(1L,
          layoutVersion,
          50, UUID.randomUUID().toString(),
          dd.getUuidString());
      Container container2 = new KeyValueContainer(containerData2, conf);
      StorageContainerException scException =
          assertThrows(StorageContainerException.class,
              () -> container2.create(volumeSet,
                  new RoundRobinVolumeChoosingPolicy(), scmId.toString()));
      assertEquals("Container creation failed, due to disk out of space",
          scException.getMessage());
    } finally {
      volumeSet.shutdown();
      ContainerMetrics.remove();
    }
  }

  @Test
  public void testCreateContainerWithWriteChunk() throws IOException {
    String testDirPath = testDir.getPath();
    try {
      UUID scmId = UUID.randomUUID();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(HDDS_DATANODE_DIR_KEY, testDirPath);
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, testDirPath);
      DatanodeDetails dd = randomDatanodeDetails();
      HddsDispatcher hddsDispatcher = createDispatcher(dd, scmId, conf);
      ContainerCommandRequestProto writeChunkRequest =
          getWriteChunkRequest(dd.getUuidString(), 1L, 1L);
      // send read chunk request and make sure container does not exist
      ContainerCommandResponseProto response =
          hddsDispatcher.dispatch(getReadChunkRequest(writeChunkRequest), null);
      assertEquals(response.getResult(),
          ContainerProtos.Result.CONTAINER_NOT_FOUND);
      // send write chunk request without sending create container
      response = hddsDispatcher.dispatch(writeChunkRequest, null);
      // container should be created as part of write chunk request
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      // send read chunk request to read the chunk written above
      response =
          hddsDispatcher.dispatch(getReadChunkRequest(writeChunkRequest), null);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      ByteString responseData = BufferUtils.concatByteStrings(
          response.getReadChunk().getDataBuffers().getBuffersList());
      assertEquals(writeChunkRequest.getWriteChunk().getData(),
          responseData);
      // put block
      ContainerCommandRequestProto putBlockRequest =
          ContainerTestHelper.getPutBlockRequest(writeChunkRequest);
      response = hddsDispatcher.dispatch(putBlockRequest, null);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      // send list block request
      ContainerCommandRequestProto listBlockRequest =
          ContainerTestHelper.getListBlockRequest(writeChunkRequest);
      response = hddsDispatcher.dispatch(listBlockRequest, null);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      assertEquals(1, response.getListBlock().getBlockDataList().size());
      for (ContainerProtos.BlockData blockData :
          response.getListBlock().getBlockDataList()) {
        assertEquals(writeChunkRequest.getWriteChunk().getBlockID(),
            blockData.getBlockID());
        assertEquals(writeChunkRequest.getWriteChunk().getChunkData()
            .getLen(), blockData.getSize());
        assertEquals(1, blockData.getChunksCount());
      }
    } finally {
      ContainerMetrics.remove();
    }
  }

  @Test
  public void testContainerNotFoundWithCommitChunk() throws IOException {
    String testDirPath = testDir.getPath();
    try {
      UUID scmId = UUID.randomUUID();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(HDDS_DATANODE_DIR_KEY, testDirPath);
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, testDirPath);
      DatanodeDetails dd = randomDatanodeDetails();
      HddsDispatcher hddsDispatcher = createDispatcher(dd, scmId, conf);
      ContainerCommandRequestProto writeChunkRequest =
          getWriteChunkRequest(dd.getUuidString(), 1L, 1L);

      // send read chunk request and make sure container does not exist
      ContainerCommandResponseProto response =
          hddsDispatcher.dispatch(getReadChunkRequest(writeChunkRequest), null);
      assertEquals(
          ContainerProtos.Result.CONTAINER_NOT_FOUND, response.getResult());

      LogCapturer logCapturer = LogCapturer.captureLogs(HddsDispatcher.class);
      // send write chunk request without sending create container
      response = hddsDispatcher.dispatch(writeChunkRequest, COMMIT_STAGE);
      // container should not be found
      assertEquals(
          ContainerProtos.Result.CONTAINER_NOT_FOUND, response.getResult());

      assertThat(logCapturer.getOutput()).contains(
          "ContainerID " + writeChunkRequest.getContainerID()
              + " does not exist");
    } finally {
      ContainerMetrics.remove();
    }
  }

  @Test
  public void testWriteChunkWithCreateContainerFailure() throws IOException {
    String testDirPath = testDir.getPath();
    try {
      UUID scmId = UUID.randomUUID();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(HDDS_DATANODE_DIR_KEY, testDirPath);
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, testDirPath);
      DatanodeDetails dd = randomDatanodeDetails();
      HddsDispatcher hddsDispatcher = createDispatcher(dd, scmId, conf);
      ContainerCommandRequestProto writeChunkRequest = getWriteChunkRequest(
          dd.getUuidString(), 1L, 1L);

      HddsDispatcher mockDispatcher = spy(hddsDispatcher);
      ContainerCommandResponseProto.Builder builder =
          getContainerCommandResponse(writeChunkRequest,
              ContainerProtos.Result.DISK_OUT_OF_SPACE, "");
      // Return DISK_OUT_OF_SPACE response when writing chunk
      // with container creation.
      doReturn(builder.build()).when(mockDispatcher)
          .createContainer(writeChunkRequest);

      LogCapturer logCapturer = LogCapturer.captureLogs(HddsDispatcher.class);
      // send write chunk request without sending create container
      mockDispatcher.dispatch(writeChunkRequest, null);
      // verify the error log
      assertThat(logCapturer.getOutput())
          .contains("ContainerID " + writeChunkRequest.getContainerID()
              + " creation failed , Result: DISK_OUT_OF_SPACE");
    } finally {
      ContainerMetrics.remove();
    }
  }

  @Test
  public void testDuplicateWriteChunkAndPutBlockRequest() throws  IOException {
    String testDirPath = testDir.getPath();
    try {
      UUID scmId = UUID.randomUUID();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(HDDS_DATANODE_DIR_KEY, testDirPath);
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, testDirPath);
      DatanodeDetails dd = randomDatanodeDetails();
      HddsDispatcher hddsDispatcher = createDispatcher(dd, scmId, conf);
      ContainerCommandRequestProto writeChunkRequest = getWriteChunkRequest(
          dd.getUuidString(), 1L, 1L);
      //Send same WriteChunkRequest
      ContainerCommandResponseProto response;
      hddsDispatcher.dispatch(writeChunkRequest, null);
      hddsDispatcher.dispatch(writeChunkRequest, null);
      response = hddsDispatcher.dispatch(writeChunkRequest, null);

      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      // Send Read Chunk request for written chunk.
      response =
          hddsDispatcher.dispatch(getReadChunkRequest(writeChunkRequest), null);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());

      ByteString responseData = BufferUtils.concatByteStrings(
          response.getReadChunk().getDataBuffers().getBuffersList());
      assertEquals(writeChunkRequest.getWriteChunk().getData(),
          responseData);

      // Put Block
      ContainerCommandRequestProto putBlockRequest =
          ContainerTestHelper.getPutBlockRequest(writeChunkRequest);

      //Send same PutBlockRequest
      hddsDispatcher.dispatch(putBlockRequest, null);
      hddsDispatcher.dispatch(putBlockRequest, null);
      response = hddsDispatcher.dispatch(putBlockRequest, null);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());

      // Check PutBlock Data
      ContainerCommandRequestProto listBlockRequest =
          ContainerTestHelper.getListBlockRequest(writeChunkRequest);
      response = hddsDispatcher.dispatch(listBlockRequest, null);
      assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      assertEquals(1, response.getListBlock().getBlockDataList().size());
      for (ContainerProtos.BlockData blockData :
          response.getListBlock().getBlockDataList()) {
        assertEquals(writeChunkRequest.getWriteChunk().getBlockID(),
            blockData.getBlockID());
        assertEquals(writeChunkRequest.getWriteChunk().getChunkData()
            .getLen(), blockData.getSize());
        assertEquals(1, blockData.getChunksCount());
      }
    } finally {
      ContainerMetrics.remove();
    }
  }

  /**
   * Creates HddsDispatcher instance with given infos.
   * @param dd datanode detail info.
   * @param scmId UUID of scm id.
   * @param conf configuration be used.
   * @return HddsDispatcher HddsDispatcher instance.
   * @throws IOException
   */
  static HddsDispatcher createDispatcher(DatanodeDetails dd, UUID scmId,
      OzoneConfiguration conf) throws IOException {
    return createDispatcher(dd, scmId, conf, null);
  }

  static HddsDispatcher createDispatcher(DatanodeDetails dd, UUID scmId,
      OzoneConfiguration conf, TokenVerifier tokenVerifier) throws IOException {
    ContainerSet containerSet = newContainerSet();
    MutableVolumeSet volumeSet = new MutableVolumeSet(dd.getUuidString(), conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    volumeSet.getVolumesList().stream().forEach(v -> {
      try {
        v.format(scmId.toString());
        v.createWorkingDir(scmId.toString(), null);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    volumeSet.startAllVolume();
    StateContext context = ContainerTestUtils.getMockContext(dd, conf);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    Map<ContainerType, Handler> handlers = Maps.newHashMap();
    for (ContainerType containerType : ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(containerType, conf,
              context.getParent().getDatanodeDetails().getUuidString(),
              containerSet, volumeSet, volumeChoosingPolicy, metrics, NO_OP_ICR_SENDER,
              new ContainerChecksumTreeManager(conf)));
    }

    final HddsDispatcher hddsDispatcher = new HddsDispatcher(conf,
        containerSet, volumeSet, handlers, context, metrics, tokenVerifier);
    hddsDispatcher.setClusterId(scmId.toString());
    return hddsDispatcher;
  }

  // This method has to be removed once we move scm/TestUtils.java
  // from server-scm project to container-service or to common project.
  private static DatanodeDetails randomDatanodeDetails() {
    DatanodeDetails.Port containerPort = DatanodeDetails.newStandalonePort(0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newRatisPort(0);
    DatanodeDetails.Port restPort = DatanodeDetails.newRestPort(0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }

  private ContainerCommandRequestProto getWriteChunkRequest(
      String datanodeId, Long containerId, Long localId) {

    ByteString data = ByteString.copyFrom(
        UUID.randomUUID().toString().getBytes(UTF_8));
    ContainerProtos.ChunkInfo chunk = ContainerProtos.ChunkInfo
        .newBuilder()
        .setChunkName(
            DigestUtils.md5Hex("dummy-key") + "_stream_"
                + containerId + "_chunk_" + localId)
        .setOffset(0)
        .setLen(data.size())
        .setChecksumData(Checksum.getNoChecksumDataProto())
        .build();

    WriteChunkRequestProto.Builder writeChunkRequest = WriteChunkRequestProto
        .newBuilder()
        .setBlockID(new BlockID(containerId, localId)
            .getDatanodeBlockIDProtobuf())
        .setChunkData(chunk)
        .setData(data);

    return ContainerCommandRequestProto
        .newBuilder()
        .setContainerID(containerId)
        .setCmdType(ContainerProtos.Type.WriteChunk)
        .setDatanodeUuid(datanodeId)
        .setWriteChunk(writeChunkRequest)
        .build();
  }

  static ChecksumData checksum(ByteString data) {
    try {
      return new Checksum(ContainerProtos.ChecksumType.CRC32, 256)
          .computeChecksum(data.asReadOnlyByteBuffer());
    } catch (OzoneChecksumException e) {
      throw new IllegalStateException(e);
    }
  }

  private ContainerCommandRequestProto getWriteChunkRequest0(
      String datanodeId, Long containerId, Long localId, int chunkNum) {
    final int lenOfBytes = 32;
    ByteString chunkData = ByteString.copyFrom(RandomUtils.secure().randomBytes(32));

    ContainerProtos.ChunkInfo chunk = ContainerProtos.ChunkInfo
        .newBuilder()
        .setChunkName(
            DigestUtils.md5Hex("dummy-key") + "_stream_"
                + containerId + "_chunk_" + localId)
        .setOffset((long) chunkNum * lenOfBytes)
        .setLen(lenOfBytes)
        .setChecksumData(checksum(chunkData).getProtoBufMessage())
        .build();

    WriteChunkRequestProto.Builder writeChunkRequest = WriteChunkRequestProto
        .newBuilder()
        .setBlockID(new BlockID(containerId, localId)
            .getDatanodeBlockIDProtobuf())
        .setChunkData(chunk)
        .setData(chunkData);

    return ContainerCommandRequestProto
        .newBuilder()
        .setContainerID(containerId)
        .setCmdType(ContainerProtos.Type.WriteChunk)
        .setDatanodeUuid(datanodeId)
        .setWriteChunk(writeChunkRequest)
        .build();
  }

  static ContainerCommandRequestProto newPutSmallFile(Long containerId, Long localId) {
    ByteString chunkData = ByteString.copyFrom(RandomUtils.secure().randomBytes(32));
    return newPutSmallFile(new BlockID(containerId, localId), chunkData);
  }

  static ContainerCommandRequestProto newPutSmallFile(
      BlockID blockID, ByteString data) {
    final ContainerProtos.BlockData.Builder blockData
        = ContainerProtos.BlockData.newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf());
    final ContainerProtos.PutBlockRequestProto.Builder putBlockRequest
        = ContainerProtos.PutBlockRequestProto.newBuilder()
        .setBlockData(blockData);
    final ContainerProtos.KeyValue keyValue = ContainerProtos.KeyValue.newBuilder()
        .setKey("OverWriteRequested")
        .setValue("true")
        .build();
    final ContainerProtos.ChunkInfo chunk = ContainerProtos.ChunkInfo.newBuilder()
        .setChunkName(blockID.getLocalID() + "_chunk")
        .setOffset(0)
        .setLen(data.size())
        .addMetadata(keyValue)
        .setChecksumData(checksum(data).getProtoBufMessage())
        .build();
    final ContainerProtos.PutSmallFileRequestProto putSmallFileRequest
        = ContainerProtos.PutSmallFileRequestProto.newBuilder()
        .setChunkInfo(chunk)
        .setBlock(putBlockRequest)
        .setData(data)
        .build();
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.PutSmallFile)
        .setContainerID(blockID.getContainerID())
        .setDatanodeUuid(UUID.randomUUID().toString())
        .setPutSmallFile(putSmallFileRequest)
        .build();
  }

  /**
   * Creates container read chunk request using input container write chunk
   * request.
   *
   * @param writeChunkRequest - Input container write chunk request
   * @return container read chunk request
   */
  private ContainerCommandRequestProto getReadChunkRequest(
      ContainerCommandRequestProto writeChunkRequest) {
    WriteChunkRequestProto writeChunk = writeChunkRequest.getWriteChunk();
    ContainerProtos.ReadChunkRequestProto.Builder readChunkRequest =
        ContainerProtos.ReadChunkRequestProto.newBuilder()
            .setBlockID(writeChunk.getBlockID())
            .setChunkData(writeChunk.getChunkData())
            .setReadChunkVersion(ContainerProtos.ReadChunkVersion.V1);
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.ReadChunk)
        .setContainerID(writeChunk.getBlockID().getContainerID())
        .setTraceID(writeChunkRequest.getTraceID())
        .setDatanodeUuid(writeChunkRequest.getDatanodeUuid())
        .setReadChunk(readChunkRequest)
        .build();
  }

  @Test
  public void testValidateToken() throws Exception {
    try {
      final OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(HDDS_DATANODE_DIR_KEY, testDir.getPath());
      conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());

      final DatanodeDetails dd = randomDatanodeDetails();
      final UUID scmId = UUID.randomUUID();
      final AtomicBoolean verified = new AtomicBoolean();
      final TokenVerifier tokenVerifier = new TokenVerifier() {
        private void verify() {
          final boolean previous = verified.getAndSet(true);
          assertFalse(previous);
        }

        @Override
        public void verify(ContainerCommandRequestProtoOrBuilder cmd,
            String encodedToken) {
          verify();
        }

        @Override
        public void verify(Token<?> token,
            ContainerCommandRequestProtoOrBuilder cmd) {
          verify();
        }
      };

      final ContainerCommandRequestProto request = getWriteChunkRequest(
          dd.getUuidString(), 1L, 1L);
      final HddsDispatcher dispatcher = createDispatcher(
          dd, scmId, conf, tokenVerifier);

      final DispatcherContext[] notVerify = {
          newContext(Op.WRITE_STATE_MACHINE_DATA, WriteChunkStage.WRITE_DATA),
          newContext(Op.READ_STATE_MACHINE_DATA),
          newContext(Op.APPLY_TRANSACTION),
          newContext(Op.STREAM_LINK, WriteChunkStage.COMMIT_DATA)
      };
      for (DispatcherContext context : notVerify) {
        LOG.info("notVerify {}", context);
        assertFalse(verified.get());
        dispatcher.dispatch(request, context);
        assertFalse(verified.get());
      }

      final Op[] verify = {
          Op.NULL,
          Op.HANDLE_GET_SMALL_FILE,
          Op.HANDLE_PUT_SMALL_FILE,
          Op.HANDLE_READ_CHUNK,
          Op.HANDLE_WRITE_CHUNK,
          Op.STREAM_INIT,
      };

      for (Op op : verify) {
        final DispatcherContext context = newContext(op);
        assertFalse(verified.get());
        dispatcher.dispatch(request, context);
        assertTrue(verified.getAndSet(false));
      }
    } finally {
      ContainerMetrics.remove();
    }
  }

  static DispatcherContext newContext(Op op) {
    return newContext(op, WriteChunkStage.COMBINED);
  }

  static DispatcherContext newContext(Op op, WriteChunkStage stage) {
    return DispatcherContext.newBuilder(op)
        .setTerm(1)
        .setLogIndex(1)
        .setStage(stage)
        .setContainer2BCSIDMap(new HashMap<>())
        .build();
  }
}
