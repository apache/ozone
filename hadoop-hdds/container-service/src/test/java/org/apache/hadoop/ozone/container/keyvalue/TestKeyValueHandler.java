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

package org.apache.hadoop.ozone.container.keyvalue;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_LAYOUT_KEY;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.security.token.TokenVerifier;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Unit tests for {@link KeyValueHandler}.
 */
@Timeout(300)
public class TestKeyValueHandler {

  @TempDir
  private Path tempDir;

  private static final String DATANODE_UUID = UUID.randomUUID().toString();

  private static final long DUMMY_CONTAINER_ID = 9999;
  private static final String DUMMY_PATH = "dummy/dir/doesnt/exist";

  private HddsDispatcher dispatcher;
  private KeyValueHandler handler;

  private VolumeChoosingPolicy volumeChoosingPolicy;

  @BeforeEach
  public void setup() throws StorageContainerException, IllegalAccessException, InstantiationException {
    // Create mock HddsDispatcher and KeyValueHandler.
    handler = mock(KeyValueHandler.class);

    HashMap<ContainerType, Handler> handlers = new HashMap<>();
    handlers.put(ContainerType.KeyValueContainer, handler);

    dispatcher = new HddsDispatcher(
        new OzoneConfiguration(),
        mock(ContainerSet.class),
        mock(VolumeSet.class),
        handlers,
        mock(StateContext.class),
        mock(ContainerMetrics.class),
        mock(TokenVerifier.class)
    );

    volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(new OzoneConfiguration());
  }

  /**
   * Test that Handler handles different command types correctly.
   */
  @Test
  public void testHandlerCommandHandling() throws Exception {
    reset(handler);
    // Test Create Container Request handling
    ContainerCommandRequestProto createContainerRequest =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CreateContainer)
            .setContainerID(DUMMY_CONTAINER_ID)
            .setDatanodeUuid(DATANODE_UUID)
            .setCreateContainer(ContainerProtos.CreateContainerRequestProto
                .getDefaultInstance())
            .build();

    KeyValueContainer container = mock(KeyValueContainer.class);
    KeyValueContainerData containerData = mock(KeyValueContainerData.class);
    Mockito.when(container.getContainerData()).thenReturn(containerData);
    Mockito.when(containerData.getReplicaIndex()).thenReturn(1);
    ContainerProtos.ContainerCommandResponseProto responseProto = KeyValueHandler.dispatchRequest(handler,
        createContainerRequest, container, null);
    assertEquals(ContainerProtos.Result.INVALID_ARGUMENT, responseProto.getResult());
    Mockito.when(handler.getDatanodeId()).thenReturn(DATANODE_UUID);
    KeyValueHandler
        .dispatchRequest(handler, createContainerRequest, container, null);
    verify(handler, times(0)).handleListBlock(
        any(ContainerCommandRequestProto.class), any());

    // Test Read Container Request handling
    ContainerCommandRequestProto readContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ReadContainer);
    KeyValueHandler
        .dispatchRequest(handler, readContainerRequest, container, null);
    verify(handler, times(1)).handleReadContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Update Container Request handling
    ContainerCommandRequestProto updateContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.UpdateContainer);
    KeyValueHandler
        .dispatchRequest(handler, updateContainerRequest, container, null);
    verify(handler, times(1)).handleUpdateContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Delete Container Request handling
    ContainerCommandRequestProto deleteContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.DeleteContainer);
    KeyValueHandler
        .dispatchRequest(handler, deleteContainerRequest, container, null);
    verify(handler, times(1)).handleDeleteContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test List Container Request handling
    ContainerCommandRequestProto listContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ListContainer);
    KeyValueHandler
        .dispatchRequest(handler, listContainerRequest, container, null);
    verify(handler, times(1)).handleUnsupportedOp(
        any(ContainerCommandRequestProto.class));

    // Test Close Container Request handling
    ContainerCommandRequestProto closeContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.CloseContainer);
    KeyValueHandler
        .dispatchRequest(handler, closeContainerRequest, container, null);
    verify(handler, times(1)).handleCloseContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Put Block Request handling
    ContainerCommandRequestProto putBlockRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.PutBlock);
    KeyValueHandler
        .dispatchRequest(handler, putBlockRequest, container, null);
    verify(handler, times(1)).handlePutBlock(
        any(ContainerCommandRequestProto.class), any(), any());

    // Test Get Block Request handling
    ContainerCommandRequestProto getBlockRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.GetBlock);
    KeyValueHandler
        .dispatchRequest(handler, getBlockRequest, container, null);
    verify(handler, times(1)).handleGetBlock(
        any(ContainerCommandRequestProto.class), any());

    // Block Deletion is handled by BlockDeletingService and need not be
    // tested here.

    ContainerCommandRequestProto listBlockRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ListBlock);
    KeyValueHandler
        .dispatchRequest(handler, listBlockRequest, container, null);
    verify(handler, times(1)).handleUnsupportedOp(
        any(ContainerCommandRequestProto.class));

    // Test Read Chunk Request handling
    ContainerCommandRequestProto readChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ReadChunk);
    KeyValueHandler
        .dispatchRequest(handler, readChunkRequest, container, null);
    verify(handler, times(1)).handleReadChunk(
        any(ContainerCommandRequestProto.class), any(), any());

    // Chunk Deletion is handled by BlockDeletingService and need not be
    // tested here.

    // Test Write Chunk Request handling
    ContainerCommandRequestProto writeChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.WriteChunk);
    KeyValueHandler
        .dispatchRequest(handler, writeChunkRequest, container, null);
    verify(handler, times(1)).handleWriteChunk(
        any(ContainerCommandRequestProto.class), any(), any());

    // Test List Chunk Request handling
    ContainerCommandRequestProto listChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ListChunk);
    KeyValueHandler
        .dispatchRequest(handler, listChunkRequest, container, null);
    verify(handler, times(2)).handleUnsupportedOp(
        any(ContainerCommandRequestProto.class));

    // Test Put Small File Request handling
    ContainerCommandRequestProto putSmallFileRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.PutSmallFile);
    KeyValueHandler
        .dispatchRequest(handler, putSmallFileRequest, container, null);
    verify(handler, times(1)).handlePutSmallFile(
        any(ContainerCommandRequestProto.class), any(), any());

    // Test Get Small File Request handling
    ContainerCommandRequestProto getSmallFileRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.GetSmallFile);
    KeyValueHandler
        .dispatchRequest(handler, getSmallFileRequest, container, null);
    verify(handler, times(1)).handleGetSmallFile(
        any(ContainerCommandRequestProto.class), any());

    // Test Finalize Block Request handling
    ContainerCommandRequestProto finalizeBlock =
        getDummyCommandRequestProto(ContainerProtos.Type.FinalizeBlock);
    KeyValueHandler
        .dispatchRequest(handler, finalizeBlock, container, null);
    verify(handler, times(1)).handleFinalizeBlock(
        any(ContainerCommandRequestProto.class), any());
  }

  @Test
  public void testVolumeSetInKeyValueHandler() throws Exception {
    File datanodeDir =
        Files.createDirectory(tempDir.resolve("datanodeDir")).toFile();
    File metadataDir =
        Files.createDirectory(tempDir.resolve("metadataDir")).toFile();

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, datanodeDir.getAbsolutePath());
    conf.set(OZONE_METADATA_DIRS, metadataDir.getAbsolutePath());
    MutableVolumeSet
        volumeSet = new MutableVolumeSet(UUID.randomUUID().toString(), conf,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);
    try {
      ContainerSet cset = newContainerSet();
      int[] interval = new int[1];
      interval[0] = 2;
      ContainerMetrics metrics = new ContainerMetrics(interval);
      DatanodeDetails datanodeDetails = mock(DatanodeDetails.class);
      StateContext context = ContainerTestUtils.getMockContext(
          datanodeDetails, conf);

      // Ensures that KeyValueHandler falls back to FILE_PER_BLOCK.
      conf.set(OZONE_SCM_CONTAINER_LAYOUT_KEY, "FILE_PER_CHUNK");
      new KeyValueHandler(conf, context.getParent().getDatanodeDetails().getUuidString(), cset, volumeSet,
          volumeChoosingPolicy, metrics, c -> { });
      assertEquals(ContainerLayoutVersion.FILE_PER_BLOCK,
          conf.getEnum(OZONE_SCM_CONTAINER_LAYOUT_KEY, ContainerLayoutVersion.FILE_PER_CHUNK));
    } finally {
      volumeSet.shutdown();
      FileUtil.fullyDelete(datanodeDir);
      FileUtil.fullyDelete(metadataDir);
    }
  }

  private ContainerCommandRequestProto getDummyCommandRequestProto(
      ContainerProtos.Type cmdType) {
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(cmdType)
        .setContainerID(DUMMY_CONTAINER_ID)
        .setDatanodeUuid(DATANODE_UUID)
        .build();
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testCloseInvalidContainer(ContainerLayoutVersion layoutVersion)
      throws IOException {
    long containerID = 1234L;
    OzoneConfiguration conf = new OzoneConfiguration();
    KeyValueContainerData kvData = new KeyValueContainerData(containerID,
        layoutVersion,
        (long) StorageUnit.GB.toBytes(1), UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    KeyValueContainer container = new KeyValueContainer(kvData, conf);
    kvData.setState(ContainerProtos.ContainerDataProto.State.INVALID);

    // Create Close container request
    ContainerCommandRequestProto closeContainerRequest =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CloseContainer)
            .setContainerID(DUMMY_CONTAINER_ID)
            .setDatanodeUuid(DATANODE_UUID)
            .setCloseContainer(ContainerProtos.CloseContainerRequestProto
                .getDefaultInstance())
            .build();
    dispatcher.dispatch(closeContainerRequest, null);

    when(handler.handleCloseContainer(any(), any()))
        .thenCallRealMethod();
    doCallRealMethod().when(handler).closeContainer(any());
    // Closing invalid container should return error response.
    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleCloseContainer(closeContainerRequest, container);

    assertEquals(ContainerProtos.Result.INVALID_CONTAINER_STATE,
        response.getResult(),
        "Close container should return Invalid container error");
  }

  @Test
  public void testDeleteContainer() throws IOException {
    final String testDir = tempDir.toString();
    try {
      // Case 1 : Regular container delete
      final long containerID = 1L;
      final String clusterId = UUID.randomUUID().toString();
      final String datanodeId = UUID.randomUUID().toString();
      final ConfigurationSource conf = new OzoneConfiguration();
      final ContainerSet containerSet = newContainerSet();
      final MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);

      HddsVolume hddsVolume = new HddsVolume.Builder(testDir).conf(conf)
          .clusterID(clusterId).datanodeUuid(datanodeId)
          .volumeSet(volumeSet)
          .build();
      hddsVolume.format(clusterId);
      hddsVolume.createWorkingDir(clusterId, null);
      hddsVolume.createTmpDirs(clusterId);

      when(volumeSet.getVolumesList())
          .thenReturn(Collections.singletonList(hddsVolume));

      List<HddsVolume> hddsVolumeList = StorageVolumeUtil
          .getHddsVolumesList(volumeSet.getVolumesList());

      assertEquals(1, hddsVolumeList.size());

      final ContainerMetrics metrics = ContainerMetrics.create(conf);

      final AtomicInteger icrReceived = new AtomicInteger(0);

      final KeyValueHandler kvHandler = new KeyValueHandler(conf,
          datanodeId, containerSet, volumeSet, volumeChoosingPolicy, metrics,
          c -> icrReceived.incrementAndGet());
      kvHandler.setClusterID(clusterId);

      final ContainerCommandRequestProto createContainer =
          createContainerRequest(datanodeId, containerID);

      kvHandler.handleCreateContainer(createContainer, null);
      assertEquals(1, icrReceived.get());
      assertNotNull(containerSet.getContainer(containerID));

      kvHandler.deleteContainer(containerSet.getContainer(containerID), true);
      assertEquals(2, icrReceived.get());
      assertNull(containerSet.getContainer(containerID));

      File[] deletedContainers =
          hddsVolume.getDeletedContainerDir().listFiles();
      assertNotNull(deletedContainers);
      assertEquals(0, deletedContainers.length);

      // Case 2 : failed move of container dir to tmp location should trigger
      // a volume scan

      final long container2ID = 2L;

      final ContainerCommandRequestProto createContainer2 =
          createContainerRequest(datanodeId, container2ID);

      kvHandler.handleCreateContainer(createContainer2, null);

      assertEquals(3, icrReceived.get());
      Container<?> container = containerSet.getContainer(container2ID);
      assertNotNull(container);
      File deletedContainerDir = hddsVolume.getDeletedContainerDir();
      // to simulate failed move
      File dummyDir = new File(DUMMY_PATH);
      hddsVolume.setDeletedContainerDir(dummyDir);
      try {
        kvHandler.deleteContainer(container, true);
      } catch (StorageContainerException sce) {
        assertThat(sce.getMessage()).contains("Failed to move container");
      }
      verify(volumeSet).checkVolumeAsync(hddsVolume);
      // cleanup
      hddsVolume.setDeletedContainerDir(deletedContainerDir);

      // Case 3:  Delete Container on a failed volume
      hddsVolume.failVolume();
      GenericTestUtils.LogCapturer kvHandlerLogs =
          GenericTestUtils.LogCapturer.captureLogs(KeyValueHandler.getLogger());
      // add the container back to containerSet as removed in previous delete
      containerSet.addContainer(container);
      kvHandler.deleteContainer(container, true);
      String expectedLog =
          "Delete container issued on containerID 2 which is " +
              "in a failed volume";
      assertThat(kvHandlerLogs.getOutput()).contains(expectedLog);
    } finally {
      FileUtils.deleteDirectory(new File(testDir));
    }
  }


  @Test
  public void testDeleteContainerTimeout() throws IOException {
    final String testDir = tempDir.toString();
    final long containerID = 1L;
    final String clusterId = UUID.randomUUID().toString();
    final String datanodeId = UUID.randomUUID().toString();
    final ConfigurationSource conf = new OzoneConfiguration();
    final ContainerSet containerSet = newContainerSet();
    final MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);
    final Clock clock = mock(Clock.class);
    long startTime = System.currentTimeMillis();

    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    when(clock.millis())
        .thenReturn(startTime)
        .thenReturn(startTime + dnConf.getDeleteContainerTimeoutMs() + 1);

    HddsVolume hddsVolume = new HddsVolume.Builder(testDir).conf(conf)
        .clusterID(clusterId).datanodeUuid(datanodeId)
        .volumeSet(volumeSet)
        .build();
    hddsVolume.format(clusterId);
    hddsVolume.createWorkingDir(clusterId, null);
    hddsVolume.createTmpDirs(clusterId);

    when(volumeSet.getVolumesList())
        .thenReturn(Collections.singletonList(hddsVolume));

    List<HddsVolume> hddsVolumeList = StorageVolumeUtil
        .getHddsVolumesList(volumeSet.getVolumesList());

    assertEquals(1, hddsVolumeList.size());

    final ContainerMetrics metrics = ContainerMetrics.create(conf);

    final AtomicInteger icrReceived = new AtomicInteger(0);

    final KeyValueHandler kvHandler = new KeyValueHandler(conf,
        datanodeId, containerSet, volumeSet, volumeChoosingPolicy, metrics,
        c -> icrReceived.incrementAndGet(), clock);
    kvHandler.setClusterID(clusterId);

    final ContainerCommandRequestProto createContainer =
        createContainerRequest(datanodeId, containerID);
    kvHandler.handleCreateContainer(createContainer, null);
    assertEquals(1, icrReceived.get());
    assertNotNull(containerSet.getContainer(containerID));

    // The delete should not have gone through due to the mocked clock. The implementation calls the clock twice:
    // Once at the start of the method prior to taking the lock, when the clock will return the start time of the test.
    // On the second call to the clock, where the implementation checks if the timeout has expired, the clock will
    // return start_time + timeout + 1. This will cause the delete to timeout and the container will not be deleted.
    kvHandler.deleteContainer(containerSet.getContainer(containerID), true);
    assertEquals(1, icrReceived.get());
    assertNotNull(containerSet.getContainer(containerID));

    // Delete the container normally, and it should go through. At this stage all calls to the clock mock will return
    // the same value, indicating no delay to the delete operation will succeed.
    kvHandler.deleteContainer(containerSet.getContainer(containerID), true);
    assertEquals(2, icrReceived.get());
    assertNull(containerSet.getContainer(containerID));
  }

  private static ContainerCommandRequestProto createContainerRequest(
      String datanodeId, long containerID) {
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.CreateContainer)
        .setDatanodeUuid(datanodeId).setCreateContainer(
            ContainerProtos.CreateContainerRequestProto.newBuilder()
                .setContainerType(ContainerType.KeyValueContainer).build())
        .setContainerID(containerID).setPipelineID(UUID.randomUUID().toString())
        .build();
  }
}
