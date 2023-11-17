/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.ozone.test.GenericTestUtils;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_CHOOSING_POLICY;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.junit.Assert.assertEquals;

import org.apache.ozone.test.JUnit5AwareTimeout;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mockito;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.times;


/**
 * Unit tests for {@link KeyValueHandler}.
 */
@RunWith(Parameterized.class)
public class TestKeyValueHandler {

  @Rule
  public final TestRule timeout = new JUnit5AwareTimeout(Timeout.seconds(300));

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  private static final String DATANODE_UUID = UUID.randomUUID().toString();

  private static final long DUMMY_CONTAINER_ID = 9999;
  private static final String DUMMY_PATH = "dummy/dir/doesnt/exist";

  private final ContainerLayoutVersion layout;

  private HddsDispatcher dispatcher;
  private KeyValueHandler handler;

  public TestKeyValueHandler(ContainerLayoutVersion layout) {
    this.layout = layout;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ContainerLayoutTestInfo.containerLayoutParameters();
  }

  @Before
  public void setup() throws StorageContainerException {
    // Create mock HddsDispatcher and KeyValueHandler.
    handler = Mockito.mock(KeyValueHandler.class);

    HashMap<ContainerType, Handler> handlers = new HashMap<>();
    handlers.put(ContainerType.KeyValueContainer, handler);

    dispatcher = new HddsDispatcher(
        new OzoneConfiguration(),
        Mockito.mock(ContainerSet.class),
        Mockito.mock(VolumeSet.class),
        handlers,
        Mockito.mock(StateContext.class),
        Mockito.mock(ContainerMetrics.class),
        Mockito.mock(TokenVerifier.class)
    );

  }

  /**
   * Test that Handler handles different command types correctly.
   */
  @Test
  public void testHandlerCommandHandling() throws Exception {
    Mockito.reset(handler);
    // Test Create Container Request handling
    ContainerCommandRequestProto createContainerRequest =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CreateContainer)
            .setContainerID(DUMMY_CONTAINER_ID)
            .setDatanodeUuid(DATANODE_UUID)
            .setCreateContainer(ContainerProtos.CreateContainerRequestProto
                .getDefaultInstance())
            .build();

    KeyValueContainer container = Mockito.mock(KeyValueContainer.class);

    KeyValueHandler
        .dispatchRequest(handler, createContainerRequest, container, null);
    Mockito.verify(handler, times(0)).handleListBlock(
        any(ContainerCommandRequestProto.class), any());

    // Test Read Container Request handling
    ContainerCommandRequestProto readContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ReadContainer);
    KeyValueHandler
        .dispatchRequest(handler, readContainerRequest, container, null);
    Mockito.verify(handler, times(1)).handleReadContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Update Container Request handling
    ContainerCommandRequestProto updateContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.UpdateContainer);
    KeyValueHandler
        .dispatchRequest(handler, updateContainerRequest, container, null);
    Mockito.verify(handler, times(1)).handleUpdateContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Delete Container Request handling
    ContainerCommandRequestProto deleteContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.DeleteContainer);
    KeyValueHandler
        .dispatchRequest(handler, deleteContainerRequest, container, null);
    Mockito.verify(handler, times(1)).handleDeleteContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test List Container Request handling
    ContainerCommandRequestProto listContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ListContainer);
    KeyValueHandler
        .dispatchRequest(handler, listContainerRequest, container, null);
    Mockito.verify(handler, times(1)).handleUnsupportedOp(
        any(ContainerCommandRequestProto.class));

    // Test Close Container Request handling
    ContainerCommandRequestProto closeContainerRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.CloseContainer);
    KeyValueHandler
        .dispatchRequest(handler, closeContainerRequest, container, null);
    Mockito.verify(handler, times(1)).handleCloseContainer(
        any(ContainerCommandRequestProto.class), any());

    // Test Put Block Request handling
    ContainerCommandRequestProto putBlockRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.PutBlock);
    KeyValueHandler
        .dispatchRequest(handler, putBlockRequest, container, null);
    Mockito.verify(handler, times(1)).handlePutBlock(
        any(ContainerCommandRequestProto.class), any(), any());

    // Test Get Block Request handling
    ContainerCommandRequestProto getBlockRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.GetBlock);
    KeyValueHandler
        .dispatchRequest(handler, getBlockRequest, container, null);
    Mockito.verify(handler, times(1)).handleGetBlock(
        any(ContainerCommandRequestProto.class), any());

    // Block Deletion is handled by BlockDeletingService and need not be
    // tested here.

    ContainerCommandRequestProto listBlockRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ListBlock);
    KeyValueHandler
        .dispatchRequest(handler, listBlockRequest, container, null);
    Mockito.verify(handler, times(1)).handleUnsupportedOp(
        any(ContainerCommandRequestProto.class));

    // Test Read Chunk Request handling
    ContainerCommandRequestProto readChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ReadChunk);
    KeyValueHandler
        .dispatchRequest(handler, readChunkRequest, container, null);
    Mockito.verify(handler, times(1)).handleReadChunk(
        any(ContainerCommandRequestProto.class), any(), any());

    // Chunk Deletion is handled by BlockDeletingService and need not be
    // tested here.

    // Test Write Chunk Request handling
    ContainerCommandRequestProto writeChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.WriteChunk);
    KeyValueHandler
        .dispatchRequest(handler, writeChunkRequest, container, null);
    Mockito.verify(handler, times(1)).handleWriteChunk(
        any(ContainerCommandRequestProto.class), any(), any());

    // Test List Chunk Request handling
    ContainerCommandRequestProto listChunkRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.ListChunk);
    KeyValueHandler
        .dispatchRequest(handler, listChunkRequest, container, null);
    Mockito.verify(handler, times(2)).handleUnsupportedOp(
        any(ContainerCommandRequestProto.class));

    // Test Put Small File Request handling
    ContainerCommandRequestProto putSmallFileRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.PutSmallFile);
    KeyValueHandler
        .dispatchRequest(handler, putSmallFileRequest, container, null);
    Mockito.verify(handler, times(1)).handlePutSmallFile(
        any(ContainerCommandRequestProto.class), any(), any());

    // Test Get Small File Request handling
    ContainerCommandRequestProto getSmallFileRequest =
        getDummyCommandRequestProto(ContainerProtos.Type.GetSmallFile);
    KeyValueHandler
        .dispatchRequest(handler, getSmallFileRequest, container, null);
    Mockito.verify(handler, times(1)).handleGetSmallFile(
        any(ContainerCommandRequestProto.class), any());
  }

  @Test
  public void testVolumeSetInKeyValueHandler() throws Exception {
    File path = tempDir.newFolder();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, path.getAbsolutePath());
    conf.set(OZONE_METADATA_DIRS, path.getAbsolutePath());
    MutableVolumeSet
        volumeSet = new MutableVolumeSet(UUID.randomUUID().toString(), conf,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);
    try {
      ContainerSet cset = new ContainerSet(1000);
      int[] interval = new int[1];
      interval[0] = 2;
      ContainerMetrics metrics = new ContainerMetrics(interval);
      DatanodeDetails datanodeDetails = Mockito.mock(DatanodeDetails.class);
      StateContext context = ContainerTestUtils.getMockContext(
          datanodeDetails, conf);
      KeyValueHandler keyValueHandler = new KeyValueHandler(conf,
          context.getParent().getDatanodeDetails().getUuidString(), cset,
          volumeSet, metrics, c -> {
      });
      assertEquals("org.apache.hadoop.ozone.container.common" +
          ".volume.CapacityVolumeChoosingPolicy",
          keyValueHandler.getVolumeChoosingPolicyForTesting()
              .getClass().getName());

      //Set a class which is not of sub class of VolumeChoosingPolicy
      conf.set(HDDS_DATANODE_VOLUME_CHOOSING_POLICY,
          "org.apache.hadoop.ozone.container.common.impl.HddsDispatcher");
      try {
        new KeyValueHandler(conf,
            context.getParent().getDatanodeDetails().getUuidString(),
            cset, volumeSet, metrics, c -> { });
      } catch (RuntimeException ex) {
        GenericTestUtils.assertExceptionContains("class org.apache.hadoop" +
            ".ozone.container.common.impl.HddsDispatcher not org.apache" +
            ".hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy",
            ex);
      }
    } finally {
      volumeSet.shutdown();
      FileUtil.fullyDelete(path);
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


  @Test
  public void testCloseInvalidContainer() throws IOException {
    long containerID = 1234L;
    OzoneConfiguration conf = new OzoneConfiguration();
    KeyValueContainerData kvData = new KeyValueContainerData(containerID,
        layout,
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

    Mockito.when(handler.handleCloseContainer(any(), any()))
        .thenCallRealMethod();
    doCallRealMethod().when(handler).closeContainer(any());
    // Closing invalid container should return error response.
    ContainerProtos.ContainerCommandResponseProto response =
        handler.handleCloseContainer(closeContainerRequest, container);

    assertEquals("Close container should return Invalid container error",
        ContainerProtos.Result.INVALID_CONTAINER_STATE, response.getResult());
  }

  @Test
  public void testDeleteContainer() throws IOException {
    final String testDir = tempDir.newFolder().getAbsolutePath();
    try {
      // Case 1 : Regular container delete
      final long containerID = 1L;
      final String clusterId = UUID.randomUUID().toString();
      final String datanodeId = UUID.randomUUID().toString();
      final ConfigurationSource conf = new OzoneConfiguration();
      final ContainerSet containerSet = new ContainerSet(1000);
      final MutableVolumeSet volumeSet = Mockito.mock(MutableVolumeSet.class);

      HddsVolume hddsVolume = new HddsVolume.Builder(testDir).conf(conf)
          .clusterID(clusterId).datanodeUuid(datanodeId)
          .volumeSet(volumeSet)
          .build();
      hddsVolume.format(clusterId);
      hddsVolume.createWorkingDir(clusterId, null);
      hddsVolume.createTmpDirs(clusterId);

      Mockito.when(volumeSet.getVolumesList())
          .thenReturn(Collections.singletonList(hddsVolume));

      List<HddsVolume> hddsVolumeList = StorageVolumeUtil
          .getHddsVolumesList(volumeSet.getVolumesList());

      assertEquals(1, hddsVolumeList.size());

      final ContainerMetrics metrics = ContainerMetrics.create(conf);

      final AtomicInteger icrReceived = new AtomicInteger(0);

      final KeyValueHandler kvHandler = new KeyValueHandler(conf,
          datanodeId, containerSet, volumeSet, metrics,
          c -> icrReceived.incrementAndGet());
      kvHandler.setClusterID(clusterId);

      final ContainerCommandRequestProto createContainer =
          createContainerRequest(datanodeId, containerID);

      kvHandler.handleCreateContainer(createContainer, null);
      Assert.assertEquals(1, icrReceived.get());
      Assert.assertNotNull(containerSet.getContainer(containerID));

      kvHandler.deleteContainer(containerSet.getContainer(containerID), true);
      Assert.assertEquals(2, icrReceived.get());
      Assert.assertNull(containerSet.getContainer(containerID));

      File[] deletedContainers =
          hddsVolume.getDeletedContainerDir().listFiles();
      assertNotNull(deletedContainers);
      Assertions.assertEquals(0, deletedContainers.length);

      // Case 2 : failed move of container dir to tmp location should trigger
      // a volume scan

      final long container2ID = 2L;

      final ContainerCommandRequestProto createContainer2 =
          createContainerRequest(datanodeId, container2ID);

      kvHandler.handleCreateContainer(createContainer2, null);

      Assert.assertEquals(3, icrReceived.get());
      Container<?> container = containerSet.getContainer(container2ID);
      Assert.assertNotNull(container);
      File deletedContainerDir = hddsVolume.getDeletedContainerDir();
      // to simulate failed move
      File dummyDir = new File(DUMMY_PATH);
      hddsVolume.setDeletedContainerDir(dummyDir);
      try {
        kvHandler.deleteContainer(container, true);
      } catch (StorageContainerException sce) {
        Assert.assertTrue(
            sce.getMessage().contains("Failed to move container"));
      }
      Mockito.verify(volumeSet).checkVolumeAsync(hddsVolume);
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
      Assert.assertTrue(kvHandlerLogs.getOutput().contains(expectedLog));
    } finally {
      FileUtils.deleteDirectory(new File(testDir));
    }
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
