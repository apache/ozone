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
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_LAYOUT_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.assertTreesSortedAndMatch;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildTestTree;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.verifyAllDataChecksumsMatch;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createBlockMetaData;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.apache.ozone.test.MetricsAsserts.assertCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.security.token.TokenVerifier;
import org.apache.hadoop.hdds.utils.io.RandomAccessFileChannel;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeWriter;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.report.IncrementalReportSender;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.OnDemandContainerScanner;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link KeyValueHandler}.
 */
public class TestKeyValueHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TestKeyValueHandler.class);

  @TempDir
  private Path tempDir;
  @TempDir
  private Path dbFile;

  private static final long DUMMY_CONTAINER_ID = 9999;
  private static final String DUMMY_PATH = "dummy/dir/doesnt/exist";
  private static final String DATANODE_UUID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();

  private HddsDispatcher dispatcher;
  private KeyValueHandler handler;
  private OzoneConfiguration conf;
  private ContainerSet mockContainerSet;
  private long maxContainerSize;

  @BeforeEach
  public void setup() throws IOException {
    // Create mock HddsDispatcher and KeyValueHandler.
    conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, tempDir.toString());
    conf.set(OZONE_METADATA_DIRS, tempDir.toString());
    handler = mock(KeyValueHandler.class);

    HashMap<ContainerType, Handler> handlers = new HashMap<>();
    handlers.put(ContainerType.KeyValueContainer, handler);

    mockContainerSet = Mockito.mock(ContainerSet.class);

    dispatcher = new HddsDispatcher(
        new OzoneConfiguration(),
        mockContainerSet,
        mock(VolumeSet.class),
        handlers,
        mock(StateContext.class),
        mock(ContainerMetrics.class),
        mock(TokenVerifier.class)
    );

    maxContainerSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
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

    conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, datanodeDir.getAbsolutePath());
    conf.set(OZONE_METADATA_DIRS, metadataDir.getAbsolutePath());
    MutableVolumeSet
        volumeSet = new MutableVolumeSet(UUID.randomUUID().toString(), conf,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);
    try {
      ContainerSet cset = newContainerSet();
      DatanodeDetails datanodeDetails = mock(DatanodeDetails.class);
      StateContext context = ContainerTestUtils.getMockContext(
          datanodeDetails, conf);

      // Ensures that KeyValueHandler falls back to FILE_PER_BLOCK.
      conf.set(OZONE_SCM_CONTAINER_LAYOUT_KEY, "FILE_PER_CHUNK");
      ContainerTestUtils.getKeyValueHandler(conf,
          context.getParent().getDatanodeDetails().getUuidString(), cset, volumeSet);
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
    KeyValueHandler keyValueHandler = createKeyValueHandler(tempDir);
    conf = new OzoneConfiguration();
    KeyValueContainerData kvData = new KeyValueContainerData(DUMMY_CONTAINER_ID,
        layoutVersion,
        (long) StorageUnit.GB.toBytes(1), UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    kvData.setMetadataPath(tempDir.toString());
    kvData.setDbFile(dbFile.toFile());
    KeyValueContainer container = new KeyValueContainer(kvData, conf);
    ContainerCommandRequestProto createContainerRequest =
        createContainerRequest(DATANODE_UUID, DUMMY_CONTAINER_ID);
    keyValueHandler.handleCreateContainer(createContainerRequest, container);

    // Make the container state as invalid.
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

    // Closing invalid container should return error response.
    ContainerProtos.ContainerCommandResponseProto response =
        keyValueHandler.handleCloseContainer(closeContainerRequest, container);
    // Checksum will not be generated for an invalid container.
    assertFalse(ContainerChecksumTreeManager.getContainerChecksumFile(kvData).exists());

    assertEquals(ContainerProtos.Result.INVALID_CONTAINER_STATE,
        response.getResult(),
        "Close container should return Invalid container error");
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testCloseRecoveringContainerTriggersScan(ContainerLayoutVersion layoutVersion) {
    final KeyValueHandler keyValueHandler = new KeyValueHandler(conf,
        DATANODE_UUID, mockContainerSet, mock(MutableVolumeSet.class),  mock(ContainerMetrics.class),
        c -> { }, new ContainerChecksumTreeManager(conf));

    conf = new OzoneConfiguration();
    KeyValueContainerData kvData = new KeyValueContainerData(DUMMY_CONTAINER_ID,
        layoutVersion,
        (long) StorageUnit.GB.toBytes(1), UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    kvData.setMetadataPath(tempDir.toString());
    kvData.setDbFile(dbFile.toFile());
    KeyValueContainer container = new KeyValueContainer(kvData, conf);
    ContainerCommandRequestProto createContainerRequest =
        createContainerRequest(DATANODE_UUID, DUMMY_CONTAINER_ID);
    keyValueHandler.handleCreateContainer(createContainerRequest, container);

    // Make the container state as invalid.
    kvData.setState(State.RECOVERING);

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

    keyValueHandler.handleCloseContainer(closeContainerRequest, container);

    verify(mockContainerSet, atLeastOnce()).scanContainer(DUMMY_CONTAINER_ID, "EC Reconstruction");
  }

  @Test
  public void testCreateContainerWithFailure() throws Exception {
    final String testDir = tempDir.toString();
    final long containerID = 1L;
    final String clusterId = UUID.randomUUID().toString();
    final String datanodeId = UUID.randomUUID().toString();
    conf = new OzoneConfiguration();
    final ContainerSet containerSet = spy(newContainerSet());
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
        datanodeId, containerSet, volumeSet, metrics,
        c -> icrReceived.incrementAndGet(), new ContainerChecksumTreeManager(conf));
    kvHandler.setClusterID(clusterId);

    final ContainerCommandRequestProto createContainer =
        createContainerRequest(datanodeId, containerID);

    Semaphore semaphore = new Semaphore(1);
    doAnswer(invocation -> {
      semaphore.acquire();
      throw new StorageContainerException(ContainerProtos.Result.IO_EXCEPTION);
    }).when(containerSet).addContainer(any());

    semaphore.acquire();
    CompletableFuture.runAsync(() ->
        kvHandler.handleCreateContainer(createContainer, null)
    );

    // commit bytes has been allocated by volumeChoosingPolicy which is called in KeyValueContainer#create
    GenericTestUtils.waitFor(() -> hddsVolume.getCommittedBytes() == maxContainerSize,
            1000, 50000);
    semaphore.release();

    LOG.info("Committed bytes: {}", hddsVolume.getCommittedBytes());

    // release committed bytes as exception is thrown
    GenericTestUtils.waitFor(() -> hddsVolume.getCommittedBytes() == 0,
            1000, 50000);
  }

  @Test
  public void testDeleteContainer() throws IOException {
    final String testDir = tempDir.toString();
    try {
      // Case 1 : Regular container delete
      final long containerID = 1L;
      final String clusterId = UUID.randomUUID().toString();
      final String datanodeId = UUID.randomUUID().toString();
      conf = new OzoneConfiguration();
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
          datanodeId, containerSet, volumeSet, metrics,
          c -> icrReceived.incrementAndGet(), new ContainerChecksumTreeManager(conf));
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
      LogCapturer kvHandlerLogs = LogCapturer.captureLogs(KeyValueHandler.class);
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

  /**
   * Tests that deleting a container decrements the cached used space of its volume.
   */
  @Test
  public void testDeleteDecrementsVolumeUsedSpace() throws IOException {
    final long containerID = 1;
    final String clusterId = UUID.randomUUID().toString();
    final String datanodeId = UUID.randomUUID().toString();
    final ContainerSet containerSet = newContainerSet();
    final MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);
    final HddsVolume hddsVolume = mock(HddsVolume.class);
    when(hddsVolume.getDeletedContainerDir()).thenReturn(new File(""));

    conf = new OzoneConfiguration();
    final ContainerMetrics metrics = ContainerMetrics.create(conf);
    final AtomicInteger icrReceived = new AtomicInteger(0);
    final long containerBytesUsed = 1024 * 1024;

    // We're testing KeyValueHandler in this test, all the other objects are mocked
    final KeyValueHandler kvHandler = new KeyValueHandler(conf,
        datanodeId, containerSet, volumeSet, metrics,
        c -> icrReceived.incrementAndGet(), new ContainerChecksumTreeManager(conf));
    kvHandler.setClusterID(clusterId);

    // Setup ContainerData and Container mocks
    KeyValueContainerData containerData = mock(KeyValueContainerData.class);
    when(containerData.getContainerID()).thenReturn(containerID);
    when(containerData.getVolume()).thenReturn(hddsVolume);
    when(containerData.getBytesUsed()).thenReturn(containerBytesUsed);
    when(containerData.getState()).thenReturn(ContainerProtos.ContainerDataProto.State.CLOSED);
    when(containerData.isOpen()).thenReturn(false);
    when(containerData.getLayoutVersion()).thenReturn(ContainerLayoutVersion.FILE_PER_BLOCK);
    when(containerData.getDbFile()).thenReturn(new File(tempDir.toFile(), "dummy.db"));
    when(containerData.getContainerPath()).thenReturn(tempDir.toString());
    when(containerData.getMetadataPath()).thenReturn(tempDir.toString());

    KeyValueContainer container = mock(KeyValueContainer.class);
    when(container.getContainerData()).thenReturn(containerData);
    when(container.hasBlocks()).thenReturn(true);

    containerSet.addContainer(container);
    assertNotNull(containerSet.getContainer(containerID));

    // This is the method we're testing. It should decrement used space in the volume when deleting this container
    kvHandler.deleteContainer(container, true);
    assertNull(containerSet.getContainer(containerID));

    // Verify ICR was sent (once for delete)
    assertEquals(1, icrReceived.get(), "ICR should be sent for delete");
    verify(container, times(1)).delete();
    // Verify decrementUsedSpace was called with the correct amount
    verify(hddsVolume, times(1)).decrementUsedSpace(eq(containerBytesUsed));
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testContainerChecksumInvocation(ContainerLayoutVersion layoutVersion) throws Exception {
    conf = new OzoneConfiguration();

    KeyValueContainerData data = new KeyValueContainerData(123L, layoutVersion, GB,
        PipelineID.randomId().toString(), randomDatanodeDetails().getUuidString());
    data.setMetadataPath(tempDir.toString());
    data.setDbFile(dbFile.toFile());

    Container container = new KeyValueContainer(data, conf);
    createBlockMetaData(data, 5, 3);
    ContainerSet containerSet = newContainerSet();
    containerSet.addContainer(container);

    // Allows checking the invocation count of the lambda.
    AtomicInteger icrCount = new AtomicInteger(0);
    IncrementalReportSender<Container> icrSender = c -> {
      // Check that the ICR contains expected info about the container.
      ContainerReplicaProto report = c.getContainerReport();
      long reportedID = report.getContainerID();
      Assertions.assertEquals(container.getContainerData().getContainerID(), reportedID);

      long reportDataChecksum = report.getDataChecksum();
      assertNotEquals(0, reportDataChecksum,
          "Container report should have populated the checksum field with a non-zero value.");
      icrCount.incrementAndGet();
    };

    KeyValueHandler keyValueHandler = new KeyValueHandler(conf, randomDatanodeDetails().getUuidString(), containerSet,
        mock(MutableVolumeSet.class), mock(ContainerMetrics.class), icrSender, new ContainerChecksumTreeManager(conf));

    Assertions.assertEquals(0, icrCount.get());
    // This should trigger container report validation in the ICR handler above.
    DNContainerOperationClient mockDnClient = mock(DNContainerOperationClient.class);
    DatanodeDetails peer1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails peer2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails peer3 = MockDatanodeDetails.randomDatanodeDetails();
    when(mockDnClient.getContainerChecksumInfo(anyLong(), any())).thenReturn(null);
    keyValueHandler.reconcileContainer(mockDnClient, container, Sets.newHashSet(peer1, peer2, peer3));
    // Make sure all the replicas are used for reconciliation.
    Mockito.verify(mockDnClient, atMostOnce()).getContainerChecksumInfo(anyLong(), eq(peer1));
    Mockito.verify(mockDnClient, atMostOnce()).getContainerChecksumInfo(anyLong(), eq(peer2));
    Mockito.verify(mockDnClient, atMostOnce()).getContainerChecksumInfo(anyLong(), eq(peer3));
    Assertions.assertEquals(1, icrCount.get());
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testUpdateContainerChecksum(ContainerLayoutVersion layoutVersion) throws Exception {
    conf = new OzoneConfiguration();
    KeyValueContainerData data = new KeyValueContainerData(123L, layoutVersion, GB,
        PipelineID.randomId().toString(), randomDatanodeDetails().getUuidString());
    data.setMetadataPath(tempDir.toString());
    data.setDbFile(dbFile.toFile());
    KeyValueContainer container = new KeyValueContainer(data, conf);
    KeyValueContainerData containerData = container.getContainerData();
    ContainerSet containerSet = ContainerImplTestUtils.newContainerSet();
    containerSet.addContainer(container);

    // Allows checking the invocation count of the lambda.
    AtomicInteger icrCount = new AtomicInteger(0);
    ContainerMerkleTreeWriter treeWriter = buildTestTree(conf);
    final long updatedDataChecksum = treeWriter.toProto().getDataChecksum();
    IncrementalReportSender<Container> icrSender = c -> {
      // Check that the ICR contains expected info about the container.
      ContainerReplicaProto report = c.getContainerReport();
      long reportedID = report.getContainerID();
      Assertions.assertEquals(containerData.getContainerID(), reportedID);

      assertEquals(updatedDataChecksum, report.getDataChecksum());
      icrCount.incrementAndGet();
    };

    ContainerChecksumTreeManager checksumManager = new ContainerChecksumTreeManager(conf);
    KeyValueHandler keyValueHandler = new KeyValueHandler(conf, randomDatanodeDetails().getUuidString(), containerSet,
        mock(MutableVolumeSet.class), mock(ContainerMetrics.class), icrSender, checksumManager);


    // Initially, container should have no checksum information.
    assertEquals(0, containerData.getDataChecksum());
    assertFalse(checksumManager.read(containerData).hasContainerMerkleTree());
    assertFalse(ContainerChecksumTreeManager.getContainerChecksumFile(containerData).exists());
    assertEquals(0, icrCount.get());

    // Update container with checksum information.
    keyValueHandler.updateContainerChecksum(container, treeWriter);
    // Check ICR sent. The ICR sender verifies that the expected checksum is present in the report.
    assertEquals(1, icrCount.get());
    // Check all data checksums are updated correctly.
    verifyAllDataChecksumsMatch(containerData, conf);
    // Check disk content.
    ContainerProtos.ContainerChecksumInfo checksumInfo = checksumManager.read(containerData);
    assertTreesSortedAndMatch(treeWriter.toProto(), checksumInfo.getContainerMerkleTree());
  }

  @Test
  public void testGetContainerChecksumInfoOnInvalidContainerStates() {
    when(handler.handleGetContainerChecksumInfo(any(), any())).thenCallRealMethod();

    // Only mock what is necessary for the request to fail. This test does not cover allowed states.
    KeyValueContainer container = mock(KeyValueContainer.class);
    KeyValueContainerData containerData = mock(KeyValueContainerData.class);
    when(container.getContainerData()).thenReturn(containerData);

    ContainerCommandRequestProto request = mock(ContainerCommandRequestProto.class);
    when(request.hasGetContainerChecksumInfo()).thenReturn(true);
    when(request.getCmdType()).thenReturn(ContainerProtos.Type.GetContainerChecksumInfo);
    when(request.getTraceID()).thenReturn("123");

    Set<State> disallowedStates = EnumSet.allOf(State.class);
    disallowedStates.removeAll(EnumSet.of(CLOSED, QUASI_CLOSED, UNHEALTHY));

    for (State state : disallowedStates) {
      when(containerData.getState()).thenReturn(state);
      ContainerProtos.ContainerCommandResponseProto response = handler.handleGetContainerChecksumInfo(request,
          container);
      assertNotNull(response);
      assertEquals(ContainerProtos.Result.UNCLOSED_CONTAINER_IO, response.getResult());
      assertTrue(response.getMessage().contains(state.toString()), "Response message did not contain the container " +
          "state " + state);
    }
  }

  @Test
  public void testDeleteContainerTimeout() throws IOException {
    final String testDir = tempDir.toString();
    final long containerID = 1L;
    final String clusterId = UUID.randomUUID().toString();
    final String datanodeId = UUID.randomUUID().toString();
    conf = new OzoneConfiguration();
    final ContainerSet containerSet = newContainerSet();
    final MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);
    final Clock clock = mock(Clock.class);
    long startTime = Time.monotonicNow();

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
        datanodeId, containerSet, volumeSet, null, metrics,
        c -> icrReceived.incrementAndGet(), clock, new ContainerChecksumTreeManager(conf));
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

  /**
   * Test to verify that immediate ICRs are sent when container state changes,
   * and deferred ICRs are sent when closing a container without a state change.
   */
  @ContainerLayoutTestInfo.ContainerTest
  public void testICRsOnContainerClose(ContainerLayoutVersion containerLayoutVersion) throws Exception {
    final long containerID = 1L;
    final ContainerSet containerSet = newContainerSet();
    final MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);

    KeyValueContainerData containerData = new KeyValueContainerData(
        containerID, containerLayoutVersion, (long) StorageUnit.GB.toBytes(1),
        UUID.randomUUID().toString(), DATANODE_UUID);

    HddsVolume hddsVolume = new HddsVolume.Builder(tempDir.toString()).conf(conf)
        .clusterID(CLUSTER_ID).datanodeUuid(DATANODE_UUID)
        .volumeSet(volumeSet)
        .build();
    hddsVolume.format(CLUSTER_ID);
    hddsVolume.createWorkingDir(CLUSTER_ID, null);
    hddsVolume.createTmpDirs(CLUSTER_ID);

    when(volumeSet.getVolumesList()).thenReturn(Collections.singletonList(hddsVolume));
    when(volumeSet.getFailedVolumesList()).thenReturn(Collections.emptyList());

    IncrementalReportSender<Container> mockIcrSender = mock(IncrementalReportSender.class);

    KeyValueHandler kvHandler = new KeyValueHandler(conf,
        DATANODE_UUID, containerSet, volumeSet, ContainerMetrics.create(conf),
        mockIcrSender, new ContainerChecksumTreeManager(conf));
    kvHandler.setClusterID(CLUSTER_ID);

    try {
      // markContainerForClose - OPEN -> CLOSING (should send immediate ICR)
      containerData.setState(ContainerProtos.ContainerDataProto.State.OPEN);
      KeyValueContainer container = new KeyValueContainer(containerData, conf);
      container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(), CLUSTER_ID);
      containerSet.addContainer(container);

      kvHandler.markContainerForClose(container);
      verify(mockIcrSender, times(1)).send(any(Container.class)); // Immediate ICR
      verify(mockIcrSender, times(0)).sendDeferred(any(Container.class)); // No deferred ICR
      assertEquals(ContainerProtos.ContainerDataProto.State.CLOSING, container.getContainerState());

      // markContainerForClose - CLOSING -> CLOSING (should send deferred ICR)
      reset(mockIcrSender);
      kvHandler.markContainerForClose(container);

      verify(mockIcrSender, times(0)).send(any(Container.class)); // No immediate ICR
      verify(mockIcrSender, times(1)).sendDeferred(any(Container.class)); // Deferred ICR
      assertEquals(ContainerProtos.ContainerDataProto.State.CLOSING, container.getContainerState());

      // closeContainer - CLOSING -> CLOSED (should send immediate ICR)
      reset(mockIcrSender);
      kvHandler.closeContainer(container);

      verify(mockIcrSender, times(1)).send(any(Container.class));         // Immediate ICR
      verify(mockIcrSender, times(0)).sendDeferred(any(Container.class)); // No deferred ICR
      assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED, container.getContainerState());

      // closeContainer - CLOSED -> CLOSED (should return, no ICR)
      reset(mockIcrSender);
      kvHandler.closeContainer(container);

      verify(mockIcrSender, times(0)).send(any(Container.class));         // No immediate ICR
      verify(mockIcrSender, times(0)).sendDeferred(any(Container.class)); // No deferred ICR
      assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED, container.getContainerState());
    } finally {
      FileUtils.deleteDirectory(tempDir.toFile());
    }
  }

  /**
   * Test that space tracking (usedSpace and committedBytes) is correctly
   * managed during successful write operations.
   */
  @Test
  public void testWriteChunkSpaceTrackingSuccess() throws Exception {
    final long containerID = 1L;
    final String testDir = tempDir.toString();
    final String clusterId = UUID.randomUUID().toString();
    final String datanodeId = UUID.randomUUID().toString();
    OzoneConfiguration testConf = new OzoneConfiguration();
    final ContainerSet containerSet = newContainerSet();
    final MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);

    HddsVolume hddsVolume = new HddsVolume.Builder(testDir).conf(testConf)
        .clusterID(clusterId).datanodeUuid(datanodeId)
        .volumeSet(volumeSet)
        .build();
    hddsVolume.format(clusterId);
    hddsVolume.createWorkingDir(clusterId, null);
    hddsVolume.createTmpDirs(clusterId);

    when(volumeSet.getVolumesList())
        .thenReturn(Collections.singletonList(hddsVolume));

    final ContainerMetrics metrics = ContainerMetrics.create(testConf);
    final KeyValueHandler kvHandler = new KeyValueHandler(testConf,
        datanodeId, containerSet, volumeSet, metrics,
        c -> { }, new ContainerChecksumTreeManager(testConf));
    kvHandler.setClusterID(clusterId);

    final ContainerCommandRequestProto createContainer =
        createContainerRequest(datanodeId, containerID);
    kvHandler.handleCreateContainer(createContainer, null);

    KeyValueContainer container = (KeyValueContainer) containerSet.getContainer(containerID);
    assertNotNull(container);

    long initialUsedSpace = hddsVolume.getCurrentUsage().getUsedSpace();
    long initialCommittedBytes = hddsVolume.getCommittedBytes();
    long initialReservedSpace = hddsVolume.getSpaceReservedForWrites();

    long chunkSize = 1024 * 1024; // 1MB
    ContainerCommandRequestProto writeRequest =
        createWriteChunkRequest(datanodeId, chunkSize);
    ContainerProtos.ContainerCommandResponseProto response =
        kvHandler.handleWriteChunk(writeRequest, container, null);
    assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());

    long finalUsedSpace = hddsVolume.getCurrentUsage().getUsedSpace();
    long finalCommittedBytes = hddsVolume.getCommittedBytes();
    long finalReservedSpace = hddsVolume.getSpaceReservedForWrites();

    assertEquals(initialUsedSpace + chunkSize, finalUsedSpace,
        "usedSpace should be incremented by chunk size after successful write");
    assertTrue(finalCommittedBytes < initialCommittedBytes,
        "committedBytes should be decremented after successful write");
    assertEquals(initialReservedSpace, finalReservedSpace,
        "spaceReservedForWrites should be back to initial value after successful write");
  }

  /**
   * Test that space tracking is correctly rolled back when write operation fails.
   * This test uses reflection to mock the ChunkManager and inject a failure during
   * writeChunk(), which happens AFTER space is reserved. This verifies that:
   * 1. usedSpace remains unchanged (never incremented on failure)
   * 2. spaceReservedForWrites is released (incremented then decremented back)
   * 3. committedBytes is restored (decremented by writeChunk, then incremented back on rollback)
   */
  @Test
  public void testWriteChunkSpaceTrackingFailure() throws Exception {
    final long containerID = 1L;
    final String testDir = tempDir.toString();
    final String clusterId = UUID.randomUUID().toString();
    final String datanodeId = UUID.randomUUID().toString();
    OzoneConfiguration testConf = new OzoneConfiguration();
    final ContainerSet containerSet = newContainerSet();
    final MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);

    HddsVolume hddsVolume = new HddsVolume.Builder(testDir).conf(testConf)
        .clusterID(clusterId).datanodeUuid(datanodeId)
        .volumeSet(volumeSet)
        .build();
    hddsVolume.format(clusterId);
    hddsVolume.createWorkingDir(clusterId, null);
    hddsVolume.createTmpDirs(clusterId);

    when(volumeSet.getVolumesList())
        .thenReturn(Collections.singletonList(hddsVolume));

    final ContainerMetrics metrics = ContainerMetrics.create(testConf);
    final KeyValueHandler kvHandler = new KeyValueHandler(testConf,
        datanodeId, containerSet, volumeSet, metrics,
        c -> { }, new ContainerChecksumTreeManager(testConf));
    kvHandler.setClusterID(clusterId);

    final ContainerCommandRequestProto createContainer =
        createContainerRequest(datanodeId, containerID);
    kvHandler.handleCreateContainer(createContainer, null);

    KeyValueContainer container = (KeyValueContainer) containerSet.getContainer(containerID);
    assertNotNull(container);

    long initialUsedSpace = hddsVolume.getCurrentUsage().getUsedSpace();
    long initialCommittedBytes = hddsVolume.getCommittedBytes();
    long initialReservedSpace = hddsVolume.getSpaceReservedForWrites();

    // Use reflection to replace the chunkManager in the handler with a spy,
    // so we can inject a failure during writeChunk()
    Field chunkManagerField = KeyValueHandler.class.getDeclaredField("chunkManager");
    chunkManagerField.setAccessible(true);
    ChunkManager originalChunkManager = (ChunkManager) chunkManagerField.get(kvHandler);
    ChunkManager spyChunkManager = spy(originalChunkManager);

    // Configure the spy to throw an IOException on writeChunk call
    doAnswer(invocation -> {
      throw new IOException("Simulated disk write failure");
    }).when(spyChunkManager).writeChunk(
        any(Container.class),
        any(BlockID.class),
        any(ChunkInfo.class),
        any(ChunkBuffer.class),
        any(DispatcherContext.class));

    chunkManagerField.set(kvHandler, spyChunkManager);

    try {
      // Attempt to write a chunk - should fail during chunkManager.writeChunk()
      // but AFTER space has been reserved
      long chunkSize = 1024 * 1024; // 1MB
      ContainerCommandRequestProto writeRequest =
          createWriteChunkRequest(datanodeId, chunkSize);
      ContainerProtos.ContainerCommandResponseProto response =
          kvHandler.handleWriteChunk(writeRequest, container, null);

      assertNotEquals(ContainerProtos.Result.SUCCESS, response.getResult(),
          "Write should fail due to injected IOException");

      long finalUsedSpace = hddsVolume.getCurrentUsage().getUsedSpace();
      long finalCommittedBytes = hddsVolume.getCommittedBytes();
      long finalReservedSpace = hddsVolume.getSpaceReservedForWrites();

      assertEquals(initialUsedSpace, finalUsedSpace,
          "usedSpace should remain unchanged after failed write");
      assertEquals(initialCommittedBytes, finalCommittedBytes,
          "committedBytes should remain unchanged after failed write (decremented then restored)");
      assertEquals(initialReservedSpace, finalReservedSpace,
          "spaceReservedForWrites should be back to initial value after failed write");
    } finally {
      chunkManagerField.set(kvHandler, originalChunkManager);
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

  private KeyValueHandler createKeyValueHandler(Path path) throws IOException {
    final ContainerSet containerSet = newContainerSet();
    final MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);

    HddsVolume hddsVolume = new HddsVolume.Builder(path.toString()).conf(conf)
        .clusterID(CLUSTER_ID).datanodeUuid(DATANODE_UUID)
        .volumeSet(volumeSet)
        .build();
    hddsVolume.format(CLUSTER_ID);
    hddsVolume.createWorkingDir(CLUSTER_ID, null);
    hddsVolume.createTmpDirs(CLUSTER_ID);
    when(volumeSet.getVolumesList()).thenReturn(Collections.singletonList(hddsVolume));
    final KeyValueHandler kvHandler = ContainerTestUtils.getKeyValueHandler(conf,
        DATANODE_UUID, containerSet, volumeSet);
    kvHandler.setClusterID(CLUSTER_ID);
    // Clean up metrics for next tests.
    hddsVolume.getVolumeInfoStats().unregister();
    hddsVolume.getVolumeIOStats().unregister();
    ContainerMetrics.remove();

    // Register the on-demand container scanner with the container set used by the KeyValueHandler.
    ContainerController controller = new ContainerController(containerSet,
        Collections.singletonMap(ContainerType.KeyValueContainer, kvHandler));
    OnDemandContainerScanner onDemandScanner = new OnDemandContainerScanner(
        conf.getObject(ContainerScannerConfiguration.class), controller);
    containerSet.registerOnDemandScanner(onDemandScanner);

    return kvHandler;
  }

  private static class HandlerWithVolumeSet {
    private final KeyValueHandler handler;
    private final MutableVolumeSet volumeSet;
    private final ContainerSet containerSet;

    HandlerWithVolumeSet(KeyValueHandler handler, MutableVolumeSet volumeSet, ContainerSet containerSet) {
      this.handler = handler;
      this.volumeSet = volumeSet;
      this.containerSet = containerSet;
    }

    KeyValueHandler getHandler() {
      return handler;
    }

    MutableVolumeSet getVolumeSet() {
      return volumeSet;
    }

    ContainerSet getContainerSet() {
      return containerSet;
    }
  }

  private HandlerWithVolumeSet createKeyValueHandlerWithVolumeSet(Path path) throws IOException {
    ContainerMetrics.remove();
    final ContainerSet containerSet = newContainerSet();
    final MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);

    HddsVolume hddsVolume = new HddsVolume.Builder(path.toString()).conf(conf)
        .clusterID(CLUSTER_ID).datanodeUuid(DATANODE_UUID)
        .volumeSet(volumeSet)
        .build();
    hddsVolume.format(CLUSTER_ID);
    hddsVolume.createWorkingDir(CLUSTER_ID, null);
    hddsVolume.createTmpDirs(CLUSTER_ID);
    when(volumeSet.getVolumesList()).thenReturn(Collections.singletonList(hddsVolume));

    final KeyValueHandler kvHandler = ContainerTestUtils.getKeyValueHandler(conf,
        DATANODE_UUID, containerSet, volumeSet);
    kvHandler.setClusterID(CLUSTER_ID);
    hddsVolume.getVolumeInfoStats().unregister();
    hddsVolume.getVolumeIOStats().unregister();

    ContainerController controller = new ContainerController(containerSet,
        Collections.singletonMap(ContainerType.KeyValueContainer, kvHandler));
    OnDemandContainerScanner onDemandScanner = new OnDemandContainerScanner(
        conf.getObject(ContainerScannerConfiguration.class), controller);
    containerSet.registerOnDemandScanner(onDemandScanner);

    return new HandlerWithVolumeSet(kvHandler, volumeSet, containerSet);
  }

  @Test
  public void testReadBlockMetrics() throws Exception {
    Path testDir = Files.createTempDirectory("testReadBlockMetrics");
    RandomAccessFileChannel blockFile = null;
    try {
      conf.set(OZONE_SCM_CONTAINER_LAYOUT_KEY, ContainerLayoutVersion.FILE_PER_BLOCK.name());
      HandlerWithVolumeSet handlerWithVolume = createKeyValueHandlerWithVolumeSet(testDir);
      KeyValueHandler kvHandler = handlerWithVolume.getHandler();
      MutableVolumeSet volumeSet = handlerWithVolume.getVolumeSet();
      ContainerSet containerSet = handlerWithVolume.getContainerSet();

      long containerID = ContainerTestHelper.getTestContainerID();
      KeyValueContainerData containerData = new KeyValueContainerData(
          containerID, ContainerLayoutVersion.FILE_PER_BLOCK,
          (long) StorageUnit.GB.toBytes(1), UUID.randomUUID().toString(),
          DATANODE_UUID);
      KeyValueContainer container = new KeyValueContainer(containerData, conf);
      container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(), CLUSTER_ID);
      containerSet.addContainer(container);

      BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
      BlockData blockData = new BlockData(blockID);
      ChunkInfo chunkInfo = new ChunkInfo("chunk1", 0, 1024);
      blockData.addChunk(chunkInfo.getProtoBufMessage());
      kvHandler.getBlockManager().putBlock(container, blockData);

      ChunkBuffer data = ChunkBuffer.wrap(ByteBuffer.allocate(1024));
      kvHandler.getChunkManager().writeChunk(container, blockID, chunkInfo, data,
          DispatcherContext.getHandleWriteChunk());

      ContainerCommandRequestProto readBlockRequest =
          ContainerCommandRequestProto.newBuilder()
              .setCmdType(ContainerProtos.Type.ReadBlock)
              .setContainerID(containerID)
              .setDatanodeUuid(DATANODE_UUID)
              .setReadBlock(ContainerProtos.ReadBlockRequestProto.newBuilder()
                  .setBlockID(blockID.getDatanodeBlockIDProtobuf())
                  .setOffset(0)
                  .setLength(1024)
                  .build())
              .build();

      final AtomicInteger responseCount = new AtomicInteger(0);

      StreamObserver<ContainerCommandResponseProto> streamObserver =
          new StreamObserver<ContainerCommandResponseProto>() {
            @Override
            public void onNext(ContainerCommandResponseProto response) {
              assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
              responseCount.incrementAndGet();
            }

            @Override
            public void onError(Throwable t) {
              fail("ReadBlock failed", t);
            }

            @Override
            public void onCompleted() {
            }
          };

      blockFile = new RandomAccessFileChannel();
      ContainerCommandResponseProto response = kvHandler.readBlock(
          readBlockRequest, container, blockFile, streamObserver);

      assertNull(response, "ReadBlock should return null on success");
      assertTrue(responseCount.get() > 0, "Should receive at least one response");

      MetricsRecordBuilder containerMetrics = getMetrics(
          ContainerMetrics.STORAGE_CONTAINER_METRICS);
      assertCounter("bytesReadBlock", 1024L, containerMetrics);
    } finally {
      if (blockFile != null) {
        blockFile.close();
      }
      FileUtils.deleteDirectory(testDir.toFile());
      ContainerMetrics.remove();
    }
  }

  /**
   * Helper method to create a WriteChunk request for testing.
   */
  private ContainerCommandRequestProto createWriteChunkRequest(
      String datanodeId, long chunkSize) {
    final long containerID = 1L;
    final long localID = 1L;
    ByteString data = ByteString.copyFrom(new byte[(int) chunkSize]);
    
    ContainerProtos.ChunkInfo chunk = ContainerProtos.ChunkInfo.newBuilder()
        .setChunkName(localID + "_chunk_1")
        .setOffset(0)
        .setLen(data.size())
        .setChecksumData(Checksum.getNoChecksumDataProto())
        .build();

    WriteChunkRequestProto.Builder writeChunkRequest = WriteChunkRequestProto.newBuilder()
        .setBlockID(new BlockID(containerID, localID).getDatanodeBlockIDProtobuf())
        .setChunkData(chunk)
        .setData(data);

    return ContainerCommandRequestProto.newBuilder()
        .setContainerID(containerID)
        .setCmdType(ContainerProtos.Type.WriteChunk)
        .setDatanodeUuid(datanodeId)
        .setWriteChunk(writeChunkRequest)
        .build();
  }
}
