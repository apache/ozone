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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_CHOOSING_POLICY;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_LAYOUT_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager.getContainerChecksumFile;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.createBlockMetaData;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.writeContainerDataTreeProto;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.COMMIT_STAGE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions.getBlock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.token.TokenVerifier;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.TokenHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.report.IncrementalReportSender;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.util.Sets;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

/**
 * Unit tests for {@link KeyValueHandler}.
 */
@Timeout(300)
public class TestKeyValueHandler {

  @TempDir
  private Path tempDir;
  @TempDir
  private Path dbFile;
  @TempDir
  private Path testRoot;

  private static final long DUMMY_CONTAINER_ID = 9999;
  private static final String DUMMY_PATH = "dummy/dir/doesnt/exist";
  private static final int UNIT_LEN = 1024;
  private static final int CHUNK_LEN = 3 * UNIT_LEN;
  private static final int CHUNKS_PER_BLOCK = 4;
  private static final String DATANODE_UUID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();

  private HddsDispatcher dispatcher;
  private KeyValueHandler handler;
  private OzoneConfiguration conf;

  public static Stream<Arguments> corruptionValues() {
    return Stream.of(
        Arguments.of(5, 0),
        Arguments.of(0, 5),
        Arguments.of(0, 10),
        Arguments.of(10, 0),
        Arguments.of(5, 10),
        Arguments.of(10, 5),
        Arguments.of(2, 3),
        Arguments.of(3, 2),
        Arguments.of(4, 6),
        Arguments.of(6, 4),
        Arguments.of(6, 9),
        Arguments.of(9, 6)
    );
  }

  @BeforeEach
  public void setup() throws IOException {
    // Create mock HddsDispatcher and KeyValueHandler.
    conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, testRoot.toString());
    conf.set(OZONE_METADATA_DIRS, testRoot.toString());
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
      ContainerSet cset = new ContainerSet(1000);
      DatanodeDetails datanodeDetails = mock(DatanodeDetails.class);
      StateContext context = ContainerTestUtils.getMockContext(
          datanodeDetails, conf);
      KeyValueHandler keyValueHandler = ContainerTestUtils.getKeyValueHandler(conf,
          context.getParent().getDatanodeDetails().getUuidString(), cset, volumeSet);
      assertEquals("org.apache.hadoop.ozone.container.common" +
          ".volume.CapacityVolumeChoosingPolicy",
          keyValueHandler.getVolumeChoosingPolicyForTesting()
              .getClass().getName());

      // Ensures that KeyValueHandler falls back to FILE_PER_BLOCK.
      conf.set(OZONE_SCM_CONTAINER_LAYOUT_KEY, "FILE_PER_CHUNK");
      ContainerTestUtils.getKeyValueHandler(conf,
          context.getParent().getDatanodeDetails().getUuidString(), cset, volumeSet);
      assertEquals(ContainerLayoutVersion.FILE_PER_BLOCK,
          conf.getEnum(OZONE_SCM_CONTAINER_LAYOUT_KEY, ContainerLayoutVersion.FILE_PER_CHUNK));

      //Set a class which is not of sub class of VolumeChoosingPolicy
      conf.set(HDDS_DATANODE_VOLUME_CHOOSING_POLICY,
          "org.apache.hadoop.ozone.container.common.impl.HddsDispatcher");
      RuntimeException exception = assertThrows(RuntimeException.class,
          () -> ContainerTestUtils.getKeyValueHandler(conf, context.getParent().getDatanodeDetails().getUuidString(),
              cset, volumeSet));

      assertThat(exception).hasMessageEndingWith(
          "class org.apache.hadoop.ozone.container.common.impl.HddsDispatcher " +
              "not org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy");
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
    assertTrue(ContainerChecksumTreeManager.checksumFileExist(container));

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
      conf = new OzoneConfiguration();
      final ContainerSet containerSet = new ContainerSet(1000);
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

  @ContainerLayoutTestInfo.ContainerTest
  public void testReconcileContainer(ContainerLayoutVersion layoutVersion) throws Exception {
    conf = new OzoneConfiguration();

    KeyValueContainerData data = new KeyValueContainerData(123L, layoutVersion, GB,
        PipelineID.randomId().toString(), randomDatanodeDetails().getUuidString());
    data.setMetadataPath(tempDir.toString());
    data.setDbFile(dbFile.toFile());

    Container container = new KeyValueContainer(data, conf);
    createBlockMetaData(data, 5, 3);
    ContainerSet containerSet = new ContainerSet(1000);
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
    Mockito.verify(mockDnClient, times(3)).getContainerChecksumInfo(anyLong(), any());
    Assertions.assertEquals(1, icrCount.get());
  }

  @ParameterizedTest
  @MethodSource("corruptionValues")
  public void testFullContainerReconciliation(int numBlocks, int numChunks) throws Exception {
    KeyValueHandler kvHandler = createKeyValueHandler(testRoot);
    ContainerChecksumTreeManager checksumManager = kvHandler.getChecksumManager();
    DNContainerOperationClient dnClient = mock(DNContainerOperationClient.class);
    XceiverClientManager xceiverClientManager = mock(XceiverClientManager.class);
    TokenHelper tokenHelper = new TokenHelper(new SecurityConfig(conf), null);
    when(dnClient.getTokenHelper()).thenReturn(tokenHelper);
    when(dnClient.getXceiverClientManager()).thenReturn(xceiverClientManager);
    final long containerID = 100L;
    // Create 3 containers with 10 blocks each and 3 replicas.
    List<KeyValueContainer> containers = createContainerWithBlocks(kvHandler, containerID, 15, 3);
    assertEquals(3, containers.size());

    // Introduce corruption in each container on different replicas.
    introduceCorruption(kvHandler, containers.get(1), numBlocks, numChunks, false);
    introduceCorruption(kvHandler, containers.get(2), numBlocks, numChunks, true);

    // Without reconciliation, checksums should be different because of the corruption.
    Set<Long> checksumsBeforeReconciliation = new HashSet<>();
    for (KeyValueContainer kvContainer : containers) {
      kvHandler.createContainerMerkleTree(kvContainer);
      Optional<ContainerProtos.ContainerChecksumInfo> containerChecksumInfo =
          checksumManager.read(kvContainer.getContainerData());
      assertTrue(containerChecksumInfo.isPresent());
      checksumsBeforeReconciliation.add(containerChecksumInfo.get().getContainerMerkleTree().getDataChecksum());
    }
    // There should be more than 1 checksum because of the corruption.
    assertTrue(checksumsBeforeReconciliation.size() > 1);

    List<DatanodeDetails> datanodes = Lists.newArrayList(randomDatanodeDetails(), randomDatanodeDetails(),
        randomDatanodeDetails());

    // Setup mock for each datanode network calls needed for reconciliation.
    try (MockedStatic<ContainerProtocolCalls> containerProtocolMock = Mockito.mockStatic(ContainerProtocolCalls.class);
         MockedStatic<DNContainerOperationClient> dnClientMock = Mockito.mockStatic(DNContainerOperationClient.class)) {

      for (int i = 0; i < datanodes.size(); i++) {
        DatanodeDetails datanode = datanodes.get(i);
        KeyValueContainer container = containers.get(i);

        Pipeline pipeline = mock(Pipeline.class);
        XceiverClientSpi client = mock(XceiverClientSpi.class);

        dnClientMock.when(() -> DNContainerOperationClient.createSingleNodePipeline(datanode)).thenReturn(pipeline);
        when(xceiverClientManager.acquireClient(pipeline)).thenReturn(client);
        doNothing().when(xceiverClientManager).releaseClient(eq(client), anyBoolean());
        when(client.getPipeline()).thenReturn(pipeline);

        // Mock checksum info
        when(dnClient.getContainerChecksumInfo(containerID, datanode))
            .thenReturn(checksumManager.read(container.getContainerData()).get());

        // Mock getBlock
        containerProtocolMock.when(() -> ContainerProtocolCalls.getBlock(eq(client), any(), any(), anyMap()))
            .thenAnswer(inv -> ContainerProtos.GetBlockResponseProto.newBuilder()
                .setBlockData(kvHandler.getBlockManager().getBlock(container, inv.getArgument(1)).getProtoBufMessage())
                .build());

        // Mock readChunk
        containerProtocolMock.when(() -> ContainerProtocolCalls.readChunk(eq(client), any(), any(), any(), any()))
            .thenAnswer(inv -> createReadChunkResponse(inv, container, kvHandler));
      }

      kvHandler.reconcileContainer(dnClient, containers.get(0), Sets.newHashSet(datanodes));
      kvHandler.reconcileContainer(dnClient, containers.get(1), Sets.newHashSet(datanodes));
      kvHandler.reconcileContainer(dnClient, containers.get(2), Sets.newHashSet(datanodes));

      // After reconciliation, checksums should be the same for all containers.
      ContainerProtos.ContainerChecksumInfo prevContainerChecksumInfo = null;
      for (KeyValueContainer kvContainer : containers) {
        kvHandler.createContainerMerkleTree(kvContainer);
        Optional<ContainerProtos.ContainerChecksumInfo> containerChecksumInfo =
            checksumManager.read(kvContainer.getContainerData());
        assertTrue(containerChecksumInfo.isPresent());
        if (prevContainerChecksumInfo != null) {
          assertEquals(prevContainerChecksumInfo.getContainerMerkleTree().getDataChecksum(),
              containerChecksumInfo.get().getContainerMerkleTree().getDataChecksum());
        }
        prevContainerChecksumInfo = containerChecksumInfo.get();
      }
    }
  }

  // Helper method to create readChunk responses
  private ContainerProtos.ReadChunkResponseProto createReadChunkResponse(InvocationOnMock inv,
                                                                         KeyValueContainer container,
                                                                         KeyValueHandler kvHandler) throws IOException {
    ContainerProtos.DatanodeBlockID blockId = inv.getArgument(2);
    ContainerProtos.ChunkInfo chunkInfo = inv.getArgument(1);
    return ContainerProtos.ReadChunkResponseProto.newBuilder()
        .setBlockID(blockId)
        .setChunkData(chunkInfo)
        .setData(kvHandler.getChunkManager().readChunk(container, BlockID.getFromProtobuf(blockId),
                ChunkInfo.getFromProtoBuf(chunkInfo), null).toByteString())
        .build();
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
    final ContainerSet containerSet = new ContainerSet(1000);
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
        datanodeId, containerSet, volumeSet, metrics,
        c -> icrReceived.incrementAndGet(), new ContainerChecksumTreeManager(conf), clock);
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

  private KeyValueHandler createKeyValueHandler(Path path) throws IOException {
    final ContainerSet containerSet = new ContainerSet(1000);
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
    return kvHandler;
  }

  /**
   * Creates a container with normal and deleted blocks.
   * First it will insert normal blocks, and then it will insert
   * deleted blocks.
   */
  protected List<KeyValueContainer> createContainerWithBlocks(KeyValueHandler kvHandler, long containerId,
                                                              int blocks, int numContainerCopy)
      throws Exception {
    String strBlock = "block";
    String strChunk = "chunkFile";
    List<KeyValueContainer> containers = new ArrayList<>();
    MutableVolumeSet volumeSet = new MutableVolumeSet(DATANODE_UUID, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    createDbInstancesForTestIfNeeded(volumeSet, CLUSTER_ID, CLUSTER_ID, conf);
    int bytesPerChecksum = 2 * UNIT_LEN;
    Checksum checksum = new Checksum(ContainerProtos.ChecksumType.SHA256,
        bytesPerChecksum);
    byte[] chunkData = RandomStringUtils.randomAscii(CHUNK_LEN).getBytes(UTF_8);
    ChecksumData checksumData = checksum.computeChecksum(chunkData);

    for (int j =  0; j < numContainerCopy; j++) {
      KeyValueContainerData containerData = new KeyValueContainerData(containerId,
          ContainerLayoutVersion.FILE_PER_BLOCK, (long) CHUNKS_PER_BLOCK * CHUNK_LEN * blocks,
          UUID.randomUUID().toString(), UUID.randomUUID().toString());
      Path kvContainerPath = Files.createDirectory(testRoot.resolve(containerId + "-" + j));
      containerData.setMetadataPath(kvContainerPath.toString());
      containerData.setDbFile(kvContainerPath.toFile());

      KeyValueContainer container = new KeyValueContainer(containerData, conf);
      StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())
          .forEach(hddsVolume -> hddsVolume.setDbParentDir(kvContainerPath.toFile()));
      container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(), UUID.randomUUID().toString());
      assertNotNull(containerData.getChunksPath());
      File chunksPath = new File(containerData.getChunksPath());
      ContainerLayoutTestInfo.FILE_PER_BLOCK.validateFileCount(chunksPath, 0, 0);

      List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();
      for (int i = 0; i < blocks; i++) {
        BlockID blockID = new BlockID(containerId, i);
        BlockData blockData = new BlockData(blockID);

        chunkList.clear();
        for (long chunkCount = 0; chunkCount < CHUNKS_PER_BLOCK; chunkCount++) {
          String chunkName = strBlock + i + strChunk + chunkCount;
          long offset = chunkCount * CHUNK_LEN;
          ChunkInfo info = new ChunkInfo(chunkName, offset, CHUNK_LEN);
          info.setChecksumData(checksumData);
          chunkList.add(info.getProtoBufMessage());
          kvHandler.getChunkManager().writeChunk(container, blockID, info,
              ByteBuffer.wrap(chunkData), WRITE_STAGE);
          kvHandler.getChunkManager().writeChunk(container, blockID, info,
              ByteBuffer.wrap(chunkData), COMMIT_STAGE);
        }
        blockData.setChunks(chunkList);
        blockData.setBlockCommitSequenceId(i);
        kvHandler.getBlockManager().putBlock(container, blockData);
      }

      ContainerLayoutTestInfo.FILE_PER_BLOCK.validateFileCount(chunksPath, blocks, (long) blocks * CHUNKS_PER_BLOCK);
      container.close();
      kvHandler.closeContainer(container);
      containers.add(container);
    }

    return containers;
  }

  private void introduceCorruption(KeyValueHandler kvHandler, KeyValueContainer keyValueContainer, int numBlocks,
                                   int numChunks, boolean reverse) throws IOException {
    Random random = new Random();
    KeyValueContainerData containerData = keyValueContainer.getContainerData();
    // Simulate missing blocks
    try (DBHandle handle = BlockUtils.getDB(containerData, conf);
         BatchOperation batch = handle.getStore().getBatchHandler().initBatchOperation()) {
      List<BlockData> blockDataList = kvHandler.getBlockManager().listBlock(keyValueContainer, -1, 100);
      int size = blockDataList.size();
      for (int i = 0; i < numBlocks; i++) {
        BlockData blockData = reverse ? blockDataList.get(size - 1 - i) : blockDataList.get(i);
        File blockFile = getBlock(keyValueContainer, blockData.getBlockID().getLocalID());
        Assertions.assertTrue(blockFile.delete());
        handle.getStore().getBlockDataTable().deleteWithBatch(batch, containerData.getBlockKey(blockData.getLocalID()));
      }
      handle.getStore().getBatchHandler().commitBatchOperation(batch);
    }
    Files.deleteIfExists(getContainerChecksumFile(keyValueContainer.getContainerData()).toPath());
    kvHandler.createContainerMerkleTree(keyValueContainer);

    // Corrupt chunks at an offset.
    List<BlockData> blockDataList = kvHandler.getBlockManager().listBlock(keyValueContainer, -1, 100);
    int size = blockDataList.size();
    for (int i = 0; i < numChunks; i++) {
      int blockIndex = reverse ? size - 1 - (i % size) : i % size;
      BlockData blockData = blockDataList.get(blockIndex);
      int chunkIndex = i / size;
      File blockFile = getBlock(keyValueContainer, blockData.getBlockID().getLocalID());
      List<ContainerProtos.ChunkInfo> chunks = new ArrayList<>(blockData.getChunks());
      ContainerProtos.ChunkInfo chunkInfo = chunks.remove(chunkIndex);
      corruptFileAtOffset(blockFile, (int) chunkInfo.getOffset(), (int) chunkInfo.getLen());

      // TODO: On-demand scanner should detect this corruption and generate container merkle tree.
      ContainerProtos.ContainerChecksumInfo.Builder builder = kvHandler.getChecksumManager()
          .read(containerData).get().toBuilder();
      List<ContainerProtos.BlockMerkleTree> blockMerkleTreeList = builder.getContainerMerkleTree()
          .getBlockMerkleTreeList();
      assertEquals(size, blockMerkleTreeList.size());

      builder.getContainerMerkleTreeBuilder().clearBlockMerkleTree();
      for (int j = 0; j < blockMerkleTreeList.size(); j++) {
        ContainerProtos.BlockMerkleTree.Builder blockMerkleTreeBuilder = blockMerkleTreeList.get(j).toBuilder();
        if (j == blockIndex) {
          List<ContainerProtos.ChunkMerkleTree.Builder> chunkMerkleTreeBuilderList =
              blockMerkleTreeBuilder.getChunkMerkleTreeBuilderList();
          chunkMerkleTreeBuilderList.get(chunkIndex).setIsHealthy(false).setDataChecksum(random.nextLong());
          blockMerkleTreeBuilder.setDataChecksum(random.nextLong());
        }
        builder.getContainerMerkleTreeBuilder().addBlockMerkleTree(blockMerkleTreeBuilder.build());
      }
      builder.getContainerMerkleTreeBuilder().setDataChecksum(random.nextLong());
      Files.deleteIfExists(getContainerChecksumFile(keyValueContainer.getContainerData()).toPath());
      writeContainerDataTreeProto(keyValueContainer.getContainerData(), builder.getContainerMerkleTree());
    }
  }

  /**
   * Overwrite the file with random bytes at an offset within the given length.
   */
  public static void corruptFileAtOffset(File file, int offset, int chunkLength) {
    try {
      final int fileLength = (int) file.length();
      assertTrue(fileLength >= offset + chunkLength);
      final int chunkEnd = offset + chunkLength;

      Path path = file.toPath();
      final byte[] original = IOUtils.readFully(Files.newInputStream(path), fileLength);

      // Corrupt the last byte and middle bytes of the block. The scanner should log this as two errors.
      final byte[] corruptedBytes = Arrays.copyOf(original, fileLength);
      corruptedBytes[chunkEnd - 1] = (byte) (original[chunkEnd - 1] << 1);
      final long chunkMid = offset + ((long) chunkLength - offset) / 2;
      corruptedBytes[(int) (chunkMid / 2)] = (byte) (original[(int) (chunkMid / 2)] << 1);


      Files.write(path, corruptedBytes,
          StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

      assertThat(IOUtils.readFully(Files.newInputStream(path), fileLength))
          .isEqualTo(corruptedBytes)
          .isNotEqualTo(original);
    } catch (IOException ex) {
      // Fail the test.
      throw new UncheckedIOException(ex);
    }
  }
}
