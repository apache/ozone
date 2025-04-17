package org.apache.hadoop.ozone.container.keyvalue;

import com.google.common.collect.ImmutableList;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.OnDemandContainerDataScanner;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions.getBlock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This unit test simulates three datanodes with replicas of a container that need to be reconciled.
 * It creates three KeyValueHandler instances to represent each datanode, and each instance is working on a container
 * replica that is stored in a local directory. The reconciliation client is mocked to return the corresponding local
 * container for each datanode peer.
 */
public class TestKeyValueHandlerContainerReconciliation {
  /**
   * Number of corrupt blocks and chunks.
   */
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

  // All container replicas will be placed in this directory, and the same replicas will be re-used for each test run.
  @TempDir
  private static Path containerDir;
  private static DNContainerOperationClient dnClient;
  private static MockedStatic<ContainerProtocolCalls> containerProtocolMock;
  private static Map<DatanodeDetails, MockDatanode> datanodes;
  private static OzoneConfiguration conf;

  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final long CONTAINER_ID = 100L;
  private static final int CHUNK_LEN = 3 * (int) OzoneConsts.KB;
  private static final int CHUNKS_PER_BLOCK = 4;
  private static final int NUM_DATANODES = 3;

  /**
   * Use the same container instances throughout the tests. Each reconciliation should make a full repair, resetting
   * the state for the next test.
   */
  @BeforeAll
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, containerDir.toString());
    conf.set(OZONE_METADATA_DIRS, containerDir.toString());
    dnClient = new DNContainerOperationClient(conf, null, null);
    datanodes = new HashMap<>();

    // Create a container with 3 replicas and 15 blocks each.
    for (int i = 0; i < NUM_DATANODES; i++) {
      DatanodeDetails dnDetails = randomDatanodeDetails();
      MockDatanode dn = new MockDatanode(dnDetails, conf, containerDir);
      dn.addContainerWithBlocks(CONTAINER_ID, 15);
      datanodes.put(dnDetails, dn);
    }

    containerProtocolMock = Mockito.mockStatic(ContainerProtocolCalls.class);
    mockContainerProtocolCalls();
  }

  @AfterAll
  public static void teardown() {
    containerProtocolMock.close();
  }


  @ParameterizedTest
  @MethodSource("corruptionValues")
  public void testFullContainerReconciliation(int numBlocks, int numChunks) throws Exception {
    KeyValueHandler kvHandler = createKeyValueHandler(containerDir);
    ContainerChecksumTreeManager checksumManager = kvHandler.getChecksumManager();

    // Introduce corruption in each container on different replicas.
    introduceCorruption(kvHandler, containers.get(1), numBlocks, numChunks, false);
    introduceCorruption(kvHandler, containers.get(2), numBlocks, numChunks, true);
    // Use synchronous on-demand scans to re-build the merkle trees after corruption.
    waitForContainerScans(containers);

    // Without reconciliation, checksums should be different because of the corruption.
    Set<Long> checksumsBeforeReconciliation = new HashSet<>();
    for (KeyValueContainer kvContainer : containers) {
      Optional<ContainerProtos.ContainerChecksumInfo> containerChecksumInfo =
          checksumManager.read(kvContainer.getContainerData());
      assertTrue(containerChecksumInfo.isPresent());
      long dataChecksum = containerChecksumInfo.get().getContainerMerkleTree().getDataChecksum();
      assertEquals(kvContainer.getContainerData().getDataChecksum(), dataChecksum);
      checksumsBeforeReconciliation.add(dataChecksum);
    }
    // There should be more than 1 checksum because of the corruption.
    assertTrue(checksumsBeforeReconciliation.size() > 1);


    // Setup mock for each datanode network calls needed for reconciliation.
    try (MockedStatic<ContainerProtocolCalls> containerProtocolMock =
             Mockito.mockStatic(ContainerProtocolCalls.class)) {
      mockContainerProtocolCalls(containerProtocolMock, dnToContainerMap, checksumManager, kvHandler, CONTAINER_ID);

      kvHandler.reconcileContainer(dnClient, containers.get(0), datanodes);
      kvHandler.reconcileContainer(dnClient, containers.get(1), datanodes);
      kvHandler.reconcileContainer(dnClient, containers.get(2), datanodes);

      // After reconciliation, checksums should be the same for all containers.
      // Reconciliation should have updated the tree based on the updated metadata that was obtained for the
      // previously corrupted data. We do not need to wait for the full data scan to complete.
      ContainerProtos.ContainerChecksumInfo prevContainerChecksumInfo = null;
      for (KeyValueContainer kvContainer : containers) {
        kvHandler.createContainerMerkleTreeFromMetadata(kvContainer);
        Optional<ContainerProtos.ContainerChecksumInfo> containerChecksumInfo =
            checksumManager.read(kvContainer.getContainerData());
        assertTrue(containerChecksumInfo.isPresent());
        long dataChecksum = containerChecksumInfo.get().getContainerMerkleTree().getDataChecksum();
        assertEquals(kvContainer.getContainerData().getDataChecksum(), dataChecksum);
        if (prevContainerChecksumInfo != null) {
          assertEquals(prevContainerChecksumInfo.getContainerMerkleTree().getDataChecksum(), dataChecksum);
        }
        prevContainerChecksumInfo = containerChecksumInfo.get();
      }
    }
  }

  public void waitForContainerScans(List<KeyValueContainer> containers) throws Exception {
    for (KeyValueContainer container: containers) {
      // The on-demand scanner has been initialized to pull from the mock container set.
      // Make it pull the corresponding container instance to scan in this run based on ID.
      long containerID = container.getContainerData().getContainerID();
      Mockito.doReturn(container).when(mockContainerSet).getContainer(containerID);

      Optional<Future<?>> scanFuture = OnDemandContainerDataScanner.scanContainer(container);
      assertTrue(scanFuture.isPresent());
      // Wait for on-demand scan to complete.
      scanFuture.get().get();
    }
  }

  private static void mockContainerProtocolCalls() {
    // Mock getContainerChecksumInfo
    containerProtocolMock.when(() -> ContainerProtocolCalls.getContainerChecksumInfo(any(), anyLong(), any()))
        .thenAnswer(inv -> {
          XceiverClientSpi xceiverClientSpi = inv.getArgument(0);
          long containerID = inv.getArgument(1);
          Pipeline pipeline = xceiverClientSpi.getPipeline();
          assertEquals(1, pipeline.size());
          DatanodeDetails dn = pipeline.getFirstNode();
          return datanodes.get(dn).getChecksumInfo(containerID);
        });

    // Mock getBlock
    containerProtocolMock.when(() -> ContainerProtocolCalls.getBlock(any(), any(), any(), any(), anyMap()))
        .thenAnswer(inv -> {
          XceiverClientSpi xceiverClientSpi = inv.getArgument(0);
          BlockID blockID = inv.getArgument(2);
          Pipeline pipeline = xceiverClientSpi.getPipeline();
          assertEquals(1, pipeline.size());
          DatanodeDetails dn = pipeline.getFirstNode();
          return datanodes.get(dn).getBlock(blockID);
        });

    // Mock readChunk
    containerProtocolMock.when(() -> ContainerProtocolCalls.readChunk(any(), any(), any(), any(), any()))
        .thenAnswer(inv -> {
          XceiverClientSpi xceiverClientSpi = inv.getArgument(0);
          ContainerProtos.ChunkInfo chunkInfo = inv.getArgument(1);
          ContainerProtos.DatanodeBlockID blockId = inv.getArgument(2);
          Pipeline pipeline = xceiverClientSpi.getPipeline();
          assertEquals(1, pipeline.size());
          DatanodeDetails dn = pipeline.getFirstNode();
          return datanodes.get(dn).readChunk(blockId, chunkInfo);
        });
  }

  /**
   * Introduce corruption in the container.
   * 1. Delete blocks from the container.
   * 2. Corrupt chunks at an offset.
   * If revers is true, the blocks and chunks are deleted in reverse order.
   */
  private void introduceCorruption(KeyValueHandler kvHandler, KeyValueContainer keyValueContainer, int numBlocks,
                                   int numChunks, boolean reverse) throws IOException {
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
//    Files.deleteIfExists(getContainerChecksumFile(keyValueContainer.getContainerData()).toPath());
//    kvHandler.createContainerMerkleTreeFromMetadata(keyValueContainer);

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

      // TODO: On-demand scanner (HDDS-10374) should detect this corruption and generate container merkle tree.
//      ContainerProtos.ContainerChecksumInfo.Builder builder = kvHandler.getChecksumManager()
//          .read(containerData).get().toBuilder();
//      List<ContainerProtos.BlockMerkleTree> blockMerkleTreeList = builder.getContainerMerkleTree()
//          .getBlockMerkleTreeList();
//      assertEquals(size, blockMerkleTreeList.size());

//      builder.getContainerMerkleTreeBuilder().clearBlockMerkleTree();
//      for (int j = 0; j < blockMerkleTreeList.size(); j++) {
//        ContainerProtos.BlockMerkleTree.Builder blockMerkleTreeBuilder = blockMerkleTreeList.get(j).toBuilder();
//        if (j == blockIndex) {
//          List<ContainerProtos.ChunkMerkleTree.Builder> chunkMerkleTreeBuilderList =
//              blockMerkleTreeBuilder.getChunkMerkleTreeBuilderList();
//          chunkMerkleTreeBuilderList.get(chunkIndex).setIsHealthy(false).setDataChecksum(random.nextLong());
//          blockMerkleTreeBuilder.setDataChecksum(random.nextLong());
//        }
//        builder.getContainerMerkleTreeBuilder().addBlockMerkleTree(blockMerkleTreeBuilder.build());
//      }
//      builder.getContainerMerkleTreeBuilder().setDataChecksum(random.nextLong());
//      Files.deleteIfExists(getContainerChecksumFile(keyValueContainer.getContainerData()).toPath());
//      writeContainerDataTreeProto(keyValueContainer.getContainerData(), builder.getContainerMerkleTree());
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

  private static class MockDatanode {
    private final KeyValueHandler handler;
    private final DatanodeDetails dnDetails;
    private final OnDemandContainerDataScanner onDemandScanner;
    private final ContainerSet containerSet;
    private final ConfigurationSource conf;

    public MockDatanode(DatanodeDetails dnDetails, ConfigurationSource conf, Path tempDir) throws IOException {
      this.dnDetails = dnDetails;
      this.conf = conf;
      containerSet = new ContainerSet(1000);
      handler = createKeyValueHandler(tempDir);
      ContainerController controller = new ContainerController(containerSet,
          Collections.singletonMap(ContainerProtos.ContainerType.KeyValueContainer, handler));
      onDemandScanner = new OnDemandContainerDataScanner(
          conf.getObject(ContainerScannerConfiguration.class), controller, handler.getChecksumManager());
      // Register the on-demand container scanner with the container set used by the KeyValueHandler.
      containerSet.registerContainerScanHandler(onDemandScanner::scanContainer);
    }

    public ContainerProtos.GetContainerChecksumInfoResponseProto getChecksumInfo(long containerID) throws IOException {
      KeyValueContainer container = getContainer(containerID);
      ByteString checksumInfo = handler.getChecksumManager().getContainerChecksumInfo(container.getContainerData());
      return ContainerProtos.GetContainerChecksumInfoResponseProto.newBuilder()
          .setContainerID(containerID)
          .setContainerChecksumInfo(checksumInfo)
          .build();
    }

    public ContainerProtos.GetBlockResponseProto getBlock(BlockID blockID) throws IOException {
      KeyValueContainer container = getContainer(blockID.getContainerID());
      ContainerProtos.BlockData blockData = handler.getBlockManager().getBlock(container, blockID).getProtoBufMessage();
      return ContainerProtos.GetBlockResponseProto.newBuilder()
          .setBlockData(blockData)
          .build();
    }

    public ContainerProtos.ReadChunkResponseProto readChunk(ContainerProtos.DatanodeBlockID blockId,
        ContainerProtos.ChunkInfo chunkInfo) throws IOException {
      KeyValueContainer container = getContainer(blockId.getContainerID());
      return ContainerProtos.ReadChunkResponseProto.newBuilder()
          .setBlockID(blockId)
          .setChunkData(chunkInfo)
          .setData(handler.getChunkManager().readChunk(container, BlockID.getFromProtobuf(blockId),
              ChunkInfo.getFromProtoBuf(chunkInfo), null).toByteString())
          .build();
    }

    public KeyValueContainer getContainer(long containerID) {
      return (KeyValueContainer) containerSet.getContainer(containerID);
    }

    public void scanContainer(long containerID) {
//      onDemandScanner.scanContainer(containerSet.getContainer(containerID));
    }

    public void reconcileContainer(DNContainerOperationClient dnClient, Collection<DatanodeDetails> peers,
        long containerID) throws IOException {
      handler.reconcileContainer(dnClient, containerSet.getContainer(containerID), peers);
    }

    private KeyValueHandler createKeyValueHandler(Path path) throws IOException {
      final String dnUUID = dnDetails.getUuidString();
      final MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);

      // TODO this path and addContainer in this class may be using different parts of the temp dir
      HddsVolume hddsVolume = new HddsVolume.Builder(path.toString())
          .conf(conf)
          .clusterID(CLUSTER_ID)
          .datanodeUuid(dnUUID)
          .volumeSet(volumeSet)
          .build();
      hddsVolume.format(CLUSTER_ID);
      hddsVolume.createWorkingDir(CLUSTER_ID, null);
      hddsVolume.createTmpDirs(CLUSTER_ID);
      when(volumeSet.getVolumesList()).thenReturn(Collections.singletonList(hddsVolume));
      final KeyValueHandler kvHandler = ContainerTestUtils.getKeyValueHandler(conf,
          dnUUID, containerSet, volumeSet);
      kvHandler.setClusterID(CLUSTER_ID);
      // Clean up metrics for next tests.
      hddsVolume.getVolumeInfoStats().unregister();
      hddsVolume.getVolumeIOStats().unregister();
      ContainerMetrics.remove();

      return kvHandler;
    }

    /**
     * Creates a container with normal and deleted blocks.
     * First it will insert normal blocks, and then it will insert
     * deleted blocks.
     */
    public void addContainerWithBlocks(long containerId, int blocks) throws Exception {
      String strBlock = "block";
      String strChunk = "chunkFile";
      MutableVolumeSet volumeSet = new MutableVolumeSet(dnDetails.getUuidString(), conf, null,
          StorageVolume.VolumeType.DATA_VOLUME, null);
      createDbInstancesForTestIfNeeded(volumeSet, CLUSTER_ID, CLUSTER_ID, conf);
      int bytesPerChecksum = 2 * (int) OzoneConsts.KB;
      Checksum checksum = new Checksum(ContainerProtos.ChecksumType.SHA256,
          bytesPerChecksum);
      byte[] chunkData = RandomStringUtils.randomAscii(CHUNK_LEN).getBytes(UTF_8);
      ChecksumData checksumData = checksum.computeChecksum(chunkData);

      KeyValueContainerData containerData = new KeyValueContainerData(containerId,
          ContainerLayoutVersion.FILE_PER_BLOCK, (long) CHUNKS_PER_BLOCK * CHUNK_LEN * blocks,
          UUID.randomUUID().toString(), UUID.randomUUID().toString());
      Path kvContainerPath = Files.createDirectory(containerDir.resolve(UUID.randomUUID().toString()));
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
          handler.getChunkManager().writeChunk(container, blockID, info,
              ByteBuffer.wrap(chunkData), WRITE_STAGE);
        }
        handler.getChunkManager().finishWriteChunks(container, blockData);
        blockData.setChunks(chunkList);
        blockData.setBlockCommitSequenceId(i);
        handler.getBlockManager().putBlock(container, blockData);

        ContainerLayoutTestInfo.FILE_PER_BLOCK.validateFileCount(chunksPath, blocks, (long) blocks * CHUNKS_PER_BLOCK);
        container.markContainerForClose();
        handler.closeContainer(container);
      }
      containerSet.addContainer(container);
    }
  }
}
