package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.OnDemandContainerDataScanner;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;

/**
 * This unit test simulates three datanodes with replicas of a container that need to be reconciled.
 * It creates three KeyValueHandler instances to represent each datanode, and each instance is working on a container
 * replica that is stored in a local directory. The reconciliation client is mocked to return the corresponding local
 * container for each datanode peer.
 */
public class TestKeyValueHandlerContainerReconciliation {
  /**
   * Number of corrupt blocks and chunks.
   *
   * TODO HDDS-11942 support more combinations of corruptions.
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
//  @TempDir
  private static Path containerDir;
  private static DNContainerOperationClient dnClient;
  private static MockedStatic<ContainerProtocolCalls> containerProtocolMock;
  private static List<MockDatanode> datanodes;
  private static long healthyDataChecksum;

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
    containerDir = Files.createTempDirectory("reconcile");
    dnClient = new DNContainerOperationClient(new OzoneConfiguration(), null, null);
    datanodes = new ArrayList<>();

    // Create a container with 15 blocks and 3 replicas.
    for (int i = 0; i < NUM_DATANODES; i++) {
      DatanodeDetails dnDetails = randomDatanodeDetails();
      MockDatanode dn = new MockDatanode(dnDetails, containerDir);
      dn.addContainerWithBlocks(CONTAINER_ID, 15);
      datanodes.add(dn);
    }

    datanodes.forEach(d -> d.scanContainer(CONTAINER_ID));
    healthyDataChecksum = assertUniqueChecksumCount(CONTAINER_ID, datanodes, 1);

    containerProtocolMock = Mockito.mockStatic(ContainerProtocolCalls.class);
    mockContainerProtocolCalls();
  }

  @AfterAll
  public static void teardown() {
    if (containerProtocolMock != null) {
      containerProtocolMock.close();
    }
  }

  @ParameterizedTest
  @MethodSource("corruptionValues")
  public void testContainerReconciliation(int numBlocksToDelete, int numChunksToCorrupt) throws Exception {
    // Introduce corruption in each container on different replicas.
    List<MockDatanode> dnsToCorrupt = datanodes.stream().limit(2).collect(Collectors.toList());

    dnsToCorrupt.get(0).introduceCorruption(CONTAINER_ID, numBlocksToDelete, numChunksToCorrupt, false);
    dnsToCorrupt.get(1).introduceCorruption(CONTAINER_ID, numBlocksToDelete, numChunksToCorrupt, true);
    // Use synchronous on-demand scans to re-build the merkle trees after corruption.
    dnsToCorrupt.forEach(d -> d.scanContainer(CONTAINER_ID));
    // Without reconciliation, checksums should be different because of the corruption.
    assertUniqueChecksumCount(CONTAINER_ID, datanodes, 3);

    List<DatanodeDetails> peers = datanodes.stream().map(MockDatanode::getDnDetails).collect(Collectors.toList());
    datanodes.forEach(d -> d.reconcileContainer(dnClient, peers, CONTAINER_ID));
    // After reconciliation, checksums should be the same for all containers.
    // Reconciliation should have updated the tree based on the updated metadata that was obtained for the
    // previously corrupted data. We do not need to wait for the full data scan to complete.
    long repairedDataChecksum = assertUniqueChecksumCount(CONTAINER_ID, datanodes, 1);
    assertEquals(healthyDataChecksum, repairedDataChecksum);
  }

  /**
   * Checks for the expected number of unique checksums among a container on the provided datanodes.
   * @return The data checksum from one of the nodes. Useful if expectedUniqueChecksums = 1.
   */
  private static long assertUniqueChecksumCount(long containerID, Collection<MockDatanode> datanodes,
      long expectedUniqueChecksums) {
    long actualUniqueChecksums = datanodes.stream()
        .mapToLong(d -> d.checkAndGetDataChecksum(containerID))
        .distinct()
        .count();
    assertEquals(expectedUniqueChecksums, actualUniqueChecksums);
    return datanodes.stream().findAny().get().checkAndGetDataChecksum(containerID);
  }

  private static void mockContainerProtocolCalls() {
    Map<DatanodeDetails, MockDatanode> dnMap = datanodes.stream()
        .collect(Collectors.toMap(MockDatanode::getDnDetails, Function.identity()));

    // Mock getContainerChecksumInfo
    containerProtocolMock.when(() -> ContainerProtocolCalls.getContainerChecksumInfo(any(), anyLong(), any()))
        .thenAnswer(inv -> {
          XceiverClientSpi xceiverClientSpi = inv.getArgument(0);
          long containerID = inv.getArgument(1);
          Pipeline pipeline = xceiverClientSpi.getPipeline();
          assertEquals(1, pipeline.size());
          DatanodeDetails dn = pipeline.getFirstNode();
          return dnMap.get(dn).getChecksumInfo(containerID);
        });

    // Mock getBlock
    containerProtocolMock.when(() -> ContainerProtocolCalls.getBlock(any(), any(), any(), any(), anyMap()))
        .thenAnswer(inv -> {
          XceiverClientSpi xceiverClientSpi = inv.getArgument(0);
          BlockID blockID = inv.getArgument(2);
          Pipeline pipeline = xceiverClientSpi.getPipeline();
          assertEquals(1, pipeline.size());
          DatanodeDetails dn = pipeline.getFirstNode();
          return dnMap.get(dn).getBlock(blockID);
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
          return dnMap.get(dn).readChunk(blockId, chunkInfo);
        });
  }

  /**
   * This class wraps a KeyValueHandler instance with just enough features to test its reconciliation functionality.
   */
  private static class MockDatanode {
    private final KeyValueHandler handler;
    private final DatanodeDetails dnDetails;
    private final OnDemandContainerDataScanner onDemandScanner;
    private final ContainerSet containerSet;
    private final OzoneConfiguration conf;

    public MockDatanode(DatanodeDetails dnDetails, Path tempDir) throws IOException {
      this.dnDetails = dnDetails;
      Path dataVolume = Paths.get(tempDir.toString(), dnDetails.getUuidString(), "data");
      Path metadataVolume = Paths.get(tempDir.toString(), dnDetails.getUuidString(), "metadata");

      this.conf = new OzoneConfiguration();
      conf.set(HDDS_DATANODE_DIR_KEY, dataVolume.toString());
      conf.set(OZONE_METADATA_DIRS, metadataVolume.toString());

      containerSet = new ContainerSet(1000);
      MutableVolumeSet volumeSet = createVolumeSet();
      handler = ContainerTestUtils.getKeyValueHandler(conf, dnDetails.getUuidString(), containerSet, volumeSet);
      handler.setClusterID(CLUSTER_ID);

      ContainerController controller = new ContainerController(containerSet,
          Collections.singletonMap(ContainerProtos.ContainerType.KeyValueContainer, handler));
      onDemandScanner = new OnDemandContainerDataScanner(
          conf.getObject(ContainerScannerConfiguration.class), controller, handler.getChecksumManager());
      // Register the on-demand container scanner with the container set used by the KeyValueHandler.
      containerSet.registerContainerScanHandler(onDemandScanner::scanContainer);
    }

    public DatanodeDetails getDnDetails() {
      return dnDetails;
    }

    /**
     * @throws IOException for general IO errors accessing the checksum file
     * @throws java.io.FileNotFoundException When the checksum file does not exist.
     */
    public ContainerProtos.GetContainerChecksumInfoResponseProto getChecksumInfo(long containerID) throws IOException {
      KeyValueContainer container = getContainer(containerID);
      ByteString checksumInfo = handler.getChecksumManager().getContainerChecksumInfo(container.getContainerData());
      return ContainerProtos.GetContainerChecksumInfoResponseProto.newBuilder()
          .setContainerID(containerID)
          .setContainerChecksumInfo(checksumInfo)
          .build();
    }

    /**
     * Verifies that the data checksum on disk matches the one in memory, and returns the data checksum.
     */
    public long checkAndGetDataChecksum(long containerID) {
      KeyValueContainer container = getContainer(containerID);
      long dataChecksum = 0;
      try {
        Optional<ContainerProtos.ContainerChecksumInfo> containerChecksumInfo =
            handler.getChecksumManager().read(container.getContainerData());
        assertTrue(containerChecksumInfo.isPresent());
        dataChecksum = containerChecksumInfo.get().getContainerMerkleTree().getDataChecksum();
        assertEquals(container.getContainerData().getDataChecksum(), dataChecksum);
      } catch (IOException ex) {
        fail("Failed to read container checksum from disk", ex);
      }
      System.err.println("data checksum on DN " + dnDetails.getUuidString() + ": " + dataChecksum);
      return dataChecksum;
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

    /**
     * Triggers a synchronous scan of the container. This method will block until the scan completes.
     */
    public void scanContainer(long containerID) {
      Optional<Future<?>> scanFuture = onDemandScanner.scanContainer(containerSet.getContainer(containerID));
      assertTrue(scanFuture.isPresent());

      try {
        scanFuture.get().get();
      } catch (InterruptedException | ExecutionException e) {
        fail("On demand container scan failed", e);
      }

      // TODO: On-demand scanner (HDDS-10374) should detect this corruption and generate container merkle tree.
//      ContainerProtos.ContainerChecksumInfo.Builder builder = kvHandler.getChecksumManager()
//          .read(containerData).get().toBuilder();
//      List<ContainerProtos.BlockMerkleTree> blockMerkleTreeList = builder.getContainerMerkleTree()
//          .getBlockMerkleTreeList();
//      assertEquals(size, blockMerkleTreeList.size());
//
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

    public void reconcileContainer(DNContainerOperationClient dnClient, Collection<DatanodeDetails> peers,
        long containerID) {
      try {
        handler.reconcileContainer(dnClient, containerSet.getContainer(containerID), peers);
      } catch (IOException ex) {
        fail("Container reconciliation failed", ex);
      }
    }

    /**
     * Creates a container with normal and deleted blocks.
     * First it will insert normal blocks, and then it will insert
     * deleted blocks.
     */
    public void addContainerWithBlocks(long containerId, int blocks) throws Exception {
      ContainerProtos.CreateContainerRequestProto createRequest =
          ContainerProtos.CreateContainerRequestProto.newBuilder()
              .setContainerType(ContainerProtos.ContainerType.KeyValueContainer)
              .build();
      ContainerProtos.ContainerCommandRequestProto request =
          ContainerProtos.ContainerCommandRequestProto.newBuilder()
              .setCmdType(ContainerProtos.Type.CreateContainer)
              .setCreateContainer(createRequest)
              .setContainerID(containerId)
              .setDatanodeUuid(dnDetails.getUuidString())
              .build();

      handler.handleCreateContainer(request,null);
      KeyValueContainer container = getContainer(containerId);

      // Verify container is initially empty.
      File chunksPath = new File(container.getContainerData().getChunksPath());
      ContainerLayoutTestInfo.FILE_PER_BLOCK.validateFileCount(chunksPath, 0, 0);

      // Create data to put in the container.
      // Seed using the container ID so that all replicas are identical.
      Random byteGenerator = new Random(containerId);
      // This array will keep getting populated with new bytes for each chunk.
      byte[] chunkData = new byte[CHUNK_LEN];
      int bytesPerChecksum = 2 * (int) OzoneConsts.KB;

      // Add data to the container.
      List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();
      for (int i = 0; i < blocks; i++) {
        BlockID blockID = new BlockID(containerId, i);
        BlockData blockData = new BlockData(blockID);

        chunkList.clear();
        for (long chunkCount = 0; chunkCount < CHUNKS_PER_BLOCK; chunkCount++) {
          String chunkName = "chunk" + chunkCount;
          long offset = chunkCount * chunkData.length;
          ChunkInfo info = new ChunkInfo(chunkName, offset, chunkData.length);
          // Generate data for the chunk and compute its checksum.
          byteGenerator.nextBytes(chunkData);
          Checksum checksum = new Checksum(ContainerProtos.ChecksumType.SHA256, bytesPerChecksum);
          ChecksumData checksumData = checksum.computeChecksum(chunkData);
          info.setChecksumData(checksumData);
          // Write chunk and checksum into the container.
          chunkList.add(info.getProtoBufMessage());
          handler.getChunkManager().writeChunk(container, blockID, info,
              ByteBuffer.wrap(chunkData), WRITE_STAGE);
        }
        handler.getChunkManager().finishWriteChunks(container, blockData);
        blockData.setChunks(chunkList);
        blockData.setBlockCommitSequenceId(i);
        handler.getBlockManager().putBlock(container, blockData);
      }
      ContainerLayoutTestInfo.FILE_PER_BLOCK.validateFileCount(chunksPath, blocks, (long) blocks * CHUNKS_PER_BLOCK);
      container.markContainerForClose();
      handler.closeContainer(container);
    }

    /**
     * Returns a list of all blocks in the container sorted numerically by blockID.
     * For example, the unsorted list would have the first blocks as 1, 10, 11...
     * The list returned by this method would have the first blocks as 1, 2, 3...
     */
    private List<BlockData> getSortedBlocks(KeyValueContainer container) throws IOException {
      List<BlockData> blockDataList = handler.getBlockManager().listBlock(container, -1, 100);
      blockDataList.sort(Comparator.comparingLong(BlockData::getLocalID));
      return blockDataList;
    }

    /**
     * Introduce corruption in the container.
     * 1. Delete blocks from the container.
     * 2. Corrupt chunks at an offset.
     * If revers is true, the blocks and chunks are deleted in reverse order.
     */
    public void introduceCorruption(long containerID, int numBlocksToDelete, int numChunksToCorrupt, boolean reverse)
        throws IOException {
      KeyValueContainer container = getContainer(containerID);
      KeyValueContainerData containerData = container.getContainerData();
      // Simulate missing blocks
      try (DBHandle handle = BlockUtils.getDB(containerData, conf);
           BatchOperation batch = handle.getStore().getBatchHandler().initBatchOperation()) {
        List<BlockData> blockDataList = getSortedBlocks(container);
        int size = blockDataList.size();
        for (int i = 0; i < numBlocksToDelete; i++) {
          BlockData blockData = reverse ? blockDataList.get(size - 1 - i) : blockDataList.get(i);
          File blockFile = TestContainerCorruptions.getBlock(container, blockData.getBlockID().getLocalID());
          Assertions.assertTrue(blockFile.delete());
          handle.getStore().getBlockDataTable().deleteWithBatch(batch, containerData.getBlockKey(blockData.getLocalID()));
        }
        handle.getStore().getBatchHandler().commitBatchOperation(batch);

        // Check the op
        blockDataList = getSortedBlocks(container);
        assertEquals(numBlocksToDelete, size - blockDataList.size());
        System.err.println(blockDataList);
      }

      // Corrupt chunks at an offset.
      List<BlockData> blockDataList = getSortedBlocks(container);
      int size = blockDataList.size();
      for (int i = 0; i < numChunksToCorrupt; i++) {
        int blockIndex = reverse ? size - 1 - (i % size) : i % size;
        BlockData blockData = blockDataList.get(blockIndex);
        int chunkIndex = i / size;
        File blockFile = TestContainerCorruptions.getBlock(container, blockData.getBlockID().getLocalID());
        List<ContainerProtos.ChunkInfo> chunks = new ArrayList<>(blockData.getChunks());
        ContainerProtos.ChunkInfo chunkInfo = chunks.remove(chunkIndex);
        corruptFileAtOffset(blockFile, chunkInfo.getOffset(), chunkInfo.getLen());
        System.err.println("datanode " + dnDetails.getUuidString() + " corrupting block " + blockData.getBlockID() + " at " +
            "offset " + chunkInfo.getOffset());
      }
    }

    private MutableVolumeSet createVolumeSet() throws IOException {
      MutableVolumeSet volumeSet = new MutableVolumeSet(dnDetails.getUuidString(), conf, null,
          StorageVolume.VolumeType.DATA_VOLUME, null);
      createDbInstancesForTestIfNeeded(volumeSet, CLUSTER_ID, CLUSTER_ID, conf);
      return volumeSet;
    }

    /**
     * Overwrite the file with random bytes at an offset within the given length.
     */
    private static void corruptFileAtOffset(File file, long offset, long chunkLength) {
      try {
        final int fileLength = (int) file.length();
        assertTrue(fileLength >= offset + chunkLength);
        final int chunkEnd = (int)(offset + chunkLength);

        Path path = file.toPath();
        final byte[] original = IOUtils.readFully(Files.newInputStream(path), fileLength);

        // Corrupt the last byte and middle bytes of the block. The scanner should log this as two errors.
        final byte[] corruptedBytes = Arrays.copyOf(original, fileLength);
        corruptedBytes[chunkEnd - 1] = (byte) (original[chunkEnd - 1] << 1);
        final long chunkMid = offset + (chunkLength - offset) / 2;
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
}
