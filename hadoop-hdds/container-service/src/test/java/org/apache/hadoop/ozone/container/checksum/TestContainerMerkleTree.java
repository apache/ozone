package org.apache.hadoop.ozone.container.checksum;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestContainerMerkleTree {
  private static final long CHUNK_SIZE = (long) new OzoneConfiguration().getStorageSize(
      ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY, ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT, StorageUnit.BYTES);
  private static final int BYTES_PER_CHECKSUM = new OzoneClientConfig().getBytesPerChecksum();

  @Test
  public void testBuildEmptyTree() {
    ContainerMerkleTree tree = new ContainerMerkleTree();
    ContainerProtos.ContainerMerkleTree treeProto = tree.toProto();
    assertEquals(0, treeProto.getDataChecksum());
    assertEquals(0, treeProto.getBlockMerkleTreeCount());
  }

  @Test
  public void testBuildOneChunkTree() throws Exception {
    // Seed the expected and actual trees with the same chunk.
    final long blockID = 1;
    ChunkInfo chunk = buildChunk(0, ByteBuffer.wrap(new byte[]{1, 2, 3}));

    // Build the expected tree proto using the test code.
    ContainerProtos.ChunkMerkleTree chunkTree = buildExpectedChunkTree(chunk);
    ContainerProtos.BlockMerkleTree blockTree = buildExpectedBlockTree(blockID,
        Collections.singletonList(chunkTree));
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(Collections.singletonList(blockTree));

    // Use the ContainerMerkleTree to build the same tree.
    ContainerMerkleTree actualTree = new ContainerMerkleTree();
    actualTree.addChunks(blockID, Collections.singletonList(chunk));

    // Ensure the trees match.
    ContainerProtos.ContainerMerkleTree actualTreeProto = actualTree.toProto();
    assertTreesSortedAndMatch(expectedTree, actualTreeProto);

    // Do some manual verification of the generated tree as well.
    assertNotEquals(0, actualTreeProto.getDataChecksum());
    assertEquals(1, actualTreeProto.getBlockMerkleTreeCount());

    ContainerProtos.BlockMerkleTree actualBlockTree = actualTreeProto.getBlockMerkleTree(0);
    assertEquals(1, actualBlockTree.getBlockID());
    assertEquals(1, actualBlockTree.getChunkMerkleTreeCount());
    assertNotEquals(0, actualBlockTree.getBlockChecksum());

    ContainerProtos.ChunkMerkleTree actualChunkTree = actualBlockTree.getChunkMerkleTree(0);
    assertEquals(0, actualChunkTree.getOffset());
    assertEquals(CHUNK_SIZE, actualChunkTree.getLength());
    assertNotEquals(0, actualChunkTree.getChunkChecksum());
  }

  @Test
  public void testBuildTreeWithMissingChunks() throws Exception {
    // These chunks will be used to seed both the expected and actual trees.
    final long blockID = 1;
    ChunkInfo chunk1 = buildChunk(0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    // Chunk 2 is missing.
    ChunkInfo chunk3 = buildChunk(2, ByteBuffer.wrap(new byte[]{4, 5, 6}));

    // Build the expected tree proto using the test code.
    ContainerProtos.BlockMerkleTree blockTree = buildExpectedBlockTree(blockID,
        Arrays.asList(buildExpectedChunkTree(chunk1), buildExpectedChunkTree(chunk3)));
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(Collections.singletonList(blockTree));

    // Use the ContainerMerkleTree to build the same tree.
    ContainerMerkleTree actualTree = new ContainerMerkleTree();
    actualTree.addChunks(blockID, Arrays.asList(chunk1, chunk3));

    // Ensure the trees match.
    ContainerProtos.ContainerMerkleTree actualTreeProto = actualTree.toProto();
    assertTreesSortedAndMatch(expectedTree, actualTreeProto);
  }

  /**
   * A container is a set of blocks. Make sure the tree implementation is not dependent on continuity of block IDs.
   */
  @Test
  public void testBuildTreeWithNonContiguousBlockIDs() throws Exception {
    // Seed the expected and actual trees with the same chunks.
    final long blockID1 = 1;
    final long blockID3 = 3;
    ChunkInfo b1c1 = buildChunk(0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ChunkInfo b1c2 = buildChunk(1, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ChunkInfo b3c1 = buildChunk(0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ChunkInfo b3c2 = buildChunk(1, ByteBuffer.wrap(new byte[]{1, 2, 3}));

    // Build the expected tree proto using the test code.
    ContainerProtos.BlockMerkleTree blockTree1 = buildExpectedBlockTree(blockID1,
        Arrays.asList(buildExpectedChunkTree(b1c1), buildExpectedChunkTree(b1c2)));
    ContainerProtos.BlockMerkleTree blockTree3 = buildExpectedBlockTree(blockID3,
        Arrays.asList(buildExpectedChunkTree(b3c1), buildExpectedChunkTree(b3c2)));
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(Arrays.asList(blockTree1, blockTree3));

    // Use the ContainerMerkleTree to build the same tree.
    // Add blocks and chunks out of order to test sorting.
    ContainerMerkleTree actualTree = new ContainerMerkleTree();
    actualTree.addChunks(blockID3, Arrays.asList(b3c2, b3c1));
    actualTree.addChunks(blockID1, Arrays.asList(b1c1, b1c2));

    // Ensure the trees match.
    ContainerProtos.ContainerMerkleTree actualTreeProto = actualTree.toProto();
    assertTreesSortedAndMatch(expectedTree, actualTreeProto);
  }

  @Test
  public void testAppendToBlocksWhileBuilding() throws Exception {
    // Seed the expected and actual trees with the same chunks.
    final long blockID1 = 1;
    final long blockID2 = 2;
    final long blockID3 = 3;
    ChunkInfo b1c1 = buildChunk(0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ChunkInfo b1c2 = buildChunk(1, ByteBuffer.wrap(new byte[]{1, 2}));
    ChunkInfo b1c3 = buildChunk(2, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ChunkInfo b2c1 = buildChunk(0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ChunkInfo b2c2 = buildChunk(1, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ChunkInfo b3c1 = buildChunk(0, ByteBuffer.wrap(new byte[]{1}));
    ChunkInfo b3c2 = buildChunk(1, ByteBuffer.wrap(new byte[]{2, 3, 4}));

    // Build the expected tree proto using the test code.
    ContainerProtos.BlockMerkleTree blockTree1 = buildExpectedBlockTree(blockID1,
        Arrays.asList(buildExpectedChunkTree(b1c1), buildExpectedChunkTree(b1c2), buildExpectedChunkTree(b1c3)));
    ContainerProtos.BlockMerkleTree blockTree2 = buildExpectedBlockTree(blockID2,
        Arrays.asList(buildExpectedChunkTree(b2c1), buildExpectedChunkTree(b2c2)));
    ContainerProtos.BlockMerkleTree blockTree3 = buildExpectedBlockTree(blockID3,
        Arrays.asList(buildExpectedChunkTree(b3c1), buildExpectedChunkTree(b3c2)));
    ContainerProtos.ContainerMerkleTree expectedTree = buildExpectedContainerTree(
        Arrays.asList(blockTree1, blockTree2, blockTree3));

    // Use the ContainerMerkleTree to build the same tree.
    // Test building by adding chunks to the blocks individually and out of order.
    ContainerMerkleTree actualTree = new ContainerMerkleTree();
    // Add all of block 2 first.
    actualTree.addChunks(blockID2, Arrays.asList(b2c1, b2c2));
    // Then add block 1 in multiple steps wth chunks out of order.
    actualTree.addChunks(blockID1, Collections.singletonList(b1c2));
    actualTree.addChunks(blockID1, Arrays.asList(b1c3, b1c1));
    // Add a duplicate chunk to block 3. It should overwrite the existing one.
    actualTree.addChunks(blockID3, Arrays.asList(b3c1, b3c2));
    actualTree.addChunks(blockID3, Collections.singletonList(b3c2));

    // Ensure the trees match.
    ContainerProtos.ContainerMerkleTree actualTreeProto = actualTree.toProto();
    assertTreesSortedAndMatch(expectedTree, actualTreeProto);
  }

  public static void assertTreesSortedAndMatch(ContainerProtos.ContainerMerkleTree expectedTree,
      ContainerProtos.ContainerMerkleTree actualTree) {
    assertEquals(expectedTree.getDataChecksum(), actualTree.getDataChecksum());
    assertEquals(expectedTree.getBlockMerkleTreeCount(), actualTree.getBlockMerkleTreeCount());

    long prevBlockID = -1;
    for (int blockIndex = 0; blockIndex < expectedTree.getBlockMerkleTreeCount(); blockIndex++) {
      ContainerProtos.BlockMerkleTree expectedBlockTree = expectedTree.getBlockMerkleTree(blockIndex);
      ContainerProtos.BlockMerkleTree actualBlockTree = actualTree.getBlockMerkleTree(blockIndex);

      // Blocks should be sorted by block ID.
      long currentBlockID = actualBlockTree.getBlockID();
      assertTrue(prevBlockID < currentBlockID);
      prevBlockID = currentBlockID;

      assertEquals(expectedBlockTree.getBlockID(), actualBlockTree.getBlockID());
      assertEquals(expectedBlockTree.getBlockChecksum(), actualBlockTree.getBlockChecksum());

      long prevChunkOffset = -1;
      for (int chunkIndex = 0; chunkIndex < expectedBlockTree.getChunkMerkleTreeCount(); chunkIndex++) {
        ContainerProtos.ChunkMerkleTree expectedChunkTree = expectedBlockTree.getChunkMerkleTree(chunkIndex);
        ContainerProtos.ChunkMerkleTree actualChunkTree = actualBlockTree.getChunkMerkleTree(chunkIndex);

        // Chunks should be sorted by offset.
        long currentChunkOffset = actualChunkTree.getOffset();
        assertTrue(prevChunkOffset < currentChunkOffset);
        prevChunkOffset = currentChunkOffset;

        assertEquals(expectedChunkTree.getOffset(), actualChunkTree.getOffset());
        assertEquals(expectedChunkTree.getLength(), actualChunkTree.getLength());
        assertEquals(expectedChunkTree.getChunkChecksum(), actualChunkTree.getChunkChecksum());
      }
    }
  }

  private ContainerProtos.ContainerMerkleTree buildExpectedContainerTree(List<ContainerProtos.BlockMerkleTree> blocks) {
    return ContainerProtos.ContainerMerkleTree.newBuilder()
        .addAllBlockMerkleTree(blocks)
        .setDataChecksum(computeExpectedChecksum(
            blocks.stream()
                .map(ContainerProtos.BlockMerkleTree::getBlockChecksum)
                .collect(Collectors.toList())))
        .build();
  }

  private ContainerProtos.BlockMerkleTree buildExpectedBlockTree(long blockID,
      List<ContainerProtos.ChunkMerkleTree> chunks) {
    return ContainerProtos.BlockMerkleTree.newBuilder()
        .setBlockID(blockID)
        .setBlockChecksum(computeExpectedChecksum(
            chunks.stream()
                .map(ContainerProtos.ChunkMerkleTree::getChunkChecksum)
                .collect(Collectors.toList())))
        .addAllChunkMerkleTree(chunks)
        .build();
  }

  private ContainerProtos.ChunkMerkleTree buildExpectedChunkTree(ChunkInfo chunk) {
    return ContainerProtos.ChunkMerkleTree.newBuilder()
        .setOffset(chunk.getOffset())
        .setLength(chunk.getLen())
        .setChunkChecksum(computeExpectedChunkChecksum(chunk.getChecksumData().getChecksums()))
        .build();
  }

  /**
   * Builds a ChunkInfo object using the provided information. No new checksums are calculated, so this can be used
   * as either the leaves of pre-computed merkle trees that serve as expected values, or as building blocks to pass
   * to ContainerMerkleTree to have it build the whole tree from this information.
   *
   * @param indexInBlock Which chunk number within a block this is. The chunk's offset is automatically calculated
   *     from this based on a fixed length.
   * @param chunkChecksums The checksums within the chunk. Each is assumed to apply to a fixed value
   *     "bytesPerChecksum" amount of data and are assumed to be contiguous.
   * @return The ChunkInfo proto object built from this information.
   */
  public static ChunkInfo buildChunk(int indexInBlock, ByteBuffer... chunkChecksums) throws IOException {
    // Each chunk checksum is added under the same ChecksumData object.
    ContainerProtos.ChecksumData checksumData = ContainerProtos.ChecksumData.newBuilder()
        .setType(ContainerProtos.ChecksumType.CRC32)
        .setBytesPerChecksum(BYTES_PER_CHECKSUM)
        .addAllChecksums(Arrays.stream(chunkChecksums)
            .map(ByteString::copyFrom)
            .collect(Collectors.toList()))
        .build();

    return ChunkInfo.getFromProtoBuf(
        ContainerProtos.ChunkInfo.newBuilder()
        .setChecksumData(checksumData)
        .setChunkName("chunk")
        .setOffset(indexInBlock * CHUNK_SIZE)
        .setLen(CHUNK_SIZE)
        .build());
  }

  private long computeExpectedChecksum(List<Long> checksums) {
    CRC32 crc32 = new CRC32();
    ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES * checksums.size());
    checksums.forEach(longBuffer::putLong);
    longBuffer.flip();
    crc32.update(longBuffer);
    return crc32.getValue();
  }

  private long computeExpectedChunkChecksum(List<ByteString> checksums) {
    CRC32 crc32 = new CRC32();
    checksums.forEach(b -> crc32.update(b.asReadOnlyByteBuffer()));
    return crc32.getValue();
  }
}
