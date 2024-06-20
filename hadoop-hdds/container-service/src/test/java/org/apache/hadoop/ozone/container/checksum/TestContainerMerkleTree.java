package org.apache.hadoop.ozone.container.checksum;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerMerkleTreeProto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestContainerMerkleTree {
  @Test
  public void testBuildEmptyTree() {
    ContainerMerkleTree tree = new ContainerMerkleTree();
    ContainerMerkleTreeProto treeProto = tree.toProto();
    assertEquals(0, treeProto.getDataChecksum());
    assertEquals(0, treeProto.getBlockMerkleTreeCount());
  }

  @Test
  public void testBuildOneChunkTree() throws Exception {
    // Seed the expected and actual trees with the same chunk.
    final long blockID = 1;
    ChunkInfo chunk = buildChunk(0, ByteBuffer.wrap(new byte[]{1, 2, 3}));

    // Build the expected tree proto using the test code.
    ContainerProtos.ChunkMerkleTreeProto chunkTree = buildExpectedChunkTree(chunk);
    ContainerProtos.BlockMerkleTreeProto blockTree = buildExpectedBlockTree(blockID,
        Collections.singletonList(chunkTree));
    ContainerMerkleTreeProto expectedTree = buildExpectedContainerTree(Collections.singletonList(blockTree));

    // Use the ContainerMerkleTree to build the same tree.
    ContainerMerkleTree actualTree = new ContainerMerkleTree();
    actualTree.addChunks(blockID, Collections.singletonList(chunk));

    // Check the difference.
    ContainerMerkleTreeProto actualTreeProto = actualTree.toProto();
    assertTreesMatch(expectedTree, actualTreeProto);

    // Do some manual verification of the generated tree as well.
    assertNotEquals(0, actualTreeProto.getDataChecksum());
    assertEquals(1, actualTreeProto.getBlockMerkleTreeCount());

    ContainerProtos.BlockMerkleTreeProto actualBlockTree = actualTreeProto.getBlockMerkleTree(0);
    assertEquals(1, actualBlockTree.getBlockID());
    assertEquals(1, actualBlockTree.getChunkMerkleTreeCount());
    assertNotEquals(0, actualBlockTree.getBlockChecksum());

    ContainerProtos.ChunkMerkleTreeProto actualChunkTree = actualBlockTree.getChunkMerkleTree(0);
    assertEquals(0, actualChunkTree.getOffset());
    // TODO use existing config for this value.
    assertEquals(5 * 1024, actualChunkTree.getLength());
    assertNotEquals(0, actualChunkTree.getChunkChecksum());
  }

//  @Test
//  public void testBuildTreeWithMissingChunks() {
//
//  }
//
//  @Test
//  public void testBuildTreeWithMissingBlocks() {
//
//  }
//
//  @Test
//  public void testBuildTreeAtOnce() {
//
//  }
//
//  @Test
//  public void testBuildTreeIncrementally() {
//
//  }

  private void assertTreesMatch(ContainerMerkleTreeProto expectedTree, ContainerMerkleTreeProto actualTree) {
    assertEquals(expectedTree.getDataChecksum(), actualTree.getDataChecksum());
    assertEquals(expectedTree.getBlockMerkleTreeCount(), actualTree.getBlockMerkleTreeCount());

    long prevBlockID = -1;
    for (int blockIndex = 0; blockIndex < expectedTree.getBlockMerkleTreeCount(); blockIndex++) {
      ContainerProtos.BlockMerkleTreeProto expectedBlockTree = expectedTree.getBlockMerkleTree(blockIndex);
      ContainerProtos.BlockMerkleTreeProto actualBlockTree = actualTree.getBlockMerkleTree(blockIndex);

      // Blocks should be sorted by block ID.
      long currentBlockID = actualBlockTree.getBlockID();
      assertTrue(prevBlockID < currentBlockID);
      prevBlockID = currentBlockID;

      assertEquals(expectedBlockTree.getBlockID(), actualBlockTree.getBlockID());
      assertEquals(expectedBlockTree.getBlockChecksum(), actualBlockTree.getBlockChecksum());

      long prevChunkOffset = -1;
      for (int chunkIndex = 0; chunkIndex < expectedBlockTree.getChunkMerkleTreeCount(); chunkIndex++) {
        ContainerProtos.ChunkMerkleTreeProto expectedChunkTree = expectedBlockTree.getChunkMerkleTree(chunkIndex);
        ContainerProtos.ChunkMerkleTreeProto actualChunkTree = actualBlockTree.getChunkMerkleTree(chunkIndex);

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

  private ContainerMerkleTreeProto buildExpectedContainerTree(List<ContainerProtos.BlockMerkleTreeProto> blocks) {
    return ContainerMerkleTreeProto.newBuilder()
        .addAllBlockMerkleTree(blocks)
        .setDataChecksum(computeExpectedChecksum(
            blocks.stream()
                .map(ContainerProtos.BlockMerkleTreeProto::getBlockChecksum)
                .collect(Collectors.toList())))
        .build();
  }

  private ContainerProtos.BlockMerkleTreeProto buildExpectedBlockTree(long blockID,
      List<ContainerProtos.ChunkMerkleTreeProto> chunks) {
    return ContainerProtos.BlockMerkleTreeProto.newBuilder()
        .setBlockID(blockID)
        .setBlockChecksum(computeExpectedChecksum(
            chunks.stream()
                .map(ContainerProtos.ChunkMerkleTreeProto::getChunkChecksum)
                .collect(Collectors.toList())))
        .addAllChunkMerkleTree(chunks)
        .build();
  }

  private ContainerProtos.ChunkMerkleTreeProto buildExpectedChunkTree(ChunkInfo chunk) {
    return ContainerProtos.ChunkMerkleTreeProto.newBuilder()
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
  private ChunkInfo buildChunk(int indexInBlock, ByteBuffer... chunkChecksums) throws IOException {
    // Arbitrary sizes chosen for testing.
    final int bytesPerChecksum = 1024;
    final long chunkLength = 1024 * 5;

    // Each chunk checksum is added under the same ChecksumData object.
    ContainerProtos.ChecksumData checksumData = ContainerProtos.ChecksumData.newBuilder()
        .setType(ContainerProtos.ChecksumType.CRC32)
        .setBytesPerChecksum(bytesPerChecksum)
        .addAllChecksums(Arrays.stream(chunkChecksums)
            .map(ByteString::copyFrom)
            .collect(Collectors.toList()))
        .build();

    return ChunkInfo.getFromProtoBuf(
        ContainerProtos.ChunkInfo.newBuilder()
        .setChecksumData(checksumData)
        .setChunkName("chunk")
        .setOffset(indexInBlock * chunkLength)
        .setLen(chunkLength)
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
