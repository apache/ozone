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

package org.apache.hadoop.ozone.debug.datanode.container;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildTestTree;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildTestTreeWithMismatches;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.updateTreeProto;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeWriter;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Test class for ChecksumSubcommand.
 */
class TestChecksumSubcommand {

  private static final long CONTAINER_ID = 12345L;

  @TempDir
  private Path tempDir;

  private OzoneConfiguration config;
  private ByteArrayOutputStream out;
  private PrintStream originalOut;
  private static final String DEFAULT_ENCODING = UTF_8.name();

  @BeforeEach
  void setUp() throws Exception {
    config = new OzoneConfiguration();
    
    // Capture stdout
    out = new ByteArrayOutputStream();
    originalOut = System.out;
    System.setOut(new PrintStream(out, false, DEFAULT_ENCODING));
  }

  @AfterEach
  void tearDown() {
    // Restore stdout
    System.setOut(originalOut);
  }

  @Test
  void testChecksumCommandWithValidFile() throws Exception {
    // Create a mock container data
    KeyValueContainerData containerData = Mockito.mock(KeyValueContainerData.class);
    Mockito.when(containerData.getContainerID()).thenReturn(CONTAINER_ID);
    Mockito.when(containerData.getMetadataPath()).thenReturn(tempDir.toString());

    // Build a test tree and write it to file
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    updateTreeProto(containerData, tree.toProto());

    File treeFile = new File(tempDir.toFile(), CONTAINER_ID + ".tree");
    JsonNode containerJson = runChecksumCommand(treeFile);

    // Verify container structure
    assertThat(containerJson.get("containerID").asLong()).isEqualTo(CONTAINER_ID);
    assertThat(containerJson.has("containerMerkleTree")).isTrue();

    // Verify merkle tree structure
    JsonNode merkleTree = containerJson.get("containerMerkleTree");
    assertThat(merkleTree.has("dataChecksum")).isTrue();
    assertThat(merkleTree.get("dataChecksum").asText()).isNotEqualTo("0");

    JsonNode blockMerkleTrees = merkleTree.get("blockMerkleTrees");
    assertThat(blockMerkleTrees.isArray()).isTrue();
    assertThat(blockMerkleTrees.size()).isEqualTo(5); // Default buildTestTree creates 5 blocks
  }

  @Test
  void testChecksumCommandWithNonExistentFile() throws Exception {
    ChecksumSubcommand command = new ChecksumSubcommand();
    command.setTreeFilePath("/non/existent/file.tree");

    RuntimeException exception = assertThrows(RuntimeException.class, command::call);
    assertThat(exception.getMessage()).contains("Tree file does not exist");
  }

  @Test
  void testChecksumCommandWithCorruptedFile() throws Exception {
    // Create a corrupted tree file
    File treeFile = new File(tempDir.toFile(), "corrupted.tree");
    try (OutputStream fos = Files.newOutputStream(treeFile.toPath())) {
      fos.write(new byte[]{1, 2, 3, 4, 5}); // Invalid protobuf data
    }

    ChecksumSubcommand command = new ChecksumSubcommand();
    command.setTreeFilePath(treeFile.getAbsolutePath());

    RuntimeException exception = assertThrows(RuntimeException.class, command::call);
    assertThat(exception.getMessage()).contains("Failed to read tree file");
  }

  @Test
  void testChecksumCommandWithEmptyFile() throws Exception {
    // Create an empty tree file
    File treeFile = new File(tempDir.toFile(), "empty.tree");
    ContainerProtos.ContainerChecksumInfo emptyInfo = ContainerProtos.ContainerChecksumInfo.newBuilder().build();
    try (OutputStream fos = Files.newOutputStream(treeFile.toPath())) {
      emptyInfo.writeTo(fos);
    }

    JsonNode containerJson = runChecksumCommand(treeFile);
    
    // Verify the structure for empty file
    assertThat(containerJson.get("containerID").asLong()).isEqualTo(0);
    assertThat(containerJson.has("containerMerkleTree")).isFalse(); // Empty file has no merkle tree
  }

  @Test
  void testChecksumCommandWithComplexTree() throws Exception {
    // Create a mock container data
    KeyValueContainerData containerData = Mockito.mock(KeyValueContainerData.class);
    Mockito.when(containerData.getContainerID()).thenReturn(CONTAINER_ID);
    Mockito.when(containerData.getMetadataPath()).thenReturn(tempDir.toString());

    // Build a test tree with 3 blocks
    ContainerMerkleTreeWriter tree = buildTestTree(config, 3); // 3 blocks with 4 chunks each
    updateTreeProto(containerData, tree.toProto());

    File treeFile = new File(tempDir.toFile(), CONTAINER_ID + ".tree");
    JsonNode containerJson = runChecksumCommand(treeFile);

    // Verify container structure
    assertThat(containerJson.get("containerID").asLong()).isEqualTo(CONTAINER_ID);
    assertThat(containerJson.has("containerMerkleTree")).isTrue();

    // Verify merkle tree structure
    JsonNode merkleTree = containerJson.get("containerMerkleTree");
    assertThat(merkleTree.has("dataChecksum")).isTrue();
    assertThat(merkleTree.get("dataChecksum").asText()).isNotEqualTo("0");
    
    JsonNode blockMerkleTrees = merkleTree.get("blockMerkleTrees");
    assertThat(blockMerkleTrees.isArray()).isTrue();
    assertThat(blockMerkleTrees.size()).isEqualTo(3);

    // Verify all blocks have sequential IDs and valid structure including chunks
    for (int i = 0; i < blockMerkleTrees.size(); i++) {
      JsonNode block = blockMerkleTrees.get(i);
      verifyBlockStructure(block, i + 1);
    }
  }

  @Test
  void testChecksumCommandWithEmptyTree() throws Exception {
    // Create a mock container data
    KeyValueContainerData containerData = Mockito.mock(KeyValueContainerData.class);
    Mockito.when(containerData.getContainerID()).thenReturn(CONTAINER_ID);
    Mockito.when(containerData.getMetadataPath()).thenReturn(tempDir.toString());

    // Create an empty tree (no blocks)
    ContainerMerkleTreeWriter tree = new ContainerMerkleTreeWriter();
    updateTreeProto(containerData, tree.toProto());

    File treeFile = new File(tempDir.toFile(), CONTAINER_ID + ".tree");
    JsonNode containerJson = runChecksumCommand(treeFile);

    // Verify container structure
    assertThat(containerJson.get("containerID").asLong()).isEqualTo(CONTAINER_ID);
    assertThat(containerJson.has("containerMerkleTree")).isTrue();

    // Verify merkle tree structure
    JsonNode merkleTree = containerJson.get("containerMerkleTree");
    assertThat(merkleTree.has("dataChecksum")).isTrue();
    assertThat(merkleTree.get("dataChecksum").asText()).isEqualTo("0");

    JsonNode blockMerkleTrees = merkleTree.get("blockMerkleTrees");
    assertThat(blockMerkleTrees.isArray()).isTrue();
    assertThat(blockMerkleTrees.size()).isEqualTo(0); // Empty tree has no blocks
  }

  @Test
  void testChecksumCommandWithDeletedBlocks() throws Exception {
    // Create a mock container data
    KeyValueContainerData containerData = Mockito.mock(KeyValueContainerData.class);
    Mockito.when(containerData.getContainerID()).thenReturn(CONTAINER_ID);
    Mockito.when(containerData.getMetadataPath()).thenReturn(tempDir.toString());

    // Build a test tree with 5 blocks, marking blocks 2 and 4 as deleted
    ContainerProtos.ContainerMerkleTree tree = buildTestTree(config, 5, 2L, 4L);
    updateTreeProto(containerData, tree);

    File treeFile = new File(tempDir.toFile(), CONTAINER_ID + ".tree");
    JsonNode containerJson = runChecksumCommand(treeFile);

    // Verify container structure
    assertThat(containerJson.get("containerID").asLong()).isEqualTo(CONTAINER_ID);
    assertThat(containerJson.has("containerMerkleTree")).isTrue();

    // Verify merkle tree structure
    JsonNode merkleTree = containerJson.get("containerMerkleTree");
    JsonNode blockMerkleTrees = merkleTree.get("blockMerkleTrees");
    assertThat(blockMerkleTrees.isArray()).isTrue();
    assertThat(blockMerkleTrees.size()).isEqualTo(5);

    // Verify blocks 1, 3, 5 are not deleted (have chunks)
    verifyBlockStructure(blockMerkleTrees.get(0), 1L, false);
    verifyBlockStructure(blockMerkleTrees.get(2), 3L, false);
    verifyBlockStructure(blockMerkleTrees.get(4), 5L, false);

    // Verify blocks 2 and 4 are deleted (no chunks)
    verifyDeletedBlockStructure(blockMerkleTrees.get(1), 2L);
    verifyDeletedBlockStructure(blockMerkleTrees.get(3), 4L);
  }

  @Test
  void testChecksumCommandWithChecksumMismatch() throws Exception {
    // Create a mock container data
    KeyValueContainerData containerData = Mockito.mock(KeyValueContainerData.class);
    Mockito.when(containerData.getContainerID()).thenReturn(CONTAINER_ID);
    Mockito.when(containerData.getMetadataPath()).thenReturn(tempDir.toString());

    // Build a test tree and introduce corrupt chunks (which sets checksumMatches to false)
    ContainerMerkleTreeWriter baseTree = buildTestTree(config, 3);
    org.apache.commons.lang3.tuple.Pair<ContainerProtos.ContainerMerkleTree, 
        org.apache.hadoop.ozone.container.checksum.ContainerDiffReport> result = 
        buildTestTreeWithMismatches(baseTree, 0, 0, 2); // 2 corrupt chunks
    ContainerProtos.ContainerMerkleTree treeWithMismatches = result.getLeft();
    updateTreeProto(containerData, treeWithMismatches);

    File treeFile = new File(tempDir.toFile(), CONTAINER_ID + ".tree");
    JsonNode containerJson = runChecksumCommand(treeFile);

    // Verify container structure
    assertThat(containerJson.get("containerID").asLong()).isEqualTo(CONTAINER_ID);

    // Verify merkle tree has blocks with corrupt chunks
    JsonNode merkleTree = containerJson.get("containerMerkleTree");
    JsonNode blockMerkleTrees = merkleTree.get("blockMerkleTrees");
    assertThat(blockMerkleTrees.isArray()).isTrue();

    // At least one chunk should have checksumMatches = false
    boolean foundCorruptChunk = false;
    for (JsonNode block : blockMerkleTrees) {
      JsonNode chunkMerkleTrees = block.get("chunkMerkleTrees");
      for (JsonNode chunk : chunkMerkleTrees) {
        if (!chunk.get("checksumMatches").asBoolean()) {
          foundCorruptChunk = true;
          // Verify the chunk still has all required fields
          assertThat(chunk.has("offset")).isTrue();
          assertThat(chunk.has("length")).isTrue();
          assertThat(chunk.has("dataChecksum")).isTrue();
          assertThat(chunk.get("dataChecksum").asText()).isNotEqualTo("0");
        }
      }
    }
    assertThat(foundCorruptChunk).as("Should have at least one chunk with checksumMatches=false").isTrue();
  }

  /**
   * Helper method to run ChecksumSubcommand and return parsed JSON output.
   */
  private JsonNode runChecksumCommand(File treeFile) throws Exception {
    ChecksumSubcommand command = new ChecksumSubcommand();
    command.setTreeFilePath(treeFile.getAbsolutePath());
    command.call();

    // Parse actual output
    String actualOutput = out.toString(DEFAULT_ENCODING).trim();
    ObjectMapper mapper = JsonUtils.getDefaultMapper();
    return mapper.readTree(actualOutput);
  }

  /**
   * Verify block structure including blockID, deleted status, dataChecksum, chunk count, and chunk details.
   */
  private void verifyBlockStructure(JsonNode block, long expectedBlockID) {
    verifyBlockStructure(block, expectedBlockID, false);
  }

  /**
   * Verify block structure including blockID, deleted status, dataChecksum, chunk count, and chunk details.
   */
  private void verifyBlockStructure(JsonNode block, long expectedBlockID, boolean expectedDeleted) {
    assertThat(block.get("blockID").asLong()).isEqualTo(expectedBlockID);
    assertThat(block.get("deleted").asBoolean()).isEqualTo(expectedDeleted);
    assertThat(block.has("dataChecksum")).isTrue();
    assertThat(block.get("dataChecksum").asText()).isNotEqualTo("0");

    JsonNode chunkMerkleTrees = block.get("chunkMerkleTrees");
    assertThat(chunkMerkleTrees.isArray()).isTrue();
    assertThat(chunkMerkleTrees.size()).isEqualTo(4);
    
    // Verify each chunk structure
    for (int i = 0; i < chunkMerkleTrees.size(); i++) {
      verifyChunkStructure(chunkMerkleTrees.get(i), i, true);
    }
  }

  /**
   * Verify deleted block structure - deleted blocks should have no chunks.
   */
  private void verifyDeletedBlockStructure(JsonNode block, long expectedBlockID) {
    assertThat(block.get("blockID").asLong()).isEqualTo(expectedBlockID);
    assertThat(block.get("deleted").asBoolean()).isTrue();
    assertThat(block.has("dataChecksum")).isTrue();

    JsonNode chunkMerkleTrees = block.get("chunkMerkleTrees");
    assertThat(chunkMerkleTrees.isArray()).isTrue();
    assertThat(chunkMerkleTrees.size()).isEqualTo(0); // Deleted blocks have no chunks
  }

  /**
   * Verify chunk structure including offset, length, checksumMatches, and dataChecksum.
   */
  private void verifyChunkStructure(JsonNode chunk, long expectedOffset, boolean expectedChecksumMatches) {
    assertThat(chunk.get("offset").asLong()).isEqualTo(expectedOffset);
    assertThat(chunk.get("length").asLong()).isGreaterThan(0);
    assertThat(chunk.get("checksumMatches").asBoolean()).isEqualTo(expectedChecksumMatches);
    assertThat(chunk.get("dataChecksum").asText()).isNotEqualTo("0");
  }
}
