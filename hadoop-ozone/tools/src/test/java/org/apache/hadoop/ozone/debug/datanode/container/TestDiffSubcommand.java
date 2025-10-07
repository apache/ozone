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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.container.checksum.ContainerDiffReport;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for DiffSubcommand.
 */
public class TestDiffSubcommand {

  private static final long CONTAINER_ID = 12345L;

  @TempDir
  private Path tempDir;

  private ByteArrayOutputStream out;
  private PrintStream originalOut;
  private OzoneConfiguration config;
  private static final String DEFAULT_ENCODING = UTF_8.name();

  @BeforeEach
  void setUp() throws Exception {
    // Capture stdout
    out = new ByteArrayOutputStream();
    originalOut = System.out;
    System.setOut(new PrintStream(out, false, DEFAULT_ENCODING));
    config = new OzoneConfiguration();
  }

  @AfterEach
  void tearDown() {
    // Restore stdout
    System.setOut(originalOut);
  }

  @Test
  void testDiffCommandWithIdenticalTrees() throws Exception {
    // Create two identical tree files
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    
    File treeFile1 = createTreeFile("tree1.tree", tree.toProto());
    File treeFile2 = createTreeFile("tree2.tree", tree.toProto());

    JsonNode result = runDiffCommand(treeFile1, treeFile2);
    // Verify container ID
    assertThat(result.get("containerID").asLong()).isEqualTo(CONTAINER_ID);
    
    // Verify bidirectional diff: both replicas should show no differences
    JsonNode replica1 = result.get("replica1");
    assertThat(replica1.get("needsRepair").asBoolean()).isFalse();
    assertThat(replica1.get("summary").get("missingBlocks").asLong()).isEqualTo(0);
    assertThat(replica1.get("summary").get("missingChunks").asLong()).isEqualTo(0);
    assertThat(replica1.get("summary").get("corruptChunks").asLong()).isEqualTo(0);
    
    JsonNode replica2 = result.get("replica2");
    assertThat(replica2.get("needsRepair").asBoolean()).isFalse();
    assertThat(replica2.get("summary").get("missingBlocks").asLong()).isEqualTo(0);
    assertThat(replica2.get("summary").get("missingChunks").asLong()).isEqualTo(0);
    assertThat(replica2.get("summary").get("corruptChunks").asLong()).isEqualTo(0);
  }

  @Test
  void testDiffCommandWithDifferentTrees() throws Exception {
    // Create base tree
    ContainerMerkleTreeWriter baseTree = buildTestTree(config);
    
    // Create tree with mismatches
    Pair<ContainerProtos.ContainerMerkleTree, ContainerDiffReport> treeWithMismatches =
        buildTestTreeWithMismatches(baseTree, 1, 2, 1); // 1 missing block, 2 missing chunks, 1 corrupt chunk
    ContainerProtos.ContainerMerkleTree modifiedTree = treeWithMismatches.getLeft();
    
    File treeFile1 = createTreeFile("tree1.tree", modifiedTree);
    File treeFile2 = createTreeFile("tree2.tree", baseTree.toProto());

    JsonNode result = runDiffCommand(treeFile1, treeFile2);

    // Verify container ID
    assertThat(result.get("containerID").asLong()).isEqualTo(CONTAINER_ID);
    
    // Verify bidirectional diff:
    // replica1 (modified tree) should show missing blocks/chunks compared to replica2
    JsonNode replica1 = result.get("replica1");
    assertThat(replica1.get("needsRepair").asBoolean()).isTrue();
    assertThat(replica1.get("summary").get("missingBlocks").asLong()).isEqualTo(1);
    assertThat(replica1.get("summary").get("missingChunks").asLong()).isEqualTo(2);
    assertThat(replica1.get("summary").get("corruptChunks").asLong()).isEqualTo(1);
    
    // replica2 (base tree) should show no missing blocks (it has everything replica1 has)
    JsonNode replica2 = result.get("replica2");
    assertThat(replica2.get("needsRepair").asBoolean()).isFalse();
    assertThat(replica2.get("summary").get("missingBlocks").asLong()).isEqualTo(0);
    assertThat(replica2.get("summary").get("missingChunks").asLong()).isEqualTo(0);
    assertThat(replica2.get("summary").get("corruptChunks").asLong()).isEqualTo(0);
  }

  @Test
  void testDiffCommandWithNonExistentFiles() throws Exception {
    // Create File objects for non-existent files (don't actually create the files)
    File nonExistentFile1 = new File(tempDir.toFile(), "non_existent_file1.tree");
    File nonExistentFile2 = new File(tempDir.toFile(), "non_existent_file2.tree");
    
    RuntimeException exception = assertThrows(RuntimeException.class, () -> 
        runDiffCommand(nonExistentFile1, nonExistentFile2));

    assertThat(exception.getMessage()).contains("First tree file does not exist");
  }

  @Test
  void testDiffCommandWithMismatchedContainerIds() throws Exception {
    // Create trees with different container IDs
    ContainerMerkleTreeWriter tree1 = buildTestTree(config);
    ContainerMerkleTreeWriter tree2 = buildTestTree(config);
    
    File treeFile1 = createTreeFile("tree1.tree", tree1.toProto(), CONTAINER_ID);
    File treeFile2 = createTreeFile("tree2.tree", tree2.toProto(), 67890L); // Different container ID

    RuntimeException exception = assertThrows(RuntimeException.class, () -> 
        runDiffCommand(treeFile1, treeFile2));

    assertThat(exception.getMessage()).contains("Failed to compare tree files");
    assertThat(exception.getCause().getMessage()).contains("Container IDs do not match");
  }

  @Test
  void testDiffCommandWithCorruptedFile() throws Exception {
    // Create one valid tree file and one corrupted file
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    File treeFile1 = createTreeFile("tree1.tree", tree.toProto());
    
    File treeFile2 = new File(tempDir.toFile(), "corrupted.tree");
    try (OutputStream fos = Files.newOutputStream(treeFile2.toPath())) {
      fos.write(new byte[]{1, 2, 3, 4, 5}); // Invalid protobuf data
    }

    RuntimeException exception = assertThrows(RuntimeException.class, () -> 
        runDiffCommand(treeFile1, treeFile2));

    assertThat(exception.getMessage()).contains("Failed to read tree files");
  }

  @Test
  void testDiffCommandWithMultipleMismatches() throws Exception {
    // Create base tree
    ContainerMerkleTreeWriter baseTree = buildTestTree(config, 10); // Larger tree for more mismatches
    
    // Create tree with multiple types of mismatches
    Pair<ContainerProtos.ContainerMerkleTree, ContainerDiffReport> treeWithMismatches =
        buildTestTreeWithMismatches(baseTree, 3, 5, 2); // 3 missing blocks, 5 missing chunks, 2 corrupt chunks
    ContainerProtos.ContainerMerkleTree modifiedTree = treeWithMismatches.getLeft();
    
    File treeFile1 = createTreeFile("tree1.tree", modifiedTree);
    File treeFile2 = createTreeFile("tree2.tree", baseTree.toProto());

    JsonNode result = runDiffCommand(treeFile1, treeFile2);

    // Verify container ID
    assertThat(result.get("containerID").asLong()).isEqualTo(CONTAINER_ID);
    
    // Verify bidirectional diff:
    // replica1 (modified tree) should show missing/corrupt blocks and chunks
    JsonNode replica1 = result.get("replica1");
    assertThat(replica1.get("needsRepair").asBoolean()).isTrue();
    assertThat(replica1.get("summary").get("missingBlocks").asLong()).isEqualTo(3);
    assertThat(replica1.get("summary").get("missingChunks").asLong()).isEqualTo(5);
    assertThat(replica1.get("summary").get("corruptChunks").asLong()).isEqualTo(2);
    
    // replica2 (base tree) should show no issues
    JsonNode replica2 = result.get("replica2");
    assertThat(replica2.get("needsRepair").asBoolean()).isFalse();
    assertThat(replica2.get("summary").get("missingBlocks").asLong()).isEqualTo(0);
    assertThat(replica2.get("summary").get("missingChunks").asLong()).isEqualTo(0);
    assertThat(replica2.get("summary").get("corruptChunks").asLong()).isEqualTo(0);
  }

  @Test
  void testDiffCommandWithEmptyTrees() throws Exception {
    // Create two empty tree files
    ContainerProtos.ContainerChecksumInfo emptyInfo1 = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerID(CONTAINER_ID)
        .setContainerMerkleTree(ContainerProtos.ContainerMerkleTree.newBuilder().setDataChecksum(0).build())
        .build();
    
    ContainerProtos.ContainerChecksumInfo emptyInfo2 = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerID(CONTAINER_ID)
        .setContainerMerkleTree(ContainerProtos.ContainerMerkleTree.newBuilder().setDataChecksum(0).build())
        .build();
    
    File treeFile1 = new File(tempDir.toFile(), "empty1.tree");
    File treeFile2 = new File(tempDir.toFile(), "empty2.tree");
    
    try (OutputStream fos1 = Files.newOutputStream(treeFile1.toPath());
         OutputStream fos2 = Files.newOutputStream(treeFile2.toPath())) {
      emptyInfo1.writeTo(fos1);
      emptyInfo2.writeTo(fos2);
    }

    JsonNode result = runDiffCommand(treeFile1, treeFile2);

    // Verify container ID
    assertThat(result.get("containerID").asLong()).isEqualTo(CONTAINER_ID);
    
    // Verify bidirectional diff: both empty replicas should show no differences
    JsonNode replica1 = result.get("replica1");
    assertThat(replica1.get("needsRepair").asBoolean()).isFalse();
    assertThat(replica1.get("summary").get("missingBlocks").asLong()).isEqualTo(0);
    assertThat(replica1.get("summary").get("missingChunks").asLong()).isEqualTo(0);
    assertThat(replica1.get("summary").get("corruptChunks").asLong()).isEqualTo(0);
    
    JsonNode replica2 = result.get("replica2");
    assertThat(replica2.get("needsRepair").asBoolean()).isFalse();
    assertThat(replica2.get("summary").get("missingBlocks").asLong()).isEqualTo(0);
    assertThat(replica2.get("summary").get("missingChunks").asLong()).isEqualTo(0);
    assertThat(replica2.get("summary").get("corruptChunks").asLong()).isEqualTo(0);
  }

  private File createTreeFile(String fileName, ContainerProtos.ContainerMerkleTree tree) throws Exception {
    return createTreeFile(fileName, tree, CONTAINER_ID);
  }

  private File createTreeFile(String fileName, ContainerProtos.ContainerMerkleTree tree, long containerId)
      throws Exception {
    ContainerProtos.ContainerChecksumInfo checksumInfo = ContainerProtos.ContainerChecksumInfo.newBuilder()
        .setContainerID(containerId)
        .setContainerMerkleTree(tree)
        .build();
    
    File treeFile = new File(tempDir.toFile(), fileName);
    try (OutputStream fos = Files.newOutputStream(treeFile.toPath())) {
      checksumInfo.writeTo(fos);
    }
    return treeFile;
  }

  /**
   * Helper method to run DiffSubcommand with two file paths and return output.
   */
  private JsonNode runDiffCommand(File tree1File, File tree2File) throws Exception {
    DiffSubcommand command = new DiffSubcommand();
    command.setTreeFilePaths(tree1File.getAbsolutePath(), tree2File.getAbsolutePath());
    command.call();

    // Parse actual output
    String actualOutput = out.toString(DEFAULT_ENCODING).trim();
    ObjectMapper mapper = JsonUtils.getDefaultMapper();
    return mapper.readTree(actualOutput).get(0);
  }
}
