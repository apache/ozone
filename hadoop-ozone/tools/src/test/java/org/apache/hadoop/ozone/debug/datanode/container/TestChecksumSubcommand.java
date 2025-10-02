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
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.updateTreeProto;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
  private ByteArrayOutputStream err;
  private PrintStream originalOut;
  private PrintStream originalErr;
  private static final String DEFAULT_ENCODING = UTF_8.name();

  @BeforeEach
  void setUp() throws Exception {
    config = new OzoneConfiguration();
    
    // Capture stdout and stderr
    out = new ByteArrayOutputStream();
    err = new ByteArrayOutputStream();
    originalOut = System.out;
    originalErr = System.err;
    System.setOut(new PrintStream(out, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(err, false, DEFAULT_ENCODING));
  }

  @AfterEach
  void tearDown() {
    // Restore stdout and stderr
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  void testChecksumCommandWithValidFile() throws Exception {
    // Create a mock container data
    KeyValueContainerData containerData = Mockito.mock(KeyValueContainerData.class);
    Mockito.when(containerData.getContainerID()).thenReturn(CONTAINER_ID);
    Mockito.when(containerData.getMetadataPath()).thenReturn(tempDir.toString());

    // Build a test tree and write it to file
    ContainerMerkleTreeWriter tree = buildTestTree(config);
    ContainerProtos.ContainerMerkleTree treeProto = tree.toProto();
    updateTreeProto(containerData, treeProto);

    File treeFile = new File(tempDir.toFile(), CONTAINER_ID + ".tree");

    JsonNode actualJson = runChecksumCommand(treeFile);

    // Verify the structure and key fields
    assertThat(actualJson.isArray()).isTrue();
    assertThat(actualJson.size()).isEqualTo(1);

    JsonNode containerJson = actualJson.get(0);
    assertThat(containerJson.get("containerID").asLong()).isEqualTo(CONTAINER_ID);
    assertThat(containerJson.get("filePath").asText()).isEqualTo(treeFile.getAbsolutePath());
    assertThat(containerJson.has("containerMerkleTree")).isTrue();

    JsonNode merkleTree = containerJson.get("containerMerkleTree");
    assertThat(merkleTree.has("dataChecksum")).isTrue();
    assertThat(merkleTree.has("blockMerkleTrees")).isTrue();
    assertThat(merkleTree.get("blockMerkleTrees").isArray()).isTrue();
    assertThat(merkleTree.get("blockMerkleTrees").size()).isEqualTo(5); // Default buildTestTree creates 5 blocks
  }

  @Test
  void testChecksumCommandWithNonExistentFile() throws Exception {
    ChecksumSubcommand command = new ChecksumSubcommand();
    command.setTreeFilePath("/non/existent/file.tree");

    RuntimeException exception = assertThrows(RuntimeException.class, command::call);

    assertThat(err.toString(DEFAULT_ENCODING)).contains("Error: Tree file does not exist");
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

    assertThat(err.toString(DEFAULT_ENCODING)).contains("Error reading tree file");
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

    JsonNode actualJson = runChecksumCommand(treeFile);
    
    // Build expected JSON string for empty container
    String expectedJson = String.format(
        "[ {" +
        "\"containerID\" : 0," +
        "\"filePath\" : \"%s\"" +
        "} ]", 
        treeFile.getAbsolutePath().replace("\\", "\\\\")
    );
    ObjectMapper mapper = JsonUtils.getDefaultMapper();
    JsonNode expectedJsonNode = mapper.readTree(expectedJson);
    
    // Compare JSON structures
    assertEquals(expectedJsonNode, actualJson);
  }

  @Test
  void testChecksumCommandWithComplexTree() throws Exception {
    // Create a mock container data
    KeyValueContainerData containerData = Mockito.mock(KeyValueContainerData.class);
    Mockito.when(containerData.getContainerID()).thenReturn(CONTAINER_ID);
    Mockito.when(containerData.getMetadataPath()).thenReturn(tempDir.toString());

    // Build a more complex test tree with more blocks
    ContainerMerkleTreeWriter tree = buildTestTree(config, 10); // 10 blocks instead of default 5
    updateTreeProto(containerData, tree.toProto());

    File treeFile = new File(tempDir.toFile(), CONTAINER_ID + ".tree");

    JsonNode actualJson = runChecksumCommand(treeFile);

    // Verify the structure and key fields for complex tree
    assertThat(actualJson.isArray()).isTrue();
    assertThat(actualJson.size()).isEqualTo(1);

    JsonNode containerJson = actualJson.get(0);
    assertThat(containerJson.get("containerID").asLong()).isEqualTo(CONTAINER_ID);
    assertThat(containerJson.get("filePath").asText()).isEqualTo(treeFile.getAbsolutePath());
    assertThat(containerJson.has("containerMerkleTree")).isTrue();

    JsonNode merkleTree = containerJson.get("containerMerkleTree");
    assertThat(merkleTree.has("dataChecksum")).isTrue();
    assertThat(merkleTree.has("blockMerkleTrees")).isTrue();
    assertThat(merkleTree.get("blockMerkleTrees").isArray()).isTrue();
    assertThat(merkleTree.get("blockMerkleTrees").size()).isEqualTo(10); // Complex tree has 10 blocks
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
}
