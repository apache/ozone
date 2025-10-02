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

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.server.JsonUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Handles {@code ozone debug datanode container checksum} command.
 * Displays the unserialized version of a container checksum tree file in JSON format.
 */
@Command(
    name = "checksum",
    description = "Display container checksum tree file in JSON format")
public class ChecksumSubcommand implements Callable<Void> {

  @CommandLine.Option(names = {"--tree", "-t"},
      required = true,
      description = "Path to the container checksum tree file (.tree)")
  private String treeFilePath;

  /**
   * Sets the tree file path. Used for testing.
   */
  @VisibleForTesting
  public void setTreeFilePath(String treeFilePath) {
    this.treeFilePath = treeFilePath;
  }

  @Override
  public Void call() throws Exception {
    File treeFile = new File(treeFilePath);
    if (!treeFile.exists()) {
      System.err.println("Error: Tree file does not exist: " + treeFilePath);
      throw new RuntimeException("Tree file does not exist: " + treeFilePath);
    }

    try {
      ContainerProtos.ContainerChecksumInfo checksumInfo = readChecksumInfo(treeFile);
      ChecksumInfoWrapper wrapper = new ChecksumInfoWrapper(checksumInfo, treeFilePath);
      
      try (SequenceWriter writer = JsonUtils.getStdoutSequenceWriter()) {
        writer.write(wrapper);
        writer.flush();
      }
      System.out.println();
      System.out.flush();
    } catch (IOException e) {
      System.err.println("Error reading tree file: " + getExceptionMessage(e));
      throw new RuntimeException("Failed to read tree file: " + treeFilePath, e);
    } catch (Exception e) {
      System.err.println("Error processing tree file: " + getExceptionMessage(e));
      throw new RuntimeException("Failed to process tree file: " + treeFilePath, e);
    }

    return null;
  }

  /**
   * Extract clean exception message without stack trace for user display.
   */
  private String getExceptionMessage(Exception ex) {
    return ex.getMessage() != null ? ex.getMessage().split("\n", 2)[0] : ex.getClass().getSimpleName();
  }

  /**
   * Reads the container checksum info from the specified file.
   */
  private ContainerProtos.ContainerChecksumInfo readChecksumInfo(File treeFile) throws IOException {
    try (InputStream inputStream = Files.newInputStream(treeFile.toPath())) {
      return ContainerProtos.ContainerChecksumInfo.parseFrom(inputStream);
    }
  }

  /**
   * Wrapper class for JSON serialization of container checksum info.
   */
  private static class ChecksumInfoWrapper {
    private final long containerID;
    private final String filePath;
    private final ContainerMerkleTreeWrapper containerMerkleTree;

    ChecksumInfoWrapper(ContainerProtos.ContainerChecksumInfo checksumInfo, String filePath) {
      this.containerID = checksumInfo.getContainerID();
      this.filePath = filePath;
      this.containerMerkleTree = checksumInfo.hasContainerMerkleTree() ? 
          new ContainerMerkleTreeWrapper(checksumInfo.getContainerMerkleTree()) : null;
    }

    public long getContainerID() {
      return containerID;
    }

    public String getFilePath() {
      return filePath;
    }

    public ContainerMerkleTreeWrapper getContainerMerkleTree() {
      return containerMerkleTree;
    }
  }

  /**
   * Wrapper class for JSON serialization of container merkle tree.
   */
  private static class ContainerMerkleTreeWrapper {
    @JsonSerialize(using = JsonUtils.ChecksumSerializer.class)
    private final long dataChecksum;
    private final List<BlockMerkleTreeWrapper> blockMerkleTrees;

    ContainerMerkleTreeWrapper(ContainerProtos.ContainerMerkleTree merkleTree) {
      this.dataChecksum = merkleTree.hasDataChecksum() ? merkleTree.getDataChecksum() : 0L;
      this.blockMerkleTrees = new ArrayList<>();
      for (ContainerProtos.BlockMerkleTree blockTree : merkleTree.getBlockMerkleTreeList()) {
        this.blockMerkleTrees.add(new BlockMerkleTreeWrapper(blockTree));
      }
    }

    public long getDataChecksum() {
      return dataChecksum;
    }

    public List<BlockMerkleTreeWrapper> getBlockMerkleTrees() {
      return blockMerkleTrees;
    }
  }

  /**
   * Wrapper class for JSON serialization of block merkle tree.
   */
  private static class BlockMerkleTreeWrapper {
    private final long blockID;
    private final boolean deleted;
    @JsonSerialize(using = JsonUtils.ChecksumSerializer.class)
    private final long dataChecksum;
    private final List<ChunkMerkleTreeWrapper> chunkMerkleTrees;

    BlockMerkleTreeWrapper(ContainerProtos.BlockMerkleTree blockTree) {
      this.blockID = blockTree.getBlockID();
      this.deleted = blockTree.getDeleted();
      this.dataChecksum = blockTree.hasDataChecksum() ? blockTree.getDataChecksum() : 0L;
      this.chunkMerkleTrees = new ArrayList<>();
      
      // Only include chunk trees if block is not deleted
      if (!deleted) {
        for (ContainerProtos.ChunkMerkleTree chunkTree : blockTree.getChunkMerkleTreeList()) {
          this.chunkMerkleTrees.add(new ChunkMerkleTreeWrapper(chunkTree));
        }
      }
    }

    public long getBlockID() {
      return blockID;
    }

    public boolean isDeleted() {
      return deleted;
    }

    public long getDataChecksum() {
      return dataChecksum;
    }

    public List<ChunkMerkleTreeWrapper> getChunkMerkleTrees() {
      return chunkMerkleTrees;
    }
  }

  /**
   * Wrapper class for JSON serialization of chunk merkle tree.
   */
  private static class ChunkMerkleTreeWrapper {
    private final long offset;
    private final long length;
    private final boolean checksumMatches;
    @JsonSerialize(using = JsonUtils.ChecksumSerializer.class)
    private final long dataChecksum;

    ChunkMerkleTreeWrapper(ContainerProtos.ChunkMerkleTree chunkTree) {
      this.offset = chunkTree.getOffset();
      this.length = chunkTree.getLength();
      this.checksumMatches = chunkTree.getChecksumMatches();
      this.dataChecksum = chunkTree.hasDataChecksum() ? chunkTree.getDataChecksum() : 0L;
    }

    public long getOffset() {
      return offset;
    }

    public long getLength() {
      return length;
    }

    public boolean isChecksumMatches() {
      return checksumMatches;
    }

    public long getDataChecksum() {
      return dataChecksum;
    }
  }
}
