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

import static org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager.hasDataChecksum;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.checksum.ContainerDiffReport;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Handles {@code ozone debug datanode container diff} command.
 * Compares two container checksum tree files and displays the differences in JSON format.
 */
@Command(
    name = "diff",
    description = "Compare two container checksum tree files and show differences in JSON format")
public class DiffSubcommand implements Callable<Void> {

  @CommandLine.Option(names = {"--tree1", "-t1"},
      required = true,
      description = "Path to the first container checksum tree file (.tree)")
  private String tree1FilePath;

  @CommandLine.Option(names = {"--tree2", "-t2"},
      required = true,
      description = "Path to the second container checksum tree file (.tree)")
  private String tree2FilePath;

  /**
   * Sets the tree file paths. Used for testing.
   */
  @VisibleForTesting
  public void setTreeFilePaths(String tree1Path, String tree2Path) {
    this.tree1FilePath = tree1Path;
    this.tree2FilePath = tree2Path;
  }

  @Override
  public Void call() throws Exception {
    File tree1File = new File(tree1FilePath);
    File tree2File = new File(tree2FilePath);

    if (!tree1File.exists()) {
      System.err.println("Error: First tree file does not exist: " + tree1FilePath);
      throw new RuntimeException("First tree file does not exist: " + tree1FilePath);
    }

    if (!tree2File.exists()) {
      System.err.println("Error: Second tree file does not exist: " + tree2FilePath);
      throw new RuntimeException("Second tree file does not exist: " + tree2FilePath);
    }

    try {
      ContainerProtos.ContainerChecksumInfo checksumInfo1 = readChecksumInfo(tree1File);
      ContainerProtos.ContainerChecksumInfo checksumInfo2 = readChecksumInfo(tree2File);

      // Validate that both trees are for the same container
      if (checksumInfo1.getContainerID() != checksumInfo2.getContainerID()) {
        String errorMsg = "Container IDs do not match. Tree1: " + 
            checksumInfo1.getContainerID() + ", Tree2: " + checksumInfo2.getContainerID();
        System.err.println("Error: " + errorMsg);
        throw new RuntimeException(errorMsg);
      }

      ContainerDiffReport diffReport = performDiff(checksumInfo1, checksumInfo2);
      DiffReportWrapper wrapper = new DiffReportWrapper(diffReport, checksumInfo1, checksumInfo2, 
          tree1FilePath, tree2FilePath);
      
      try (SequenceWriter writer = JsonUtils.getStdoutSequenceWriter()) {
        writer.write(wrapper);
        writer.flush();
      }
      System.out.println();
      System.out.flush();
    } catch (IOException e) {
      System.err.println("Error reading tree files: " + getExceptionMessage(e));
      throw new RuntimeException("Failed to read tree files", e);
    } catch (Exception e) {
      System.err.println("Error comparing tree files: " + getExceptionMessage(e));
      throw new RuntimeException("Failed to compare tree files", e);
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
   * Performs the diff operation using ContainerChecksumTreeManager.
   */
  private ContainerDiffReport performDiff(ContainerProtos.ContainerChecksumInfo checksumInfo1,
      ContainerProtos.ContainerChecksumInfo checksumInfo2) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    ContainerChecksumTreeManager manager = new ContainerChecksumTreeManager(conf);
    return manager.diff(checksumInfo1, checksumInfo2);
  }

  /**
   * Wrapper class for JSON serialization of diff report.
   */
  private static class DiffReportWrapper {
    private final long containerID;
    private final boolean needsRepair;
    private final TreeInfoWrapper tree1;
    private final TreeInfoWrapper tree2;
    private final SummaryWrapper summary;
    private final DifferencesWrapper differences;

    DiffReportWrapper(ContainerDiffReport diffReport, 
        ContainerProtos.ContainerChecksumInfo checksumInfo1,
        ContainerProtos.ContainerChecksumInfo checksumInfo2,
        String tree1FilePath, String tree2FilePath) {
      this.containerID = diffReport.getContainerID();
      this.needsRepair = diffReport.needsRepair();
      this.tree1 = new TreeInfoWrapper(checksumInfo1, tree1FilePath);
      this.tree2 = new TreeInfoWrapper(checksumInfo2, tree2FilePath);
      this.summary = new SummaryWrapper(diffReport);
      this.differences = new DifferencesWrapper(diffReport);
    }

    public long getContainerID() {
      return containerID;
    }

    public boolean isNeedsRepair() {
      return needsRepair;
    }

    public TreeInfoWrapper getTree1() {
      return tree1;
    }

    public TreeInfoWrapper getTree2() {
      return tree2;
    }

    public SummaryWrapper getSummary() {
      return summary;
    }

    public DifferencesWrapper getDifferences() {
      return differences;
    }
  }

  /**
   * Wrapper class for tree information.
   */
  private static class TreeInfoWrapper {
    private final String filePath;
    @JsonSerialize(using = JsonUtils.ChecksumSerializer.class)
    private final long dataChecksum;

    TreeInfoWrapper(ContainerProtos.ContainerChecksumInfo checksumInfo, String filePath) {
      this.filePath = filePath;
      this.dataChecksum = hasDataChecksum(checksumInfo) ? checksumInfo.getContainerMerkleTree().getDataChecksum() : 0L;
    }

    public String getFilePath() {
      return filePath;
    }

    public long getDataChecksum() {
      return dataChecksum;
    }
  }

  /**
   * Wrapper class for summary statistics.
   */
  private static class SummaryWrapper {
    private final long missingBlocks;
    private final long missingChunks;
    private final long corruptChunks;
    private final long divergedDeletedBlocks;

    SummaryWrapper(ContainerDiffReport diffReport) {
      this.missingBlocks = diffReport.getNumMissingBlocks();
      this.missingChunks = diffReport.getNumMissingChunks();
      this.corruptChunks = diffReport.getNumCorruptChunks();
      this.divergedDeletedBlocks = diffReport.getNumdivergedDeletedBlocks();
    }

    public long getMissingBlocks() {
      return missingBlocks;
    }

    public long getMissingChunks() {
      return missingChunks;
    }

    public long getCorruptChunks() {
      return corruptChunks;
    }

    public long getDivergedDeletedBlocks() {
      return divergedDeletedBlocks;
    }
  }

  /**
   * Wrapper class for detailed differences.
   */
  private static class DifferencesWrapper {
    private final List<MissingBlockWrapper> missingBlocks;
    private final Map<String, List<ChunkWrapper>> missingChunks;
    private final Map<String, List<ChunkWrapper>> corruptChunks;
    private final List<DeletedBlockWrapper> divergedDeletedBlocks;

    DifferencesWrapper(ContainerDiffReport diffReport) {
      // Missing blocks
      this.missingBlocks = new ArrayList<>();
      for (ContainerProtos.BlockMerkleTree missingBlock : diffReport.getMissingBlocks()) {
        this.missingBlocks.add(new MissingBlockWrapper(missingBlock));
      }

      // Missing chunks
      this.missingChunks = new HashMap<>();
      diffReport.getMissingChunks().forEach((blockId, chunks) -> {
        List<ChunkWrapper> chunkWrappers = new ArrayList<>();
        for (ContainerProtos.ChunkMerkleTree chunk : chunks) {
          chunkWrappers.add(new ChunkWrapper(chunk));
        }
        this.missingChunks.put(String.valueOf(blockId), chunkWrappers);
      });

      // Corrupt chunks
      this.corruptChunks = new HashMap<>();
      diffReport.getCorruptChunks().forEach((blockId, chunks) -> {
        List<ChunkWrapper> chunkWrappers = new ArrayList<>();
        for (ContainerProtos.ChunkMerkleTree chunk : chunks) {
          chunkWrappers.add(new ChunkWrapper(chunk));
        }
        this.corruptChunks.put(String.valueOf(blockId), chunkWrappers);
      });

      // Diverged deleted blocks
      this.divergedDeletedBlocks = new ArrayList<>();
      for (ContainerDiffReport.DeletedBlock deletedBlock : diffReport.getDivergedDeletedBlocks()) {
        this.divergedDeletedBlocks.add(new DeletedBlockWrapper(deletedBlock));
      }
    }

    public List<MissingBlockWrapper> getMissingBlocks() {
      return missingBlocks;
    }

    public Map<String, List<ChunkWrapper>> getMissingChunks() {
      return missingChunks;
    }

    public Map<String, List<ChunkWrapper>> getCorruptChunks() {
      return corruptChunks;
    }

    public List<DeletedBlockWrapper> getDivergedDeletedBlocks() {
      return divergedDeletedBlocks;
    }
  }

  /**
   * Wrapper class for missing block information.
   */
  private static class MissingBlockWrapper {
    private final long blockID;
    private final boolean deleted;
    @JsonSerialize(using = JsonUtils.ChecksumSerializer.class)
    private final long dataChecksum;

    MissingBlockWrapper(ContainerProtos.BlockMerkleTree blockTree) {
      this.blockID = blockTree.getBlockID();
      this.deleted = blockTree.getDeleted();
      this.dataChecksum = blockTree.hasDataChecksum() ? blockTree.getDataChecksum() : 0L;
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
  }

  /**
   * Wrapper class for chunk information.
   */
  private static class ChunkWrapper {
    private final long offset;
    private final long length;
    private final boolean checksumMatches;
    @JsonSerialize(using = JsonUtils.ChecksumSerializer.class)
    private final long dataChecksum;

    ChunkWrapper(ContainerProtos.ChunkMerkleTree chunkTree) {
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

  /**
   * Wrapper class for deleted block information.
   */
  private static class DeletedBlockWrapper {
    private final long blockID;
    @JsonSerialize(using = JsonUtils.ChecksumSerializer.class)
    private final long dataChecksum;

    DeletedBlockWrapper(ContainerDiffReport.DeletedBlock deletedBlock) {
      this.blockID = deletedBlock.getBlockID();
      this.dataChecksum = deletedBlock.getDataChecksum();
    }

    public long getBlockID() {
      return blockID;
    }

    public long getDataChecksum() {
      return dataChecksum;
    }
  }
}
