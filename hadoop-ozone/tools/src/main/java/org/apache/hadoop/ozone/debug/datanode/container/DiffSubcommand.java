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

  @CommandLine.Parameters(index = "0", arity = "2",
      description = "Paths to two container checksum tree files (.tree) to compare")
  private List<String> treeFilePaths;

  /**
   * Sets the tree file paths. Used for testing.
   */
  @VisibleForTesting
  public void setTreeFilePaths(String tree1Path, String tree2Path) {
    this.treeFilePaths = new ArrayList<>();
    this.treeFilePaths.add(tree1Path);
    this.treeFilePaths.add(tree2Path);
  }

  @Override
  public Void call() throws Exception {
    String tree1FilePath = treeFilePaths.get(0);
    String tree2FilePath = treeFilePaths.get(1);
    
    File tree1File = new File(tree1FilePath);
    File tree2File = new File(tree2FilePath);

    if (!tree1File.exists()) {
      throw new RuntimeException("First tree file does not exist: " + tree1FilePath);
    }

    if (!tree2File.exists()) {
      throw new RuntimeException("Second tree file does not exist: " + tree2FilePath);
    }

    try {
      ContainerProtos.ContainerChecksumInfo checksumInfo1 = readChecksumInfo(tree1File);
      ContainerProtos.ContainerChecksumInfo checksumInfo2 = readChecksumInfo(tree2File);

      // Validate that both trees are for the same container
      if (checksumInfo1.getContainerID() != checksumInfo2.getContainerID()) {
        throw new RuntimeException("Container IDs do not match. Tree1: " + checksumInfo1.getContainerID() +
            ", Tree2: " + checksumInfo2.getContainerID());
      }

      // Perform bidirectional diff
      ContainerDiffReport diffReport1to2 = performDiff(checksumInfo1, checksumInfo2);
      ContainerDiffReport diffReport2to1 = performDiff(checksumInfo2, checksumInfo1);
      
      DiffReportWrapper wrapper = new DiffReportWrapper(
          diffReport1to2, diffReport2to1, checksumInfo1, checksumInfo2);
      
      try (SequenceWriter writer = JsonUtils.getStdoutSequenceWriter()) {
        writer.write(wrapper);
        writer.flush();
      }
      System.out.println();
    } catch (IOException e) {
      throw new RuntimeException("Failed to read tree files", e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to compare tree files", e);
    }

    return null;
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
    private final TreeDiffWrapper replica1;
    private final TreeDiffWrapper replica2;

    DiffReportWrapper(ContainerDiffReport diffReport1to2,
        ContainerDiffReport diffReport2to1,
        ContainerProtos.ContainerChecksumInfo checksumInfo1,
        ContainerProtos.ContainerChecksumInfo checksumInfo2) {
      this.containerID = diffReport1to2.getContainerID();
      this.replica1 = new TreeDiffWrapper(checksumInfo1, diffReport1to2);
      this.replica2 = new TreeDiffWrapper(checksumInfo2, diffReport2to1);
    }

    public long getContainerID() {
      return containerID;
    }

    public TreeDiffWrapper getReplica1() {
      return replica1;
    }

    public TreeDiffWrapper getReplica2() {
      return replica2;
    }
  }

  /**
   * Wrapper class for tree information with its diff results.
   */
  private static class TreeDiffWrapper {
    @JsonSerialize(using = JsonUtils.ChecksumSerializer.class)
    private final long dataChecksum;
    private final boolean needsRepair;
    private final SummaryWrapper summary;
    private final DifferencesWrapper differences;

    TreeDiffWrapper(ContainerProtos.ContainerChecksumInfo checksumInfo, ContainerDiffReport diffReport) {
      this.dataChecksum = hasDataChecksum(checksumInfo) ? checksumInfo.getContainerMerkleTree().getDataChecksum() : 0L;
      this.needsRepair = diffReport.needsRepair();
      this.summary = new SummaryWrapper(diffReport);
      this.differences = new DifferencesWrapper(diffReport);
    }

    public long getDataChecksum() {
      return dataChecksum;
    }

    public boolean isNeedsRepair() {
      return needsRepair;
    }

    public SummaryWrapper getSummary() {
      return summary;
    }

    public DifferencesWrapper getDifferences() {
      return differences;
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
