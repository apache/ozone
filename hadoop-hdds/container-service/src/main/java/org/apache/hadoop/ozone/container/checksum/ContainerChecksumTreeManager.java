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

package org.apache.hadoop.ozone.container.checksum;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static org.apache.hadoop.hdds.HddsUtils.checksumToString;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DATA_CHECKSUM_EXTENSION;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Striped;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class coordinates reading and writing Container checksum information for all containers.
 */
public class ContainerChecksumTreeManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerChecksumTreeManager.class);

  // Used to coordinate writes to each container's checksum file.
  // Each container ID is mapped to a stripe.
  // The file is atomically renamed into place, so readers do not need coordination.
  private final Striped<Lock> fileLocks;
  private final ContainerMerkleTreeMetrics metrics;

  /**
   * Creates one instance that should be used to coordinate all container checksum info within a datanode.
   */
  public ContainerChecksumTreeManager(ConfigurationSource conf) {
    fileLocks = Striped.custom(conf.getObject(DatanodeConfiguration.class).getContainerChecksumLockStripes(),
        () -> new ReentrantLock(true));
    metrics = ContainerMerkleTreeMetrics.create();
  }

  public void stop() {
    ContainerMerkleTreeMetrics.unregister();
  }

  /**
   * Compares the checksum info of the container with the peer's checksum info and returns a report of the differences.
   * @param thisChecksumInfo The checksum info of the container on this datanode.
   * @param peerChecksumInfo The checksum info of the container on the peer datanode.
   */
  public ContainerDiffReport diff(ContainerProtos.ContainerChecksumInfo thisChecksumInfo,
                                  ContainerProtos.ContainerChecksumInfo peerChecksumInfo) throws
      StorageContainerException {

    ContainerDiffReport report = new ContainerDiffReport(thisChecksumInfo.getContainerID());
    try {
      Preconditions.assertNotNull(thisChecksumInfo, "Datanode's checksum info is null.");
      Preconditions.assertNotNull(peerChecksumInfo, "Peer checksum info is null.");
      if (thisChecksumInfo.getContainerID() != peerChecksumInfo.getContainerID()) {
        throw new StorageContainerException("Container ID does not match. Local container ID "
            + thisChecksumInfo.getContainerID() + " , Peer container ID " + peerChecksumInfo.getContainerID(),
            ContainerProtos.Result.CONTAINER_ID_MISMATCH);
      }
      captureLatencyNs(metrics.getMerkleTreeDiffLatencyNS(), () -> {
        compareContainerMerkleTree(thisChecksumInfo, peerChecksumInfo, report);
      });
    } catch (IOException ex) {
      metrics.incrementMerkleTreeDiffFailures();
      throw new StorageContainerException("Container Diff failed for container #" + thisChecksumInfo.getContainerID(),
          ex, ContainerProtos.Result.IO_EXCEPTION);
    }

    // Update Container Diff metrics based on the diff report.
    if (report.needsRepair()) {
      metrics.incrementRepairContainerDiffs();
      metrics.incrementCorruptChunksIdentified(report.getNumCorruptChunks());
      metrics.incrementMissingBlocksIdentified(report.getNumMissingBlocks());
      metrics.incrementMissingChunksIdentified(report.getNumMissingChunks());
      metrics.incrementDivergedDeletedBlocksIdentified(report.getNumdivergedDeletedBlocks());
    } else {
      metrics.incrementNoRepairContainerDiffs();
    }
    return report;
  }

  private void compareContainerMerkleTree(ContainerProtos.ContainerChecksumInfo thisChecksumInfo,
                                          ContainerProtos.ContainerChecksumInfo peerChecksumInfo,
                                          ContainerDiffReport report) {
    ContainerProtos.ContainerMerkleTree thisMerkleTree = thisChecksumInfo.getContainerMerkleTree();
    ContainerProtos.ContainerMerkleTree peerMerkleTree = peerChecksumInfo.getContainerMerkleTree();

    if (thisMerkleTree.getDataChecksum() == peerMerkleTree.getDataChecksum()) {
      return;
    }

    List<ContainerProtos.BlockMerkleTree> thisBlockMerkleTreeList = thisMerkleTree.getBlockMerkleTreeList();
    List<ContainerProtos.BlockMerkleTree> peerBlockMerkleTreeList = peerMerkleTree.getBlockMerkleTreeList();
    int thisIdx = 0, peerIdx = 0;

    // Step 1: Process both lists while elements are present in both
    while (thisIdx < thisBlockMerkleTreeList.size() && peerIdx < peerBlockMerkleTreeList.size()) {
      ContainerProtos.BlockMerkleTree thisBlockMerkleTree = thisBlockMerkleTreeList.get(thisIdx);
      ContainerProtos.BlockMerkleTree peerBlockMerkleTree = peerBlockMerkleTreeList.get(peerIdx);

      if (thisBlockMerkleTree.getBlockID() == peerBlockMerkleTree.getBlockID()) {
        if (thisBlockMerkleTree.getDataChecksum() != peerBlockMerkleTree.getDataChecksum()) {
          compareBlockMerkleTree(thisBlockMerkleTree, peerBlockMerkleTree, report);
        }
        thisIdx++;
        peerIdx++;
      } else if (thisBlockMerkleTree.getBlockID() < peerBlockMerkleTree.getBlockID()) {
        // this block merkle tree's block id is smaller. Which means our merkle tree has some blocks which the peer
        // doesn't have. We can skip these, the peer will pick up these block when it reconciles with our merkle tree.
        thisIdx++;
      } else {
        // Peer block's ID is smaller, so we do not have this block. Add it to the corresponding list of missing blocks
        // and advance peerIdx
        if (peerBlockMerkleTree.getDeleted()) {
          report.addDivergedDeletedBlock(peerBlockMerkleTree);
        } else {
          report.addMissingBlock(peerBlockMerkleTree);
        }
        peerIdx++;
      }
    }

    // Step 2: Process remaining blocks in the peer list
    while (peerIdx < peerBlockMerkleTreeList.size()) {
      ContainerProtos.BlockMerkleTree peerBlockMerkleTree = peerBlockMerkleTreeList.get(peerIdx);
      if (peerBlockMerkleTree.getDeleted()) {
        report.addDivergedDeletedBlock(peerBlockMerkleTree);
      } else {
        report.addMissingBlock(peerBlockMerkleTree);
      }
      peerIdx++;
    }

    // If we have remaining block in thisMerkleTree, we can skip these blocks. The peers will pick this block from
    // us when they reconcile.
  }

  /**
   * When comparing blocks, resolve checksum conflicts using the following function:
   * - If both blocks are live: compute the checksum from a union of the blocks' chunks
   * - If one block is live and one is deleted: overwrite the live block with the deleted block using the checksum of
   * the deleted block
   * - If both blocks are deleted: use the largest checksum
   * This should be commutative, associative, and idempotent, so that all replicas converge after a single round of
   * reconciliation when all peers have communicated.
   */
  private void compareBlockMerkleTree(ContainerProtos.BlockMerkleTree thisBlockMerkleTree,
      ContainerProtos.BlockMerkleTree peerBlockMerkleTree, ContainerDiffReport report) {

    boolean thisBlockDeleted = thisBlockMerkleTree.getDeleted();
    boolean peerBlockDeleted = peerBlockMerkleTree.getDeleted();

    if (thisBlockDeleted) {
      // Our block has been deleted.
      if (peerBlockDeleted && thisBlockMerkleTree.getDataChecksum() < peerBlockMerkleTree.getDataChecksum()) {
        // If the peer's block is also deleted, use the largest checksum value as the winner so that the values converge
        // since there is no data corresponding to this block.
        report.addDivergedDeletedBlock(peerBlockMerkleTree);
      }
      // Else, either the peer has not deleted the block or they have a lower checksum for their deleted block.
      // In these cases the peer needs to update their block.
      // If the peer's block is deleted and its checksum matches ours, no update is required.
    } else {
      if (peerBlockDeleted) {
        // Our block has not yet been deleted, but peer's block has been.
        // Mark our block as deleted to bring it in sync with the peer.
        // Our block deleting service will eventually catch up.
        // Our container scanner will not update this deleted block in the merkle tree further even if it is still on
        // disk so that we remain in sync with the peer.
        // TODO HDDS-11765 Add support for deleting blocks from our replica when a peer has already deleted the block.
        report.addDivergedDeletedBlock(peerBlockMerkleTree);
      } else {
        // Neither our nor peer's block is deleted. Walk the chunk list to find differences.
        compareChunkMerkleTrees(thisBlockMerkleTree, peerBlockMerkleTree, report);
      }
    }
  }

  private void compareChunkMerkleTrees(ContainerProtos.BlockMerkleTree thisBlockMerkleTree,
                                      ContainerProtos.BlockMerkleTree peerBlockMerkleTree,
                                      ContainerDiffReport report) {

    List<ContainerProtos.ChunkMerkleTree> thisChunkMerkleTreeList = thisBlockMerkleTree.getChunkMerkleTreeList();
    List<ContainerProtos.ChunkMerkleTree> peerChunkMerkleTreeList = peerBlockMerkleTree.getChunkMerkleTreeList();
    int thisIdx = 0, peerIdx = 0;
    long containerID = report.getContainerID();
    long blockID = thisBlockMerkleTree.getBlockID();

    // Step 1: Process both lists while elements are present in both
    while (thisIdx < thisChunkMerkleTreeList.size() && peerIdx < peerChunkMerkleTreeList.size()) {
      ContainerProtos.ChunkMerkleTree thisChunkMerkleTree = thisChunkMerkleTreeList.get(thisIdx);
      ContainerProtos.ChunkMerkleTree peerChunkMerkleTree = peerChunkMerkleTreeList.get(peerIdx);

      if (thisChunkMerkleTree.getOffset() == peerChunkMerkleTree.getOffset()) {
        // Possible state when this Checksum != peer Checksum:
        // thisTree = Healthy, peerTree = Healthy -> Both are healthy, No repair needed. Skip.
        // thisTree = Unhealthy, peerTree = Healthy -> Add to corrupt chunk.
        // thisTree = Healthy, peerTree = unhealthy -> Do nothing as thisTree is healthy.
        // thisTree = Unhealthy, peerTree = Unhealthy -> Do Nothing as both are corrupt.
        if (thisChunkMerkleTree.getDataChecksum() != peerChunkMerkleTree.getDataChecksum() &&
            !thisChunkMerkleTree.getChecksumMatches()) {
          reportChunkIfHealthy(containerID, blockID, peerChunkMerkleTree, report::addCorruptChunk);
        }
        thisIdx++;
        peerIdx++;
      } else if (thisChunkMerkleTree.getOffset() < peerChunkMerkleTree.getOffset()) {
        // this chunk merkle tree's offset is smaller. Which means our merkle tree has some chunks which the peer
        // doesn't have. We can skip these, the peer will pick up these chunks when it reconciles with our merkle tree.
        thisIdx++;
      } else {
        // Peer chunk's offset is smaller; record missing chunk and advance peerIdx
        reportChunkIfHealthy(containerID, blockID, peerChunkMerkleTree, report::addMissingChunk);
        peerIdx++;
      }
    }

    // Step 2: Process remaining chunks in the peer list
    while (peerIdx < peerChunkMerkleTreeList.size()) {
      reportChunkIfHealthy(containerID, blockID, peerChunkMerkleTreeList.get(peerIdx), report::addMissingChunk);
      peerIdx++;
    }

    // If we have remaining chunks in thisBlockMerkleTree, we can skip these chunks. The peers will pick these
    // chunks from us when they reconcile.
  }

  private void reportChunkIfHealthy(long containerID, long blockID, ContainerProtos.ChunkMerkleTree peerTree,
      BiConsumer<Long, ContainerProtos.ChunkMerkleTree> addToReport) {
    if (peerTree.getChecksumMatches()) {
      addToReport.accept(blockID, peerTree);
    } else {
      LOG.warn("Skipping chunk at offset {} in block {} of container {} since peer reported it as " +
          "unhealthy.", peerTree.getOffset(), blockID, containerID);
    }
  }

  public static long getDataChecksum(ContainerProtos.ContainerChecksumInfo checksumInfo) {
    return checksumInfo.getContainerMerkleTree().getDataChecksum();
  }

  /**
   * Returns the container checksum tree file for the specified container without deserializing it.
   */
  public static File getContainerChecksumFile(ContainerData data) {
    return new File(data.getMetadataPath(), data.getContainerID() + CONTAINER_DATA_CHECKSUM_EXTENSION);
  }

  /**
   * Returns true if the {@link ContainerProtos.ContainerChecksumInfo} provided is not null, and its merkle tree has a
   * data checksum field present. Returns false otherwise, indicating a scan of the data in this container has not yet
   * been done.
   */
  public static boolean hasDataChecksum(ContainerProtos.ContainerChecksumInfo checksumInfo) {
    return checksumInfo != null &&
        checksumInfo.hasContainerMerkleTree() &&
        checksumInfo.getContainerMerkleTree().hasDataChecksum();
  }

  @VisibleForTesting
  public static File getTmpContainerChecksumFile(ContainerData data) {
    return new File(data.getMetadataPath(), data.getContainerID() + CONTAINER_DATA_CHECKSUM_EXTENSION + ".tmp");
  }

  private Lock getLock(long containerID) {
    return fileLocks.get(containerID);
  }

  /**
   * Reads the checksum info of the specified container. If the tree file with the information does not exist, an empty
   * instance is returned.
   * Callers are not required to hold a lock while calling this since writes are done to a tmp file and atomically
   * swapped into place.
   */
  public ContainerProtos.ContainerChecksumInfo read(ContainerData data) throws IOException {
    try {
      return captureLatencyNs(metrics.getReadContainerMerkleTreeLatencyNS(), () -> readChecksumInfo(data));
    } catch (IOException ex) {
      metrics.incrementMerkleTreeReadFailures();
      throw ex;
    }
  }

  /**
   * Called by the container scanner and reconciliation to update the merkle tree persisted to disk.
   * For live (non-deleted) blocks, only those in the incoming treeWriter parameter are used.
   * For deleted blocks, those in the incoming treeWriter are merged with those on disk.
   */
  public ContainerProtos.ContainerChecksumInfo updateTree(ContainerData data, ContainerMerkleTreeWriter treeWriter)
      throws IOException {
    return write(data, treeWriter::update);
  }

  /**
   * Called by block deletion to update the merkle tree persisted to disk with more deleted blocks.
   * If a block with the same ID already exists in the tree, it is overwritten as deleted with the checksum computed
   * from the chunk checksums in the BlockData.
   *
   * The top level container data checksum is only updated if the existing tree on disk already has this value present.
   * This lets the block deleting service add blocks to the tree before the scanner has reached the container, and that
   * list of deleted blocks will not be mistaken for the list of all blocks seen in the container.
   * See {@link #hasDataChecksum(ContainerProtos.ContainerChecksumInfo)}.
   */
  public void addDeletedBlocks(ContainerData data, Collection<BlockData> blocks) throws IOException {
    write(data, existingTree -> {
      ContainerMerkleTreeWriter treeWriter = new ContainerMerkleTreeWriter(existingTree);
      return treeWriter.addDeletedBlocks(blocks, existingTree.hasDataChecksum());
    });
  }

  /**
   * Reads the checksum info of the specified container. If the tree file with the information does not exist, or there
   * is an exception trying to read the file, an empty instance is returned.
   */
  private ContainerProtos.ContainerChecksumInfo readOrCreate(ContainerData data) {
    try {
      // If the file is not present, we will create the data for the first time. This happens under a write lock.
      return read(data);
    } catch (IOException ex) {
      LOG.error("Failed to read container checksum tree file for container {}. Overwriting it with a new instance.",
          data.getContainerID(), ex);
      return ContainerProtos.ContainerChecksumInfo.newBuilder().build();
    }
  }

  /**
   * Performs a read-modify-write cycle on the container's checksum file.
   * 1. The lock is taken
   * 2. The file's contents are read into memory
   * 3. A new set of file contents are created using the specified merge function
   * 4. The new contents are written back to the file
   * 5. The lock is released
   */
  private ContainerProtos.ContainerChecksumInfo write(ContainerData data, Function<ContainerProtos.ContainerMerkleTree,
      ContainerProtos.ContainerMerkleTree> mergeFunction) throws IOException {
    long containerID = data.getContainerID();
    Lock fileLock = getLock(containerID);
    fileLock.lock();
    try {
      ContainerProtos.ContainerChecksumInfo currentChecksumInfo = readOrCreate(data);
      ContainerProtos.ContainerChecksumInfo.Builder newChecksumInfoBuilder = currentChecksumInfo.toBuilder();

      // Merge the incoming merkle tree with the content already on the disk.
      ContainerProtos.ContainerMerkleTree treeProto = captureLatencyNs(metrics.getCreateMerkleTreeLatencyNS(),
          () -> mergeFunction.apply(currentChecksumInfo.getContainerMerkleTree()));
      ContainerProtos.ContainerChecksumInfo newChecksumInfo = newChecksumInfoBuilder
          .setContainerMerkleTree(treeProto)
          .setContainerID(containerID)
          .build();

      // Write the updated merkle tree to the file.
      File checksumFile = getContainerChecksumFile(data);
      File tmpChecksumFile = getTmpContainerChecksumFile(data);

      try (OutputStream tmpOutputStream = Files.newOutputStream(tmpChecksumFile.toPath())) {
        // Write to a tmp file and rename it into place.
        captureLatencyNs(metrics.getWriteContainerMerkleTreeLatencyNS(), () -> {
          newChecksumInfo.writeTo(tmpOutputStream);
          Files.move(tmpChecksumFile.toPath(), checksumFile.toPath(), ATOMIC_MOVE);
        });
        LOG.debug("Merkle tree for container {} updated with container data checksum {}", containerID,
            checksumToString(treeProto.getDataChecksum()));
      } catch (IOException ex) {
        // If the move failed and left behind the tmp file, the tmp file will be overwritten on the next successful
        // write. Nothing reads directly from the tmp file.
        metrics.incrementMerkleTreeWriteFailures();
        throw new IOException("Error occurred when writing container merkle tree for containerID "
            + data.getContainerID(), ex);
      }
      return newChecksumInfo;
    } finally {
      fileLock.unlock();
    }
  }

  /**
   * Reads the container checksum info file from the disk as bytes.
   * Callers are not required to hold a lock while calling this since writes are done to a tmp file and atomically
   * swapped into place.
   *
   * @throws FileNotFoundException When the file does not exist. It may not have been generated yet for this container.
   * @throws IOException On error reading the file.
   */
  public ByteString getContainerChecksumInfo(KeyValueContainerData data) throws IOException {
    File checksumFile = getContainerChecksumFile(data);
    if (!checksumFile.exists()) {
      throw new FileNotFoundException("Checksum file does not exist for container #" + data.getContainerID());
    }

    try (InputStream inStream = Files.newInputStream(checksumFile.toPath())) {
      return ByteString.readFrom(inStream);
    }
  }

  /**
   * Reads the container checksum info file (containerID.tree) from the disk.
   * Callers are not required to hold a lock while calling this since writes are done to a tmp file and atomically
   * swapped into place.
   */
  public static ContainerProtos.ContainerChecksumInfo readChecksumInfo(ContainerData data)
      throws IOException {
    long containerID = data.getContainerID();
    File checksumFile = getContainerChecksumFile(data);
    try {
      if (!checksumFile.exists()) {
        LOG.debug("No checksum file currently exists for container {} at the path {}", containerID, checksumFile);
        return ContainerProtos.ContainerChecksumInfo.newBuilder().build();
      }

      try (InputStream inStream = Files.newInputStream(checksumFile.toPath())) {
        return ContainerProtos.ContainerChecksumInfo.parseFrom(inStream);
      }
    } catch (IOException ex) {
      throw new IOException("Error occurred when reading container merkle tree for containerID "
          + data.getContainerID() + " at path " + checksumFile, ex);
    }
  }

  @VisibleForTesting
  public ContainerMerkleTreeMetrics getMetrics() {
    return this.metrics;
  }
}
