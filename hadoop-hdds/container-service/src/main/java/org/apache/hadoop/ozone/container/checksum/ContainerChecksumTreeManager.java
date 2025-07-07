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
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Striped;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.SimpleStriped;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
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
  private final Striped<Lock> fileLock;
  private final ContainerMerkleTreeMetrics metrics;

  /**
   * Creates one instance that should be used to coordinate all container checksum info within a datanode.
   */
  public ContainerChecksumTreeManager(ConfigurationSource conf) {
    fileLock = SimpleStriped.custom(conf.getObject(DatanodeConfiguration.class).getContainerChecksumLockStripes(),
        () -> new ReentrantLock(true));
    metrics = ContainerMerkleTreeMetrics.create();
  }

  public void stop() {
    ContainerMerkleTreeMetrics.unregister();
  }

  /**
   * Writes the specified container merkle tree to the specified container's checksum file.
   * The data merkle tree within the file is replaced with the {@code tree} parameter, but all other content of the
   * file remains unchanged.
   * Concurrent writes to the same file are coordinated internally.
   */
  public ContainerProtos.ContainerChecksumInfo writeContainerDataTree(ContainerData data,
      ContainerMerkleTreeWriter tree) throws IOException {
    long containerID = data.getContainerID();
    ContainerProtos.ContainerChecksumInfo checksumInfo = null;
    Lock writeLock = getLock(containerID);
    writeLock.lock();
    try {
      ContainerProtos.ContainerChecksumInfo.Builder checksumInfoBuilder = null;
      try {
        // If the file is not present, we will create the data for the first time. This happens under a write lock.
        checksumInfoBuilder = readBuilder(data).orElse(ContainerProtos.ContainerChecksumInfo.newBuilder());
      } catch (IOException ex) {
        LOG.error("Failed to read container checksum tree file for container {}. Creating a new instance.",
            containerID, ex);
        checksumInfoBuilder = ContainerProtos.ContainerChecksumInfo.newBuilder();
      }

      ContainerProtos.ContainerMerkleTree treeProto = captureLatencyNs(metrics.getCreateMerkleTreeLatencyNS(),
          tree::toProto);
      checksumInfoBuilder
          .setContainerID(containerID)
          .setContainerMerkleTree(treeProto);
      checksumInfo = checksumInfoBuilder.build();
      write(data, checksumInfo);
      LOG.debug("Data merkle tree for container {} updated with container checksum {}", containerID,
          checksumToString(treeProto.getDataChecksum()));
    } finally {
      writeLock.unlock();
    }
    return checksumInfo;
  }

  /**
   * Adds the specified blocks to the list of deleted blocks specified in the container's checksum file.
   * All other content of the file remains unchanged.
   * Concurrent writes to the same file are coordinated internally.
   */
  public void markBlocksAsDeleted(KeyValueContainerData data, Collection<Long> deletedBlockIDs) throws IOException {
    long containerID = data.getContainerID();
    Lock writeLock = getLock(containerID);
    writeLock.lock();
    try {
      ContainerProtos.ContainerChecksumInfo.Builder checksumInfoBuilder = null;
      try {
        // If the file is not present, we will create the data for the first time. This happens under a write lock.
        checksumInfoBuilder = readBuilder(data)
            .orElse(ContainerProtos.ContainerChecksumInfo.newBuilder());
      } catch (IOException ex) {
        LOG.error("Failed to read container checksum tree file for container {}. Overwriting it with a new instance.",
            data.getContainerID(), ex);
        checksumInfoBuilder = ContainerProtos.ContainerChecksumInfo.newBuilder();
      }

      // Although the persisted block list should already be sorted, we will sort it here to make sure.
      // This will automatically fix any bugs in the persisted order that may show up.
      // TODO HDDS-13245 this conversion logic will be replaced and block checksums will be populated.
      // Create BlockMerkleTree to wrap each input block ID.
      List<ContainerProtos.BlockMerkleTree> deletedBlocks = deletedBlockIDs.stream()
          .map(blockID ->
              ContainerProtos.BlockMerkleTree.newBuilder().setBlockID(blockID).build())
          .collect(Collectors.toList());
      // Add the original blocks to the list.
      deletedBlocks.addAll(checksumInfoBuilder.getDeletedBlocksList());
      // Sort and deduplicate the list.
      Map<Long, ContainerProtos.BlockMerkleTree> sortedDeletedBlocks = deletedBlocks.stream()
          .collect(Collectors.toMap(ContainerProtos.BlockMerkleTree::getBlockID,
              Function.identity(),
              (a, b) -> a,
              TreeMap::new));

      checksumInfoBuilder
          .setContainerID(containerID)
          .clearDeletedBlocks()
          .addAllDeletedBlocks(sortedDeletedBlocks.values());

      write(data, checksumInfoBuilder.build());
      LOG.debug("Deleted block list for container {} updated with {} new blocks", data.getContainerID(),
          sortedDeletedBlocks.size());
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Compares the checksum info of the container with the peer's checksum info and returns a report of the differences.
   * @param thisChecksumInfo The checksum info of the container on this datanode.
   * @param peerChecksumInfo The checksum info of the container on the peer datanode.
   */
  public ContainerDiffReport diff(ContainerProtos.ContainerChecksumInfo thisChecksumInfo,
                                  ContainerProtos.ContainerChecksumInfo peerChecksumInfo) throws
      StorageContainerException {

    ContainerDiffReport report = new ContainerDiffReport();
    try {
      captureLatencyNs(metrics.getMerkleTreeDiffLatencyNS(), () -> {
        Preconditions.assertNotNull(thisChecksumInfo, "Datanode's checksum info is null.");
        Preconditions.assertNotNull(peerChecksumInfo, "Peer checksum info is null.");
        if (thisChecksumInfo.getContainerID() != peerChecksumInfo.getContainerID()) {
          throw new StorageContainerException("Container ID does not match. Local container ID "
              + thisChecksumInfo.getContainerID() + " , Peer container ID " + peerChecksumInfo.getContainerID(),
              ContainerProtos.Result.CONTAINER_ID_MISMATCH);
        }

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
    Set<Long> thisDeletedBlockSet = getDeletedBlockIDs(thisChecksumInfo);
    Set<Long> peerDeletedBlockSet = getDeletedBlockIDs(peerChecksumInfo);

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
        // Matching block ID; check if the block is deleted and handle the cases;
        // 1) If the block is deleted in both the block merkle tree, We can ignore comparing them.
        // 2) If the block is only deleted in our merkle tree, The BG service should have deleted our
        //    block and the peer's BG service hasn't run yet. We can ignore comparing them.
        // 3) If the block is only deleted in peer merkle tree, we can't reconcile for this block. It might be
        //    deleted by peer's BG service. We can ignore comparing them.
        // TODO: HDDS-11765 - Handle missed block deletions from the deleted block ids.
        if (!thisDeletedBlockSet.contains(thisBlockMerkleTree.getBlockID()) &&
            !peerDeletedBlockSet.contains(thisBlockMerkleTree.getBlockID()) &&
            thisBlockMerkleTree.getDataChecksum() != peerBlockMerkleTree.getDataChecksum()) {
          compareBlockMerkleTree(thisBlockMerkleTree, peerBlockMerkleTree, report);
        }
        thisIdx++;
        peerIdx++;
      } else if (thisBlockMerkleTree.getBlockID() < peerBlockMerkleTree.getBlockID()) {
        // this block merkle tree's block id is smaller. Which means our merkle tree has some blocks which the peer
        // doesn't have. We can skip these, the peer will pick up these block when it reconciles with our merkle tree.
        thisIdx++;
      } else {
        // Peer block's ID is smaller; record missing block if peerDeletedBlockSet doesn't contain the blockId
        // and advance peerIdx
        if (!peerDeletedBlockSet.contains(peerBlockMerkleTree.getBlockID())) {
          report.addMissingBlock(peerBlockMerkleTree);
        }
        peerIdx++;
      }
    }

    // Step 2: Process remaining blocks in the peer list
    while (peerIdx < peerBlockMerkleTreeList.size()) {
      ContainerProtos.BlockMerkleTree peerBlockMerkleTree = peerBlockMerkleTreeList.get(peerIdx);
      if (!peerDeletedBlockSet.contains(peerBlockMerkleTree.getBlockID())) {
        report.addMissingBlock(peerBlockMerkleTree);
      }
      peerIdx++;
    }

    // If we have remaining block in thisMerkleTree, we can skip these blocks. The peers will pick this block from
    // us when they reconcile.
  }

  private void compareBlockMerkleTree(ContainerProtos.BlockMerkleTree thisBlockMerkleTree,
                                      ContainerProtos.BlockMerkleTree peerBlockMerkleTree,
                                      ContainerDiffReport report) {

    List<ContainerProtos.ChunkMerkleTree> thisChunkMerkleTreeList = thisBlockMerkleTree.getChunkMerkleTreeList();
    List<ContainerProtos.ChunkMerkleTree> peerChunkMerkleTreeList = peerBlockMerkleTree.getChunkMerkleTreeList();
    int thisIdx = 0, peerIdx = 0;

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
            !thisChunkMerkleTree.getChecksumMatches() && peerChunkMerkleTree.getChecksumMatches()) {
          report.addCorruptChunk(peerBlockMerkleTree.getBlockID(), peerChunkMerkleTree);
        }
        thisIdx++;
        peerIdx++;
      } else if (thisChunkMerkleTree.getOffset() < peerChunkMerkleTree.getOffset()) {
        // this chunk merkle tree's offset is smaller. Which means our merkle tree has some chunks which the peer
        // doesn't have. We can skip these, the peer will pick up these chunks when it reconciles with our merkle tree.
        thisIdx++;
      } else {
        // Peer chunk's offset is smaller; record missing chunk and advance peerIdx
        report.addMissingChunk(peerBlockMerkleTree.getBlockID(), peerChunkMerkleTree);
        peerIdx++;
      }
    }

    // Step 2: Process remaining chunks in the peer list
    while (peerIdx < peerChunkMerkleTreeList.size()) {
      report.addMissingChunk(peerBlockMerkleTree.getBlockID(), peerChunkMerkleTreeList.get(peerIdx));
      peerIdx++;
    }

    // If we have remaining chunks in thisBlockMerkleTree, we can skip these chunks. The peers will pick these
    // chunks from us when they reconcile.
  }

  public static long getDataChecksum(ContainerProtos.ContainerChecksumInfo checksumInfo) {
    return checksumInfo.getContainerMerkleTree().getDataChecksum();
  }

  /**
   * Returns whether the container checksum tree file for the specified container exists without deserializing it.
   */
  public static boolean hasContainerChecksumFile(ContainerData data) {
    return getContainerChecksumFile(data).exists();
  }

  /**
   * Returns the container checksum tree file for the specified container without deserializing it.
   */
  @VisibleForTesting
  public static File getContainerChecksumFile(ContainerData data) {
    return new File(data.getMetadataPath(), data.getContainerID() + ".tree");
  }

  @VisibleForTesting
  public static File getTmpContainerChecksumFile(ContainerData data) {
    return new File(data.getMetadataPath(), data.getContainerID() + ".tree.tmp");
  }

  private Lock getLock(long containerID) {
    return fileLock.get(containerID);
  }

  /**
   * Callers are not required to hold a lock while calling this since writes are done to a tmp file and atomically
   * swapped into place.
   */
  public Optional<ContainerProtos.ContainerChecksumInfo> read(ContainerData data) throws IOException {
    try {
      return captureLatencyNs(metrics.getReadContainerMerkleTreeLatencyNS(), () -> readChecksumInfo(data));
    } catch (IOException ex) {
      metrics.incrementMerkleTreeReadFailures();
      throw new IOException(ex);
    }
  }

  private Optional<ContainerProtos.ContainerChecksumInfo.Builder> readBuilder(ContainerData data) throws IOException {
    Optional<ContainerProtos.ContainerChecksumInfo> checksumInfo = read(data);
    return checksumInfo.map(ContainerProtos.ContainerChecksumInfo::toBuilder);
  }

  /**
   * Callers should have acquired the write lock before calling this method.
   */
  private void write(ContainerData data, ContainerProtos.ContainerChecksumInfo checksumInfo) throws IOException {
    // Make sure callers filled in required fields before writing.
    Preconditions.assertTrue(checksumInfo.hasContainerID());

    File checksumFile = getContainerChecksumFile(data);
    File tmpChecksumFile = getTmpContainerChecksumFile(data);

    try (OutputStream tmpOutputStream = Files.newOutputStream(tmpChecksumFile.toPath())) {
      // Write to a tmp file and rename it into place.
      captureLatencyNs(metrics.getWriteContainerMerkleTreeLatencyNS(), () -> {
        checksumInfo.writeTo(tmpOutputStream);
        Files.move(tmpChecksumFile.toPath(), checksumFile.toPath(), ATOMIC_MOVE);
      });
    } catch (IOException ex) {
      // If the move failed and left behind the tmp file, the tmp file will be overwritten on the next successful write.
      // Nothing reads directly from the tmp file.
      metrics.incrementMerkleTreeWriteFailures();
      throw new IOException("Error occurred when writing container merkle tree for containerID "
          + data.getContainerID(), ex);
    }
  }

  // TODO HDDS-13245 This method will no longer be required.
  private SortedSet<Long> getDeletedBlockIDs(ContainerProtos.ContainerChecksumInfoOrBuilder checksumInfo) {
    return checksumInfo.getDeletedBlocksList().stream()
        .map(ContainerProtos.BlockMerkleTree::getBlockID)
        .collect(Collectors.toCollection(TreeSet::new));
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
      throw new NoSuchFileException("Checksum file does not exist for container #" + data.getContainerID());
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
  public static Optional<ContainerProtos.ContainerChecksumInfo> readChecksumInfo(ContainerData data)
      throws IOException {
    long containerID = data.getContainerID();
    File checksumFile = getContainerChecksumFile(data);
    try {
      if (!checksumFile.exists()) {
        LOG.debug("No checksum file currently exists for container {} at the path {}", containerID, checksumFile);
        return Optional.empty();
      }
      try (InputStream inStream = Files.newInputStream(checksumFile.toPath())) {
        return Optional.of(ContainerProtos.ContainerChecksumInfo.parseFrom(inStream));
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

  public static boolean checksumFileExist(Container<?> container) {
    File checksumFile = getContainerChecksumFile(container.getContainerData());
    return checksumFile.exists();
  }

}
