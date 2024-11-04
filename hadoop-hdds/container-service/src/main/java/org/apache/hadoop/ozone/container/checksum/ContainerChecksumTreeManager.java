/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.checksum;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Collection;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.Striped;
import org.apache.hadoop.hdds.utils.SimpleStriped;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

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
  public void writeContainerDataTree(ContainerData data, ContainerMerkleTree tree) throws IOException {
    long containerID = data.getContainerID();
    Lock writeLock = getLock(containerID);
    writeLock.lock();
    try {
      ContainerProtos.ContainerChecksumInfo.Builder checksumInfoBuilder = null;
      try {
        // If the file is not present, we will create the data for the first time. This happens under a write lock.
        checksumInfoBuilder = read(data)
            .orElse(ContainerProtos.ContainerChecksumInfo.newBuilder());
      } catch (IOException ex) {
        LOG.error("Failed to read container checksum tree file for container {}. Overwriting it with a new instance.",
            containerID, ex);
        checksumInfoBuilder = ContainerProtos.ContainerChecksumInfo.newBuilder();
      }

      checksumInfoBuilder
          .setContainerID(containerID)
          .setContainerMerkleTree(captureLatencyNs(metrics.getCreateMerkleTreeLatencyNS(), tree::toProto));
      write(data, checksumInfoBuilder.build());
      LOG.debug("Data merkle tree for container {} updated", containerID);
    } finally {
      writeLock.unlock();
    }
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
        checksumInfoBuilder = read(data)
            .orElse(ContainerProtos.ContainerChecksumInfo.newBuilder());
      } catch (IOException ex) {
        LOG.error("Failed to read container checksum tree file for container {}. Overwriting it with a new instance.",
            data.getContainerID(), ex);
        checksumInfoBuilder = ContainerProtos.ContainerChecksumInfo.newBuilder();
      }

      // Although the persisted block list should already be sorted, we will sort it here to make sure.
      // This will automatically fix any bugs in the persisted order that may show up.
      SortedSet<Long> sortedDeletedBlockIDs = new TreeSet<>(checksumInfoBuilder.getDeletedBlocksList());
      sortedDeletedBlockIDs.addAll(deletedBlockIDs);

      checksumInfoBuilder
          .setContainerID(containerID)
          .clearDeletedBlocks()
          .addAllDeletedBlocks(sortedDeletedBlockIDs);
      write(data, checksumInfoBuilder.build());
      LOG.debug("Deleted block list for container {} updated with {} new blocks", data.getContainerID(),
          sortedDeletedBlockIDs.size());
    } finally {
      writeLock.unlock();
    }
  }

  public ContainerDiff diff(KeyValueContainerData thisContainer, ContainerProtos.ContainerChecksumInfo peerChecksumInfo)
      throws IOException {
    Preconditions.assertNotNull(thisContainer, "Container data is null");
    Preconditions.assertNotNull(peerChecksumInfo, "Peer checksum info is null");
    Optional<ContainerProtos.ContainerChecksumInfo.Builder> thisContainerChecksumInfoBuilder =
        read(thisContainer);
    if (!thisContainerChecksumInfoBuilder.isPresent()) {
      throw new IOException("The container #" + thisContainer.getContainerID() + " doesn't have container checksum");
    }

    if (thisContainer.getContainerID() != peerChecksumInfo.getContainerID()) {
      throw new IOException("Container Id does not match");
    }

    ContainerProtos.ContainerChecksumInfo thisChecksumInfo = thisContainerChecksumInfoBuilder.get().build();

    ContainerProtos.ContainerMerkleTree thisMerkleTree = thisChecksumInfo.getContainerMerkleTree();
    ContainerProtos.ContainerMerkleTree peerMerkleTree = peerChecksumInfo.getContainerMerkleTree();

    return compareContainerMerkleTree(thisMerkleTree, peerMerkleTree);
  }

  private ContainerDiff compareContainerMerkleTree(ContainerProtos.ContainerMerkleTree thisMerkleTree,
                                                   ContainerProtos.ContainerMerkleTree peerMerkleTree) {

    ContainerDiff report = new ContainerDiff();
    if (thisMerkleTree.getDataChecksum() == peerMerkleTree.getDataChecksum()) {
      return new ContainerDiff();
    }

    List<ContainerProtos.BlockMerkleTree> thisBlockMerkleTreeList = thisMerkleTree.getBlockMerkleTreeList();
    List<ContainerProtos.BlockMerkleTree> peerBlockMerkleTreeList = peerMerkleTree.getBlockMerkleTreeList();
    // Since we are reconciling our container with a peer, We only need to go through the peer's block list
    int peerIdx = 0, thisIdx = 0;
    while (peerIdx < peerBlockMerkleTreeList.size() || thisIdx < thisBlockMerkleTreeList.size()) {
      ContainerProtos.BlockMerkleTree thisBlockMerkleTree = thisIdx < thisBlockMerkleTreeList.size() ?
          thisBlockMerkleTreeList.get(thisIdx) : null;
      ContainerProtos.BlockMerkleTree peerBlockMerkleTree = peerIdx < peerBlockMerkleTreeList.size() ?
          peerBlockMerkleTreeList.get(peerIdx) : null;

      // We have checked all the peer blocks, So we can return the ContainerDiff report
      if (peerBlockMerkleTree == null) {
        return report;
      }

      // peerBlockMerkleTree is not null, but thisBlockMerkleTree is null. Means we have missing blocks.
      if (thisBlockMerkleTree == null) {
        report.addMissingBlock(peerBlockMerkleTree);
        peerIdx++;
        continue;
      }

      // BlockId matches for both thisBlockMerkleTree and peerBlockMerkleTree
      if (thisBlockMerkleTree.getBlockID() == peerBlockMerkleTree.getBlockID()) {
        if (thisBlockMerkleTree.getBlockChecksum() != peerBlockMerkleTree.getBlockChecksum()) {
          compareBlockMerkleTree(report, thisBlockMerkleTree, peerBlockMerkleTree);
        }
        peerIdx++;
        thisIdx++;
      } else {
        if (peerBlockMerkleTree.getBlockID() > thisBlockMerkleTree.getBlockID()) {
          thisIdx++;
        } else {
          report.addMissingBlock(peerBlockMerkleTree);
          peerIdx++;
        }
      }
    }
    return report;
  }

  private void compareBlockMerkleTree(ContainerDiff report, ContainerProtos.BlockMerkleTree thisBlockMerkleTree,
                                      ContainerProtos.BlockMerkleTree peerBlockMerkleTree) {

    List<ContainerProtos.ChunkMerkleTree> thisChunkMerkleTreeList = thisBlockMerkleTree.getChunkMerkleTreeList();
    List<ContainerProtos.ChunkMerkleTree> peerChunkMerkleTreeList = peerBlockMerkleTree.getChunkMerkleTreeList();

    // Since we are reconciling our container with a peer, We only need to go through the peer's chunk list
    int peerIdx = 0, thisIdx = 0;
    while (peerIdx < peerChunkMerkleTreeList.size() || thisIdx < thisChunkMerkleTreeList.size()) {
      ContainerProtos.ChunkMerkleTree thisChunkMerkleTree = thisIdx < thisChunkMerkleTreeList.size() ?
          thisChunkMerkleTreeList.get(thisIdx) : null;
      ContainerProtos.ChunkMerkleTree peerChunkMerkleTree = peerIdx < peerChunkMerkleTreeList.size() ?
          peerChunkMerkleTreeList.get(peerIdx) : null;

      // We have checked all the peer chunks, So we can return the ContainerDiff report
      if (peerChunkMerkleTree == null) {
        return;
      }

      // peerBlockMerkleTree is not null, but thisBlockMerkleTree is null. Means we have missing blocks.
      if (thisChunkMerkleTree == null) {
        report.addMissingChunk(peerBlockMerkleTree.getBlockID(), peerChunkMerkleTree);
        peerIdx++;
        continue;
      }

      if (thisChunkMerkleTree.getOffset() == peerChunkMerkleTree.getOffset()) {
        // Possible state when this Checksum != peer Checksum:
        // thisTree = Healthy, peerTree = Healthy -> We don't know what is healthy. Skip.
        // thisTree = Unhealthy, peerTree = Healthy -> Add to corrupt chunk.
        // thisTree = Healthy, peerTree = unhealthy -> Do nothing as thisTree is healthy.
        // thisTree = Unhealthy, peerTree = Unhealthy -> Do Nothing as both are corrupt.
        if (thisChunkMerkleTree.getChunkChecksum() != peerChunkMerkleTree.getChunkChecksum() &&
            !thisChunkMerkleTree.getIsHealthy() && peerChunkMerkleTree.getIsHealthy()) {
          report.addCorruptChunks(peerBlockMerkleTree.getBlockID(), peerChunkMerkleTree);
        }
        peerIdx++;
        thisIdx++;
      } else {
        if (peerChunkMerkleTree.getOffset() > thisChunkMerkleTree.getOffset()) {
          thisIdx++;
        } else {
          report.addMissingChunk(peerBlockMerkleTree.getBlockID(), peerChunkMerkleTree);
          peerIdx++;
        }
      }
    }
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
  private Optional<ContainerProtos.ContainerChecksumInfo.Builder> read(ContainerData data) throws IOException {
    long containerID = data.getContainerID();
    File checksumFile = getContainerChecksumFile(data);
    try {
      if (!checksumFile.exists()) {
        LOG.debug("No checksum file currently exists for container {} at the path {}", containerID, checksumFile);
        return Optional.empty();
      }
      try (FileInputStream inStream = new FileInputStream(checksumFile)) {
        return captureLatencyNs(metrics.getReadContainerMerkleTreeLatencyNS(),
            () -> Optional.of(ContainerProtos.ContainerChecksumInfo.parseFrom(inStream).toBuilder()));
      }
    } catch (IOException ex) {
      metrics.incrementMerkleTreeReadFailures();
      throw new IOException("Error occurred when reading container merkle tree for containerID "
              + data.getContainerID() + " at path " + checksumFile, ex);
    }
  }

  /**
   * Callers should have acquired the write lock before calling this method.
   */
  private void write(ContainerData data, ContainerProtos.ContainerChecksumInfo checksumInfo) throws IOException {
    // Make sure callers filled in required fields before writing.
    Preconditions.assertTrue(checksumInfo.hasContainerID());

    File checksumFile = getContainerChecksumFile(data);
    File tmpChecksumFile = getTmpContainerChecksumFile(data);

    try (FileOutputStream tmpOutputStream = new FileOutputStream(tmpChecksumFile)) {
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
    try (FileInputStream inStream = new FileInputStream(checksumFile)) {
      return ByteString.readFrom(inStream);
    }
  }

  @VisibleForTesting
  public ContainerMerkleTreeMetrics getMetrics() {
    return this.metrics;
  }

  public static boolean checksumFileExist(Container container) {
    File checksumFile = getContainerChecksumFile(container.getContainerData());
    return checksumFile.exists();
  }

  /**
   * This class represents the difference between our replica of a container and a peer's replica of a container.
   * It summarizes the operations we need to do to reconcile our replica with the peer replica it was compared to.
   *
   */
  public static class ContainerDiff {
    private final List<ContainerProtos.BlockMerkleTree> missingBlocks;
    private final SortedMap<Long, List<ContainerProtos.ChunkMerkleTree>> missingChunks;
    private final SortedMap<Long, List<ContainerProtos.ChunkMerkleTree>> corruptChunks;

    public ContainerDiff() {
      this.missingBlocks = new ArrayList<>();
      this.missingChunks = new TreeMap<>();
      this.corruptChunks = new TreeMap<>();
    }

    public void addMissingBlock(ContainerProtos.BlockMerkleTree missingBlockMerkleTree) {
      this.missingBlocks.add(missingBlockMerkleTree);
    }

    public void addMissingChunk(long blockId, ContainerProtos.ChunkMerkleTree missingChunkMerkleTree) {
      this.missingChunks.computeIfAbsent(blockId, any -> new ArrayList<>()).add(missingChunkMerkleTree);
    }

    public void addCorruptChunks(long blockId, ContainerProtos.ChunkMerkleTree corruptChunk) {
      this.corruptChunks.computeIfAbsent(blockId, any -> new ArrayList<>()).add(corruptChunk);
    }

    public List<ContainerProtos.BlockMerkleTree> getMissingBlocks() {
      return missingBlocks;
    }

    public SortedMap<Long, List<ContainerProtos.ChunkMerkleTree>> getMissingChunks() {
      return missingChunks;
    }

    public SortedMap<Long, List<ContainerProtos.ChunkMerkleTree>> getCorruptChunks() {
      return corruptChunks;
    }

    public boolean isHealthy() {
      if (missingBlocks.isEmpty() && missingChunks.isEmpty() && corruptChunks.isEmpty()) {
        return true;
      }
      return false;
    }
  }
}
