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
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.Lock;

import com.google.common.util.concurrent.Striped;
import org.apache.hadoop.hdds.utils.SimpleStriped;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

/**
 * This class coordinates reading and writing Container checksum information for all containers.
 */
public class ContainerChecksumTreeManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerChecksumTreeManager.class);

  // Used to coordinate reads and writes to each container's checksum file.
  // Each container ID is mapped to a stripe.
  private final Striped<ReadWriteLock> fileLock;
  private final ContainerMerkleTreeMetrics metrics;

  /**
   * Creates one instance that should be used to coordinate all container checksum info within a datanode.
   */
  public ContainerChecksumTreeManager(ConfigurationSource conf) {
    fileLock = SimpleStriped.readWriteLock(
        conf.getObject(DatanodeConfiguration.class).getContainerChecksumLockStripes(), true);
    // TODO: TO unregister metrics on stop.
    metrics = ContainerMerkleTreeMetrics.create();
  }

  /**
   * Writes the specified container merkle tree to the specified container's checksum file.
   * The data merkle tree within the file is replaced with the {@code tree} parameter, but all other content of the
   * file remains unchanged.
   * Concurrent writes to the same file are coordinated internally.
   */
  public void writeContainerDataTree(ContainerData data, ContainerMerkleTree tree) throws IOException {
    Lock writeLock = getWriteLock(data.getContainerID());
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

      checksumInfoBuilder
          .setContainerMerkleTree(captureLatencyNs(metrics.getCreateMerkleTreeLatencyNS(), tree::toProto));
      write(data, checksumInfoBuilder.build());
      LOG.debug("Data merkle tree for container {} updated", data.getContainerID());
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
    Lock writeLock = getWriteLock(data.getContainerID());
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
          .clearDeletedBlocks()
          .addAllDeletedBlocks(sortedDeletedBlockIDs);
      write(data, checksumInfoBuilder.build());
      LOG.debug("Deleted block list for container {} updated", data.getContainerID());
    } finally {
      writeLock.unlock();
    }
  }

  public ContainerDiff diff(KeyValueContainerData thisContainer, ContainerProtos.ContainerChecksumInfo otherInfo)
      throws IOException {
    // TODO HDDS-10928 compare the checksum info of the two containers and return a summary.
    //  Callers can act on this summary to repair their container replica using the peer's replica.
    //  This method will use the read lock, which is unused in the current implementation.
    return new ContainerDiff();
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

  private Lock getReadLock(long containerID) {
    return fileLock.get(containerID).readLock();
  }

  private Lock getWriteLock(long containerID) {
    return fileLock.get(containerID).writeLock();
  }

  private Optional<ContainerProtos.ContainerChecksumInfo.Builder> read(ContainerData data) throws IOException {
    long containerID = data.getContainerID();
    File checksumFile = getContainerChecksumFile(data);
    Lock readLock = getReadLock(containerID);
    readLock.lock();
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
    } finally {
      readLock.unlock();
    }
  }

  private void write(ContainerData data, ContainerProtos.ContainerChecksumInfo checksumInfo) throws IOException {
    File checksumFile = getContainerChecksumFile(data);
    File tmpChecksumFile = getTmpContainerChecksumFile(data);

    Lock writeLock = getWriteLock(data.getContainerID());
    writeLock.lock();
    try (FileOutputStream tmpOutputStream = new FileOutputStream(tmpChecksumFile)) {
      // Write to a tmp file and rename it into place.
      captureLatencyNs(metrics.getWriteContainerMerkleTreeLatencyNS(), () -> {
        checksumInfo.writeTo(tmpOutputStream);
        Files.move(tmpChecksumFile.toPath(), checksumFile.toPath(), REPLACE_EXISTING);
        // Best effort cleanup of the tmp file. If this fails it will be overwritten on the next write.
        // No operations should read from the tmp file.
        Files.deleteIfExists(tmpChecksumFile.toPath());
      });
    } catch (IOException ex) {
      metrics.incrementMerkleTreeWriteFailures();
      throw new IOException("Error occurred when writing container merkle tree for containerID "
          + data.getContainerID(), ex);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Reads the container checksum info file from the disk as bytes.
   *
   * @throws FileNotFoundException When the file does not exist. It may not have been generated yet for this container.
   * @throws IOException On error reading the file.
   */
  public ByteString getContainerChecksumInfo(KeyValueContainerData data) throws IOException {
    long containerID = data.getContainerID();
    Lock readLock = getReadLock(containerID);
    readLock.lock();
    try {
      File checksumFile = getContainerChecksumFile(data);
      try (FileInputStream inStream = new FileInputStream(checksumFile)) {
        return ByteString.readFrom(inStream);
      }
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  public ContainerMerkleTreeMetrics getMetrics() {
    return this.metrics;
  }

  /**
   * This class represents the difference between our replica of a container and a peer's replica of a container.
   * It summarizes the operations we need to do to reconcile our replica with the peer replica it was compared to.
   *
   * TODO HDDS-10928
   */
  public static class ContainerDiff {
    public ContainerDiff() {

    }
  }
}
