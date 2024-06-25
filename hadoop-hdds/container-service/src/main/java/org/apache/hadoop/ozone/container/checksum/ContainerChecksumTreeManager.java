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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.Lock;

import com.google.common.util.concurrent.Striped;
import org.apache.hadoop.hdds.utils.SimpleStriped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class coordinates reading and writing Container checksum information for all containers.
 */
public class ContainerChecksumTreeManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerChecksumTreeManager.class);

  // Used to coordinate reads and writes to each container's checksum file.
  // Each container ID is mapped to a stripe.
  private final Striped<ReadWriteLock> fileLock;

  /**
   * Creates one instance that should be used to coordinate all container checksum info within a datanode.
   */
  public ContainerChecksumTreeManager(DatanodeConfiguration dnConf) {
    fileLock = SimpleStriped.readWriteLock(dnConf.getContainerChecksumLockStripes(), true);
  }

  /**
   * Writes the specified container merkle tree to the specified container's checksum file.
   * The data merkle tree within the file is replaced with the {@code tree} parameter, but all other content of the
   * file remains unchanged.
   * Concurrent writes to the same file are coordinated internally.
   */
  public void writeContainerDataTree(KeyValueContainerData data, ContainerMerkleTree tree) throws IOException {
    Lock writeLock = getWriteLock(data.getContainerID());
    writeLock.lock();
    try {
      ContainerProtos.ContainerChecksumInfo newChecksumInfo = read(data).toBuilder()
          .setDataMerkleTree(tree.toProto())
          .build();
      write(data, newChecksumInfo);
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
  public void markBlocksAsDeleted(KeyValueContainerData data, SortedSet<Long> deletedBlockIDs) throws IOException {
    Lock writeLock = getWriteLock(data.getContainerID());
    writeLock.lock();
    try {
      ContainerProtos.ContainerChecksumInfo.Builder checksumInfoBuilder = read(data).toBuilder();
      // Although the persisted block list should already be sorted, we will sort it here to make sure.
      // This will automatically fix any bugs in the persisted order that may show up.
      SortedSet<Long> sortedDeletedBlockIDs = new TreeSet<>(checksumInfoBuilder.getDeletedBlocksList());
      // Since the provided list of block IDs is already sorted, this is a linear time addition.
      sortedDeletedBlockIDs.addAll(deletedBlockIDs);

      checksumInfoBuilder
          .clearDeletedBlocks()
          .addAllDeletedBlocks(sortedDeletedBlockIDs)
          .build();
      write(data, checksumInfoBuilder.build());
      LOG.debug("Deleted block list for container {} updated", data.getContainerID());
    } finally {
      writeLock.unlock();
    }
  }

  public ContainerDiff diff(KeyValueContainerData thisContainer, File otherContainerTree)
      throws IOException {
    // TODO HDDS-10928 compare the checksum info of the two containers and return a summary.
    //  Callers can act on this summary to repair their container replica using the peer's replica.
    //  This method will use the read lock, which is unused in the current implementation.
    return new ContainerDiff();
  }

  /**
   * Returns the container checksum tree file for the specified container without deserializing it.
   */
  public File getContainerChecksumFile(KeyValueContainerData data) {
    return new File(data.getMetadataPath(), data.getContainerID() + ".tree");
  }

  private Lock getReadLock(long containerID) {
    return fileLock.get(containerID).readLock();
  }

  private Lock getWriteLock(long containerID) {
    return fileLock.get(containerID).writeLock();
  }

  private ContainerProtos.ContainerChecksumInfo read(KeyValueContainerData data) throws IOException {
    long containerID = data.getContainerID();
    Lock readLock = getReadLock(containerID);
    readLock.lock();
    try {
      File checksumFile = getContainerChecksumFile(data);
      // If the checksum file has not been created yet, return an empty instance.
      // Since all writes happen as part of an atomic read-modify-write cycle that requires a write lock, two empty
      // instances for the same container obtained only under the read lock will not conflict.
      if (!checksumFile.exists()) {
        LOG.debug("No checksum file currently exists for container {} at the path {}. Returning an empty instance.",
            containerID, checksumFile);
        return ContainerProtos.ContainerChecksumInfo.newBuilder()
            .setContainerID(containerID)
            .build();
      }
      try (FileInputStream inStream = new FileInputStream(checksumFile)) {
        return ContainerProtos.ContainerChecksumInfo.parseFrom(inStream);
      }
    } finally {
      readLock.unlock();
    }
  }

  private void write(KeyValueContainerData data, ContainerProtos.ContainerChecksumInfo checksumInfo)
      throws IOException {
    Lock writeLock = getWriteLock(data.getContainerID());
    writeLock.lock();
    try (FileOutputStream outStream = new FileOutputStream(getContainerChecksumFile(data))) {
      checksumInfo.writeTo(outStream);
    } finally {
      writeLock.unlock();
    }
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
