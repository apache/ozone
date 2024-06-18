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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerChecksumInfo;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.SortedSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.Lock;

import com.google.common.util.concurrent.Striped;
import org.apache.hadoop.hdds.utils.SimpleStriped;

/**
 * This class coordinates reading and writing Container checksum information for all containers.
 */
public class ContainerChecksumManager {

  private Striped<ReadWriteLock> fileLock;

  /**
   * Creates one instance that should be used to coordinate all container checksum info within a datanode.
   */
  public ContainerChecksumManager(DatanodeConfiguration dnConf) {
    fileLock = SimpleStriped.readWriteLock(dnConf.getContainerChecksumLockStripes(), true);
  }

  /**
   * Writes the specified container merkle tree to the specified container's checksum file.
   * The data merkle tree within the file is replaced with the {@code tree} parameter, but all other content of the
   * file remains unchanged.
   * Concurrent writes to the same file are coordinated internally.
   */
  public void writeContainerMerkleTree(KeyValueContainerData data, ContainerMerkleTree tree) throws IOException {
    Lock writeLock = getWriteLock(data.getContainerID());
    writeLock.lock();
    try {
      ContainerChecksumInfo newChecksumInfo = read(data).toBuilder()
          .setDataMerkleTree(tree.toProto())
          .build();
      write(data, newChecksumInfo);
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
      ContainerChecksumInfo newChecksumInfo = read(data).toBuilder()
          // TODO actually need to merge here to keep blocks in sorted order.
          .addAllDeletedBlocks(deletedBlockIDs)
          .build();
      write(data, newChecksumInfo);
    } finally {
      writeLock.unlock();
    }
  }

  public ContainerDiff diff(KeyValueContainerData thisContainer, ContainerChecksumInfo otherInfo) throws IOException {
    // TODO HDDS-10928 compare the checksum info of the two containers and return a summary.
    //  Callers can act on this summary to repair their container replica using the peer's replica.
    return new ContainerDiff();
  }

  private Lock getReadLock(long containerID) {
    return fileLock.get(containerID).readLock();
  }

  private Lock getWriteLock(long containerID) {
    return fileLock.get(containerID).writeLock();
  }

  private ContainerChecksumInfo read(KeyValueContainerData data) throws IOException {
    Lock readLock = getReadLock(data.getContainerID());
    readLock.lock();
    try (FileInputStream inStream = new FileInputStream(getContainerChecksumFile(data))) {
      return ContainerChecksumInfo.parseFrom(inStream);
    } finally {
      readLock.unlock();
    }
  }

  private void write(KeyValueContainerData data, ContainerChecksumInfo checksumInfo) throws IOException {
    Lock writeLock = getWriteLock(data.getContainerID());
    writeLock.lock();
    try (FileOutputStream outStream = new FileOutputStream(getContainerChecksumFile(data))) {
      checksumInfo.writeTo(outStream);
    } finally {
      writeLock.unlock();
    }
  }

  private File getContainerChecksumFile(KeyValueContainerData data) {
    return new File(data.getMetadataPath(), data.getContainerID() + ".checksum");
  }

  /**
   * This class represents the different between our replica of a container, and a peer's replica of a container.
   * It summarizes the operations we need to do to reconcile our replica the peer replica it was compared to.
   *
   * TODO HDDS-10928
   */
  public static class ContainerDiff {
    public ContainerDiff() {

    }
  }
}
