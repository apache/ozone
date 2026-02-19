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

package org.apache.hadoop.ozone.container.common.interfaces;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.ozoneimpl.DataScanResult;
import org.apache.hadoop.ozone.container.ozoneimpl.MetadataScanResult;

/**
 * Interface for Container Operations.
 */
public interface Container<CONTAINERDATA extends ContainerData> {

  /**
   * Creates a container.
   *
   * @throws StorageContainerException
   */
  void create(VolumeSet volumeSet, VolumeChoosingPolicy volumeChoosingPolicy,
              String scmId) throws StorageContainerException;

  /**
   * Deletes the container.
   *
   * @throws StorageContainerException
   */
  void delete() throws StorageContainerException;

  /**
   * Returns true if container has some block.
   * @return true if container has some block.
   * @throws IOException if was unable to check container status.
   */
  boolean hasBlocks() throws IOException;

  /**
   * Update the container.
   *
   * @param metaData
   * @param forceUpdate if true, update container forcibly.
   * @throws StorageContainerException
   */
  void update(Map<String, String> metaData, boolean forceUpdate)
      throws StorageContainerException;

  void update(Map<String, String> metaData, boolean forceUpdate, String containerMetadataPath)
      throws StorageContainerException;

  void updateDataScanTimestamp(Instant timestamp)
      throws StorageContainerException;

  /**
   * Get metadata about the container.
   *
   * @return ContainerData - Container Data.
   */
  CONTAINERDATA getContainerData();

  /**
   * Get the Container Lifecycle state.
   *
   * @return ContainerLifeCycleState - Container State.
   */
  ContainerProtos.ContainerDataProto.State getContainerState();

  /**
   * Marks the container for closing. Moves the container to CLOSING state.
   */
  void markContainerForClose() throws StorageContainerException;

  /**
   * Marks the container replica as unhealthy.
   */
  void markContainerUnhealthy() throws StorageContainerException;

  /**
   * Marks the container replica as deleted.
   */
  void markContainerForDelete();

  /**
   * Quasi Closes a open container, if it is already closed or does not exist a
   * StorageContainerException is thrown.
   *
   * @throws StorageContainerException
   */
  void quasiClose() throws StorageContainerException;

  /**
   * Closes a open/quasi closed container, if it is already closed or does not
   * exist a StorageContainerException is thrown.
   *
   * @throws StorageContainerException
   */
  void close() throws StorageContainerException;

  /**
   * Return the ContainerType for the container.
   */
  ContainerProtos.ContainerType getContainerType();

  /**
   * Returns containerFile.
   */
  File getContainerFile();

  /**
   * updates the DeleteTransactionId.
   * @param deleteTransactionId
   */
  void updateDeleteTransactionId(long deleteTransactionId);

  /**
   * Import the container from an external archive.
   */
  void importContainerData(InputStream stream,
      ContainerPacker<CONTAINERDATA> packer) throws IOException;

  /**
   * Export all the data of the container to one output archive with the help
   * of the packer.
   *
   */
  void exportContainerData(OutputStream stream,
      ContainerPacker<CONTAINERDATA> packer) throws IOException;

  /**
   * Returns containerReport for the container.
   */
  ContainerReplicaProto getContainerReport()
      throws StorageContainerException;

  /**
   * updates the blockCommitSequenceId.
   */
  void updateBlockCommitSequenceId(long blockCommitSequenceId);

  /**
   * Returns the blockCommitSequenceId.
   */
  long getBlockCommitSequenceId();

  /**
   * check and report the structural integrity of the container.
   * @return A {@link MetadataScanResult} encapsulating the result of the scan.
   * @throws InterruptedException if the scanning thread is interrupted before it completes.
   */
  MetadataScanResult scanMetaData() throws InterruptedException;

  /**
   * Return if the container data should be checksum verified to detect
   * corruption. The result depends upon the current state of the container
   * (e.g. if a container is accepting writes, it may not be a good idea to
   * perform checksum verification to avoid concurrency issues).
   */
  boolean shouldScanData();

  /**
   * Perform checksum verification for the container data.
   *
   * @param throttler       A reference of {@link DataTransferThrottler} used to
   *                        perform I/O bandwidth throttling
   * @param canceler        A reference of {@link Canceler} used to cancel the
   *                        I/O bandwidth throttling (e.g. for shutdown purpose).
   * @return A {@link DataScanResult} encapsulating the result of the scan.
   * @throws InterruptedException if the scanning thread is interrupted before it completes.
   */
  DataScanResult scanData(DataTransferThrottler throttler, Canceler canceler)
      throws InterruptedException;

  /**
   * Copy all the data of the container to the destination path.
   */
  void copyContainerDirectory(Path destPath) throws IOException;

  /** Acquire read lock. */
  void readLock();

  /** Acquire read lock, unless interrupted while waiting. */
  void readLockInterruptibly() throws InterruptedException;

  /** Release read lock. */
  void readUnlock();

  /** Check if the current thread holds read lock. */
  boolean hasReadLock();

  /** Acquire write lock. */
  void writeLock();

  /** Acquire write lock, unless interrupted while waiting. */
  void writeLockInterruptibly() throws InterruptedException;

  /** Release write lock. */
  void writeUnlock();

  /** Check if the current thread holds write lock. */
  boolean hasWriteLock();
}
