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

package org.apache.hadoop.ozone.container.keyvalue;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSING;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.DELETED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_ALREADY_EXISTS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_FILES_CREATE_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_NOT_OPEN;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.DISK_OUT_OF_SPACE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.ERROR_IN_COMPACT_DB;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.ERROR_IN_DB_SYNC;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.INVALID_CONTAINER_STATE;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.IO_EXCEPTION;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNSUPPORTED_REQUEST;
import static org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil.onFailure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerPacker;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.DataScanResult;
import org.apache.hadoop.ozone.container.ozoneimpl.MetadataScanResult;
import org.apache.hadoop.ozone.container.replication.ContainerImporter;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to perform KeyValue Container operations. Any modifications to
 * KeyValueContainer object should ideally be done via api exposed in
 * KeyValueHandler class.
 */
public class KeyValueContainer implements Container<KeyValueContainerData> {

  private static final Logger LOG =
          LoggerFactory.getLogger(KeyValueContainer.class);

  // Use a non-fair RW lock for better throughput, we may revisit this decision
  // if this causes fairness issues.
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  // Simple lock to synchronize container metadata dump operation.
  private final Object dumpLock = new Object();

  private final KeyValueContainerData containerData;
  private final ConfigurationSource config;

  // Cache of Blocks (LocalIDs) awaiting final PutBlock call after the stream
  // is closed. When a block is added to the DB as part of putBlock, it is
  // added to the cache here. It is cleared from the Cache when the putBlock
  // is called on the block as part of stream.close() (with endOfBlock = true
  // in BlockManagerImpl#putBlock). Or when the container is marked for
  // close, the whole cache is cleared as there can be no more writes to this
  // container.
  // We do not need to explicitly synchronize this cache as the writes to
  // container are synchronous.
  private Set<Long> pendingPutBlockCache;

  private boolean bCheckChunksFilePath;
  private static FaultInjector faultInjector;

  public KeyValueContainer(KeyValueContainerData containerData,
      ConfigurationSource ozoneConfig) {
    Objects.requireNonNull(containerData, "containerData == null");
    Objects.requireNonNull(ozoneConfig, "ozoneConfig == null");
    this.config = ozoneConfig;
    this.containerData = containerData;
    if (this.containerData.isOpen() || this.containerData.isClosing()) {
      // If container is not in OPEN or CLOSING state, there cannot be block
      // writes to the container. So pendingPutBlockCache is not needed.
      this.pendingPutBlockCache = new HashSet<>();
    } else {
      this.pendingPutBlockCache = Collections.emptySet();
    }
    DatanodeConfiguration dnConf =
        config.getObject(DatanodeConfiguration.class);
    bCheckChunksFilePath = dnConf.getCheckEmptyContainerDir();
  }

  @VisibleForTesting
  public void setCheckChunksFilePath(boolean bCheckChunksDirFilePath) {
    this.bCheckChunksFilePath = bCheckChunksDirFilePath;
  }

  @Override
  public void create(VolumeSet volumeSet, VolumeChoosingPolicy
      volumeChoosingPolicy, String clusterId) throws StorageContainerException {
    Objects.requireNonNull(volumeChoosingPolicy, "VolumeChoosingPolicy == null");
    Objects.requireNonNull(volumeSet, "volumeSet == null");
    Objects.requireNonNull(clusterId, "clusterId == null");

    File containerMetaDataPath = null;
    //acquiring volumeset read lock
    long maxSize = containerData.getMaxSize();
    volumeSet.readLock();
    try {
      List<HddsVolume> volumes
          = StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList());
      while (true) {
        HddsVolume containerVolume;
        String hddsVolumeDir;
        try {
          containerVolume = volumeChoosingPolicy.chooseVolume(volumes, maxSize);
          hddsVolumeDir = containerVolume.getHddsRootDir().toString();
          // Set volume before getContainerDBFile(), because we may need the
          // volume to deduce the db file.
          containerData.setVolume(containerVolume);
          // commit bytes have been reserved in volumeChoosingPolicy#chooseVolume
          containerData.setCommittedSpace(true);
        } catch (DiskOutOfSpaceException ex) {
          throw new StorageContainerException("Container creation failed, " +
              "due to disk out of space", ex, DISK_OUT_OF_SPACE);
        } catch (IOException ex) {
          throw new StorageContainerException(
              "Container creation failed. " + ex.getMessage(), ex,
              CONTAINER_INTERNAL_ERROR);
        }

        try {
          long containerID = containerData.getContainerID();
          String idDir = VersionedDatanodeFeatures.ScmHA.chooseContainerPathID(
              containerVolume, clusterId);
          // Set schemaVersion before the dbFile since we have to
          // choose the dbFile location based on schema version.
          String schemaVersion = VersionedDatanodeFeatures.SchemaV3
              .chooseSchemaVersion(config);
          containerData.setSchemaVersion(schemaVersion);

          containerMetaDataPath = KeyValueContainerLocationUtil
              .getContainerMetaDataPath(hddsVolumeDir, idDir, containerID);
          containerData.setMetadataPath(containerMetaDataPath.getPath());

          File chunksPath = KeyValueContainerLocationUtil.getChunksLocationPath(
              hddsVolumeDir, idDir, containerID);

          // Check if it is new Container.
          ContainerUtils.verifyIsNewContainer(containerMetaDataPath);

          //Create Metadata path chunks path and metadata db
          File dbFile = getContainerDBFile();

          createContainerMetaData(containerMetaDataPath, chunksPath, dbFile,
              containerData.getSchemaVersion(), config);

          //Set containerData for the KeyValueContainer.
          containerData.setChunksPath(chunksPath.getPath());
          containerData.setDbFile(dbFile);

          // Create .container file
          File containerFile = getContainerFile();
          createContainerFile(containerFile);
          return;
        } catch (StorageContainerException ex) {
          if (containerMetaDataPath != null
              && containerMetaDataPath.getParentFile().exists()) {
            FileUtil.fullyDelete(containerMetaDataPath.getParentFile());
          }
          throw ex;
        } catch (FileAlreadyExistsException ex) {
          throw new StorageContainerException("Container creation failed " +
              "because ContainerFile already exists", ex,
              CONTAINER_ALREADY_EXISTS);
        } catch (IOException ex) {
          // This is a general catch all - no space left of device, which should
          // not happen as the volume Choosing policy should filter out full
          // disks, but it may still be possible if the disk quickly fills,
          // or some IO error on the disk etc. In this case we try again with a
          // different volume if there are any left to try.
          if (containerMetaDataPath != null &&
              containerMetaDataPath.getParentFile().exists()) {
            FileUtil.fullyDelete(containerMetaDataPath.getParentFile());
          }
          volumes.remove(containerVolume);
          LOG.error("Failed to create {} on volume {}, remaining volumes: {}]",
              containerData, containerVolume.getHddsRootDir(), volumes.size(), ex);
          if (volumes.isEmpty()) {
            throw new StorageContainerException(
                "Failed to create " + containerData + " on all volumes: " + volumeSet.getVolumesList(),
                ex, CONTAINER_INTERNAL_ERROR);
          }
        }
      }
    } finally {
      volumeSet.readUnlock();
    }
  }


  /**
   * The Static method call is wrapped in a protected instance method so it can
   * be overridden in tests.
   */
  @VisibleForTesting
  protected void createContainerMetaData(File containerMetaDataPath,
      File chunksPath, File dbFile, String schemaVersion,
      ConfigurationSource configuration) throws IOException {
    KeyValueContainerUtil.createContainerMetaData(containerMetaDataPath,
        chunksPath, dbFile, schemaVersion, configuration);
  }

  /**
   * Set all of the path realted container data fields based on the name
   * conventions.
   *
   * @param clusterId
   * @param containerVolume
   */
  public void populatePathFields(String clusterId,
      HddsVolume containerVolume) {

    long containerId = containerData.getContainerID();
    String hddsVolumeDir = containerVolume.getHddsRootDir().getAbsolutePath();

    File containerMetaDataPath = KeyValueContainerLocationUtil
        .getContainerMetaDataPath(hddsVolumeDir, clusterId, containerId);

    File chunksPath = KeyValueContainerLocationUtil.getChunksLocationPath(
        hddsVolumeDir, clusterId, containerId);

    //Set containerData for the KeyValueContainer.
    containerData.setMetadataPath(containerMetaDataPath.getPath());
    containerData.setChunksPath(chunksPath.getPath());
    containerData.setVolume(containerVolume);
    containerData.setDbFile(getContainerDBFile());
  }

  /**
   * Writes to .container file.
   *
   * @param containerFile container file name
   * @param isCreate True if creating a new file. False is updating an
   *                 existing container file.
   * @throws StorageContainerException
   */
  private void writeToContainerFile(File containerFile, boolean isCreate)
      throws StorageContainerException {
    File tempContainerFile = null;
    try {
      tempContainerFile = createTempFile(containerFile);
      ContainerDataYaml.createContainerFile(containerData, tempContainerFile);

      // NativeIO.renameTo is an atomic function. But it might fail if the
      // container file already exists. Hence, we handle the two cases
      // separately.
      if (isCreate) {
        NativeIO.renameTo(tempContainerFile, containerFile);
      } else {
        Files.move(tempContainerFile.toPath(), containerFile.toPath(),
            StandardCopyOption.REPLACE_EXISTING);
      }

    } catch (IOException ex) {
      onFailure(containerData.getVolume());
      final String op = tempContainerFile == null ? "create tmp file for "
          : (isCreate ? "create" : "update") + " container file ";
      throw new StorageContainerException(
          "Failed to " + op + containerFile.getAbsolutePath() + ": " + containerData,
          ex, CONTAINER_FILES_CREATE_ERROR);
    } finally {
      if (tempContainerFile != null && tempContainerFile.exists()) {
        if (!tempContainerFile.delete()) {
          LOG.warn("Unable to delete container temporary file: {}.",
              tempContainerFile.getAbsolutePath());
        }
      }
    }
  }

  private void createContainerFile(File containerFile)
      throws StorageContainerException {
    writeToContainerFile(containerFile, true);
  }

  private void updateContainerFile(File containerFile)
      throws StorageContainerException {
    writeToContainerFile(containerFile, false);
  }

  @Override
  public void delete() throws StorageContainerException {
    try {
      // Delete the Container from tmp directory.
      File tmpDirectoryPath = KeyValueContainerUtil.getTmpDirectoryPath(
          containerData, containerData.getVolume()).toFile();
      FileUtils.deleteDirectory(tmpDirectoryPath);
    } catch (StorageContainerException ex) {
      // Disk needs replacement.
      throw ex;
    } catch (IOException ex) {
      // Container will be removed from tmp directory under the volume.
      // On datanode shutdown/restart any partial artifacts left
      // will be wiped from volume's tmp directory.
      onFailure(containerData.getVolume());
      final String errMsg = "Failed to cleanup " + containerData;
      LOG.error(errMsg, ex);
      throw new StorageContainerException(errMsg, ex, CONTAINER_INTERNAL_ERROR);
    }
  }

  @Override
  public boolean hasBlocks() throws IOException {
    try (DBHandle db = BlockUtils.getDB(containerData, config)) {
      return !KeyValueContainerUtil.noBlocksInContainer(db.getStore(),
          containerData, bCheckChunksFilePath);
    }
  }

  @Override
  public void markContainerForClose() throws StorageContainerException {
    writeLock();
    try {
      if (!HddsUtils.isOpenToWriteState(getContainerState())) {
        throw new StorageContainerException(
            "Attempting to close a " + getContainerState() + " container.",
            CONTAINER_NOT_OPEN);
      }
      updateContainerState(CLOSING);
      // Do not clear the pendingBlockCache here as a follower can still
      // receive transactions from leader in CLOSING state. Refer to
      // KeyValueHandler#checkContainerOpen()
    } finally {
      writeUnlock();
    }
  }

  @Override
  public void markContainerUnhealthy() throws StorageContainerException {
    writeLock();
    final State prevState = containerData.getState();
    try {
      updateContainerState(UNHEALTHY);
      clearPendingPutBlockCache();
    } finally {
      writeUnlock();
    }
    LOG.warn("Marked container UNHEALTHY from {}: {}", prevState, containerData);
  }

  @Override
  public void markContainerForDelete() {
    writeLock();
    final State prevState = containerData.getState();
    try {
      containerData.setState(DELETED);
      // update the new container data to .container File
      updateContainerFile(getContainerFile());
    } catch (IOException ioe) {
      LOG.error("Failed to updateContainerFile: {}", containerData, ioe);
    } finally {
      writeUnlock();
    }
    LOG.warn("Marked container DELETED from {}: {}", prevState, containerData);
  }

  @Override
  public void quasiClose() throws StorageContainerException {
    closeAndFlushIfNeeded(containerData::quasiCloseContainer);
  }

  @Override
  public void close() throws StorageContainerException {
    try (DBHandle db = BlockUtils.getDB(containerData, config)) {
      containerData.clearFinalizedBlock(db);
    } catch (IOException ex) {
      LOG.error("Error in deleting entry from Finalize Block table", ex);
      throw new StorageContainerException(ex, IO_EXCEPTION);
    }
    closeAndFlushIfNeeded(containerData::closeContainer);
    LOG.info("Closed container: {}", containerData);
  }

  @Override
  public void updateDataScanTimestamp(Instant time)
      throws StorageContainerException {
    writeLock();
    try {
      updateContainerData(() -> containerData.updateDataScanTime(time));
    } finally {
      writeUnlock();
    }
  }

  /**
   * Sync RocksDB WAL on closing of a single container.
   *
   * @param closer
   * @throws StorageContainerException
   */
  private void closeAndFlushIfNeeded(Runnable closer)
      throws StorageContainerException {
    flushAndSyncDB();

    writeLock();
    try {
      // Second sync should be a very light operation as sync has already
      // been done outside the lock.
      flushAndSyncDB();
      updateContainerData(closer);
      clearPendingPutBlockCache();
    } finally {
      writeUnlock();
    }
  }

  private void updateContainerState(State newState) throws StorageContainerException {
    updateContainerData(() -> containerData.setState(newState));
  }

  /**
   *
   * Must be invoked with the writeLock held.
   *
   * @param update
   * @throws StorageContainerException
   */
  private void updateContainerData(Runnable update)
      throws StorageContainerException {
    Preconditions.checkState(hasWriteLock());
    final State oldState = containerData.getState();
    try {
      update.run();
      // update the new container data to .container File
      updateContainerFile(getContainerFile());

    } catch (StorageContainerException ex) {
      if (containerData.getState() != UNHEALTHY) {
        // Failed to update .container file. Reset the state to old state only
        // if the current state is not unhealthy.
        containerData.setState(oldState);
      }
      throw ex;
    }
  }

  private void compactDB() throws StorageContainerException {
    try {
      try (DBHandle db = BlockUtils.getDB(containerData, config)) {
        db.getStore().compactDB();
      }
    } catch (StorageContainerException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error("Error in DB compaction while closing container", ex);
      onFailure(containerData.getVolume());
      throw new StorageContainerException(ex, ERROR_IN_COMPACT_DB);
    }
  }

  private void flushAndSyncDB() throws StorageContainerException {
    try {
      try (DBHandle db = BlockUtils.getDB(containerData, config)) {
        db.getStore().flushLog(true);
        LOG.info("Synced container: {}", containerData);
      }
    } catch (StorageContainerException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error("Error in DB sync while closing container", ex);
      onFailure(containerData.getVolume());
      throw new StorageContainerException(ex, ERROR_IN_DB_SYNC);
    }
  }

  @Override
  public KeyValueContainerData getContainerData()  {
    return containerData;
  }

  @Override
  public ContainerProtos.ContainerDataProto.State getContainerState() {
    return containerData.getState();
  }

  @Override
  public ContainerType getContainerType() {
    return ContainerType.KeyValueContainer;
  }

  @Override
  public void update(Map<String, String> metadata, boolean forceUpdate)
      throws StorageContainerException {
    update(metadata, forceUpdate, containerData.getMetadataPath());
  }

  @Override
  public void update(Map<String, String> metadata, boolean forceUpdate, String containerMetadataPath)
      throws StorageContainerException {
    // TODO: Now, when writing the updated data to .container file, we are
    //  holding lock and writing data to disk. We can have async implementation
    //  to flush the update container data to disk.
    if (!containerData.isValid()) {
      throw new StorageContainerException("Invalid container data: " + containerData, INVALID_CONTAINER_STATE);
    }
    if (!forceUpdate && !containerData.isOpen()) {
      throw new StorageContainerException(
          "Updating a closed container without force option is disallowed: " + containerData, UNSUPPORTED_REQUEST);
    }

    Map<String, String> oldMetadata = containerData.getMetadata();
    try {
      writeLock();
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        containerData.addMetadata(entry.getKey(), entry.getValue());
      }

      File containerFile = getContainerFile(containerMetadataPath, containerData.getContainerID());
      // update the new container data to .container File
      updateContainerFile(containerFile);
    } catch (StorageContainerException  ex) {
      containerData.setMetadata(oldMetadata);
      throw ex;
    } finally {
      writeUnlock();
    }
  }

  @Override
  public void updateDeleteTransactionId(long deleteTransactionId) {
    containerData.updateDeleteTransactionId(deleteTransactionId);
  }

  @Override
  public void importContainerData(InputStream input,
      ContainerPacker<KeyValueContainerData> packer)
      throws IOException {
    HddsVolume hddsVolume = containerData.getVolume();
    String idDir = VersionedDatanodeFeatures.ScmHA.chooseContainerPathID(
        hddsVolume, hddsVolume.getClusterID());
    long containerId = containerData.getContainerID();
    Path destContainerDir =
        Paths.get(KeyValueContainerLocationUtil.getBaseContainerLocation(
            hddsVolume.getHddsRootDir().toString(), idDir, containerId));
    Path tmpDir = ContainerImporter.getUntarDirectory(hddsVolume);
    writeLock();
    try {
      //copy the values from the input stream to the final destination
      // directory.
      byte[] descriptorContent = packer.unpackContainerData(this, input, tmpDir,
          destContainerDir);

      Objects.requireNonNull(descriptorContent,
          () -> "Missing container descriptor from the archive: " + getContainerData());

      //now, we have extracted the container descriptor from the previous
      //datanode. We can load it and upload it with the current data
      // (original metadata + current filepath fields)
      KeyValueContainerData originalContainerData =
          (KeyValueContainerData) ContainerDataYaml
              .readContainer(descriptorContent);
      importContainerData(originalContainerData);
    } catch (Exception ex) {
      // clean data under tmp directory
      try {
        Path containerUntarDir = tmpDir.resolve(String.valueOf(containerId));
        if (containerUntarDir.toFile().exists()) {
          FileUtils.deleteDirectory(containerUntarDir.toFile());
        }
      } catch (Exception deleteex) {
        LOG.error(
            "Can not cleanup container directory under {} for container {}",
            tmpDir, containerId, deleteex);
      }

      // Throw exception for existed containers
      if (ex instanceof StorageContainerException &&
          ((StorageContainerException) ex).getResult() ==
              CONTAINER_ALREADY_EXISTS) {
        throw ex;
      }

      // delete all other temporary data in case of any exception.
      cleanupFailedImport();
      throw ex;
    } finally {
      writeUnlock();
    }
  }

  public void importContainerData(KeyValueContainerData originalContainerData)
      throws IOException {
    containerData
        .setContainerDBType(originalContainerData.getContainerDBType());
    containerData.setSchemaVersion(originalContainerData.getSchemaVersion());

    if (containerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
      // load metadata from received dump files before we try to parse kv
      BlockUtils.loadKVContainerDataFromFiles(containerData, config);
    }

    //fill in memory stat counter (keycount, byte usage)
    KeyValueContainerUtil.parseKVContainerData(containerData, config, true);

    // rewriting the yaml file with new checksum calculation
    // restore imported container's state to the original state and flush the yaml file
    containerData.setState(originalContainerData.getState());
    update(originalContainerData.getMetadata(), true);
  }

  private void cleanupFailedImport() {
    try {
      if (containerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
        BlockUtils.removeContainerFromDB(containerData, config);
      }
      FileUtils.deleteDirectory(new File(containerData.getMetadataPath()));
      FileUtils.deleteDirectory(new File(containerData.getChunksPath()));
      FileUtils.deleteDirectory(new File(getContainerData().getContainerPath()));
    } catch (Exception ex) {
      LOG.error("Failed to cleanup destination directories for container {}",
          containerData.getContainerID(), ex);
    }
  }

  @Override
  public void exportContainerData(OutputStream destination,
      ContainerPacker<KeyValueContainerData> packer) throws IOException {
    writeLock();
    try {
      // Closed/ Quasi closed and unhealthy containers are considered for
      // replication by replication manager if they are under-replicated.
      final State state = getContainerData().getState();
      // Only CLOSED, QUASI_CLOSED and UNHEALTHY containers can be exported.
      if (state != CLOSED && state != QUASI_CLOSED && state != UNHEALTHY) {
        throw new IllegalStateException("Failed to export: Unexpected state in " + getContainerData());
      }

      try {
        if (!containerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
          compactDB();
          // Close DB (and remove from cache) to avoid concurrent modification
          // while packing it.
          BlockUtils.removeDB(containerData, config);
        }
      } finally {
        readLock();
        writeUnlock();
      }

      packContainerToDestination(destination, packer);
    } finally {
      if (lock.isWriteLockedByCurrentThread()) {
        writeUnlock();
      } else {
        readUnlock();
      }
    }
  }

  /**
   * Acquire read lock.
   */
  @Override
  public void readLock() {
    this.lock.readLock().lock();

  }

  /**
   * Release read lock.
   */
  @Override
  public void readUnlock() {
    this.lock.readLock().unlock();
  }

  /**
   * Check if the current thread holds read lock.
   */
  @Override
  public boolean hasReadLock() {
    return this.lock.readLock().tryLock();
  }

  /**
   * Acquire write lock.
   */
  @Override
  public void writeLock() {
    // TODO: The lock for KeyValueContainer object should not be exposed
    // publicly.
    this.lock.writeLock().lock();
  }

  /**
   * Release write lock.
   */
  @Override
  public void writeUnlock() {
    this.lock.writeLock().unlock();

  }

  /**
   * Check if the current thread holds write lock.
   */
  @Override
  public boolean hasWriteLock() {
    return this.lock.writeLock().isHeldByCurrentThread();
  }

  /**
   * Acquire read lock, unless interrupted while waiting.
   * @throws InterruptedException
   */
  @Override
  public void readLockInterruptibly() throws InterruptedException {
    this.lock.readLock().lockInterruptibly();
  }

  /**
   * Acquire write lock, unless interrupted while waiting.
   * @throws InterruptedException
   */
  @Override
  public void writeLockInterruptibly() throws InterruptedException {
    this.lock.writeLock().lockInterruptibly();

  }

  public boolean writeLockTryLock(long time, TimeUnit unit)
      throws InterruptedException {
    return this.lock.writeLock().tryLock(time, unit);
  }

  /**
   * Returns containerFile.
   * @return .container File name
   */
  @Override
  public File getContainerFile() {
    return getContainerFile(containerData.getMetadataPath(),
            containerData.getContainerID());
  }

  public static File getContainerFile(String metadataPath, long containerId) {
    return new File(metadataPath,
        containerId + OzoneConsts.CONTAINER_EXTENSION);
  }

  @Override
  public void updateBlockCommitSequenceId(long blockCommitSequenceId) {
    containerData.updateBlockCommitSequenceId(blockCommitSequenceId);
  }

  @Override
  public long getBlockCommitSequenceId() {
    return containerData.getBlockCommitSequenceId();
  }

  /**
   * Return whether the given localID of a block is present in the
   * pendingPutBlockCache or not.
   */
  public boolean isBlockInPendingPutBlockCache(long localID) {
    return pendingPutBlockCache.contains(localID);
  }

  /**
   * Add the given localID of a block to the pendingPutBlockCache.
   */
  public void addToPendingPutBlockCache(long localID)
      throws StorageContainerException {
    try {
      pendingPutBlockCache.add(localID);
    } catch (UnsupportedOperationException e) {
      // Getting an UnsupportedOperationException here implies that the
      // pendingPutBlockCache is an Empty Set. This should not happen if the
      // container is in OPEN or CLOSING state. Log the exception here and
      // throw a non-Runtime exception so that putBlock request fails.
      String msg = "Failed to add block " + localID + " to pendingPutBlockCache for " + containerData;
      LOG.error(msg, e);
      throw new StorageContainerException(msg, CONTAINER_INTERNAL_ERROR);
    }
  }

  /**
   * Remove the given localID of a block from the pendingPutBlockCache.
   */
  public void removeFromPendingPutBlockCache(long localID) {
    pendingPutBlockCache.remove(localID);
  }

  /**
   * When a container is closed, quasi-closed or marked unhealthy, clear the
   * pendingPutBlockCache as there won't be any more writes to the container.
   */
  private void clearPendingPutBlockCache() {
    pendingPutBlockCache.clear();
    pendingPutBlockCache = Collections.emptySet();
  }

  /**
   * Returns KeyValueContainerReport for the KeyValueContainer.
   */
  @Override
  public ContainerReplicaProto getContainerReport() throws StorageContainerException {
    return containerData.buildContainerReplicaProto();
  }

  /**
   * Returns container DB file.
   */
  public File getContainerDBFile() {
    return KeyValueContainerLocationUtil.getContainerDBFile(containerData);

  }

  @Override
  public MetadataScanResult scanMetaData() throws InterruptedException {
    KeyValueContainerCheck checker =
        new KeyValueContainerCheck(config, this);
    return checker.fastCheck();
  }

  @Override
  public boolean shouldScanData() {
    boolean shouldScan =
        getContainerState() == CLOSED
        || getContainerState() == QUASI_CLOSED
        || getContainerState() == UNHEALTHY;
    if (!shouldScan && LOG.isDebugEnabled()) {
      LOG.debug("Container {} in state {} should not have its data scanned.",
          containerData.getContainerID(), containerData.getState());
    }

    return shouldScan;
  }

  @Override
  public DataScanResult scanData(DataTransferThrottler throttler, Canceler canceler)
      throws InterruptedException {
    if (!shouldScanData()) {
      throw new IllegalStateException("The checksum verification can not be" +
          " done for container in state "
          + containerData.getState());
    }

    KeyValueContainerCheck checker = new KeyValueContainerCheck(config, this);
    return checker.fullCheck(throttler, canceler);
  }

  @Override
  public void copyContainerDirectory(Path destination) throws IOException {
    readLock();
    try {
      // Closed/ Quasi closed containers are considered for replication by
      // replication manager if they are under-replicated.
      ContainerProtos.ContainerDataProto.State state =
          getContainerData().getState();
      if (!(state == ContainerProtos.ContainerDataProto.State.CLOSED ||
          state == ContainerProtos.ContainerDataProto.State.QUASI_CLOSED)) {
        throw new IllegalStateException(
            "Only (quasi)closed containers can be exported, but " +
                "ContainerId=" + getContainerData().getContainerID() +
                " is in state " + state);
      }

      if (!containerData.getSchemaVersion().equals(OzoneConsts.SCHEMA_V3)) {
        compactDB();
        // Close DB (and remove from cache) to avoid concurrent modification
        // while copying it.
        BlockUtils.removeDB(containerData, config);
      }

      if (containerData.getSchemaVersion().equals(OzoneConsts.SCHEMA_V3)) {
        // Synchronize the dump and copy operation,
        // so concurrent copy don't get dump files overwritten.
        // We seldom got concurrent exports for a container,
        // so it should not influence performance much.
        synchronized (dumpLock) {
          BlockUtils.dumpKVContainerDataToFiles(containerData, config);
          copyContainerToDestination(destination);
        }
      } else {
        copyContainerToDestination(destination);
      }
      if (getInjector() != null && getInjector().getException() != null) {
        throw new IOException("Fault injection", getInjector().getException());
      }
    } catch (IOException e) {
      LOG.error("Got exception when copying container {} to {}",
          containerData.getContainerID(), destination, e);
      throw e;
    } finally {
      readUnlock();
    }
  }

  @VisibleForTesting
  public static FaultInjector getInjector() {
    return faultInjector;
  }

  @VisibleForTesting
  public static void setInjector(FaultInjector instance) {
    faultInjector = instance;
  }

  /**
   * Set all of the path realted container data fields based on the name
   * conventions.
   *
   */
  public void populatePathFields(HddsVolume volume, Path containerPath) {
    containerData.setMetadataPath(
        KeyValueContainerLocationUtil.getContainerMetaDataPath(
            containerPath.toString()).toString());
    containerData.setChunksPath(
        KeyValueContainerLocationUtil.getChunksLocationPath(
            containerPath.toString()).toString()
    );
    containerData.setVolume(volume);
    containerData.setDbFile(getContainerDBFile());
  }

  private enum ContainerCheckLevel {
    NO_CHECK, FAST_CHECK, FULL_CHECK
  }

  /**
   * Creates a temporary file.
   * @param file
   * @return
   * @throws IOException
   */
  private File createTempFile(File file) throws IOException {
    return File.createTempFile("tmp_" + System.currentTimeMillis() + "_",
        file.getName(), file.getParentFile());
  }

  private void packContainerToDestination(OutputStream destination,
      ContainerPacker<KeyValueContainerData> packer)
      throws IOException {
    if (containerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
      // Synchronize the dump and pack operation,
      // so concurrent exports don't get dump files overwritten.
      // We seldom got concurrent exports for a container,
      // so it should not influence performance much.
      synchronized (dumpLock) {
        BlockUtils.dumpKVContainerDataToFiles(containerData, config);
        packer.pack(this, destination);
      }
    } else {
      packer.pack(this, destination);
    }
  }

  /**
   * Copy container directory to destination path.
   * @param destination destination path
   * @throws IOException file operation exception
   */
  private void copyContainerToDestination(Path destination)
      throws IOException {
    try {
      if (Files.exists(destination)) {
        FileUtils.deleteDirectory(destination.toFile());
      }
      FileUtils.copyDirectory(new File(containerData.getContainerPath()),
          destination.toFile());

    } catch (IOException e) {
      LOG.error("Failed when copying container to {}", destination, e);
      throw e;
    }
  }
}
