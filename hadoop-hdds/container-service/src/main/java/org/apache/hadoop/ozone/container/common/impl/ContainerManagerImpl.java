/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerLifeCycleState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMNodeReport;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMStorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.helpers.KeyUtils;
import org.apache.hadoop.ozone.container.common.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces
    .ContainerDeletionChoosingPolicy;
import org.apache.hadoop.ozone.container.common.interfaces
    .ContainerLocationManager;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.container.common.interfaces
    .ContainerReportManager;
import org.apache.hadoop.ozone.container.common.interfaces.KeyManager;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.utils.MetadataKeyFilters;
import org.apache.hadoop.utils.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_EXISTS;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.CONTAINER_NOT_FOUND;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.ERROR_IN_COMPACT_DB;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.INVALID_CONFIG;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.IO_EXCEPTION;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.NO_SUCH_ALGORITHM;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.UNABLE_TO_READ_METADATA_DB;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.UNCLOSED_CONTAINER_IO;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .Result.UNSUPPORTED_REQUEST;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    Result.INVALID_CONTAINER_STATE;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_EXTENSION;

/**
 * A Generic ContainerManagerImpl that will be called from Ozone
 * ContainerManagerImpl. This allows us to support delta changes to ozone
 * version without having to rewrite the containerManager.
 */
public class ContainerManagerImpl implements ContainerManager {
  static final Logger LOG =
      LoggerFactory.getLogger(ContainerManagerImpl.class);

  // TODO: consider primitive collection like eclipse-collections
  // to avoid autoboxing overhead
  private final ConcurrentSkipListMap<Long, ContainerData>
      containerMap = new ConcurrentSkipListMap<>();

  // Use a non-fair RW lock for better throughput, we may revisit this decision
  // if this causes fairness issues.
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private ContainerLocationManager locationManager;
  private ChunkManager chunkManager;
  private KeyManager keyManager;
  private Configuration conf;
  private DatanodeDetails datanodeDetails;

  private ContainerDeletionChoosingPolicy containerDeletionChooser;
  private ContainerReportManager containerReportManager;

  /**
   * Init call that sets up a container Manager.
   *
   * @param config - Configuration.
   * @param containerDirs - List of Metadata Container locations.
   * @param dnDetails - DatanodeDetails.
   * @throws IOException
   */
  @Override
  public void init(
      Configuration config, List<StorageLocation> containerDirs,
      DatanodeDetails dnDetails) throws IOException {
    Preconditions.checkNotNull(config, "Config must not be null");
    Preconditions.checkNotNull(containerDirs, "Container directories cannot " +
        "be null");
    Preconditions.checkNotNull(dnDetails, "Datanode Details cannot " +
        "be null");

    Preconditions.checkState(containerDirs.size() > 0, "Number of container" +
        " directories must be greater than zero.");

    this.conf = config;
    this.datanodeDetails = dnDetails;

    readLock();
    try {
      containerDeletionChooser = ReflectionUtils.newInstance(conf.getClass(
          ScmConfigKeys.OZONE_SCM_CONTAINER_DELETION_CHOOSING_POLICY,
          TopNOrderedContainerDeletionChoosingPolicy.class,
          ContainerDeletionChoosingPolicy.class), conf);

      for (StorageLocation path : containerDirs) {
        File directory = Paths.get(path.getNormalizedUri()).toFile();
        if (!directory.exists() && !directory.mkdirs()) {
          LOG.error("Container metadata directory doesn't exist "
              + "and cannot be created. Path: {}", path.toString());
          throw new StorageContainerException("Container metadata "
              + "directory doesn't exist and cannot be created " + path
              .toString(), INVALID_CONFIG);
        }

        // TODO: This will fail if any directory is invalid.
        // We should fix this to handle invalid directories and continue.
        // Leaving it this way to fail fast for time being.
        if (!directory.isDirectory()) {
          LOG.error("Invalid path to container metadata directory. path: {}",
              path.toString());
          throw new StorageContainerException("Invalid path to container " +
              "metadata directory." + path, INVALID_CONFIG);
        }
        LOG.info("Loading containers under {}", path);
        File[] files = directory.listFiles(new ContainerFilter());
        if (files != null) {
          for (File containerFile : files) {
            LOG.debug("Loading container {}", containerFile);
            String containerPath =
                ContainerUtils.getContainerNameFromFile(containerFile);
            Preconditions.checkNotNull(containerPath, "Container path cannot" +
                " be null");
            readContainerInfo(containerPath);
          }
        }
      }

      List<StorageLocation> dataDirs = new LinkedList<>();
      for (String dir : config.getStrings(DFS_DATANODE_DATA_DIR_KEY)) {
        StorageLocation location = StorageLocation.parse(dir);
        dataDirs.add(location);
      }
      this.locationManager =
          new ContainerLocationManagerImpl(containerDirs, dataDirs, config);

      this.containerReportManager =
          new ContainerReportManagerImpl(config);
    } finally {
      readUnlock();
    }
  }

  /**
   * Reads the Container Info from a file and verifies that checksum match. If
   * the checksums match, then that file is added to containerMap.
   *
   * @param containerName - Name which points to the persisted container.
   * @throws StorageContainerException
   */
  private void readContainerInfo(String containerName)
      throws StorageContainerException {
    Preconditions.checkState(containerName.length() > 0,
        "Container name length cannot be zero.");
    FileInputStream containerStream = null;
    DigestInputStream dis = null;
    FileInputStream metaStream = null;
    Path cPath = Paths.get(containerName).getFileName();
    String keyName = null;
    if (cPath != null) {
      keyName = cPath.toString();
    }
    Preconditions.checkNotNull(keyName,
        "Container Name  to container key mapping is null");

    long containerID = Long.parseLong(keyName);
    try {
      String containerFileName = containerName.concat(CONTAINER_EXTENSION);

      containerStream = new FileInputStream(containerFileName);

      ContainerProtos.ContainerData containerDataProto =
          ContainerProtos.ContainerData.parseDelimitedFrom(containerStream);
      ContainerData containerData;
      if (containerDataProto == null) {
        // Sometimes container metadata might have been created but empty,
        // when loading the info we get a null, this often means last time
        // SCM was ending up at some middle phase causing that the metadata
        // was not populated. Such containers are marked as inactive.
        ContainerData cData = new ContainerData(containerID, conf,
            ContainerLifeCycleState.INVALID);
        containerMap.put(containerID, cData);
        return;
      }
      containerData = ContainerData.getFromProtBuf(containerDataProto, conf);

      // Initialize pending deletion blocks count in in-memory
      // container status.
      MetadataStore metadata = KeyUtils.getDB(containerData, conf);
      List<Map.Entry<byte[], byte[]>> underDeletionBlocks = metadata
          .getSequentialRangeKVs(null, Integer.MAX_VALUE,
              MetadataKeyFilters.getDeletingKeyFilter());
      containerData.incrPendingDeletionBlocks(underDeletionBlocks.size());

      List<Map.Entry<byte[], byte[]>> liveKeys = metadata
          .getRangeKVs(null, Integer.MAX_VALUE,
              MetadataKeyFilters.getNormalKeyFilter());

      // Get container bytesUsed upon loading container
      // The in-memory state is updated upon key write or delete
      // TODO: update containerDataProto and persist it into container MetaFile
      long bytesUsed = 0;
      bytesUsed = liveKeys.parallelStream().mapToLong(e-> {
        KeyData keyData;
        try {
          keyData = KeyUtils.getKeyData(e.getValue());
          return keyData.getSize();
        } catch (IOException ex) {
          return 0L;
        }
      }).sum();
      containerData.setBytesUsed(bytesUsed);

      containerMap.put(containerID, containerData);
    } catch (IOException ex) {
      LOG.error("read failed for file: {} ex: {}", containerName,
          ex.getMessage());

      // TODO : Add this file to a recovery Queue.

      // Remember that this container is busted and we cannot use it.
      ContainerData cData = new ContainerData(containerID, conf,
          ContainerLifeCycleState.INVALID);
      containerMap.put(containerID, cData);
      throw new StorageContainerException("Unable to read container info",
          UNABLE_TO_READ_METADATA_DB);
    } finally {
      IOUtils.closeStream(dis);
      IOUtils.closeStream(containerStream);
      IOUtils.closeStream(metaStream);
    }
  }

  /**
   * Creates a container with the given name.
   *
   * @param containerData - Container Name and metadata.
   * @throws StorageContainerException - Exception
   */
  @Override
  public void createContainer(ContainerData containerData)
      throws StorageContainerException {
    Preconditions.checkNotNull(containerData, "Container data cannot be null");
    writeLock();
    try {
      if (containerMap.containsKey(containerData.getContainerID())) {
        LOG.debug("container already exists. {}", containerData.getContainerID());
        throw new StorageContainerException("container already exists.",
            CONTAINER_EXISTS);
      }

      // This is by design. We first write and close the
      // container Info and metadata to a directory.
      // Then read back and put that info into the containerMap.
      // This allows us to make sure that our write is consistent.

      writeContainerInfo(containerData, false);
      File cFile = new File(containerData.getContainerPath());
      readContainerInfo(ContainerUtils.getContainerNameFromFile(cFile));
    } catch (NoSuchAlgorithmException ex) {
      LOG.error("Internal error: We seem to be running a JVM without a " +
          "needed hash algorithm.");
      throw new StorageContainerException("failed to create container",
          NO_SUCH_ALGORITHM);
    } finally {
      writeUnlock();
    }

  }

  /**
   * Writes a container to a chosen location and updates the container Map.
   *
   * The file formats of ContainerData and Container Meta is the following.
   *
   * message ContainerData {
   * required string name = 1;
   * repeated KeyValue metadata = 2;
   * optional string dbPath = 3;
   * optional string containerPath = 4;
   * optional int64 bytesUsed = 5;
   * optional int64 size = 6;
   * }
   *
   * message ContainerMeta {
   * required string fileName = 1;
   * required string hash = 2;
   * }
   *
   * @param containerData - container Data
   * @param overwrite - Whether we are overwriting.
   * @throws StorageContainerException, NoSuchAlgorithmException
   */
  private void writeContainerInfo(ContainerData containerData,
      boolean  overwrite)
      throws StorageContainerException, NoSuchAlgorithmException {

    Preconditions.checkNotNull(this.locationManager,
        "Internal error: location manager cannot be null");

    FileOutputStream containerStream = null;
    DigestOutputStream dos = null;
    FileOutputStream metaStream = null;

    try {
      Path metadataPath = null;
      Path location = (!overwrite) ? locationManager.getContainerPath():
          Paths.get(containerData.getContainerPath()).getParent();
      if (location == null) {
        throw new StorageContainerException(
            "Failed to get container file path.",
            CONTAINER_INTERNAL_ERROR);
      }

      File containerFile = ContainerUtils.getContainerFile(containerData,
          location);
      String containerName = Long.toString(containerData.getContainerID());

      if(!overwrite) {
        ContainerUtils.verifyIsNewContainer(containerFile);
        metadataPath = this.locationManager.getDataPath(containerName);
        metadataPath = ContainerUtils.createMetadata(metadataPath,
            containerName, conf);
      }  else {
        metadataPath = ContainerUtils.getMetadataDirectory(containerData);
      }

      containerStream = new FileOutputStream(containerFile);

      MessageDigest sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);

      dos = new DigestOutputStream(containerStream, sha);
      containerData.setDBPath(metadataPath.resolve(
          ContainerUtils.getContainerDbFileName(containerName))
          .toString());
      containerData.setContainerPath(containerFile.toString());

      if(containerData.getContainerDBType() == null) {
        String impl = conf.getTrimmed(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL,
            OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_DEFAULT);
        containerData.setContainerDBType(impl);
      }

      ContainerProtos.ContainerData protoData = containerData
          .getProtoBufMessage();
      protoData.writeDelimitedTo(dos);

    } catch (IOException ex) {
      // TODO : we need to clean up partially constructed files
      // The proper way to do would be for a thread
      // to read all these 3 artifacts and make sure they are
      // sane. That info needs to come from the replication
      // pipeline, and if not consistent delete these file.

      // In case of ozone this is *not* a deal breaker since
      // SCM is guaranteed to generate unique container names.
      // The saving grace is that we check if we have residue files
      // lying around when creating a new container. We need to queue
      // this information to a cleaner thread.

      LOG.error("Creation of container failed. Name: {}, we might need to " +
              "cleanup partially created artifacts. ",
          containerData.getContainerID(), ex);
      throw new StorageContainerException("Container creation failed. ",
          ex, CONTAINER_INTERNAL_ERROR);
    } finally {
      IOUtils.closeStream(dos);
      IOUtils.closeStream(containerStream);
      IOUtils.closeStream(metaStream);
    }
  }

  /**
   * Deletes an existing container.
   *
   * @param containerID - ID of the container.
   * @param forceDelete - whether this container should be deleted forcibly.
   * @throws StorageContainerException
   */
  @Override
  public void deleteContainer(long containerID,
      boolean forceDelete) throws StorageContainerException {
    Preconditions.checkState(containerID >= 0,
        "Container ID cannot be negative.");
    writeLock();
    try {
      if (isOpen(containerID)) {
        throw new StorageContainerException(
            "Deleting an open container is not allowed.",
            UNCLOSED_CONTAINER_IO);
      }

      ContainerData containerData = containerMap.get(containerID);
      if (containerData == null) {
        LOG.debug("No such container. ID: {}", containerID);
        throw new StorageContainerException("No such container. ID : " +
            containerID, CONTAINER_NOT_FOUND);
      }

      if(!containerData.isValid()) {
        LOG.debug("Invalid container data. ID: {}", containerID);
        throw new StorageContainerException("Invalid container data. Name : " +
            containerID, CONTAINER_NOT_FOUND);
      }
      ContainerUtils.removeContainer(containerData, conf, forceDelete);
      containerMap.remove(containerID);
    } catch (StorageContainerException e) {
      throw e;
    } catch (IOException e) {
      // TODO : An I/O error during delete can leave partial artifacts on the
      // disk. We will need the cleaner thread to cleanup this information.
      String errMsg = String.format("Failed to cleanup container. ID: %d",
          containerID);
      LOG.error(errMsg, e);
      throw new StorageContainerException(errMsg, e, IO_EXCEPTION);
    } finally {
      writeUnlock();
    }
  }

  /**
   * A simple interface for container Iterations.
   * <p/>
   * This call make no guarantees about consistency of the data between
   * different list calls. It just returns the best known data at that point of
   * time. It is possible that using this iteration you can miss certain
   * container from the listing.
   *
   * @param startContainerID -  Return containers with ID >= startContainerID.
   * @param count - how many to return
   * @param data - Actual containerData
   * @throws StorageContainerException
   */
  @Override
  public void listContainer(long startContainerID, long count,
      List<ContainerData> data) throws StorageContainerException {
    Preconditions.checkNotNull(data,
        "Internal assertion: data cannot be null");
    Preconditions.checkState(startContainerID >= 0,
        "Start container ID cannot be negative");
    Preconditions.checkState(count > 0,
        "max number of containers returned " +
            "must be positive");

    readLock();
    try {
      ConcurrentNavigableMap<Long, ContainerData> map;
      if (startContainerID == 0) {
        map = containerMap.tailMap(containerMap.firstKey(), true);
      } else {
        map = containerMap.tailMap(startContainerID, false);
      }

      int currentCount = 0;
      for (ContainerData entry : map.values()) {
        if (currentCount < count) {
          data.add(entry);
          currentCount++;
        } else {
          return;
        }
      }
    } finally {
      readUnlock();
    }
  }

  /**
   * Get metadata about a specific container.
   *
   * @param containerID - ID of the container
   * @return ContainerData - Container Data.
   * @throws StorageContainerException
   */
  @Override
  public ContainerData readContainer(long containerID)
      throws StorageContainerException {
    Preconditions.checkState(containerID >= 0,
        "Container ID cannot be negative.");
    if (!containerMap.containsKey(containerID)) {
      throw new StorageContainerException("Unable to find the container. ID: "
          + containerID, CONTAINER_NOT_FOUND);
    }
    ContainerData cData = containerMap.get(containerID);
    if (cData == null) {
      throw new StorageContainerException("Invalid container data. ID: "
          + containerID, CONTAINER_INTERNAL_ERROR);
    }
    return cData;
  }

  /**
   * Closes a open container, if it is already closed or does not exist a
   * StorageContainerException is thrown.
   *
   * @param containerID - ID of the container.
   * @throws StorageContainerException
   */
  @Override
  public void closeContainer(long containerID)
      throws StorageContainerException, NoSuchAlgorithmException {
    ContainerData containerData = readContainer(containerID);
    containerData.closeContainer();
    writeContainerInfo(containerData, true);
    MetadataStore db = KeyUtils.getDB(containerData, conf);

    // It is ok if this operation takes a bit of time.
    // Close container is not expected to be instantaneous.
    try {
      db.compactDB();
    } catch (IOException e) {
      LOG.error("Error in DB compaction while closing container", e);
      throw new StorageContainerException(e, ERROR_IN_COMPACT_DB);
    }

    // Active is different from closed. Closed means it is immutable, active
    // false means we have some internal error that is happening to this
    // container. This is a way to track damaged containers if we have an
    // I/O failure, this allows us to take quick action in case of container
    // issues.

    containerMap.put(containerID, containerData);
  }

  @Override
  public void updateContainer(long containerID, ContainerData data,
      boolean forceUpdate) throws StorageContainerException {
    Preconditions.checkState(containerID >= 0, "Container ID cannot be negative.");
    Preconditions.checkNotNull(data, "Container data cannot be null");
    FileOutputStream containerStream = null;
    DigestOutputStream dos = null;
    MessageDigest sha = null;
    File containerFileBK = null, containerFile = null;
    boolean deleted = false;

    if(!containerMap.containsKey(containerID)) {
      throw new StorageContainerException("Container doesn't exist. Name :"
          + containerID, CONTAINER_NOT_FOUND);
    }

    try {
      sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    } catch (NoSuchAlgorithmException e) {
      throw new StorageContainerException("Unable to create Message Digest,"
          + " usually this is a java configuration issue.",
          NO_SUCH_ALGORITHM);
    }

    try {
      Path location = locationManager.getContainerPath();
      ContainerData orgData = containerMap.get(containerID);
      if (orgData == null) {
        // updating a invalid container
        throw new StorageContainerException("Update a container with invalid" +
            "container meta data", CONTAINER_INTERNAL_ERROR);
      }

      if (!forceUpdate && !orgData.isOpen()) {
        throw new StorageContainerException(
            "Update a closed container is not allowed. ID: " + containerID,
            UNSUPPORTED_REQUEST);
      }

      containerFile = ContainerUtils.getContainerFile(orgData, location);
      // If forceUpdate is true, there is no need to check
      // whether the container file exists.
      if (!forceUpdate) {
        if (!containerFile.exists() || !containerFile.canWrite()) {
          throw new StorageContainerException(
              "Container file not exists or corrupted. ID: " + containerID,
              CONTAINER_INTERNAL_ERROR);
        }

        // Backup the container file
        containerFileBK = File.createTempFile(
            "tmp_" + System.currentTimeMillis() + "_",
            containerFile.getName(), containerFile.getParentFile());
        FileUtils.copyFile(containerFile, containerFileBK);

        deleted = containerFile.delete();
        containerStream = new FileOutputStream(containerFile);
        dos = new DigestOutputStream(containerStream, sha);

        ContainerProtos.ContainerData protoData = data.getProtoBufMessage();
        protoData.writeDelimitedTo(dos);
      }

      // Update the in-memory map
      containerMap.replace(containerID, data);
    } catch (IOException e) {
      // Restore the container file from backup
      if(containerFileBK != null && containerFileBK.exists() && deleted) {
        if(containerFile.delete()
            && containerFileBK.renameTo(containerFile)) {
          throw new StorageContainerException("Container update failed,"
              + " container data restored from the backup.",
              CONTAINER_INTERNAL_ERROR);
        } else {
          throw new StorageContainerException(
              "Failed to restore container data from the backup. ID: "
                  + containerID, CONTAINER_INTERNAL_ERROR);
        }
      } else {
        throw new StorageContainerException(
            e.getMessage(), CONTAINER_INTERNAL_ERROR);
      }
    } finally {
      if (containerFileBK != null && containerFileBK.exists()) {
        if(!containerFileBK.delete()) {
          LOG.warn("Unable to delete container file backup : {}.",
              containerFileBK.getAbsolutePath());
        }
      }
      IOUtils.closeStream(dos);
      IOUtils.closeStream(containerStream);
    }
  }

  @VisibleForTesting
  protected File getContainerFile(ContainerData data) throws IOException {
    return ContainerUtils.getContainerFile(data,
        this.locationManager.getContainerPath());
  }

  /**
   * Checks if a container exists.
   *
   * @param containerID - ID of the container.
   * @return true if the container is open false otherwise.
   * @throws StorageContainerException - Throws Exception if we are not able to
   *                                   find the container.
   */
  @Override
  public boolean isOpen(long containerID) throws StorageContainerException {
    final ContainerData containerData = containerMap.get(containerID);
    if (containerData == null) {
      throw new StorageContainerException(
          "Container not found: " + containerID, CONTAINER_NOT_FOUND);
    }
    return containerData.isOpen();
  }

  /**
   * Returns LifeCycle State of the container
   * @param containerID - Id of the container
   * @return LifeCycle State of the container
   * @throws StorageContainerException
   */
  private HddsProtos.LifeCycleState getState(long containerID)
      throws StorageContainerException {
    LifeCycleState state;
    final ContainerData data = containerMap.get(containerID);
    if (data == null) {
      throw new StorageContainerException(
          "Container status not found: " + containerID, CONTAINER_NOT_FOUND);
    }
    switch (data.getState()) {
    case OPEN:
      state = LifeCycleState.OPEN;
      break;
    case CLOSING:
      state = LifeCycleState.CLOSING;
      break;
    case CLOSED:
      state = LifeCycleState.CLOSED;
      break;
    default:
      throw new StorageContainerException(
          "Invalid Container state found: " + containerID,
          INVALID_CONTAINER_STATE);
    }

    return state;
  }

  /**
   * Supports clean shutdown of container.
   *
   * @throws IOException
   */
  @Override
  public void shutdown() throws IOException {
    Preconditions.checkState(this.hasWriteLock(),
        "Assumption that we are holding the lock violated.");
    this.containerMap.clear();
    this.locationManager.shutdown();
  }


  @VisibleForTesting
  public ConcurrentSkipListMap<Long, ContainerData> getContainerMap() {
    return containerMap;
  }

  /**
   * Acquire read lock.
   */
  @Override
  public void readLock() {
    this.lock.readLock().lock();

  }

  @Override
  public void readLockInterruptibly() throws InterruptedException {
    this.lock.readLock().lockInterruptibly();
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
    this.lock.writeLock().lock();
  }

  /**
   * Acquire write lock, unless interrupted while waiting.
   */
  @Override
  public void writeLockInterruptibly() throws InterruptedException {
    this.lock.writeLock().lockInterruptibly();

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

  public ChunkManager getChunkManager() {
    return this.chunkManager;
  }

  /**
   * Sets the chunk Manager.
   *
   * @param chunkManager - Chunk Manager
   */
  public void setChunkManager(ChunkManager chunkManager) {
    this.chunkManager = chunkManager;
  }

  /**
   * Gets the Key Manager.
   *
   * @return KeyManager.
   */
  @Override
  public KeyManager getKeyManager() {
    return this.keyManager;
  }

  /**
   * Get the node report.
   * @return node report.
   */
  @Override
  public SCMNodeReport getNodeReport() throws IOException {
    StorageLocationReport[] reports = locationManager.getLocationReport();
    SCMNodeReport.Builder nrb = SCMNodeReport.newBuilder();
    for (int i = 0; i < reports.length; i++) {
      SCMStorageReport.Builder srb = SCMStorageReport.newBuilder();
      nrb.addStorageReport(reports[i].getProtoBufMessage());
    }
    return nrb.build();
  }


  /**
   * Gets container reports.
   *
   * @return List of all closed containers.
   * @throws IOException
   */
  @Override
  public List<ContainerData> getClosedContainerReports() throws IOException {
    LOG.debug("Starting container report iteration.");
    // No need for locking since containerMap is a ConcurrentSkipListMap
    // And we can never get the exact state since close might happen
    // after we iterate a point.
    return containerMap.entrySet().stream()
        .filter(containerData ->
            containerData.getValue().isClosed())
        .map(containerData -> containerData.getValue())
        .collect(Collectors.toList());
  }

  /**
   * Get container report.
   *
   * @return The container report.
   * @throws IOException
   */
  @Override
  public ContainerReportsRequestProto getContainerReport() throws IOException {
    LOG.debug("Starting container report iteration.");
    // No need for locking since containerMap is a ConcurrentSkipListMap
    // And we can never get the exact state since close might happen
    // after we iterate a point.
    List<ContainerData> containers = containerMap.values().stream()
        .collect(Collectors.toList());

    ContainerReportsRequestProto.Builder crBuilder =
        ContainerReportsRequestProto.newBuilder();

    // TODO: support delta based container report
    crBuilder.setDatanodeDetails(datanodeDetails.getProtoBufMessage())
        .setType(ContainerReportsRequestProto.reportType.fullReport);

    for (ContainerData container: containers) {
      long containerId = container.getContainerID();
      StorageContainerDatanodeProtocolProtos.ContainerInfo.Builder ciBuilder =
          StorageContainerDatanodeProtocolProtos.ContainerInfo.newBuilder();
      ciBuilder.setContainerID(container.getContainerID())
          .setSize(container.getMaxSize())
          .setUsed(container.getBytesUsed())
          .setKeyCount(container.getKeyCount())
          .setReadCount(container.getReadCount())
          .setWriteCount(container.getWriteCount())
          .setReadBytes(container.getReadBytes())
          .setWriteBytes(container.getWriteBytes())
          .setState(getState(containerId));

      crBuilder.addReports(ciBuilder.build());
    }

    return crBuilder.build();
  }

  /**
   * Sets the Key Manager.
   *
   * @param keyManager - Key Manager.
   */
  @Override
  public void setKeyManager(KeyManager keyManager) {
    this.keyManager = keyManager;
  }

  /**
   * Filter out only container files from the container metadata dir.
   */
  private static class ContainerFilter implements FilenameFilter {
    /**
     * Tests if a specified file should be included in a file list.
     *
     * @param dir the directory in which the file was found.
     * @param name the name of the file.
     * @return <code>true</code> if and only if the name should be included in
     * the file list; <code>false</code> otherwise.
     */
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(CONTAINER_EXTENSION);
    }
  }

  @Override
  public List<ContainerData> chooseContainerForBlockDeletion(
      int count) throws StorageContainerException {
    readLock();
    try {
      return containerDeletionChooser.chooseContainerForBlockDeletion(
          count, containerMap);
    } finally {
      readUnlock();
    }
  }

  @VisibleForTesting
  public ContainerDeletionChoosingPolicy getContainerDeletionChooser() {
    return containerDeletionChooser;
  }

  @Override
  public void incrPendingDeletionBlocks(int numBlocks, long containerId) {
    writeLock();
    try {
      ContainerData cData = containerMap.get(containerId);
      cData.incrPendingDeletionBlocks(numBlocks);
    } finally {
      writeUnlock();
    }
  }

  @Override
  public void decrPendingDeletionBlocks(int numBlocks, long containerId) {
    writeLock();
    try {
      ContainerData cData = containerMap.get(containerId);
      cData.decrPendingDeletionBlocks(numBlocks);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Increase the read count of the container.
   *
   * @param containerId - ID of the container.
   */
  @Override
  public void incrReadCount(long containerId) {
    ContainerData cData = containerMap.get(containerId);
    cData.incrReadCount();
  }

  public long getReadCount(long containerId) {
    ContainerData cData = containerMap.get(containerId);
    return cData.getReadCount();
  }

  /**
   * Increase the read counter for bytes read from the container.
   *
   * @param containerId - ID of the container.
   * @param readBytes     - bytes read from the container.
   */
  @Override
  public void incrReadBytes(long containerId, long readBytes) {
    ContainerData cData = containerMap.get(containerId);
    cData.incrReadBytes(readBytes);
  }

  /**
   * Returns number of bytes read from the container.
   * @param containerId
   * @return
   */
  public long getReadBytes(long containerId) {
    readLock();
    try {
      ContainerData cData = containerMap.get(containerId);
      return cData.getReadBytes();
    } finally {
      readUnlock();
    }
  }

  /**
   * Increase the write count of the container.
   *
   * @param containerId - Name of the container.
   */
  @Override
  public void incrWriteCount(long containerId) {
    ContainerData cData = containerMap.get(containerId);
    cData.incrWriteCount();
  }

  public long getWriteCount(long containerId) {
    ContainerData cData = containerMap.get(containerId);
    return cData.getWriteCount();
  }

  /**
   * Increse the write counter for bytes write into the container.
   *
   * @param containerId   - ID of the container.
   * @param writeBytes    - bytes write into the container.
   */
  @Override
  public void incrWriteBytes(long containerId, long writeBytes) {
    ContainerData cData = containerMap.get(containerId);
    cData.incrWriteBytes(writeBytes);
  }

  public long getWriteBytes(long containerId) {
    ContainerData cData = containerMap.get(containerId);
    return cData.getWriteBytes();
  }

  /**
   * Increase the bytes used by the container.
   *
   * @param containerId   - ID of the container.
   * @param used          - additional bytes used by the container.
   * @return the current bytes used.
   */
  @Override
  public long incrBytesUsed(long containerId, long used) {
    ContainerData cData = containerMap.get(containerId);
    return cData.incrBytesUsed(used);
  }

  /**
   * Decrease the bytes used by the container.
   *
   * @param containerId   - ID of the container.
   * @param used          - additional bytes reclaimed by the container.
   * @return the current bytes used.
   */
  @Override
  public long decrBytesUsed(long containerId, long used) {
    ContainerData cData = containerMap.get(containerId);
    return cData.decrBytesUsed(used);
  }

  public long getBytesUsed(long containerId) {
    ContainerData cData = containerMap.get(containerId);
    return cData.getBytesUsed();
  }

  /**
   * Get the number of keys in the container.
   *
   * @param containerId - ID of the container.
   * @return the current key count.
   */
  @Override
  public long getNumKeys(long containerId) {
    ContainerData cData = containerMap.get(containerId);
    return cData.getKeyCount();
  }


}
