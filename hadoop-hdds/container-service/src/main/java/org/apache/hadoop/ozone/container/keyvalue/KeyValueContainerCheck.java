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

package org.apache.hadoop.ozone.container.keyvalue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTree;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScanError;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScanError.FailureType;
import org.apache.hadoop.ozone.container.ozoneimpl.DataScanResult;
import org.apache.hadoop.ozone.container.ozoneimpl.MetadataScanResult;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_TYPE_ROCKSDB;

/**
 * Class to run integrity checks on Datanode Containers.
 * Provide infra for Data Scrubbing
 */

public class KeyValueContainerCheck {

  private static final Logger LOG =
      LoggerFactory.getLogger(KeyValueContainerCheck.class);

  private long containerID;
  private KeyValueContainerData onDiskContainerData; //loaded from fs/disk
  private ConfigurationSource checkConfig;

  private String metadataPath;
  private HddsVolume volume;
  private KeyValueContainer container;
  private static final DirectBufferPool BUFFER_POOL = new DirectBufferPool();

  public KeyValueContainerCheck(String metadataPath, ConfigurationSource conf,
      long containerID, HddsVolume volume, KeyValueContainer container) {
    Preconditions.checkArgument(metadataPath != null);

    this.checkConfig = conf;
    this.containerID = containerID;
    this.onDiskContainerData = null;
    this.metadataPath = metadataPath;
    this.volume = volume;
    this.container = container;
  }

  /**
   * Run basic integrity checks on container metadata.
   * These checks do not look inside the metadata files.
   * Applicable for OPEN containers.
   *
   * @return true : integrity checks pass, false : otherwise.
   */
  public MetadataScanResult fastCheck() throws InterruptedException {
    LOG.debug("Running metadata checks for container {}", containerID);
    List<ContainerScanError> metadataErrors = new ArrayList<>();

    try {
      // Container directory should exist.
      // If it does not, we cannot continue the scan.
      File containerDir = new File(metadataPath).getParentFile();
      if (!containerDir.exists()) {
        metadataErrors.add(new ContainerScanError(FailureType.MISSING_CONTAINER_DIR,
            containerDir, new FileNotFoundException("Container directory " + containerDir + " not found.")));
        return MetadataScanResult.fromErrors(metadataErrors);
      }

      // Chunks directory should exist.
      // The metadata scan can continue even if this fails, since it does not look at the data inside the chunks
      // directory.
      File chunksDir = new File(onDiskContainerData.getChunksPath());
      if (!chunksDir.exists()) {
        metadataErrors.add(new ContainerScanError(FailureType.MISSING_CHUNKS_DIR, chunksDir,
            new FileNotFoundException("Chunks directory " + chunksDir + " not found.")));
      }

      // Metadata directory within the container directory should exist.
      // If it does not, no further scanning can be done.
      File metadataDir = new File(metadataPath);
      if (!metadataDir.exists()) {
        metadataErrors.add(new ContainerScanError(FailureType.MISSING_METADATA_DIR, metadataDir,
            new FileNotFoundException("Metadata directory " + metadataDir + " not found.")));
        return MetadataScanResult.fromErrors(metadataErrors);
      }

      // Container file should be valid.
      // If it is not, no further scanning can be done.
      File containerFile = KeyValueContainer.getContainerFile(metadataPath, containerID);
      try {
        loadContainerData(containerFile);
      } catch (FileNotFoundException ex) {
        metadataErrors.add(new ContainerScanError(FailureType.MISSING_CONTAINER_FILE, containerFile, ex));
        return MetadataScanResult.fromErrors(metadataErrors);
      } catch (IOException ex) {
        metadataErrors.add(new ContainerScanError(FailureType.CORRUPT_CONTAINER_FILE, containerFile, ex));
        return MetadataScanResult.fromErrors(metadataErrors);
      }

      metadataErrors.addAll(checkContainerFile(containerFile));

      try (DBHandle db = BlockUtils.getDB(onDiskContainerData, checkConfig)) {
        if (containerIsDeleted(db)) {
          return MetadataScanResult.deleted();
        }
      } catch (IOException ex) {
        metadataErrors.add(new ContainerScanError(FailureType.INACCESSIBLE_DB, onDiskContainerData.getDbFile(), ex));
      }
      return MetadataScanResult.fromErrors(metadataErrors);
    } finally {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException("Metadata scan of container " +
            containerID + " interrupted.");
      }
    }
  }

  /**
   * full checks comprise scanning all metadata inside the container.
   * Including the KV database. These checks are intrusive, consume more
   * resources compared to fast checks and should only be done on Closed
   * or Quasi-closed Containers. Concurrency being limited to delete
   * workflows.
   * <p>
   * fullCheck is a superset of fastCheck
   *
   * @return true : integrity checks pass, false : otherwise.
   */
  public DataScanResult fullCheck(DataTransferThrottler throttler,
      Canceler canceler) throws InterruptedException {
    // If the metadata check fails, we cannot do the data check. 
    // The DataScanResult will have an empty tree with 0 checksums to indicate this.
    MetadataScanResult metadataResult = fastCheck();
    if (!metadataResult.isHealthy()) {
      return DataScanResult.unhealthyMetadata(metadataResult);
    } else if (metadataResult.isDeleted()) {
      return DataScanResult.deleted();
    }

    DataScanResult dataResult = scanData(throttler, canceler);
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException("Data scan of container " + containerID +
          " interrupted.");
    }

    return dataResult;
  }

  private List<ContainerScanError> checkContainerFile(File containerFile) {
    /*
     * compare the values in the container file loaded from disk,
     * with the values we are expecting
     */
    String dbType;
    Preconditions
        .checkState(onDiskContainerData != null, "Container File not loaded");
    
    List<ContainerScanError> errors = new ArrayList<>();

    // If the file checksum does not match, we will not try to read the file.
    try {
      ContainerUtils.verifyContainerFileChecksum(onDiskContainerData, checkConfig);
    } catch (IOException ex) {
      errors.add(new ContainerScanError(FailureType.CORRUPT_CONTAINER_FILE, containerFile, ex));
      return errors;
    }

    // All other failures are independent.
    if (onDiskContainerData.getContainerType()
        != ContainerProtos.ContainerType.KeyValueContainer) {
      String errStr = "Bad Container type in Containerdata for " + containerID;
      errors.add(new ContainerScanError(FailureType.CORRUPT_CONTAINER_FILE, containerFile, new IOException(errStr)));
    }

    if (onDiskContainerData.getContainerID() != containerID) {
      String errStr =
          "Bad ContainerID field in Containerdata for " + containerID;
      errors.add(new ContainerScanError(FailureType.CORRUPT_CONTAINER_FILE, containerFile, new IOException(errStr)));
    }

    dbType = onDiskContainerData.getContainerDBType();
    if (!dbType.equals(CONTAINER_DB_TYPE_ROCKSDB)) {
      String errStr = "Unknown DBType [" + dbType
          + "] in Container File for  [" + containerID + "]";
      errors.add(new ContainerScanError(FailureType.CORRUPT_CONTAINER_FILE, containerFile, new IOException(errStr)));
    }

    KeyValueContainerData kvData = onDiskContainerData;
    if (!metadataPath.equals(kvData.getMetadataPath())) {
      String errStr =
          "Bad metadata path in Containerdata for " + containerID + "Expected ["
              + metadataPath + "] Got [" + kvData.getMetadataPath()
              + "]";
      errors.add(new ContainerScanError(FailureType.CORRUPT_CONTAINER_FILE, containerFile, new IOException(errStr)));
    }

    return errors;
  }

  private DataScanResult scanData(DataTransferThrottler throttler, Canceler canceler) {
    Preconditions.checkState(onDiskContainerData != null,
        "invoke loadContainerData prior to calling this function");

    List<ContainerScanError> errors = new ArrayList<>();
    // TODO HDDS-10374 this tree will get updated with the container's contents as it is scanned.
    ContainerMerkleTree currentTree = new ContainerMerkleTree();

    File dbFile = KeyValueContainerLocationUtil
        .getContainerDBFile(onDiskContainerData);

    // If the DB cannot be loaded, we cannot proceed with the data scan.
    if (!dbFile.exists() || !dbFile.canRead()) {
      String dbFileErrorMsg = "Unable to access DB File [" + dbFile.toString()
          + "] for Container [" + containerID + "] metadata path ["
          + metadataPath + "]";
      errors.add(new ContainerScanError(FailureType.INACCESSIBLE_DB, dbFile, new IOException(dbFileErrorMsg)));
      return DataScanResult.fromErrors(errors, currentTree);
    }

    onDiskContainerData.setDbFile(dbFile);

    boolean containerDeleted = false;
    try {
      try (DBHandle db = BlockUtils.getDB(onDiskContainerData, checkConfig);
          BlockIterator<BlockData> kvIter = db.getStore().getBlockIterator(
              onDiskContainerData.getContainerID(),
              onDiskContainerData.getUnprefixedKeyFilter())) {
        while (kvIter.hasNext() && !containerDeleted) {
          List<ContainerScanError> blockErrors = scanBlock(db, dbFile, kvIter.nextBlock(), throttler, canceler,
              currentTree);
          // In the common case there will be no errors reading the block. Only if there are errors do we need to
          // check if the container was deleted during the scan.
          containerDeleted = !blockErrors.isEmpty() && containerIsDeleted(db);
          errors.addAll(blockErrors);
        }
      }
    } catch (IOException ex) {
      errors.add(new ContainerScanError(FailureType.INACCESSIBLE_DB, dbFile, ex));
    }

    if (containerDeleted) {
      return DataScanResult.deleted();
    } else {
      return DataScanResult.fromErrors(errors, currentTree);
    }
  }

  /**
   * Checks if a container has been deleted using the database.
   *
   * A Schema V2 contianer is determined to be deleted if its database is no longer present on disk.
   * A Schema V3 container is determined to be deleted if its entries have been removed from the DB.
   * This method checks the block count key for existence since this is used by EC and Ratis containers.
   */
  private boolean containerIsDeleted(DBHandle db) throws IOException {
    if (onDiskContainerData.hasSchema(OzoneConsts.SCHEMA_V3)) {
      // TODO use iterator and check for entries.
      return db.getStore().getMetadataTable().get(onDiskContainerData.getBlockCountKey()) == null;
    } else {
      return !onDiskContainerData.getDbFile().exists();
    }
  }

  /**
   *  Attempt to read the block data without the container lock.
   *  The block onDisk might be in modification by other thread and not yet
   *  flushed to DB, so the content might be outdated.
   *
   * @param db DB of container
   * @param block last queried blockData
   * @return blockData in DB
   * @throws IOException
   */
  private BlockData getBlockDataFromDB(DBHandle db, BlockData block)
      throws IOException {
    String blockKey =
        onDiskContainerData.getBlockKey(block.getBlockID().getLocalID());
    return db.getStore().getBlockDataTable().get(blockKey);
  }

  /**
   *  Attempt to read the block data with the container lock.
   *  The container lock ensure the latest DB record could be retrieved, since
   *  other block related write operation will acquire the container write lock.
   *
   * @param db DB of container
   * @param block last queried blockData
   * @return blockData in DB
   * @throws IOException
   */
  private boolean blockInDBWithLock(DBHandle db, BlockData block)
      throws IOException {
    container.readLock();
    try {
      return getBlockDataFromDB(db, block) != null;
    } finally {
      container.readUnlock();
    }
  }

  private List<ContainerScanError> scanBlock(DBHandle db, File dbFile, BlockData block,
      DataTransferThrottler throttler, Canceler canceler, ContainerMerkleTree currentTree) {
    ContainerLayoutVersion layout = onDiskContainerData.getLayoutVersion();

    List<ContainerScanError> blockErrors = new ArrayList<>();

    // If the chunk or block disappears from the disk during this scan, stop checking it.
    // Future checksum checks will likely fail and the block may have been deleted.
    // At the end we will check the DB with a lock to determine whether the file was actually deleted.
    boolean fileMissing = false;
    Iterator<ContainerProtos.ChunkInfo> chunkIter = block.getChunks().iterator();
    while (chunkIter.hasNext() && !fileMissing) {
      ContainerProtos.ChunkInfo chunk = chunkIter.next();
      // This is populated with a file if we are able to locate the correct directory.
      Optional<File> optionalFile = Optional.empty();

      // If we cannot locate where to read chunk files from, then we cannot proceed with scanning this block.
      try {
        optionalFile = Optional.of(layout.getChunkFile(onDiskContainerData,
            block.getBlockID(), chunk.getChunkName()));
      } catch (StorageContainerException ex) {
        // The parent directory that contains chunk files does not exist.
        if (ex.getResult() == ContainerProtos.Result.UNABLE_TO_FIND_DATA_DIR) {
          blockErrors.add(new ContainerScanError(FailureType.MISSING_CHUNKS_DIR,
              new File(onDiskContainerData.getChunksPath()), ex));
        } else {
          // Unknown exception occurred trying to locate the file.
          blockErrors.add(new ContainerScanError(FailureType.CORRUPT_CHUNK,
              new File(onDiskContainerData.getChunksPath()), ex));
        }
      }

      if (optionalFile.isPresent()) {
        File chunkFile = optionalFile.get();
        if (!chunkFile.exists()) {
          // In EC, client may write empty putBlock in padding block nodes.
          // So, we need to make sure, chunk length > 0, before declaring
          // the missing chunk file.
          if (!block.getChunks().isEmpty() && block.getChunks().get(0).getLen() > 0) {
            ContainerScanError error = new ContainerScanError(FailureType.MISSING_CHUNK_FILE,
                new File(onDiskContainerData.getChunksPath()), new IOException("Missing chunk file " +
                chunkFile.getAbsolutePath()));
            blockErrors.add(error);
          }
        } else if (chunk.getChecksumData().getType() != ContainerProtos.ChecksumType.NONE) {
          int bytesPerChecksum = chunk.getChecksumData().getBytesPerChecksum();
          ByteBuffer buffer = BUFFER_POOL.getBuffer(bytesPerChecksum);
          // Keep scanning the block even if there are errors with individual chunks.
          blockErrors.addAll(verifyChecksum(block, chunk, chunkFile, layout, buffer, currentTree, throttler, canceler));
          buffer.clear();
          BUFFER_POOL.returnBuffer(buffer);
        }
      }

      fileMissing = !optionalFile.isPresent() || !optionalFile.get().exists();
    }

    try {
      if (fileMissing && !blockInDBWithLock(db, block)) {
        // The chunk/block file was missing from the disk, but after checking the DB with a lock it is not there either.
        // This means the block was deleted while the scan was running (without a lock) and all errors in this block
        // can be ignored.
        blockErrors.clear();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scanned outdated blockData {} in container {}", block, containerID);
        }
      }
    } catch (IOException ex) {
      // Failed to read the block metadata from the DB.
      blockErrors.add(new ContainerScanError(FailureType.INACCESSIBLE_DB, dbFile, ex));
    }

    return blockErrors;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private static List<ContainerScanError> verifyChecksum(BlockData block,
      ContainerProtos.ChunkInfo chunk, File chunkFile, ContainerLayoutVersion layout, ByteBuffer buffer,
      ContainerMerkleTree currentTree, DataTransferThrottler throttler, Canceler canceler) {

    List<ContainerScanError> scanErrors = new ArrayList<>();

    ChecksumData checksumData =
        ChecksumData.getFromProtoBuf(chunk.getChecksumData());
    int checksumCount = checksumData.getChecksums().size();
    int bytesPerChecksum = checksumData.getBytesPerChecksum();
    Checksum cal = new Checksum(checksumData.getChecksumType(),
        bytesPerChecksum);
    long bytesRead = 0;
    try (FileChannel channel = FileChannel.open(chunkFile.toPath(),
        ChunkUtils.READ_OPTIONS, ChunkUtils.NO_ATTRIBUTES)) {
      if (layout == ContainerLayoutVersion.FILE_PER_BLOCK) {
        channel.position(chunk.getOffset());
      }
      for (int i = 0; i < checksumCount; i++) {
        // limit last read for FILE_PER_BLOCK, to avoid reading next chunk
        if (layout == ContainerLayoutVersion.FILE_PER_BLOCK &&
            i == checksumCount - 1 &&
            chunk.getLen() % bytesPerChecksum != 0) {
          buffer.limit((int) (chunk.getLen() % bytesPerChecksum));
        }

        int v = channel.read(buffer);
        if (v == -1) {
          break;
        }
        bytesRead += v;
        buffer.flip();

        throttler.throttle(v, canceler);

        ByteString expected = checksumData.getChecksums().get(i);
        ByteString actual = cal.computeChecksum(buffer)
            .getChecksums().get(0);
        if (!expected.equals(actual)) {
          String message = String
              .format("Inconsistent read for chunk=%s" +
                  " checksum item %d" +
                  " expected checksum %s" +
                  " actual checksum %s" +
                  " for block %s",
                  ChunkInfo.getFromProtoBuf(chunk),
                  i,
                  StringUtils.bytes2Hex(expected.asReadOnlyByteBuffer()),
                  StringUtils.bytes2Hex(actual.asReadOnlyByteBuffer()),
                  block.getBlockID());
          scanErrors.add(new ContainerScanError(FailureType.CORRUPT_CHUNK, chunkFile,
              new OzoneChecksumException(message)));
        }
      }
      if (bytesRead != chunk.getLen()) {
        String message = String
            .format("Inconsistent read for chunk=%s expected length=%d"
                    + " actual length=%d for block %s",
                chunk.getChunkName(),
                chunk.getLen(), bytesRead, block.getBlockID());
        scanErrors.add(new ContainerScanError(FailureType.CORRUPT_CHUNK, chunkFile, new IOException(message)));
      }
    } catch (IOException ex) {
      scanErrors.add(new ContainerScanError(FailureType.MISSING_CHUNK_FILE, chunkFile, ex));
    }

    return scanErrors;
  }

  private void loadContainerData(File containerFile) throws IOException {
    onDiskContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    onDiskContainerData.setVolume(volume);
  }

  @VisibleForTesting
  void setContainerData(KeyValueContainerData containerData) {
    onDiskContainerData = containerData;
  }

  @VisibleForTesting
  ScanResult scanContainer(DataTransferThrottler throttler,
                           Canceler canceler) {
    return scanData(throttler, canceler);
  }

}
