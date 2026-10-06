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

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_TYPE_ROCKSDB;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeWriter;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScanError;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScanError.FailureType;
import org.apache.hadoop.ozone.container.ozoneimpl.DataScanResult;
import org.apache.hadoop.ozone.container.ozoneimpl.MetadataScanResult;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to run integrity checks on Datanode Containers.
 * Provide infra for Data Scrubbing
 */
public class KeyValueContainerCheck {

  private static final Logger LOG =
      LoggerFactory.getLogger(KeyValueContainerCheck.class);

  private final long containerID;
  private final ConfigurationSource checkConfig;

  private final String metadataPath;
  private final HddsVolume volume;
  private final KeyValueContainer container;
  // Container data already loaded in the datanode's memory. Used when the container data cannot be loaded from the
  // disk, for example, because the container was deleted during a scan.
  private final KeyValueContainerData containerDataFromMemory;
  // Container data read from the container file on disk. Used to verify the integrity of the container.
  // This is not loaded until a scan begins.
  private KeyValueContainerData containerDataFromDisk;
  private static final DirectBufferPool BUFFER_POOL = new DirectBufferPool();

  public KeyValueContainerCheck(ConfigurationSource conf, KeyValueContainer container) {
    this.checkConfig = conf;
    this.container = container;
    this.containerDataFromDisk = null;
    this.containerDataFromMemory = this.container.getContainerData();
    this.containerID = containerDataFromMemory.getContainerID();
    this.metadataPath = containerDataFromMemory.getMetadataPath();
    this.volume = containerDataFromMemory.getVolume();
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

    try {
      List<ContainerScanError> metadataErrors = scanMetadata();
      if (containerIsDeleted()) {
        return MetadataScanResult.deleted();
      }
      return MetadataScanResult.fromErrors(metadataErrors);
    } finally {
      // IO operations during the scan will throw different types of exceptions if the thread is interrupted.
      // the only consistent indicator of interruption in this case is the thread's interrupt flag.
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException("Metadata scan of container " +
            containerID + " interrupted.");
      }
    }
  }

  private List<ContainerScanError> scanMetadata() {
    List<ContainerScanError> metadataErrors = new ArrayList<>();
    // Container directory should exist.
    // If it does not, we cannot continue the scan.
    File containerDir = new File(metadataPath).getParentFile();
    if (!containerDir.exists()) {
      metadataErrors.add(new ContainerScanError(FailureType.MISSING_CONTAINER_DIR,
          containerDir, new FileNotFoundException("Container directory " + containerDir + " not found.")));
      return metadataErrors;
    }

    // Metadata directory within the container directory should exist.
    // If it does not, no further scanning can be done.
    File metadataDir = new File(metadataPath);
    if (!metadataDir.exists()) {
      metadataErrors.add(new ContainerScanError(FailureType.MISSING_METADATA_DIR, metadataDir,
          new FileNotFoundException("Metadata directory " + metadataDir + " not found.")));
      return metadataErrors;
    }

    // Container file inside the metadata directory should be valid.
    // If it is not, no further scanning can be done.
    File containerFile = KeyValueContainer.getContainerFile(metadataPath, containerID);
    try {
      loadContainerData(containerFile);
    } catch (FileNotFoundException | NoSuchFileException ex) {
      metadataErrors.add(new ContainerScanError(FailureType.MISSING_CONTAINER_FILE, containerFile, ex));
      return metadataErrors;
    } catch (IOException ex) {
      metadataErrors.add(new ContainerScanError(FailureType.CORRUPT_CONTAINER_FILE, containerFile, ex));
      return metadataErrors;
    }
    metadataErrors.addAll(checkContainerFile(containerFile));

    // Chunks directory should exist.
    // The metadata scan can continue even if this fails, since it does not look at the data inside the chunks
    // directory.
    File chunksDir = new File(containerDataFromDisk.getChunksPath());
    if (!chunksDir.exists()) {
      metadataErrors.add(new ContainerScanError(FailureType.MISSING_CHUNKS_DIR, chunksDir,
          new FileNotFoundException("Chunks directory " + chunksDir + " not found.")));
    }

    return metadataErrors;
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
  public DataScanResult fullCheck(DataTransferThrottler throttler, Canceler canceler) throws InterruptedException {
    // If the metadata check fails, we cannot do the data check.
    // The DataScanResult will have an empty tree with 0 checksums to indicate this.
    MetadataScanResult metadataResult = fastCheck();
    if (metadataResult.isDeleted()) {
      return DataScanResult.deleted();
    } else if (metadataResult.hasErrors()) {
      return DataScanResult.unhealthyMetadata(metadataResult);
    }

    LOG.debug("Running data checks for container {}", containerID);
    try {
      ContainerMerkleTreeWriter dataTree = new ContainerMerkleTreeWriter();
      List<ContainerScanError> dataErrors = scanData(dataTree, throttler, canceler);
      if (containerIsDeleted()) {
        return DataScanResult.deleted();
      }
      return DataScanResult.fromErrors(dataErrors, dataTree);
    } finally {
      // IO operations during the scan will throw different types of exceptions if the thread is interrupted.
      // the only consistent indicator of interruption in this case is the thread's interrupt flag.
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException("Data scan of container " + containerID +
            " interrupted.");
      }
    }
  }

  private List<ContainerScanError> scanData(ContainerMerkleTreeWriter currentTree, DataTransferThrottler throttler,
                                            Canceler canceler) {
    Preconditions.checkState(containerDataFromDisk != null,
        "invoke loadContainerData prior to calling this function");

    List<ContainerScanError> errors = new ArrayList<>();

    // If the DB cannot be loaded, we cannot proceed with the data scan.
    File dbFile = containerDataFromDisk.getDbFile();
    if (!dbFile.exists() || !dbFile.canRead()) {
      String dbFileErrorMsg = "Unable to access DB File [" + dbFile.toString()
          + "] for Container [" + containerID + "] metadata path ["
          + metadataPath + "]";
      errors.add(new ContainerScanError(FailureType.INACCESSIBLE_DB, dbFile, new IOException(dbFileErrorMsg)));
      return errors;
    }

    try {
      try (DBHandle db = BlockUtils.getDB(containerDataFromDisk, checkConfig);
           BlockIterator<BlockData> kvIter = db.getStore().getBlockIterator(
               containerDataFromDisk.getContainerID(),
               containerDataFromDisk.getUnprefixedKeyFilter())) {
        // If the container was deleted during the scan, stop trying to process its data.
        while (kvIter.hasNext() && !containerIsDeleted()) {
          List<ContainerScanError> blockErrors = scanBlock(db, dbFile, kvIter.nextBlock(), throttler, canceler,
              currentTree);
          errors.addAll(blockErrors);
        }
      }
    } catch (IOException ex) {
      errors.add(new ContainerScanError(FailureType.INACCESSIBLE_DB, dbFile, ex));
    }

    return errors;
  }

  private List<ContainerScanError> checkContainerFile(File containerFile) {
    /*
     * compare the values in the container file loaded from disk,
     * with the values we are expecting
     */
    String dbType;
    Preconditions
        .checkState(containerDataFromDisk != null, "Container File not loaded");

    List<ContainerScanError> errors = new ArrayList<>();

    // If the file checksum does not match, we will not try to read the file.
    try {
      ContainerUtils.verifyContainerFileChecksum(containerDataFromDisk, checkConfig);
    } catch (IOException ex) {
      errors.add(new ContainerScanError(FailureType.CORRUPT_CONTAINER_FILE, containerFile, ex));
      return errors;
    }

    // All other failures are independent.
    if (containerDataFromDisk.getContainerType()
        != ContainerProtos.ContainerType.KeyValueContainer) {
      String errStr = "Bad Container type in Containerdata for " + containerID;
      errors.add(new ContainerScanError(FailureType.CORRUPT_CONTAINER_FILE, containerFile, new IOException(errStr)));
    }

    if (containerDataFromDisk.getContainerID() != containerID) {
      String errStr =
          "Bad ContainerID field in Containerdata for " + containerID;
      errors.add(new ContainerScanError(FailureType.CORRUPT_CONTAINER_FILE, containerFile, new IOException(errStr)));
    }

    dbType = containerDataFromDisk.getContainerDBType();
    if (!dbType.equals(CONTAINER_DB_TYPE_ROCKSDB)) {
      String errStr = "Unknown DBType [" + dbType
          + "] in Container File for  [" + containerID + "]";
      errors.add(new ContainerScanError(FailureType.CORRUPT_CONTAINER_FILE, containerFile, new IOException(errStr)));
    }

    if (!metadataPath.equals(containerDataFromDisk.getMetadataPath())) {
      String errStr =
          "Bad metadata path in Containerdata for " + containerID + "Expected ["
              + metadataPath + "] Got [" + containerDataFromDisk.getMetadataPath()
              + "]";
      errors.add(new ContainerScanError(FailureType.CORRUPT_CONTAINER_FILE, containerFile, new IOException(errStr)));
    }

    return errors;
  }

  /**
   * Checks if a container has been deleted based on its state in datanode memory. This state change is the first
   * step in deleting a container on a datanode and is done in a thread-safe manner. See KeyValueHandler#deleteInternal.
   */
  private boolean containerIsDeleted() {
    return containerDataFromMemory.getState() == State.DELETED;
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
        containerDataFromDisk.getBlockKey(block.getBlockID().getLocalID());
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
      DataTransferThrottler throttler, Canceler canceler, ContainerMerkleTreeWriter currentTree) {
    ContainerLayoutVersion layout = containerDataFromDisk.getLayoutVersion();

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
        optionalFile = Optional.of(layout.getChunkFile(containerDataFromDisk,
            block.getBlockID(), chunk.getChunkName()));
      } catch (StorageContainerException ex) {
        // The parent directory that contains chunk files does not exist.
        if (ex.getResult() == ContainerProtos.Result.UNABLE_TO_FIND_DATA_DIR) {
          blockErrors.add(new ContainerScanError(FailureType.MISSING_CHUNKS_DIR,
              new File(containerDataFromDisk.getChunksPath()), ex));
        } else {
          // Unknown exception occurred trying to locate the file.
          blockErrors.add(new ContainerScanError(FailureType.CORRUPT_CHUNK,
              new File(containerDataFromDisk.getChunksPath()), ex));
        }
      }

      if (optionalFile.isPresent()) {
        File chunkFile = optionalFile.get();
        if (!chunkFile.exists()) {
          // In EC, client may write empty putBlock in padding block nodes.
          // So, we need to make sure, chunk length > 0, before declaring
          // the missing chunk file.
          if (!block.getChunks().isEmpty() && block.getChunks().get(0).getLen() > 0) {
            ContainerScanError error = new ContainerScanError(FailureType.MISSING_DATA_FILE,
                new File(containerDataFromDisk.getChunksPath()), new IOException("Missing chunk file " +
                chunkFile.getAbsolutePath()));
            blockErrors.add(error);
          }
        } else if (chunk.getChecksumData().getType() != ContainerProtos.ChecksumType.NONE) {
          // Before adding chunks, add a block entry to the tree to represent cases where the block exists but has no
          // chunks.
          currentTree.addBlock(block.getBlockID().getLocalID());
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
      ContainerMerkleTreeWriter currentTree, DataTransferThrottler throttler, Canceler canceler) {

    List<ContainerScanError> scanErrors = new ArrayList<>();

    // Information used to populate the merkle tree. Chunk metadata will be the same, but we must fill in the
    // checksums with what we actually observe.
    ContainerProtos.ChunkInfo.Builder observedChunkBuilder = chunk.toBuilder();
    ContainerProtos.ChecksumData.Builder observedChecksumData = chunk.getChecksumData().toBuilder();
    observedChecksumData.clearChecksums();
    boolean chunkHealthy = true;
    boolean chunkMissing = false;

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
        observedChecksumData.addChecksums(actual);
        // Only report one error per chunk. Reporting corruption at every "bytes per checksum" interval will lead to a
        // large amount of errors when a full chunk is corrupted.
        // Continue scanning the chunk even after the first error so the full merkle tree can be built.
        if (chunkHealthy && !expected.equals(actual)) {
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
          chunkHealthy = false;
          scanErrors.add(new ContainerScanError(FailureType.CORRUPT_CHUNK, chunkFile,
              new OzoneChecksumException(message)));
        }
      }

      observedChunkBuilder.setLen(bytesRead);
      // If we haven't seen any errors after scanning the whole chunk, verify that the length stored in the metadata
      // matches the number of bytes seen on the disk.
      if (chunkHealthy && bytesRead != chunk.getLen()) {
        if (bytesRead == 0) {
          // If we could not find any data for the chunk, report it as missing.
          chunkMissing = true;
          chunkHealthy = false;
          String message = String.format("Missing chunk=%s with expected length=%d for block %s",
                  chunk.getChunkName(), chunk.getLen(), block.getBlockID());
          scanErrors.add(new ContainerScanError(FailureType.MISSING_CHUNK, chunkFile, new IOException(message)));
        } else {
          // We found data for the chunk, but it was shorter than expected.
          String message = String
              .format("Inconsistent read for chunk=%s expected length=%d"
                      + " actual length=%d for block %s",
                  chunk.getChunkName(),
                  chunk.getLen(), bytesRead, block.getBlockID());
          chunkHealthy = false;
          scanErrors.add(new ContainerScanError(FailureType.INCONSISTENT_CHUNK_LENGTH, chunkFile,
              new IOException(message)));
        }
      }
    } catch (IOException ex) {
      // An unknown error occurred trying to access the chunk. Report it as corrupted.
      chunkHealthy = false;
      scanErrors.add(new ContainerScanError(FailureType.CORRUPT_CHUNK, chunkFile, ex));
    }

    // Missing chunks should not be added to the merkle tree.
    if (!chunkMissing) {
      observedChunkBuilder.setChecksumData(observedChecksumData);
      currentTree.addChunks(block.getBlockID().getLocalID(), chunkHealthy, observedChunkBuilder.build());
    }
    return scanErrors;
  }

  private void loadContainerData(File containerFile) throws IOException {
    containerDataFromDisk = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    containerDataFromDisk.setVolume(volume);
    containerDataFromDisk.setDbFile(KeyValueContainerLocationUtil.getContainerDBFile(containerDataFromDisk));
  }
}
