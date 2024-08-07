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
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult;
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
  public ScanResult fastCheck() throws InterruptedException {
    LOG.debug("Running basic checks for container {};", containerID);

    try {
      // Container directory should exist.
      File containerDir = new File(metadataPath).getParentFile();
      if (!containerDir.exists()) {
        return ScanResult.unhealthy(
            ScanResult.FailureType.MISSING_CONTAINER_DIR,
            containerDir, new FileNotFoundException("Container directory " +
                containerDir + " not found."));
      }

      // Metadata directory should exist.
      File metadataDir = new File(metadataPath);
      if (!metadataDir.exists()) {
        return ScanResult.unhealthy(ScanResult.FailureType.MISSING_METADATA_DIR,
            metadataDir, new FileNotFoundException("Metadata directory " +
                metadataDir + " not found."));
      }

      // Container file should be valid.
      File containerFile = KeyValueContainer
          .getContainerFile(metadataPath, containerID);
      try {
        loadContainerData(containerFile);
      } catch (FileNotFoundException ex) {
        return ScanResult.unhealthy(
            ScanResult.FailureType.MISSING_CONTAINER_FILE, containerFile, ex);
      } catch (IOException ex) {
        return ScanResult.unhealthy(
            ScanResult.FailureType.CORRUPT_CONTAINER_FILE, containerFile, ex);
      }

      // Chunks directory should exist.
      File chunksDir = new File(onDiskContainerData.getChunksPath());
      if (!chunksDir.exists()) {
        return ScanResult.unhealthy(ScanResult.FailureType.MISSING_CHUNKS_DIR,
            chunksDir, new FileNotFoundException("Chunks directory " +
                chunksDir + " not found."));
      }

      return checkContainerFile(containerFile);
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
  public ScanResult fullCheck(DataTransferThrottler throttler,
      Canceler canceler) throws InterruptedException {
    ScanResult result = fastCheck();
    if (result.isHealthy()) {
      result = scanData(throttler, canceler);
    }

    if (!result.isHealthy() && Thread.currentThread().isInterrupted()) {
      throw new InterruptedException("Data scan of container " + containerID +
          " interrupted.");
    }

    return result;
  }

  private ScanResult checkContainerFile(File containerFile) {
    /*
     * compare the values in the container file loaded from disk,
     * with the values we are expecting
     */
    String dbType;
    Preconditions
        .checkState(onDiskContainerData != null, "Container File not loaded");

    try {
      ContainerUtils.verifyChecksum(onDiskContainerData, checkConfig);
    } catch (IOException ex) {
      return ScanResult.unhealthy(ScanResult.FailureType.CORRUPT_CONTAINER_FILE,
          containerFile, ex);
    }

    if (onDiskContainerData.getContainerType()
        != ContainerProtos.ContainerType.KeyValueContainer) {
      String errStr = "Bad Container type in Containerdata for " + containerID;
      return ScanResult.unhealthy(ScanResult.FailureType.CORRUPT_CONTAINER_FILE,
          containerFile, new IOException(errStr));
    }

    if (onDiskContainerData.getContainerID() != containerID) {
      String errStr =
          "Bad ContainerID field in Containerdata for " + containerID;
      return ScanResult.unhealthy(ScanResult.FailureType.CORRUPT_CONTAINER_FILE,
          containerFile, new IOException(errStr));
    }

    dbType = onDiskContainerData.getContainerDBType();
    if (!dbType.equals(CONTAINER_DB_TYPE_ROCKSDB)) {
      String errStr = "Unknown DBType [" + dbType
          + "] in Container File for  [" + containerID + "]";
      return ScanResult.unhealthy(ScanResult.FailureType.CORRUPT_CONTAINER_FILE,
          containerFile, new IOException(errStr));
    }

    KeyValueContainerData kvData = onDiskContainerData;
    if (!metadataPath.equals(kvData.getMetadataPath())) {
      String errStr =
          "Bad metadata path in Containerdata for " + containerID + "Expected ["
              + metadataPath + "] Got [" + kvData.getMetadataPath()
              + "]";
      return ScanResult.unhealthy(ScanResult.FailureType.CORRUPT_CONTAINER_FILE,
          containerFile, new IOException(errStr));
    }

    return ScanResult.healthy();
  }

  private ScanResult scanData(DataTransferThrottler throttler,
      Canceler canceler) {
    /*
     * Check the integrity of the DB inside each container.
     * 1. iterate over each key (Block) and locate the chunks for the block
     * 2. garbage detection (TBD): chunks which exist in the filesystem,
     *    but not in the DB. This function will be implemented in HDDS-1202
     * 3. chunk checksum verification.
     */
    Preconditions.checkState(onDiskContainerData != null,
        "invoke loadContainerData prior to calling this function");

    File dbFile = KeyValueContainerLocationUtil
        .getContainerDBFile(onDiskContainerData);

    if (!dbFile.exists() || !dbFile.canRead()) {
      String dbFileErrorMsg = "Unable to access DB File [" + dbFile.toString()
          + "] for Container [" + containerID + "] metadata path ["
          + metadataPath + "]";
      return ScanResult.unhealthy(ScanResult.FailureType.INACCESSIBLE_DB,
          dbFile, new IOException(dbFileErrorMsg));
    }

    onDiskContainerData.setDbFile(dbFile);

    try {
      try (DBHandle db = BlockUtils.getDB(onDiskContainerData, checkConfig);
          BlockIterator<BlockData> kvIter = db.getStore().getBlockIterator(
              onDiskContainerData.getContainerID(),
              onDiskContainerData.getUnprefixedKeyFilter())) {

        while (kvIter.hasNext()) {
          BlockData block = kvIter.nextBlock();

          // If holding read lock for the entire duration, including wait()
          // calls in DataTransferThrottler, would effectively make other
          // threads throttled.
          // Here try optimistically and retry with the container lock to
          // make sure reading the latest record. If the record is just removed,
          // the block should be skipped to scan.
          ScanResult result = scanBlock(block, throttler, canceler);
          if (!result.isHealthy()) {
            if (result.getFailureType() ==
                ScanResult.FailureType.MISSING_CHUNK_FILE) {
              if (getBlockDataFromDBWithLock(db, block) != null) {
                // Block was not deleted, the failure is legitimate.
                return result;
              } else {
                // If schema V3 and container details not in DB or
                // if containerDBPath is removed
                if ((onDiskContainerData.hasSchema(OzoneConsts.SCHEMA_V3) &&
                    db.getStore().getMetadataTable().get(
                      onDiskContainerData.getBcsIdKey()) == null)  ||
                    !new File(onDiskContainerData.getDbFile()
                        .getAbsolutePath()).exists()) {
                  // Container has been deleted. Skip the rest of the blocks.
                  return ScanResult.unhealthy(
                      ScanResult.FailureType.DELETED_CONTAINER,
                      result.getUnhealthyFile(), result.getException());
                }

                // Block may have been deleted during the scan.
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Scanned outdated blockData {} in container {}.",
                      block, containerID);
                }
              }
            } else {
              // All other failures should be treated as errors.
              return result;
            }
          }
        }
      }
    } catch (IOException ex) {
      return ScanResult.unhealthy(ScanResult.FailureType.INACCESSIBLE_DB,
          dbFile, ex);
    }

    return ScanResult.healthy();
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
  private BlockData getBlockDataFromDBWithLock(DBHandle db, BlockData block)
      throws IOException {
    container.readLock();
    try {
      return getBlockDataFromDB(db, block);
    } finally {
      container.readUnlock();
    }
  }

  private ScanResult scanBlock(BlockData block, DataTransferThrottler throttler,
      Canceler canceler) {
    ContainerLayoutVersion layout = onDiskContainerData.getLayoutVersion();

    for (ContainerProtos.ChunkInfo chunk : block.getChunks()) {
      File chunkFile;
      try {
        chunkFile = layout.getChunkFile(onDiskContainerData,
            block.getBlockID(), chunk.getChunkName());
      } catch (IOException ex) {
        return ScanResult.unhealthy(
            ScanResult.FailureType.MISSING_CHUNK_FILE,
            new File(onDiskContainerData.getChunksPath()), ex);
      }

      if (!chunkFile.exists()) {
        // In EC, client may write empty putBlock in padding block nodes.
        // So, we need to make sure, chunk length > 0, before declaring
        // the missing chunk file.
        if (block.getChunks().size() > 0 && block
            .getChunks().get(0).getLen() > 0) {
          return ScanResult.unhealthy(ScanResult.FailureType.MISSING_CHUNK_FILE,
              chunkFile, new IOException("Missing chunk file " +
                  chunkFile.getAbsolutePath()));
        }
      } else if (chunk.getChecksumData().getType()
          != ContainerProtos.ChecksumType.NONE) {
        int bytesPerChecksum = chunk.getChecksumData().getBytesPerChecksum();
        ByteBuffer buffer = BUFFER_POOL.getBuffer(bytesPerChecksum);
        ScanResult result = verifyChecksum(block, chunk, chunkFile, layout, buffer,
            throttler, canceler);
        buffer.clear();
        BUFFER_POOL.returnBuffer(buffer);
        if (!result.isHealthy()) {
          return result;
        }
      }
    }

    return ScanResult.healthy();
  }

  private static ScanResult verifyChecksum(BlockData block,
      ContainerProtos.ChunkInfo chunk, File chunkFile,
      ContainerLayoutVersion layout, ByteBuffer buffer,
      DataTransferThrottler throttler, Canceler canceler) {
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
          return ScanResult.unhealthy(
              ScanResult.FailureType.CORRUPT_CHUNK, chunkFile,
              new IOException(message));
        }
      }
      if (bytesRead != chunk.getLen()) {
        String message = String
            .format("Inconsistent read for chunk=%s expected length=%d"
                    + " actual length=%d for block %s",
                chunk.getChunkName(),
                chunk.getLen(), bytesRead, block.getBlockID());
        return ScanResult.unhealthy(
            ScanResult.FailureType.INCONSISTENT_CHUNK_LENGTH, chunkFile,
            new IOException(message));
      }
    } catch (IOException ex) {
      return ScanResult.unhealthy(
          ScanResult.FailureType.MISSING_CHUNK_FILE, chunkFile, ex);
    }

    return ScanResult.healthy();
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
