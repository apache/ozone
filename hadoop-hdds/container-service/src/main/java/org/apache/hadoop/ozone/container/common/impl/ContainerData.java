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

package org.apache.hadoop.ozone.container.common.impl;

import static org.apache.hadoop.ozone.OzoneConsts.CHECKSUM;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_ID;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_TYPE;
import static org.apache.hadoop.ozone.OzoneConsts.DATA_SCAN_TIMESTAMP;
import static org.apache.hadoop.ozone.OzoneConsts.LAYOUTVERSION;
import static org.apache.hadoop.ozone.OzoneConsts.MAX_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.METADATA;
import static org.apache.hadoop.ozone.OzoneConsts.ORIGIN_NODE_ID;
import static org.apache.hadoop.ozone.OzoneConsts.ORIGIN_PIPELINE_ID;
import static org.apache.hadoop.ozone.OzoneConsts.STATE;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.ratis.util.Preconditions;
import org.yaml.snakeyaml.Yaml;

/**
 * ContainerData is the in-memory representation of container metadata and is
 * represented on disk by the .container file.
 */
public abstract class ContainerData {

  //Type of the container.
  // For now, we support only KeyValueContainer.
  private final ContainerType containerType;

  private final AtomicBoolean immediateCloseActionSent = new AtomicBoolean(false);

  // Unique identifier for the container
  private final long containerID;

  // Layout version of the container data
  private final int layOutVersion;

  // Metadata of the container will be a key value pair.
  // This can hold information like volume name, owner etc.,
  private final Map<String, String> metadata;

  // Path to Physical file system where chunks are stored.
  private String chunksPath;

  // State of the Container
  private ContainerDataProto.State state;

  private final long maxSize;

  private boolean committedSpace;

  //ID of the pipeline where this container is created
  private final String originPipelineId;
  //ID of the datanode where this container is created
  private final String originNodeId;

  /** Read/write/block statistics of this container. **/
  private final Statistics statistics = new Statistics();

  private HddsVolume volume;

  // Checksum of just the container file.
  private String checksum;

  // Checksum of the data within the container.
  private long dataChecksum;
  private static final long UNSET_DATA_CHECKSUM = -1;

  private boolean isEmpty;

  private int replicaIndex;

  /** Timestamp of last data scan (milliseconds since Unix Epoch).
   * {@code null} if not yet scanned (or timestamp not recorded,
   * eg. in prior versions). */
  private Long dataScanTimestamp; // for serialization
  private transient Optional<Instant> lastDataScanTime = Optional.empty();

  public static final Charset CHARSET_ENCODING = StandardCharsets.UTF_8;
  public static final String ZERO_CHECKSUM = new String(new byte[64],
      CHARSET_ENCODING);

  // Common Fields need to be stored in .container file.
  protected static final List<String> YAML_FIELDS =
      Collections.unmodifiableList(Lists.newArrayList(
      CONTAINER_TYPE,
      CONTAINER_ID,
      LAYOUTVERSION,
      STATE,
      METADATA,
      MAX_SIZE,
      CHECKSUM,
      DATA_SCAN_TIMESTAMP,
      ORIGIN_PIPELINE_ID,
      ORIGIN_NODE_ID));

  /**
   * Creates a ContainerData Object, which holds metadata of the container.
   * @param type - ContainerType
   * @param containerId - ContainerId
   * @param layoutVersion - Container layoutVersion
   * @param size - Container maximum size in bytes
   * @param originPipelineId - Pipeline Id where this container is/was created
   * @param originNodeId - Node Id where this container is/was created
   */
  protected ContainerData(ContainerType type, long containerId,
                          ContainerLayoutVersion layoutVersion, long size,
                          String originPipelineId,
                          String originNodeId) {
    this.containerType = Objects.requireNonNull(type, "type == null");
    this.containerID = containerId;
    this.layOutVersion = layoutVersion.getVersion();
    this.metadata = new TreeMap<>();
    this.state = ContainerDataProto.State.OPEN;
    this.maxSize = size;
    Preconditions.assertTrue(maxSize > 0, () -> "maxSize = " + maxSize + " <= 0");

    this.originPipelineId = originPipelineId;
    this.originNodeId = originNodeId;
    this.isEmpty = false;
    this.checksum = ZERO_CHECKSUM;
    this.dataChecksum = UNSET_DATA_CHECKSUM;
  }

  protected ContainerData(ContainerData source) {
    this(source.getContainerType(), source.getContainerID(),
        source.getLayoutVersion(), source.getMaxSize(),
        source.getOriginPipelineId(), source.getOriginNodeId());
    replicaIndex = source.getReplicaIndex();
  }

  /**
   * Returns the containerID.
   */
  public long getContainerID() {
    return containerID;
  }

  public AtomicBoolean getImmediateCloseActionSent() {
    return immediateCloseActionSent;
  }

  /**
   * Returns the path to base dir of the container.
   * @return Path to base dir.
   */
  public abstract String getContainerPath();

  /**
   * Returns container metadata path.
   * @return - Physical path where container file and checksum is stored.
   */
  public abstract String getMetadataPath();

  /**
   * Returns the type of the container.
   * @return ContainerType
   */
  public ContainerType getContainerType() {
    return containerType;
  }

  /**
   * Returns the state of the container.
   * @return ContainerLifeCycleState
   */
  public synchronized ContainerDataProto.State getState() {
    return state;
  }

  public int getReplicaIndex() {
    return replicaIndex;
  }

  public void setReplicaIndex(int replicaIndex) {
    this.replicaIndex = replicaIndex;
  }

  /**
   * Set the state of the container.
   * @param state
   */
  public synchronized void setState(ContainerDataProto.State state) {
    ContainerDataProto.State oldState = this.state;
    this.state = state;

    if ((oldState == ContainerDataProto.State.OPEN) &&
        (state != oldState)) {
      releaseCommitSpace();
    }
  }

  public boolean isCommittedSpace() {
    return committedSpace;
  }

  public void setCommittedSpace(boolean committed) {
    committedSpace = committed;
  }

  /**
   * Return's maximum size of the container in bytes.
   * @return maxSize in bytes
   */
  public long getMaxSize() {
    return maxSize;
  }

  /**
   * Returns the layoutVersion of the actual container data format.
   * @return layoutVersion
   */
  public ContainerLayoutVersion getLayoutVersion() {
    return ContainerLayoutVersion.getContainerLayoutVersion(layOutVersion);
  }

  /**
   * Get chunks path.
   * @return - Path where chunks are stored
   */
  public String getChunksPath() {
    return chunksPath;
  }

  /**
   * Set chunks Path.
   * @param chunkPath - File path.
   */
  public void setChunksPath(String chunkPath) {
    this.chunksPath = chunkPath;
  }

  /**
   * Add/Update metadata.
   * We should hold the container lock before updating the metadata as this
   * will be persisted on disk. Unless, we are reconstructing ContainerData
   * from protoBuf or from on disk .container file in which case lock is not
   * required.
   */
  public void addMetadata(String key, String value) {
    metadata.put(key, value);
  }

  /**
   * Returns metadata of the container.
   * @return metadata
   */
  public Map<String, String> getMetadata() {
    return Collections.unmodifiableMap(this.metadata);
  }

  /**
   * Set metadata.
   * We should hold the container lock before updating the metadata as this
   * will be persisted on disk. Unless, we are reconstructing ContainerData
   * from protoBuf or from on disk .container file in which case lock is not
   * required.
   */
  public void setMetadata(Map<String, String> metadataMap) {
    metadata.clear();
    metadata.putAll(metadataMap);
  }

  /**
   * checks if the container is open.
   * @return - boolean
   */
  public synchronized  boolean isOpen() {
    return ContainerDataProto.State.OPEN == state;
  }

  /**
   * checks if the container is closing.
   * @return - boolean
   */
  public synchronized  boolean isClosing() {
    return ContainerDataProto.State.CLOSING == state;
  }

  /**
   * checks if the container is invalid.
   * @return - boolean
   */
  public synchronized boolean isValid() {
    return ContainerDataProto.State.INVALID != state;
  }

  /**
   * checks if the container is closed.
   * @return - boolean
   */
  public synchronized boolean isClosed() {
    return ContainerDataProto.State.CLOSED == state;
  }

  /**
   * checks if the container is quasi closed.
   * @return - boolean
   */
  public synchronized boolean isQuasiClosed() {
    return ContainerDataProto.State.QUASI_CLOSED == state;
  }

  /**
   * checks if the container is unhealthy.
   * @return - boolean
   */
  public synchronized boolean isUnhealthy() {
    return ContainerDataProto.State.UNHEALTHY == state;
  }

  /**
   * Marks this container as quasi closed.
   */
  public synchronized void quasiCloseContainer() {
    setState(ContainerDataProto.State.QUASI_CLOSED);
  }

  /**
   * Marks this container as closed.
   */
  public synchronized void closeContainer() {
    setState(ContainerDataProto.State.CLOSED);
  }

  public void releaseCommitSpace() {
    long unused = getMaxSize() - getBytesUsed();

    // only if container size < max size
    if (unused > 0 && committedSpace) {
      getVolume().incCommittedBytes(0 - unused);
    }
    committedSpace = false;
  }

  /**
   * add available space in the container to the committed space in the volume.
   * available space is the number of bytes remaining till max capacity.
   */
  public void commitSpace() {
    long unused = getMaxSize() - getBytesUsed();
    ContainerDataProto.State myState = getState();
    HddsVolume cVol;

    //we don't expect duplicate calls
    Preconditions.assertTrue(!committedSpace);

    // Only Open Containers have Committed Space
    if (myState != ContainerDataProto.State.OPEN) {
      return;
    }

    // junit tests do not always set up volume
    cVol = getVolume();
    if (unused > 0 && (cVol != null)) {
      cVol.incCommittedBytes(unused);
      committedSpace = true;
    }
  }

  public Statistics getStatistics() {
    return statistics;
  }

  /**
   * Increase the number of bytes write into the container.
   * Also decrement committed bytes against the bytes written.
   * @param bytes the number of bytes write into the container.
   */
  private void incrWriteBytes(long bytes) {
    /*
       Increase the cached Used Space in VolumeInfo as it
       maybe not updated, DU or DedicatedDiskSpaceUsage runs
       periodically to update the Used Space in VolumeInfo.
     */
    this.getVolume().incrementUsedSpace(bytes);
    // Calculate bytes used before this write operation.
    // Note that getBytesUsed() already includes the 'bytes' from the current write.
    long bytesUsedBeforeWrite = getBytesUsed() - bytes;
    // Calculate how much space was available within the max size limit before this write
    long availableSpaceBeforeWrite = getMaxSize() - bytesUsedBeforeWrite;
    if (committedSpace && availableSpaceBeforeWrite > 0) {
      // Decrement committed space only by the portion of the write that fits within the originally committed space,
      // up to maxSize
      long decrement = Math.min(bytes, availableSpaceBeforeWrite);
      this.getVolume().incCommittedBytes(-decrement);
    }
  }

  /**
   * Get the number of bytes used by the container.
   * @return the number of bytes used by the container.
   */
  public long getBytesUsed() {
    return getStatistics().getBlockBytes();
  }

  /**
   * Set the Volume for the Container.
   * This should be called only from the createContainer.
   * @param hddsVolume
   */
  public void setVolume(HddsVolume hddsVolume) {
    this.volume = hddsVolume;
  }

  /**
   * Returns the volume of the Container.
   * @return HddsVolume
   */
  @JsonIgnore
  public HddsVolume getVolume() {
    return volume;
  }

  /** For testing only. */
  @VisibleForTesting
  public long getBlockCount() {
    return getStatistics().getBlockByteAndCounts().getCount();
  }

  public boolean isEmpty() {
    return isEmpty;
  }

  /**
   * Indicates that this container has no more data, and is eligible for
   * deletion. Once this flag is set on a container, it cannot leave this state.
   */
  public void markAsEmpty() {
    this.isEmpty = true;
  }

  public void setContainerFileChecksum(String checkSum) {
    this.checksum = checkSum;
  }

  public String getContainerFileChecksum() {
    return this.checksum;
  }

  /**
   * @return {@code Optional} with the timestamp of last data scan.
   * {@code absent} if not yet scanned or timestamp was not recorded.
   */
  public Optional<Instant> lastDataScanTime() {
    return lastDataScanTime;
  }

  public void updateDataScanTime(@Nullable Instant time) {
    lastDataScanTime = Optional.ofNullable(time);
    dataScanTimestamp = time != null ? time.toEpochMilli() : null;
  }

  // for deserialization
  public void setDataScanTimestamp(Long timestamp) {
    dataScanTimestamp = timestamp;
    lastDataScanTime = timestamp != null
        ? Optional.of(Instant.ofEpochMilli(timestamp))
        : Optional.empty();
  }

  public Long getDataScanTimestamp() {
    return dataScanTimestamp;
  }

  /**
   * Returns the origin pipeline Id of this container.
   * @return origin node Id
   */
  public String getOriginPipelineId() {
    return originPipelineId;
  }

  /**
   * Returns the origin node Id of this container.
   * @return origin node Id
   */
  public String getOriginNodeId() {
    return originNodeId;
  }

  /**
   * Compute the checksum for ContainerData using the specified Yaml (based
   * on ContainerType) and set the checksum.
   *
   * Checksum of ContainerData is calculated by setting the
   * {@link ContainerData#checksum} field to a 64-byte array with all 0's -
   * {@link ContainerData#ZERO_CHECKSUM}. After the checksum is calculated,
   * the checksum field is updated with this value.
   *
   * @param yaml Yaml for ContainerType to get the ContainerData as Yaml String
   * @throws IOException
   */
  public void computeAndSetContainerFileChecksum(Yaml yaml) throws IOException {
    // Set checksum to dummy value - 0 byte array, to calculate the checksum
    // of rest of the data.
    this.checksum = ZERO_CHECKSUM;

    // Dump yaml data into a string to compute its checksum
    String containerDataYamlStr = yaml.dump(this);

    this.checksum = ContainerUtils.getContainerFileChecksum(containerDataYamlStr);
  }

  public void setDataChecksum(long dataChecksum) {
    if (dataChecksum < 0) {
      throw new IllegalArgumentException("Data checksum cannot be set to a negative number.");
    }
    this.dataChecksum = dataChecksum;
  }

  public long getDataChecksum() {
    // UNSET_DATA_CHECKSUM is an internal placeholder, it should not be used outside this class.
    if (needsDataChecksum()) {
      return 0;
    }
    return dataChecksum;
  }

  public boolean needsDataChecksum() {
    return dataChecksum == UNSET_DATA_CHECKSUM;
  }

  /**
   * Returns a ProtoBuf Message from ContainerData.
   *
   * @return Protocol Buffer Message
   */
  public abstract ContainerProtos.ContainerDataProto getProtoBufMessage();

  /**
   * Returns the blockCommitSequenceId.
   */
  public abstract long getBlockCommitSequenceId();

  public void updateWriteStats(long bytesWritten, boolean overwrite) {
    getStatistics().updateWrite(bytesWritten, overwrite);
    incrWriteBytes(bytesWritten);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " #" + containerID
        + " (" + state
        + ", " + (isEmpty ? "empty" : "non-empty")
        + ", ri=" + replicaIndex
        + ", origin=[dn_" + originNodeId + ", pipeline_" + originPipelineId + "])";
  }

  /**
   * Block byte used, block count and pending deletion count.
   * This class is immutable.
   */
  public static class BlockByteAndCounts {
    private final long bytes;
    private final long count;
    private final long pendingDeletion;
    private final long pendingDeletionBytes;

    public BlockByteAndCounts(long bytes, long count, long pendingDeletion, long pendingDeletionBytes) {
      this.bytes = bytes;
      this.count = count;
      this.pendingDeletion = pendingDeletion;
      this.pendingDeletionBytes = pendingDeletionBytes;
    }

    public long getBytes() {
      return bytes;
    }

    public long getCount() {
      return count;
    }

    public long getPendingDeletion() {
      return pendingDeletion;
    }

    public long getPendingDeletionBytes() {
      return pendingDeletionBytes;
    }
  }

  /**
   * Read/write/block statistics of a container.
   * This class is thread-safe -- all methods are synchronized.
   */
  public static class Statistics {
    private long readBytes;
    private long readCount;

    private long writeBytes;
    private long writeCount;

    private long blockBytes;
    private long blockCount;
    private long blockPendingDeletion;
    private long blockPendingDeletionBytes;

    public synchronized long getWriteBytes() {
      return writeBytes;
    }

    public synchronized long getBlockBytes() {
      return blockBytes;
    }

    public synchronized BlockByteAndCounts getBlockByteAndCounts() {
      return new BlockByteAndCounts(blockBytes, blockCount, blockPendingDeletion, blockPendingDeletionBytes);
    }

    public synchronized long getBlockPendingDeletion() {
      return blockPendingDeletion;
    }

    public synchronized long getBlockPendingDeletionBytes() {
      return blockPendingDeletionBytes;
    }

    public synchronized void incrementBlockCount() {
      blockCount++;
    }

    /** Update for reading a block with the given length. */
    public synchronized void updateRead(long length) {
      readCount++;
      readBytes += length;
    }

    /** Update for writing a block with the given length. */
    public synchronized void updateWrite(long length, boolean overwrite) {
      if (!overwrite) {
        blockBytes += length;
      }
      writeCount++;
      writeBytes += length;
    }

    public synchronized void decDeletion(long deletedBytes, long processedBytes, long deletedBlockCount,
        long processedBlockCount) {
      blockBytes -= deletedBytes;
      blockCount -= deletedBlockCount;
      blockPendingDeletion -= processedBlockCount;
      blockPendingDeletionBytes -= processedBytes;
    }

    public synchronized void updateBlocks(long bytes, long count) {
      blockBytes = bytes;
      blockCount = count;
    }

    public synchronized ContainerDataProto.Builder setContainerDataProto(ContainerDataProto.Builder b) {
      if (blockBytes > 0) {
        b.setBytesUsed(blockBytes);
      }
      return b.setBlockCount(blockCount);
    }

    public synchronized ContainerReplicaProto.Builder setContainerReplicaProto(ContainerReplicaProto.Builder b) {
      return b.setReadBytes(readBytes)
          .setReadCount(readCount)
          .setWriteBytes(writeBytes)
          .setWriteCount(writeCount)
          .setUsed(blockBytes)
          .setKeyCount(blockCount);
    }

    public synchronized void setBlockPendingDeletion(long count, long bytes) {
      blockPendingDeletion = count;
      blockPendingDeletionBytes = bytes;
    }

    public synchronized void addBlockPendingDeletion(long count, long bytes) {
      blockPendingDeletion += count;
      blockPendingDeletionBytes += bytes;
    }

    public synchronized void resetBlockPendingDeletion() {
      blockPendingDeletion = 0;
      blockPendingDeletionBytes = 0;
    }

    public synchronized void assertRead(long expectedBytes, long expectedCount) {
      Preconditions.assertSame(expectedBytes, readBytes, "readBytes");
      Preconditions.assertSame(expectedCount, readCount, "readCount");
    }

    public synchronized void assertWrite(long expectedBytes, long expectedCount) {
      Preconditions.assertSame(expectedBytes, writeBytes, "writeBytes");
      Preconditions.assertSame(expectedCount, writeCount, "writeCount");
    }

    public synchronized void assertBlock(long expectedBytes, long expectedCount, long expectedPendingDeletion) {
      Preconditions.assertSame(expectedBytes, blockBytes, "blockBytes");
      Preconditions.assertSame(expectedCount, blockCount, "blockCount");
      Preconditions.assertSame(expectedPendingDeletion, blockPendingDeletion, "blockPendingDeletion");
    }

    public synchronized void setBlockCountForTesting(long count) {
      blockCount = count;
    }

    public synchronized void setBlockBytesForTesting(long bytes) {
      blockBytes = bytes;
    }

    @Override
    public synchronized String toString() {
      return "Statistics{read(" + readBytes + " bytes, #" + readCount + ")"
          + ", write(" + writeBytes + " bytes, #" + writeCount + ")"
          + ", block(" + blockBytes + " bytes, #" + blockCount
          + ", pendingDelete=" + blockPendingDeletion
          + ", pendingDeleteBytes=" + blockPendingDeletionBytes + ")}";
    }
  }
}
