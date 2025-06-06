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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.yaml.snakeyaml.Yaml;

/**
 * ContainerData is the in-memory representation of container metadata and is
 * represented on disk by the .container file.
 */
public abstract class ContainerData {

  //Type of the container.
  // For now, we support only KeyValueContainer.
  private final ContainerType containerType;

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

  /** parameters for read/write statistics on the container. **/
  private final AtomicLong readBytes;
  private final AtomicLong writeBytes;
  private final AtomicLong readCount;
  private final AtomicLong writeCount;
  private final AtomicLong bytesUsed;
  private final AtomicLong blockCount;

  private HddsVolume volume;

  private String checksum;

  private boolean isEmpty;

  private int replicaIndex;

  /** Timestamp of last data scan (milliseconds since Unix Epoch).
   * {@code null} if not yet scanned (or timestamp not recorded,
   * eg. in prior versions). */
  private Long dataScanTimestamp; // for serialization
  private transient Optional<Instant> lastDataScanTime = Optional.empty();

  public static final Charset CHARSET_ENCODING = StandardCharsets.UTF_8;
  private static final String DUMMY_CHECKSUM = new String(new byte[64],
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
    Preconditions.checkNotNull(type);

    this.containerType = type;
    this.containerID = containerId;
    this.layOutVersion = layoutVersion.getVersion();
    this.metadata = new TreeMap<>();
    this.state = ContainerDataProto.State.OPEN;
    this.readCount = new AtomicLong(0L);
    this.readBytes =  new AtomicLong(0L);
    this.writeCount =  new AtomicLong(0L);
    this.writeBytes =  new AtomicLong(0L);
    this.bytesUsed = new AtomicLong(0L);
    this.blockCount = new AtomicLong(0L);
    this.maxSize = size;
    this.originPipelineId = originPipelineId;
    this.originNodeId = originNodeId;
    this.isEmpty = false;
    setChecksumTo0ByteArray();
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

  /**
   * Returns the path to base dir of the container.
   * @return Path to base dir.
   */
  public abstract String getContainerPath();

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
    Preconditions.checkState(!committedSpace);

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

  /**
   * Get the number of bytes read from the container.
   * @return the number of bytes read from the container.
   */
  public long getReadBytes() {
    return readBytes.get();
  }

  /**
   * Increase the number of bytes read from the container.
   * @param bytes number of bytes read.
   */
  public void incrReadBytes(long bytes) {
    this.readBytes.addAndGet(bytes);
  }

  /**
   * Get the number of times the container is read.
   * @return the number of times the container is read.
   */
  public long getReadCount() {
    return readCount.get();
  }

  /**
   * Increase the number of container read count by 1.
   */
  public void incrReadCount() {
    this.readCount.incrementAndGet();
  }

  /**
   * Get the number of bytes write into the container.
   * @return the number of bytes write into the container.
   */
  public long getWriteBytes() {
    return writeBytes.get();
  }

  /**
   * Increase the number of bytes write into the container.
   * Also decrement committed bytes against the bytes written.
   * @param bytes the number of bytes write into the container.
   */
  public void incrWriteBytes(long bytes) {
    this.writeBytes.addAndGet(bytes);
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
   * Get the number of writes into the container.
   * @return the number of writes into the container.
   */
  public long getWriteCount() {
    return writeCount.get();
  }

  /**
   * Increase the number of writes into the container by 1.
   */
  public void incrWriteCount() {
    this.writeCount.incrementAndGet();
  }

  /**
   * Sets the number of bytes used by the container.
   * @param used
   */
  public void setBytesUsed(long used) {
    this.bytesUsed.set(used);
  }

  /**
   * Get the number of bytes used by the container.
   * @return the number of bytes used by the container.
   */
  public long getBytesUsed() {
    return bytesUsed.get();
  }

  /**
   * Increase the number of bytes used by the container.
   * @param used number of bytes used by the container.
   * @return the current number of bytes used by the container afert increase.
   */
  public long incrBytesUsed(long used) {
    return this.bytesUsed.addAndGet(used);
  }

  /**
   * Decrease the number of bytes used by the container.
   * @param reclaimed the number of bytes reclaimed from the container.
   * @return the current number of bytes used by the container after decrease.
   */
  public long decrBytesUsed(long reclaimed) {
    return this.bytesUsed.addAndGet(-1L * reclaimed);
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

  /**
   * Increments the number of blocks in the container.
   */
  public void incrBlockCount() {
    this.blockCount.incrementAndGet();
  }

  /**
   * Decrements number of blocks in the container.
   */
  public void decrBlockCount() {
    this.blockCount.decrementAndGet();
  }

  /**
   * Decrease the count of blocks (blocks) in the container.
   *
   * @param deletedBlockCount
   */
  public void decrBlockCount(long deletedBlockCount) {
    this.blockCount.addAndGet(-1 * deletedBlockCount);
  }

  /**
   * Returns number of blocks in the container.
   * @return block count
   */
  public long getBlockCount() {
    return this.blockCount.get();
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

  /**
   * Set's number of blocks in the container.
   * @param count
   */
  public void setBlockCount(long count) {
    this.blockCount.set(count);
  }

  public void setChecksumTo0ByteArray() {
    this.checksum = DUMMY_CHECKSUM;
  }

  public void setChecksum(String checkSum) {
    this.checksum = checkSum;
  }

  public String getChecksum() {
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
   * {@link ContainerData#DUMMY_CHECKSUM}. After the checksum is calculated,
   * the checksum field is updated with this value.
   *
   * @param yaml Yaml for ContainerType to get the ContainerData as Yaml String
   * @throws IOException
   */
  public void computeAndSetChecksum(Yaml yaml) throws IOException {
    // Set checksum to dummy value - 0 byte array, to calculate the checksum
    // of rest of the data.
    setChecksumTo0ByteArray();

    // Dump yaml data into a string to compute its checksum
    String containerDataYamlStr = yaml.dump(this);

    this.checksum = ContainerUtils.getChecksum(containerDataYamlStr);
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

  public void updateReadStats(long length) {
    incrReadCount();
    incrReadBytes(length);
  }

  public void updateWriteStats(long bytesWritten, boolean overwrite) {
    if (!overwrite) {
      incrBytesUsed(bytesWritten);
    }
    incrWriteCount();
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
}
