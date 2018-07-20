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

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    ContainerLifeCycleState;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.max;

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

  // State of the Container
  private ContainerLifeCycleState state;

  private final int maxSizeGB;

  /** parameters for read/write statistics on the container. **/
  private final AtomicLong readBytes;
  private final AtomicLong writeBytes;
  private final AtomicLong readCount;
  private final AtomicLong writeCount;
  private final AtomicLong bytesUsed;
  private final AtomicLong keyCount;

  private HddsVolume volume;

  private long deleteTransactionId;

  /**
   * Number of pending deletion blocks in container.
   */
  private final AtomicInteger numPendingDeletionBlocks;

  /**
   * Creates a ContainerData Object, which holds metadata of the container.
   * @param type - ContainerType
   * @param containerId - ContainerId
   * @param size - container maximum size
   */
  protected ContainerData(ContainerType type, long containerId, int size) {
    this(type, containerId,
        ChunkLayOutVersion.getLatestVersion().getVersion(), size);
  }

  /**
   * Creates a ContainerData Object, which holds metadata of the container.
   * @param type - ContainerType
   * @param containerId - ContainerId
   * @param layOutVersion - Container layOutVersion
   * @param size - Container maximum size
   */
  protected ContainerData(ContainerType type, long containerId,
    int layOutVersion, int size) {
    Preconditions.checkNotNull(type);

    this.containerType = type;
    this.containerID = containerId;
    this.layOutVersion = layOutVersion;
    this.metadata = new TreeMap<>();
    this.state = ContainerLifeCycleState.OPEN;
    this.readCount = new AtomicLong(0L);
    this.readBytes =  new AtomicLong(0L);
    this.writeCount =  new AtomicLong(0L);
    this.writeBytes =  new AtomicLong(0L);
    this.bytesUsed = new AtomicLong(0L);
    this.keyCount = new AtomicLong(0L);
    this.maxSizeGB = size;
    this.numPendingDeletionBlocks = new AtomicInteger(0);
    this.deleteTransactionId = 0;
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
  public synchronized ContainerLifeCycleState getState() {
    return state;
  }

  /**
   * Set the state of the container.
   * @param state
   */
  public synchronized void setState(ContainerLifeCycleState state) {
    this.state = state;
  }

  /**
   * Return's maximum size of the container in GB.
   * @return maxSizeGB
   */
  public int getMaxSizeGB() {
    return maxSizeGB;
  }

  /**
   * Returns the layOutVersion of the actual container data format.
   * @return layOutVersion
   */
  public int getLayOutVersion() {
    return ChunkLayOutVersion.getChunkLayOutVersion(layOutVersion).getVersion();
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
   * Retuns metadata of the container.
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
    return ContainerLifeCycleState.OPEN == state;
  }

  /**
   * checks if the container is invalid.
   * @return - boolean
   */
  public synchronized boolean isValid() {
    return !(ContainerLifeCycleState.INVALID == state);
  }

  /**
   * checks if the container is closed.
   * @return - boolean
   */
  public synchronized  boolean isClosed() {
    return ContainerLifeCycleState.CLOSED == state;
  }

  /**
   * Marks this container as closed.
   */
  public synchronized void closeContainer() {
    // TODO: closed or closing here
    setState(ContainerLifeCycleState.CLOSED);
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
   * @param bytes the number of bytes write into the container.
   */
  public void incrWriteBytes(long bytes) {
    this.writeBytes.addAndGet(bytes);
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
  public HddsVolume getVolume() {
    return volume;
  }

  /**
   * Increments the number of keys in the container.
   */
  public void incrKeyCount() {
    this.keyCount.incrementAndGet();
  }

  /**
   * Decrements number of keys in the container.
   */
  public void decrKeyCount() {
    this.keyCount.decrementAndGet();
  }

  /**
   * Returns number of keys in the container.
   * @return key count
   */
  public long getKeyCount() {
    return this.keyCount.get();
  }

  /**
   * Set's number of keys in the container.
   * @param count
   */
  public void setKeyCount(long count) {
    this.keyCount.set(count);
  }

  /**
   * Increase the count of pending deletion blocks.
   *
   * @param numBlocks increment number
   */
  public void incrPendingDeletionBlocks(int numBlocks) {
    this.numPendingDeletionBlocks.addAndGet(numBlocks);
  }

  /**
   * Decrease the count of pending deletion blocks.
   *
   * @param numBlocks decrement number
   */
  public void decrPendingDeletionBlocks(int numBlocks) {
    this.numPendingDeletionBlocks.addAndGet(-1 * numBlocks);
  }

  /**
   * Get the number of pending deletion blocks.
   */
  public int getNumPendingDeletionBlocks() {
    return this.numPendingDeletionBlocks.get();
  }

  /**
   * Returns a ProtoBuf Message from ContainerData.
   *
   * @return Protocol Buffer Message
   */
  public abstract ContainerProtos.ContainerData getProtoBufMessage();

  /**
   * Sets deleteTransactionId to latest delete transactionId for the container.
   *
   * @param transactionId latest transactionId of the container.
   */
  public void updateDeleteTransactionId(long transactionId) {
    deleteTransactionId = max(transactionId, deleteTransactionId);
  }

  /**
   * Return the latest deleteTransactionId of the container.
   */
  public long getDeleteTransactionId() {
    return deleteTransactionId;
  }
}
