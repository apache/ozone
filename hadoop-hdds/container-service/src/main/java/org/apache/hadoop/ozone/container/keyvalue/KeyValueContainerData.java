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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collections;

import com.google.common.primitives.Longs;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerDataProto;
import org.apache.hadoop.hdds.utils.BatchOperation;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.yaml.snakeyaml.nodes.Tag;


import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.max;
import static org.apache.hadoop.ozone.OzoneConsts.DB_BLOCK_COUNT_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.CHUNKS_PATH;
import static org.apache.hadoop.ozone.OzoneConsts.DB_CONTAINER_BYTES_USED_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_TYPE;
import static org.apache.hadoop.ozone.OzoneConsts.METADATA_PATH;
import static org.apache.hadoop.ozone.OzoneConsts.DB_PENDING_DELETE_BLOCK_COUNT_KEY;

/**
 * This class represents the KeyValueContainer metadata, which is the
 * in-memory representation of container metadata and is represented on disk
 * by the .container file.
 */
public class KeyValueContainerData extends ContainerData {

  // Yaml Tag used for KeyValueContainerData.
  public static final Tag KEYVALUE_YAML_TAG = new Tag("KeyValueContainerData");

  // Fields need to be stored in .container file.
  private static final List<String> KV_YAML_FIELDS;

  // Path to Container metadata Level DB/RocksDB Store and .container file.
  private String metadataPath;

  //Type of DB used to store key to chunks mapping
  private String containerDBType;

  private File dbFile = null;

  /**
   * Number of pending deletion blocks in KeyValueContainer.
   */
  private final AtomicInteger numPendingDeletionBlocks;

  private long deleteTransactionId;

  private long blockCommitSequenceId;

  static {
    // Initialize YAML fields
    KV_YAML_FIELDS = Lists.newArrayList();
    KV_YAML_FIELDS.addAll(YAML_FIELDS);
    KV_YAML_FIELDS.add(METADATA_PATH);
    KV_YAML_FIELDS.add(CHUNKS_PATH);
    KV_YAML_FIELDS.add(CONTAINER_DB_TYPE);
  }

  /**
   * Constructs KeyValueContainerData object.
   * @param id - ContainerId
   * @param layOutVersion chunk layout
   * @param size - maximum size of the container in bytes
   */
  public KeyValueContainerData(long id, ChunkLayOutVersion layOutVersion,
      long size, String originPipelineId, String originNodeId) {
    super(ContainerProtos.ContainerType.KeyValueContainer, id, layOutVersion,
        size, originPipelineId, originNodeId);
    this.numPendingDeletionBlocks = new AtomicInteger(0);
    this.deleteTransactionId = 0;
  }

  public KeyValueContainerData(ContainerData source) {
    super(source);
    Preconditions.checkArgument(source.getContainerType()
        == ContainerProtos.ContainerType.KeyValueContainer);
    this.numPendingDeletionBlocks = new AtomicInteger(0);
    this.deleteTransactionId = 0;
  }


  /**
   * Sets Container dbFile. This should be called only during creation of
   * KeyValue container.
   * @param containerDbFile
   */
  public void setDbFile(File containerDbFile) {
    dbFile = containerDbFile;
  }

  /**
   * Returns container DB file.
   * @return dbFile
   */
  public File getDbFile() {
    return dbFile;
  }

  /**
   * Returns container metadata path.
   * @return - Physical path where container file and checksum is stored.
   */
  public String getMetadataPath() {
    return metadataPath;
  }

  /**
   * Sets container metadata path.
   *
   * @param path - String.
   */
  public void setMetadataPath(String path) {
    this.metadataPath = path;
  }

  /**
   * Returns the path to base dir of the container.
   * @return Path to base dir
   */
  public String getContainerPath() {
    return new File(metadataPath).getParent();
  }

  /**
   * Returns the blockCommitSequenceId.
   */
  public long getBlockCommitSequenceId() {
    return blockCommitSequenceId;
  }

  /**
   * updates the blockCommitSequenceId.
   */
  public void updateBlockCommitSequenceId(long id) {
    this.blockCommitSequenceId = id;
  }

  /**
   * Returns the DBType used for the container.
   * @return containerDBType
   */
  public String getContainerDBType() {
    return containerDBType;
  }

  /**
   * Sets the DBType used for the container.
   * @param containerDBType
   */
  public void setContainerDBType(String containerDBType) {
    this.containerDBType = containerDBType;
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

  /**
   * Returns a ProtoBuf Message from ContainerData.
   *
   * @return Protocol Buffer Message
   */
  public ContainerDataProto getProtoBufMessage() {
    ContainerDataProto.Builder builder = ContainerDataProto.newBuilder();
    builder.setContainerID(this.getContainerID());
    builder.setContainerPath(this.getContainerPath());
    builder.setState(this.getState());

    for (Map.Entry<String, String> entry : getMetadata().entrySet()) {
      ContainerProtos.KeyValue.Builder keyValBuilder =
          ContainerProtos.KeyValue.newBuilder();
      builder.addMetadata(keyValBuilder.setKey(entry.getKey())
          .setValue(entry.getValue()).build());
    }

    if (this.getBytesUsed() >= 0) {
      builder.setBytesUsed(this.getBytesUsed());
    }

    if(this.getContainerType() != null) {
      builder.setContainerType(ContainerProtos.ContainerType.KeyValueContainer);
    }

    return builder.build();
  }

  public static List<String> getYamlFields() {
    return Collections.unmodifiableList(KV_YAML_FIELDS);
  }

  /**
   * Update DB counters related to block metadata.
   * @param db - Reference to container DB.
   * @param batchOperation - Batch Operation to batch DB operations.
   * @param deletedBlockCount - Number of blocks deleted.
   * @throws IOException
   */
  public void updateAndCommitDBCounters(
      ReferenceCountedDB db, BatchOperation batchOperation,
      int deletedBlockCount) throws IOException {
    // Set Bytes used and block count key.
    batchOperation.put(DB_CONTAINER_BYTES_USED_KEY,
        Longs.toByteArray(getBytesUsed()));
    batchOperation.put(DB_BLOCK_COUNT_KEY, Longs.toByteArray(
        getKeyCount() - deletedBlockCount));
    batchOperation.put(DB_PENDING_DELETE_BLOCK_COUNT_KEY, Longs.toByteArray(
        getNumPendingDeletionBlocks() - deletedBlockCount));
    db.getStore().writeBatch(batchOperation);
  }


}
