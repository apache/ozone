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

import static java.lang.Math.max;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.INVALID_CONTAINER_STATE;
import static org.apache.hadoop.ozone.OzoneConsts.BLOCK_COMMIT_SEQUENCE_ID;
import static org.apache.hadoop.ozone.OzoneConsts.BLOCK_COUNT;
import static org.apache.hadoop.ozone.OzoneConsts.CHUNKS_PATH;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_BYTES_USED;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DATA_CHECKSUM;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_TYPE;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_TYPE_ROCKSDB;
import static org.apache.hadoop.ozone.OzoneConsts.DELETE_TRANSACTION_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.DELETING_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.METADATA_PATH;
import static org.apache.hadoop.ozone.OzoneConsts.PENDING_DELETE_BLOCK_BYTES;
import static org.apache.hadoop.ozone.OzoneConsts.PENDING_DELETE_BLOCK_COUNT;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V1;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_VERSION;
import static org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition.getContainerKeyPrefix;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.yaml.snakeyaml.nodes.Tag;

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
  private String containerDBType = CONTAINER_DB_TYPE_ROCKSDB;

  private File dbFile = null;

  private String schemaVersion;

  private long deleteTransactionId;

  private long blockCommitSequenceId;

  private final Set<Long> finalizedBlockSet;

  static {
    // Initialize YAML fields
    KV_YAML_FIELDS = Lists.newArrayList();
    KV_YAML_FIELDS.addAll(YAML_FIELDS);
    KV_YAML_FIELDS.add(METADATA_PATH);
    KV_YAML_FIELDS.add(CHUNKS_PATH);
    KV_YAML_FIELDS.add(CONTAINER_DB_TYPE);
    KV_YAML_FIELDS.add(SCHEMA_VERSION);
  }

  /**
   * Constructs KeyValueContainerData object.
   * @param id - ContainerId
   * @param layoutVersion container layout
   * @param size - maximum size of the container in bytes
   */
  public KeyValueContainerData(long id, ContainerLayoutVersion layoutVersion,
      long size, String originPipelineId, String originNodeId) {
    super(ContainerProtos.ContainerType.KeyValueContainer, id, layoutVersion,
        size, originPipelineId, originNodeId);
    this.deleteTransactionId = 0;
    finalizedBlockSet =  ConcurrentHashMap.newKeySet();
  }

  public KeyValueContainerData(KeyValueContainerData source) {
    super(source);
    Preconditions.checkArgument(source.getContainerType()
        == ContainerProtos.ContainerType.KeyValueContainer);
    this.deleteTransactionId = 0;
    this.schemaVersion = source.getSchemaVersion();
    finalizedBlockSet = ConcurrentHashMap.newKeySet();
  }

  /**
   * @param version The schema version indicating the table layout of the
   * container's database.
   */
  public void setSchemaVersion(String version) {
    schemaVersion = version;
  }

  /**
   * @return The schema version describing the container database's table
   * layout.
   */
  public String getSchemaVersion() {
    return schemaVersion;
  }

  /**
   * Returns schema version or the default value when the
   * {@link KeyValueContainerData#schemaVersion} is null. The default value can
   * be referred to {@link KeyValueContainerUtil#isSameSchemaVersion}.
   *
   * @return Schema version as a string.
   * @throws UnsupportedOperationException If no valid schema version is found.
   */
  public String getSupportedSchemaVersionOrDefault() {
    String[] versions = {SCHEMA_V1, SCHEMA_V2, SCHEMA_V3};

    for (String version : versions) {
      if (this.hasSchema(version)) {
        return version;
      }
    }
    throw new UnsupportedOperationException("No valid schema version found.");
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
  @Override
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
  @Override
  public String getContainerPath() {
    return new File(metadataPath).getParent();
  }

  /**
   * Returns the blockCommitSequenceId.
   */
  @Override
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
  public void incrPendingDeletionBlocks(long numBlocks, long bytes) {
    getStatistics().addBlockPendingDeletion(numBlocks, bytes);
  }

  /**
   * Get the number of pending deletion blocks.
   */
  public long getNumPendingDeletionBlocks() {
    return getStatistics().getBlockPendingDeletion();
  }

  /**
   * Get the total bytes used by pending deletion blocks.
   */
  public long getBlockPendingDeletionBytes() {
    return getStatistics().getBlockPendingDeletionBytes();
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

  ContainerReplicaProto buildContainerReplicaProto() throws StorageContainerException {
    return getStatistics().setContainerReplicaProto(ContainerReplicaProto.newBuilder())
        .setContainerID(getContainerID())
        .setState(getContainerReplicaProtoState(getState()))
        .setIsEmpty(isEmpty())
        .setOriginNodeId(getOriginNodeId())
        .setReplicaIndex(getReplicaIndex())
        .setBlockCommitSequenceId(getBlockCommitSequenceId())
        .setDeleteTransactionId(getDeleteTransactionId())
        .setDataChecksum(getDataChecksum())
        .build();
  }

  // TODO remove one of the State from proto
  static ContainerReplicaProto.State getContainerReplicaProtoState(ContainerDataProto.State state)
      throws StorageContainerException {
    switch (state) {
    case OPEN:
      return ContainerReplicaProto.State.OPEN;
    case CLOSING:
      return ContainerReplicaProto.State.CLOSING;
    case QUASI_CLOSED:
      return ContainerReplicaProto.State.QUASI_CLOSED;
    case CLOSED:
      return ContainerReplicaProto.State.CLOSED;
    case UNHEALTHY:
      return ContainerReplicaProto.State.UNHEALTHY;
    case DELETED:
      return ContainerReplicaProto.State.DELETED;
    default:
      throw new StorageContainerException("Invalid container state: " + state, INVALID_CONTAINER_STATE);
    }
  }

  /**
   * Add the given localID of a block to the finalizedBlockSet.
   */
  public void addToFinalizedBlockSet(long localID) {
    finalizedBlockSet.add(localID);
  }

  public Set<Long> getFinalizedBlockSet() {
    return finalizedBlockSet;
  }

  public boolean isFinalizedBlockExist(long localID) {
    return finalizedBlockSet.contains(localID);
  }

  public void clearFinalizedBlock(DBHandle db) throws IOException {
    if (!finalizedBlockSet.isEmpty()) {
      // delete from db and clear memory
      // Should never fail.
      Objects.requireNonNull(db, "db == null");
      try (BatchOperation batch = db.getStore().getBatchHandler().initBatchOperation()) {
        db.getStore().getFinalizeBlocksTable().deleteBatchWithPrefix(batch, containerPrefix());
        db.getStore().getBatchHandler().commitBatchOperation(batch);
      }
      finalizedBlockSet.clear();
    }
  }

  /**
   * Returns a ProtoBuf Message from ContainerData.
   *
   * @return Protocol Buffer Message
   */
  @Override
  @JsonIgnore
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

    getStatistics().setContainerDataProto(builder);

    if (this.getContainerType() != null) {
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
   * @param releasedBytes - Number of bytes released.
   * @throws IOException
   */
  public void updateAndCommitDBCounters(DBHandle db,
      BatchOperation batchOperation, int deletedBlockCount,
      long releasedBytes) throws IOException {
    Table<String, Long> metadataTable = db.getStore().getMetadataTable();

    // Set Bytes used and block count key.
    final BlockByteAndCounts b = getStatistics().getBlockByteAndCounts();
    metadataTable.putWithBatch(batchOperation, getBytesUsedKey(), b.getBytes() - releasedBytes);
    metadataTable.putWithBatch(batchOperation, getBlockCountKey(), b.getCount() - deletedBlockCount);
    metadataTable.putWithBatch(batchOperation, getPendingDeleteBlockCountKey(),
        b.getPendingDeletion() - deletedBlockCount);
    if (VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION)) {
      metadataTable.putWithBatch(batchOperation, getPendingDeleteBlockBytesKey(),
          b.getPendingDeletionBytes() - releasedBytes);
    }

    db.getStore().getBatchHandler().commitBatchOperation(batchOperation);
  }

  public void resetPendingDeleteBlockCount(DBHandle db) throws IOException {
    // Reset the in memory metadata.
    getStatistics().resetBlockPendingDeletion();
    // Reset the metadata on disk.
    Table<String, Long> metadataTable = db.getStore().getMetadataTable();
    metadataTable.put(getPendingDeleteBlockCountKey(), 0L);
    if (VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION)) {
      metadataTable.put(getPendingDeleteBlockBytesKey(), 0L);
    }
  }

  // NOTE: Below are some helper functions to format keys according
  // to container schemas, we should use them instead of using
  // raw const variables defined.

  public String getBlockKey(long localID) {
    return formatKey(Long.toString(localID));
  }

  public String getDeletingBlockKey(long localID) {
    return formatKey(DELETING_KEY_PREFIX + localID);
  }

  public String getDeleteTxnKey(long txnID) {
    return formatKey(Long.toString(txnID));
  }

  public String getLatestDeleteTxnKey() {
    return formatKey(DELETE_TRANSACTION_KEY);
  }

  public String getBcsIdKey() {
    return formatKey(BLOCK_COMMIT_SEQUENCE_ID);
  }

  public String getBlockCountKey() {
    return formatKey(BLOCK_COUNT);
  }

  public String getBytesUsedKey() {
    return formatKey(CONTAINER_BYTES_USED);
  }

  public String getPendingDeleteBlockCountKey() {
    return formatKey(PENDING_DELETE_BLOCK_COUNT);
  }

  public String getContainerDataChecksumKey() {
    return formatKey(CONTAINER_DATA_CHECKSUM);
  }
  
  public String getPendingDeleteBlockBytesKey() {
    return formatKey(PENDING_DELETE_BLOCK_BYTES);
  }

  public String getDeletingBlockKeyPrefix() {
    return formatKey(DELETING_KEY_PREFIX);
  }

  public KeyPrefixFilter getUnprefixedKeyFilter() {
    String schemaPrefix = containerPrefix();
    return KeyPrefixFilter.newFilter(schemaPrefix + "#", true);
  }

  public KeyPrefixFilter getDeletingBlockKeyFilter() {
    return KeyPrefixFilter.newFilter(getDeletingBlockKeyPrefix());
  }

  /**
   * Schema v3 use a prefix as startKey,
   * for other schemas just return null.
   */
  public String startKeyEmpty() {
    if (hasSchema(SCHEMA_V3)) {
      return getContainerKeyPrefix(getContainerID());
    }
    return null;
  }

  /**
   * Schema v3 use containerID as key prefix,
   * for other schemas just return null.
   */
  public String containerPrefix() {
    if (hasSchema(SCHEMA_V3)) {
      return getContainerKeyPrefix(getContainerID());
    }
    return "";
  }

  /**
   * Format the raw key to a schema specific format key.
   * Schema v3 use container ID as key prefix,
   * for other schemas just return the raw key.
   * @param key raw key
   * @return formatted key
   */
  private String formatKey(String key) {
    if (hasSchema(SCHEMA_V3)) {
      key = getContainerKeyPrefix(getContainerID()) + key;
    }
    return key;
  }

  public boolean hasSchema(String version) {
    return KeyValueContainerUtil.isSameSchemaVersion(schemaVersion, version);
  }

}
