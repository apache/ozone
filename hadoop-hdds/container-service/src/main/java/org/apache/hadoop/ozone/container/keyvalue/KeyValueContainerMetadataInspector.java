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

import static org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil.isSameSchemaVersion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerInspector;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreWithIncrementalChunkList;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container inspector for key value container metadata. It is capable of
 * logging metadata information about a container, and repairing the metadata
 * database values of #BLOCKCOUNT and #BYTESUSED.
 *
 * To enable this inspector in inspect mode, pass the java system property
 * -Dozone.datanode.container.metadata.inspector=inspect on datanode startup.
 * This will cause the inspector to log metadata information about all
 * containers on datanode startup whose aggregate values of #BLOCKCOUNT and
 * #BYTESUSED do not match the sum of their parts in the database at the
 * ERROR level, and information about correct containers at the TRACE level.
 * Changing the `inspect` argument to `repair` will update these aggregate
 * values to match the database.
 *
 * When run, the inspector will output json to the logger named in the
 * {@link KeyValueContainerMetadataInspector#REPORT_LOG} variable. The log4j
 * configuration can be modified to send this output to a separate file
 * without log information prefixes interfering with the json. For example:
 *
 * log4j.logger.ContainerMetadataInspectorReport=INFO,inspectorAppender
 * log4j.appender.inspectorAppender=org.apache.log4j.FileAppender
 * log4j.appender.inspectorAppender.File=${hadoop.log.dir}/\
 * containerMetadataInspector.log
 * log4j.appender.inspectorAppender.layout=org.apache.log4j.PatternLayout
 */
public class KeyValueContainerMetadataInspector implements ContainerInspector {
  private static final Logger LOG = LoggerFactory.getLogger(KeyValueContainerMetadataInspector.class);
  public static final Logger REPORT_LOG = LoggerFactory.getLogger("ContainerMetadataInspectorReport");

  public static final String SYSTEM_PROPERTY = "ozone.datanode.container.metadata.inspector";

  private Mode mode;

  /**
   * The mode to run the inspector in.
   */
  public enum Mode {
    REPAIR("repair"),
    INSPECT("inspect"),
    OFF("off");

    private final String name;

    Mode(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public KeyValueContainerMetadataInspector(Mode mode) {
    this.mode = mode;
  }

  public KeyValueContainerMetadataInspector() {
    mode = Mode.OFF;
  }

  /**
   * Validate configuration here so that an invalid config value is only
   * logged once, and not once per container.
   */
  @Override
  public boolean load() {
    String propertyValue = System.getProperty(SYSTEM_PROPERTY);
    boolean propertyPresent =
        (propertyValue != null && !propertyValue.isEmpty());
    boolean propertyValid = false;

    if (propertyPresent) {
      if (propertyValue.equals(Mode.REPAIR.toString())) {
        mode = Mode.REPAIR;
        propertyValid = true;
      } else if (propertyValue.equals(Mode.INSPECT.toString())) {
        mode = Mode.INSPECT;
        propertyValid = true;
      }

      if (propertyValid) {
        LOG.info("Container metadata inspector enabled in {} mode. Report" +
            "will be output to the {} log.", mode, REPORT_LOG.getName());
      } else {
        mode = Mode.OFF;
        LOG.error("{} system property specified with invalid mode {}. " +
                "Valid options are {} and {}. Container metadata inspection " +
                "will not be run.", SYSTEM_PROPERTY, propertyValue,
            Mode.REPAIR, Mode.INSPECT);
      }
    } else {
      mode = Mode.OFF;
    }

    return propertyPresent && propertyValid;
  }

  @Override
  public void unload() {
    mode = Mode.OFF;
  }

  @Override
  public boolean isReadOnly() {
    return mode != Mode.REPAIR;
  }

  @Override
  public void process(ContainerData containerData, DatanodeStore store) {
    process(containerData, store, REPORT_LOG);
  }

  public String process(ContainerData containerData, DatanodeStore store,
      Logger log) {
    // If the system property to process container metadata was not
    // specified, or the inspector is unloaded, this method is a no-op.
    if (mode == Mode.OFF) {
      return null;
    }

    KeyValueContainerData kvData = null;
    if (containerData instanceof KeyValueContainerData) {
      kvData = (KeyValueContainerData) containerData;
    } else {
      LOG.error("This inspector only works on KeyValueContainers. Inspection " +
          "will not be run for container {}", containerData.getContainerID());
      return null;
    }

    ObjectNode containerJson = inspectContainer(kvData, store);
    boolean correct = checkAndRepair(containerJson, kvData, store);

    String jsonReport = JsonUtils.toJsonStringWIthIndent(containerJson);
    if (log != null) {
      if (correct) {
        log.trace(jsonReport);
      } else {
        log.error(jsonReport);
      }
    }

    return jsonReport;
  }

  static ObjectNode inspectContainer(KeyValueContainerData containerData,
      DatanodeStore store) {

    ObjectNode containerJson = JsonUtils.createObjectNode(null);

    try {
      // Build top level container properties.
      containerJson.put("containerID", containerData.getContainerID());
      String schemaVersion = containerData.getSchemaVersion();
      containerJson.put("schemaVersion", schemaVersion);
      containerJson.put("containerState", containerData.getState().toString());
      containerJson.put("currentDatanodeID",
          containerData.getVolume().getDatanodeUuid());
      containerJson.put("originDatanodeID", containerData.getOriginNodeId());

      // Build DB metadata values.
      // Assuming getDBMetadataJson and getAggregateValues methods return ObjectNode and are refactored to use Jackson
      ObjectNode dBMetadata = getDBMetadataJson(store.getMetadataTable(), containerData);
      containerJson.set("dBMetadata", dBMetadata);

      // Build aggregate values.
      ObjectNode aggregates = getAggregateValues(store, containerData, schemaVersion);
      containerJson.set("aggregates", aggregates);

      // Build info about chunks directory.
      // Assuming getChunksDirectoryJson method returns ObjectNode and is refactored to use Jackson
      ObjectNode chunksDirectory = getChunksDirectoryJson(new File(containerData.getChunksPath()));
      containerJson.set("chunksDirectory", chunksDirectory);
    } catch (IOException ex) {
      LOG.error("Inspecting container {} failed",
          containerData.getContainerID(), ex);
    }

    return containerJson;
  }

  static ObjectNode getDBMetadataJson(Table<String, Long> metadataTable,
      KeyValueContainerData containerData) throws IOException {
    ObjectNode dBMetadata = JsonUtils.createObjectNode(null);

    dBMetadata.put(OzoneConsts.BLOCK_COUNT,
        metadataTable.get(containerData.getBlockCountKey()));
    dBMetadata.put(OzoneConsts.CONTAINER_BYTES_USED,
        metadataTable.get(containerData.getBytesUsedKey()));
    dBMetadata.put(OzoneConsts.PENDING_DELETE_BLOCK_COUNT,
        metadataTable.get(containerData.getPendingDeleteBlockCountKey()));
    if (metadataTable.get(containerData.getPendingDeleteBlockBytesKey()) != null) {
      dBMetadata.put(OzoneConsts.PENDING_DELETE_BLOCK_BYTES,
          metadataTable.get(containerData.getPendingDeleteBlockBytesKey()));
    }
    dBMetadata.put(OzoneConsts.DELETE_TRANSACTION_KEY,
        metadataTable.get(containerData.getLatestDeleteTxnKey()));
    dBMetadata.put(OzoneConsts.BLOCK_COMMIT_SEQUENCE_ID,
        metadataTable.get(containerData.getBcsIdKey()));

    return dBMetadata;
  }

  private static ObjectNode getAggregateValues(DatanodeStore store,
      KeyValueContainerData containerData, String schemaVersion)
      throws IOException {

    ObjectNode aggregates = JsonUtils.createObjectNode(null);

    long usedBytesTotal = 0;
    long blockCountTotal = 0;
    // Count normal blocks.
    try (BlockIterator<BlockData> blockIter =
             store.getBlockIterator(containerData.getContainerID(),
                 containerData.getUnprefixedKeyFilter())) {

      while (blockIter.hasNext()) {
        blockCountTotal++;
        usedBytesTotal += getBlockLength(blockIter.nextBlock());
      }
    }

    // Count pending delete blocks.
    final PendingDelete pendingDelete = getAggregatePendingDelete(store, containerData, schemaVersion);

    if (isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V1)) {
      blockCountTotal += pendingDelete.getCount();
      usedBytesTotal += pendingDelete.getBytes();
    }

    aggregates.put("blockCount", blockCountTotal);
    aggregates.put("usedBytes", usedBytesTotal);
    pendingDelete.addToJson(aggregates);

    return aggregates;
  }

  public static PendingDelete getAggregatePendingDelete(DatanodeStore store,
      KeyValueContainerData containerData, String schemaVersion)
      throws IOException {
    final PendingDelete pendingDelete;
    if (isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V1)) {
      long pendingDeleteBlockCountTotal = 0;
      long pendingDeleteBytes = 0;
      try (BlockIterator<BlockData> blockIter =
               store.getBlockIterator(containerData.getContainerID(),
                   containerData.getDeletingBlockKeyFilter())) {

        while (blockIter.hasNext()) {
          pendingDeleteBlockCountTotal++;
          final long bytes = getBlockLength(blockIter.nextBlock());
          pendingDeleteBytes += bytes;
        }
      }
      pendingDelete = new PendingDelete(
          pendingDeleteBlockCountTotal, pendingDeleteBytes);
    } else if (isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V2)) {
      DatanodeStoreSchemaTwoImpl schemaTwoStore =
          (DatanodeStoreSchemaTwoImpl) store;
      pendingDelete =
          countPendingDeletesSchemaV2(schemaTwoStore, containerData);
    } else if (isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V3)) {
      DatanodeStoreSchemaThreeImpl schemaThreeStore =
          (DatanodeStoreSchemaThreeImpl) store;
      pendingDelete =
          countPendingDeletesSchemaV3(schemaThreeStore, containerData);
    } else {
      throw new IOException("Failed to process deleted blocks for unknown " +
          "container schema " + schemaVersion);
    }
    return pendingDelete;
  }

  static ObjectNode getChunksDirectoryJson(File chunksDir) throws IOException {
    ObjectNode chunksDirectory = JsonUtils.createObjectNode(null);

    chunksDirectory.put("path", chunksDir.getAbsolutePath());
    boolean chunksDirPresent = FileUtils.isDirectory(chunksDir);
    chunksDirectory.put("present", chunksDirPresent);

    long fileCount = 0;
    if (chunksDirPresent) {
      try (Stream<Path> stream = Files.list(chunksDir.toPath())) {
        fileCount = stream.count();
      }
    }
    chunksDirectory.put("fileCount", fileCount);

    return chunksDirectory;
  }

  private boolean checkAndRepair(ObjectNode parent,
      KeyValueContainerData containerData, DatanodeStore store) {
    ArrayNode errors = JsonUtils.createArrayNode();
    boolean passed = true;
    Table<String, Long> metadataTable = store.getMetadataTable();
    ObjectNode dBMetadata = (ObjectNode) parent.get("dBMetadata");
    ObjectNode aggregates = (ObjectNode) parent.get("aggregates");

    // Check and repair block count.
    JsonNode blockCountDB = dBMetadata.get(OzoneConsts.BLOCK_COUNT);
    JsonNode blockCountAggregate = aggregates.get("blockCount");

    // If block count is absent from the DB, it is only an error if there are
    // a non-zero amount of block keys in the DB.
    long blockCountDBLong = blockCountDB.isNull() ? 0 : blockCountDB.asLong();
    if (blockCountDBLong != blockCountAggregate.asLong()) {
      passed = false;

      BooleanSupplier keyRepairAction = () -> {
        boolean repaired = false;
        try {
          metadataTable.put(containerData.getBlockCountKey(),
              blockCountAggregate.asLong());
          repaired = true;
        } catch (IOException ex) {
          LOG.error("Error repairing block count for container {}.",
              containerData.getContainerID(), ex);
        }
        return repaired;
      };

      ObjectNode blockCountError = buildErrorAndRepair("dBMetadata." +
              OzoneConsts.BLOCK_COUNT, blockCountAggregate, blockCountDB,
          keyRepairAction);
      errors.add(blockCountError);
    }

    // Check and repair used bytes.
    JsonNode usedBytesDB = parent.path("dBMetadata").path(OzoneConsts.CONTAINER_BYTES_USED);
    JsonNode usedBytesAggregate = parent.path("aggregates").path("usedBytes");

    // If used bytes is absent from the DB, it is only an error if there is
    // a non-zero aggregate of used bytes among the block keys.
    long usedBytesDBLong = 0;
    if (!usedBytesDB.isNull()) {
      usedBytesDBLong = usedBytesDB.asLong();
    }

    if (usedBytesDBLong != usedBytesAggregate.asLong()) {
      passed = false;

      BooleanSupplier keyRepairAction = () -> {
        boolean repaired = false;
        try {
          metadataTable.put(containerData.getBytesUsedKey(),
              usedBytesAggregate.asLong());
          repaired = true;
        } catch (IOException ex) {
          LOG.error("Error repairing used bytes for container {}.",
              containerData.getContainerID(), ex);
        }
        return repaired;
      };

      ObjectNode usedBytesError = buildErrorAndRepair("dBMetadata." +
              OzoneConsts.CONTAINER_BYTES_USED, usedBytesAggregate, usedBytesDB, keyRepairAction);
      errors.add(usedBytesError);
    }

    // check and repair if db delete count mismatches delete transaction count.
    JsonNode pendingDeleteCountDB = dBMetadata.path(
        OzoneConsts.PENDING_DELETE_BLOCK_COUNT);
    final long dbDeleteCount = jsonToLong(pendingDeleteCountDB);
    final JsonNode pendingDeleteCountAggregate = aggregates.path(PendingDelete.COUNT);
    final long deleteTransactionCount = jsonToLong(pendingDeleteCountAggregate);
    if (dbDeleteCount != deleteTransactionCount) {
      passed = false;

      final BooleanSupplier deleteCountRepairAction = () -> {
        final String key = containerData.getPendingDeleteBlockCountKey();
        try {
          // set delete block count metadata table to delete transaction count
          metadataTable.put(key, deleteTransactionCount);
          return true;
        } catch (IOException ex) {
          LOG.error("Failed to reset {} for container {}.",
              key, containerData.getContainerID(), ex);
        }
        return false;
      };

      final ObjectNode deleteCountError = buildErrorAndRepair(
          "dBMetadata." + OzoneConsts.PENDING_DELETE_BLOCK_COUNT,
          pendingDeleteCountAggregate, pendingDeleteCountDB, deleteCountRepairAction);
      errors.add(deleteCountError);
    }

    if (VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION)) {
      // check and repair if db delete bytes mismatches delete transaction
      JsonNode pendingDeletionBlockSize = dBMetadata.path(
          OzoneConsts.PENDING_DELETE_BLOCK_BYTES);
      final long dbDeleteBytes = jsonToLong(pendingDeletionBlockSize);
      final JsonNode pendingDeleteBytesAggregate = aggregates.path(PendingDelete.BYTES);
      final long deleteTransactionBytes = jsonToLong(pendingDeleteBytesAggregate);
      if (dbDeleteBytes != deleteTransactionBytes) {
        passed = false;
        final BooleanSupplier deleteBytesRepairAction = () -> {
          final String key = containerData.getPendingDeleteBlockBytesKey();
          try {
            metadataTable.put(key, deleteTransactionBytes);
          } catch (IOException ex) {
            LOG.error("Failed to reset {} for container {}.",
                key, containerData.getContainerID(), ex);
          }
          return false;
        };
        final ObjectNode deleteBytesError = buildErrorAndRepair(
            "dBMetadata." + OzoneConsts.PENDING_DELETE_BLOCK_BYTES,
            pendingDeleteBytesAggregate, pendingDeletionBlockSize, deleteBytesRepairAction);
        errors.add(deleteBytesError);
      }
    }

    // check and repair chunks dir.
    JsonNode chunksDirPresent = parent.path("chunksDirectory").path("present");
    if (!chunksDirPresent.asBoolean()) {
      passed = false;

      BooleanSupplier dirRepairAction = () -> {
        boolean repaired = false;
        try {
          File chunksDir = new File(containerData.getChunksPath());
          Files.createDirectories(chunksDir.toPath());
          repaired = true;
        } catch (IOException ex) {
          LOG.error("Error recreating empty chunks directory for container {}.",
              containerData.getContainerID(), ex);
        }
        return repaired;
      };

      ObjectNode chunksDirError = buildErrorAndRepair("chunksDirectory.present",
          JsonNodeFactory.instance.booleanNode(true), chunksDirPresent, dirRepairAction);
      errors.add(chunksDirError);
    }
    parent.put("correct", passed);
    parent.set("errors", errors);
    return passed;
  }

  private static long jsonToLong(JsonNode e) {
    return e == null || e.isNull() ? 0 : e.asLong();
  }

  private ObjectNode buildErrorAndRepair(String property, JsonNode expected,
      JsonNode actual, BooleanSupplier repairAction) {
    ObjectNode error = JsonUtils.createObjectNode(null);
    error.put("property", property);
    error.set("expected", expected);
    error.set("actual", actual);

    boolean repaired = false;
    if (mode == Mode.REPAIR) {
      repaired = repairAction.getAsBoolean();
    }
    error.put("repaired", repaired);

    return error;
  }

  static PendingDelete countPendingDeletesSchemaV2(
      DatanodeStoreSchemaTwoImpl schemaTwoStore,
      KeyValueContainerData containerData) throws IOException {
    long pendingDeleteBlockCountTotal = 0;
    long pendingDeleteBytes = 0;

    Table<Long, DeletedBlocksTransaction> delTxTable =
        schemaTwoStore.getDeleteTransactionTable();

    try (TableIterator<Long, DeletedBlocksTransaction> iterator = delTxTable.valueIterator()) {
      while (iterator.hasNext()) {
        final DeletedBlocksTransaction txn = iterator.next();
        final List<Long> localIDs = txn.getLocalIDList();
        // In schema 2, pending delete blocks are stored in the
        // transaction object. Since the actual blocks still exist in the
        // block data table with no prefix, they have already been
        // counted towards bytes used and total block count above.
        pendingDeleteBlockCountTotal += localIDs.size();
        pendingDeleteBytes += computePendingDeleteBytes(
            localIDs, containerData, schemaTwoStore);
      }
    }

    return new PendingDelete(pendingDeleteBlockCountTotal,
        pendingDeleteBytes);
  }

  static long computePendingDeleteBytes(List<Long> localIDs,
      KeyValueContainerData containerData,
      DatanodeStoreWithIncrementalChunkList store) {
    long pendingDeleteBytes = 0;
    for (long id : localIDs) {
      try {
        final String blockKey = containerData.getBlockKey(id);
        final BlockData blockData = store.getBlockByID(null, blockKey);
        if (blockData != null) {
          pendingDeleteBytes += blockData.getSize();
        }
      } catch (IOException e) {
        LOG.error("Failed to get block " + id
            + " in container " + containerData.getContainerID()
            + " from blockDataTable", e);
      }
    }
    return pendingDeleteBytes;
  }

  static PendingDelete countPendingDeletesSchemaV3(
      DatanodeStoreSchemaThreeImpl store,
      KeyValueContainerData containerData) throws IOException {
    long pendingDeleteBlockCountTotal = 0;
    long pendingDeleteBytes = 0;
    try (TableIterator<String, DeletedBlocksTransaction> iterator
        = store.getDeleteTransactionTable().valueIterator(containerData.containerPrefix())) {
      while (iterator.hasNext()) {
        final DeletedBlocksTransaction delTx = iterator.next();
        final List<Long> localIDs = delTx.getLocalIDList();
        pendingDeleteBlockCountTotal += localIDs.size();
        pendingDeleteBytes += computePendingDeleteBytes(
            localIDs, containerData, store);
      }
    }
    return new PendingDelete(pendingDeleteBlockCountTotal,
        pendingDeleteBytes);
  }

  private static long getBlockLength(BlockData block) {
    long blockLen = 0;
    List<ContainerProtos.ChunkInfo> chunkInfoList = block.getChunks();

    for (ContainerProtos.ChunkInfo chunk : chunkInfoList) {
      blockLen += chunk.getLen();
    }

    return blockLen;
  }
}
