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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
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
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil.isSameSchemaVersion;

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
  public static final Logger LOG =
      LoggerFactory.getLogger(KeyValueContainerMetadataInspector.class);
  public static final Logger REPORT_LOG = LoggerFactory.getLogger(
      "ContainerMetadataInspectorReport");

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

    public String toString() {
      return name;
    }
  }

  public static final String SYSTEM_PROPERTY = "ozone.datanode.container" +
      ".metadata.inspector";

  private Mode mode;

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

    JsonObject containerJson = inspectContainer(kvData, store);
    boolean correct = checkAndRepair(containerJson, kvData, store);

    Gson gson = new GsonBuilder()
        .setPrettyPrinting()
        .serializeNulls()
        .create();
    String jsonReport = gson.toJson(containerJson);
    if (log != null) {
      if (correct) {
        log.trace(jsonReport);
      } else {
        log.error(jsonReport);
      }
    }
    return jsonReport;
  }

  static JsonObject inspectContainer(KeyValueContainerData containerData,
      DatanodeStore store) {

    JsonObject containerJson = new JsonObject();

    try {
      // Build top level container properties.
      containerJson.addProperty("containerID", containerData.getContainerID());
      String schemaVersion = containerData.getSchemaVersion();
      containerJson.addProperty("schemaVersion", schemaVersion);
      containerJson.addProperty("containerState",
          containerData.getState().toString());
      containerJson.addProperty("currentDatanodeID",
          containerData.getVolume().getDatanodeUuid());
      containerJson.addProperty("originDatanodeID",
          containerData.getOriginNodeId());

      // Build DB metadata values.
      Table<String, Long> metadataTable = store.getMetadataTable();
      JsonObject dBMetadata = getDBMetadataJson(metadataTable, containerData);
      containerJson.add("dBMetadata", dBMetadata);

      // Build aggregate values.
      JsonObject aggregates = getAggregateValues(store,
          containerData, schemaVersion);
      containerJson.add("aggregates", aggregates);

      // Build info about chunks directory.
      JsonObject chunksDirectory =
          getChunksDirectoryJson(new File(containerData.getChunksPath()));
      containerJson.add("chunksDirectory", chunksDirectory);
    } catch (IOException ex) {
      LOG.error("Inspecting container {} failed",
          containerData.getContainerID(), ex);
    }

    return containerJson;
  }

  static JsonObject getDBMetadataJson(Table<String, Long> metadataTable,
      KeyValueContainerData containerData) throws IOException {
    JsonObject dBMetadata = new JsonObject();

    dBMetadata.addProperty(OzoneConsts.BLOCK_COUNT,
        metadataTable.get(containerData.getBlockCountKey()));
    dBMetadata.addProperty(OzoneConsts.CONTAINER_BYTES_USED,
        metadataTable.get(containerData.getBytesUsedKey()));
    dBMetadata.addProperty(OzoneConsts.PENDING_DELETE_BLOCK_COUNT,
        metadataTable.get(containerData.getPendingDeleteBlockCountKey()));
    dBMetadata.addProperty(OzoneConsts.DELETE_TRANSACTION_KEY,
        metadataTable.get(containerData.getLatestDeleteTxnKey()));
    dBMetadata.addProperty(OzoneConsts.BLOCK_COMMIT_SEQUENCE_ID,
        metadataTable.get(containerData.getBcsIdKey()));

    return dBMetadata;
  }

  static JsonObject getAggregateValues(DatanodeStore store,
      KeyValueContainerData containerData, String schemaVersion)
      throws IOException {
    JsonObject aggregates = new JsonObject();

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
    final PendingDelete pendingDelete;
    if (isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V1)) {
      long pendingDeleteBlockCountTotal = 0;
      long pendingDeleteBytes = 0;
      try (BlockIterator<BlockData> blockIter =
               store.getBlockIterator(containerData.getContainerID(),
                   containerData.getDeletingBlockKeyFilter())) {

        while (blockIter.hasNext()) {
          blockCountTotal++;
          pendingDeleteBlockCountTotal++;
          final long bytes = getBlockLength(blockIter.nextBlock());
          usedBytesTotal += bytes;
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

    aggregates.addProperty("blockCount", blockCountTotal);
    aggregates.addProperty("usedBytes", usedBytesTotal);
    pendingDelete.addToJson(aggregates);

    return aggregates;
  }

  static JsonObject getChunksDirectoryJson(File chunksDir) throws IOException {
    JsonObject chunksDirectory = new JsonObject();

    chunksDirectory.addProperty("path", chunksDir.getAbsolutePath());
    boolean chunksDirPresent = FileUtils.isDirectory(chunksDir);
    chunksDirectory.addProperty("present", chunksDirPresent);

    long fileCount = 0;
    if (chunksDirPresent) {
      try (Stream<Path> stream = Files.list(chunksDir.toPath())) {
        fileCount = stream.count();
      }
    }
    chunksDirectory.addProperty("fileCount", fileCount);

    return chunksDirectory;
  }

  private boolean checkAndRepair(JsonObject parent,
      KeyValueContainerData containerData, DatanodeStore store) {
    JsonArray errors = new JsonArray();
    boolean passed = true;

    Table<String, Long> metadataTable = store.getMetadataTable();

    final JsonObject dBMetadata = parent.getAsJsonObject("dBMetadata");
    final JsonObject aggregates = parent.getAsJsonObject("aggregates");

    // Check and repair block count.
    JsonElement blockCountDB = parent.getAsJsonObject("dBMetadata")
        .get(OzoneConsts.BLOCK_COUNT);

    JsonElement blockCountAggregate = parent.getAsJsonObject("aggregates")
        .get("blockCount");

    // If block count is absent from the DB, it is only an error if there are
    // a non-zero amount of block keys in the DB.
    long blockCountDBLong = 0;
    if (!blockCountDB.isJsonNull()) {
      blockCountDBLong = blockCountDB.getAsLong();
    }

    if (blockCountDBLong != blockCountAggregate.getAsLong()) {
      passed = false;

      BooleanSupplier keyRepairAction = () -> {
        boolean repaired = false;
        try {
          metadataTable.put(containerData.getBlockCountKey(),
              blockCountAggregate.getAsLong());
          repaired = true;
        } catch (IOException ex) {
          LOG.error("Error repairing block count for container {}.",
              containerData.getContainerID(), ex);
        }
        return repaired;
      };

      JsonObject blockCountError = buildErrorAndRepair("dBMetadata." +
              OzoneConsts.BLOCK_COUNT, blockCountAggregate, blockCountDB,
          keyRepairAction);
      errors.add(blockCountError);
    }

    // Check and repair used bytes.
    JsonElement usedBytesDB = parent.getAsJsonObject("dBMetadata")
        .get(OzoneConsts.CONTAINER_BYTES_USED);
    JsonElement usedBytesAggregate = parent.getAsJsonObject("aggregates")
        .get("usedBytes");

    // If used bytes is absent from the DB, it is only an error if there is
    // a non-zero aggregate of used bytes among the block keys.
    long usedBytesDBLong = 0;
    if (!usedBytesDB.isJsonNull()) {
      usedBytesDBLong = usedBytesDB.getAsLong();
    }

    if (usedBytesDBLong != usedBytesAggregate.getAsLong()) {
      passed = false;

      BooleanSupplier keyRepairAction = () -> {
        boolean repaired = false;
        try {
          metadataTable.put(containerData.getBytesUsedKey(),
              usedBytesAggregate.getAsLong());
          repaired = true;
        } catch (IOException ex) {
          LOG.error("Error repairing used bytes for container {}.",
              containerData.getContainerID(), ex);
        }
        return repaired;
      };

      JsonObject usedBytesError = buildErrorAndRepair("dBMetadata." +
              OzoneConsts.CONTAINER_BYTES_USED, usedBytesAggregate, usedBytesDB,
          keyRepairAction);
      errors.add(usedBytesError);
    }

    // check and repair if db delete count mismatches delete transaction count.
    final JsonElement pendingDeleteCountDB = dBMetadata.get(
        OzoneConsts.PENDING_DELETE_BLOCK_COUNT);
    final long dbDeleteCount = jsonToLong(pendingDeleteCountDB);
    final JsonElement pendingDeleteCountAggregate
        = aggregates.get(PendingDelete.COUNT);
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

      final JsonObject deleteCountError = buildErrorAndRepair(
          "dBMetadata." + OzoneConsts.PENDING_DELETE_BLOCK_COUNT,
          pendingDeleteCountAggregate, pendingDeleteCountDB,
          deleteCountRepairAction);
      errors.add(deleteCountError);
    }

    // check and repair chunks dir.
    JsonElement chunksDirPresent = parent.getAsJsonObject("chunksDirectory")
        .get("present");
    if (!chunksDirPresent.getAsBoolean()) {
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

      JsonObject chunksDirError = buildErrorAndRepair("chunksDirectory.present",
          new JsonPrimitive(true), chunksDirPresent, dirRepairAction);
      errors.add(chunksDirError);
    }

    parent.addProperty("correct", passed);
    parent.add("errors", errors);
    return passed;
  }

  static long jsonToLong(JsonElement e) {
    return e == null || e.isJsonNull() ? 0 : e.getAsLong();
  }

  private JsonObject buildErrorAndRepair(String property, JsonElement expected,
      JsonElement actual, BooleanSupplier repairAction) {
    JsonObject error = new JsonObject();
    error.addProperty("property", property);
    error.add("expected", expected);
    error.add("actual", actual);

    boolean repaired = false;
    if (mode == Mode.REPAIR) {
      repaired = repairAction.getAsBoolean();
    }
    error.addProperty("repaired", repaired);

    return error;
  }

  static class PendingDelete {
    static final String COUNT = "pendingDeleteBlocks";
    static final String BYTES = "pendingDeleteBytes";

    private final long count;
    private final long bytes;

    PendingDelete(long count, long bytes) {
      this.count = count;
      this.bytes = bytes;
    }

    void addToJson(JsonObject json) {
      json.addProperty(COUNT, count);
      json.addProperty(BYTES, bytes);
    }
  }

  static PendingDelete countPendingDeletesSchemaV2(
      DatanodeStoreSchemaTwoImpl schemaTwoStore,
      KeyValueContainerData containerData) throws IOException {
    long pendingDeleteBlockCountTotal = 0;
    long pendingDeleteBytes = 0;

    Table<Long, DeletedBlocksTransaction> delTxTable =
        schemaTwoStore.getDeleteTransactionTable();
    final Table<String, BlockData> blockDataTable
        = schemaTwoStore.getBlockDataTable();

    try (TableIterator<Long, ? extends Table.KeyValue<Long,
        DeletedBlocksTransaction>> iterator = delTxTable.iterator()) {
      while (iterator.hasNext()) {
        DeletedBlocksTransaction txn = iterator.next().getValue();
        final List<Long> localIDs = txn.getLocalIDList();
        // In schema 2, pending delete blocks are stored in the
        // transaction object. Since the actual blocks still exist in the
        // block data table with no prefix, they have already been
        // counted towards bytes used and total block count above.
        pendingDeleteBlockCountTotal += localIDs.size();
        pendingDeleteBytes += computePendingDeleteBytes(
            localIDs, containerData, blockDataTable);
      }
    }

    return new PendingDelete(pendingDeleteBlockCountTotal,
        pendingDeleteBytes);
  }

  static long computePendingDeleteBytes(List<Long> localIDs,
      KeyValueContainerData containerData,
      Table<String, BlockData> blockDataTable) {
    long pendingDeleteBytes = 0;
    for (long id : localIDs) {
      try {
        final String blockKey = containerData.getBlockKey(id);
        final BlockData blockData = blockDataTable.get(blockKey);
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
      DatanodeStoreSchemaThreeImpl schemaThreeStore,
      KeyValueContainerData containerData) throws IOException {
    long pendingDeleteBlockCountTotal = 0;
    long pendingDeleteBytes = 0;
    final Table<String, BlockData> blockDataTable
        = schemaThreeStore.getBlockDataTable();
    try (
        TableIterator<String, ? extends Table.KeyValue<String,
            DeletedBlocksTransaction>>
            iter = schemaThreeStore.getDeleteTransactionTable()
            .iterator(containerData.containerPrefix())) {
      while (iter.hasNext()) {
        DeletedBlocksTransaction delTx = iter.next().getValue();
        final List<Long> localIDs = delTx.getLocalIDList();
        pendingDeleteBlockCountTotal += localIDs.size();
        pendingDeleteBytes += computePendingDeleteBytes(
            localIDs, containerData, blockDataTable);
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
