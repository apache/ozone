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
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerInspector;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
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

/**
 * Container inspector for key value container metadata. It is capable of
 * logging metadata information about a container, and repairing the metadata
 * database values of #BLOCKCOUNT and #BYTESUSED.
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
    // If the system property to process container metadata was not
    // specified, or the inspector is unloaded, this method is a no-op.
    if (mode == Mode.OFF) {
      return;
    }

    KeyValueContainerData kvData = null;
    if (containerData instanceof KeyValueContainerData) {
      kvData = (KeyValueContainerData) containerData;
    } else {
      LOG.error("This inspector only works on KeyValueContainers. Inspection " +
          "will not be run for container {}", containerData.getContainerID());
      return;
    }

    JsonObject containerJson = inspectContainer(kvData, store);
    boolean correct = checkAndRepair(containerJson, kvData, store);

    Gson gson = new GsonBuilder()
        .setPrettyPrinting()
        .serializeNulls()
        .create();
    String jsonReport = gson.toJson(containerJson);
    if (correct) {
      REPORT_LOG.trace(jsonReport);
    } else {
      REPORT_LOG.error(jsonReport);
    }
  }

  private JsonObject inspectContainer(KeyValueContainerData containerData,
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
      JsonObject dBMetadata = getDBMetadataJson(metadataTable);
      containerJson.add("dBMetadata", dBMetadata);

      // Build aggregate values.
      JsonObject aggregates = getAggregateValues(store, schemaVersion);
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

  private JsonObject getDBMetadataJson(Table<String, Long> metadataTable)
      throws IOException {
    JsonObject dBMetadata = new JsonObject();

    dBMetadata.addProperty(OzoneConsts.BLOCK_COUNT,
        metadataTable.get(OzoneConsts.BLOCK_COUNT));
    dBMetadata.addProperty(OzoneConsts.CONTAINER_BYTES_USED,
        metadataTable.get(OzoneConsts.CONTAINER_BYTES_USED));
    dBMetadata.addProperty(OzoneConsts.PENDING_DELETE_BLOCK_COUNT,
        metadataTable.get(OzoneConsts.PENDING_DELETE_BLOCK_COUNT));
    dBMetadata.addProperty(OzoneConsts.DELETE_TRANSACTION_KEY,
        metadataTable.get(OzoneConsts.DELETE_TRANSACTION_KEY));
    dBMetadata.addProperty(OzoneConsts.BLOCK_COMMIT_SEQUENCE_ID,
        metadataTable.get(OzoneConsts.BLOCK_COMMIT_SEQUENCE_ID));

    return dBMetadata;
  }

  private JsonObject getAggregateValues(DatanodeStore store,
      String schemaVersion) throws IOException {
    JsonObject aggregates = new JsonObject();

    long usedBytesTotal = 0;
    long blockCountTotal = 0;
    long pendingDeleteBlockCountTotal = 0;
    // Count normal blocks.
    try (BlockIterator<BlockData> blockIter =
             store.getBlockIterator(
                 MetadataKeyFilters.getUnprefixedKeyFilter())) {

      while (blockIter.hasNext()) {
        blockCountTotal++;
        usedBytesTotal += getBlockLength(blockIter.nextBlock());
      }
    }

    // Count pending delete blocks.
    if (schemaVersion.equals(OzoneConsts.SCHEMA_V1)) {
      try (BlockIterator<BlockData> blockIter =
               store.getBlockIterator(
                   MetadataKeyFilters.getDeletingKeyFilter())) {

        while (blockIter.hasNext()) {
          blockCountTotal++;
          pendingDeleteBlockCountTotal++;
          usedBytesTotal += getBlockLength(blockIter.nextBlock());
        }
      }
    } else if (schemaVersion.equals(OzoneConsts.SCHEMA_V2)) {
      DatanodeStoreSchemaTwoImpl schemaTwoStore =
          (DatanodeStoreSchemaTwoImpl) store;
      pendingDeleteBlockCountTotal =
          countPendingDeletesSchemaV2(schemaTwoStore);
    } else {
      throw new IOException("Failed to process deleted blocks for unknown " +
              "container schema " + schemaVersion);
    }

    aggregates.addProperty("blockCount", blockCountTotal);
    aggregates.addProperty("usedBytes", usedBytesTotal);
    aggregates.addProperty("pendingDeleteBlocks",
        pendingDeleteBlockCountTotal);

    return aggregates;
  }

  private JsonObject getChunksDirectoryJson(File chunksDir) throws IOException {
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

  private boolean checkAndRepair(JsonObject parent, ContainerData containerData,
      DatanodeStore store) {
    JsonArray errors = new JsonArray();
    boolean passed = true;

    Table<String, Long> metadataTable = store.getMetadataTable();

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
          metadataTable.put(OzoneConsts.BLOCK_COUNT,
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
          metadataTable.put(OzoneConsts.CONTAINER_BYTES_USED,
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

  private long countPendingDeletesSchemaV2(DatanodeStoreSchemaTwoImpl
      schemaTwoStore) throws IOException {
    long pendingDeleteBlockCountTotal = 0;
    Table<Long, DeletedBlocksTransaction> delTxTable =
        schemaTwoStore.getDeleteTransactionTable();
    try (TableIterator<Long, ? extends Table.KeyValue<Long,
        DeletedBlocksTransaction>> iterator = delTxTable.iterator()) {
      while (iterator.hasNext()) {
        DeletedBlocksTransaction txn = iterator.next().getValue();
        // In schema 2, pending delete blocks are stored in the
        // transaction object. Since the actual blocks still exist in the
        // block data table with no prefix, they have already been
        // counted towards bytes used and total block count above.
        pendingDeleteBlockCountTotal += txn.getLocalIDList().size();
      }
    }

    return pendingDeleteBlockCountTotal;
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
