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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
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
import java.util.stream.Stream;

/**
 * Container inspector for key value container metadata. It is capable of
 * logging metadata information about a container, and repairing the metadata
 * database values of #BLOCKCOUNT and #BYTESUSED.
 */
public class KeyValueContainerMetadataInspector implements ContainerInspector {
  public static final Logger LOG =
      LoggerFactory.getLogger(KeyValueContainerMetadataInspector.class);

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
    boolean propertySet = false;

    if (propertyValue != null && !propertyValue.isEmpty()) {
      if (propertyValue.equals(Mode.REPAIR.toString())) {
        mode = Mode.REPAIR;
        propertySet = true;
      } else if (propertyValue.equals(Mode.INSPECT.toString())) {
        mode = Mode.INSPECT;
        propertySet = true;
      } else {
        LOG.error("{} system property specified with invalid mode {}. " +
                "Valid options are {} and {}. Container metadata inspection " +
                "will not be run.", SYSTEM_PROPERTY, propertyValue,
            Mode.REPAIR, Mode.INSPECT);
      }
    }

    if (!propertySet) {
      mode = Mode.OFF;
    }

    return propertySet;
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
    // specified, this method is a no-op.
    if (mode == Mode.OFF) {
      return;
    }

    StringBuilder messageBuilder = new StringBuilder();
    boolean passed = false;

    try {
      messageBuilder.append(String.format("Audit of container %d metadata%n",
          containerData.getContainerID()));

      // Read metadata values.
      Table<String, Long> metadataTable = store.getMetadataTable();
      Long pendingDeleteBlockCount = getAndAppend(metadataTable,
          OzoneConsts.PENDING_DELETE_BLOCK_COUNT, messageBuilder);
      Long blockCount = getAndAppend(metadataTable,
          OzoneConsts.BLOCK_COUNT, messageBuilder);
      Long bytesUsed = getAndAppend(metadataTable,
          OzoneConsts.CONTAINER_BYTES_USED, messageBuilder);
      // No repair action taken on these values.
      getAndAppend(metadataTable,
          OzoneConsts.DELETE_TRANSACTION_KEY, messageBuilder);
      getAndAppend(metadataTable,
          OzoneConsts.BLOCK_COMMIT_SEQUENCE_ID, messageBuilder);

      // Count number of block keys and total bytes used in the DB.
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

      String schemaVersion =
          ((KeyValueContainerData) containerData).getSchemaVersion();

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
        messageBuilder.append("Cannot process deleted blocks for unknown " +
            "container schema ").append(schemaVersion);
      }

      // Count number of files in chunks directory.
      countFilesAndAppend(containerData.getChunksPath(), messageBuilder);

      messageBuilder.append("Schema Version: ")
          .append(schemaVersion).append("\n");
      messageBuilder.append("Total block keys in DB: ").append(blockCountTotal)
          .append("\n");
      messageBuilder.append("Total pending delete block keys in DB: ")
          .append(pendingDeleteBlockCountTotal).append("\n");
      messageBuilder.append("Total used bytes in DB: ").append(usedBytesTotal)
          .append("\n");

      boolean blockCountPassed =
          checkMetadataMatchAndAppend(metadataTable, OzoneConsts.BLOCK_COUNT,
              blockCount,
              blockCountTotal, messageBuilder);
      boolean bytesUsedPassed =
          checkMetadataMatchAndAppend(metadataTable,
              OzoneConsts.CONTAINER_BYTES_USED,
              bytesUsed, usedBytesTotal, messageBuilder);
      // This value gets filled in when block deleting service first runs.
      // Before that it is ok to be missing.
      boolean pendingDeleteCountPassed =
          pendingDeleteBlockCount == null ||
              checkMetadataMatchAndAppend(metadataTable,
                  OzoneConsts.PENDING_DELETE_BLOCK_COUNT,
                  pendingDeleteBlockCount, pendingDeleteBlockCountTotal,
              messageBuilder);
      passed = blockCountPassed && pendingDeleteCountPassed && bytesUsedPassed;
    } catch(IOException ex) {
      LOG.error("Inspecting container {} failed",
          containerData.getContainerID(), ex);
    }

    if (passed) {
      LOG.trace(messageBuilder.toString());
    } else {
      LOG.error(messageBuilder.toString());
    }
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

  private Long getAndAppend(Table<String, Long> metadataTable, String key,
                            StringBuilder messageBuilder) throws IOException {
    Long value = metadataTable.get(key);
    messageBuilder.append(key)
        .append(": ")
        .append(value)
        .append("\n");
    return value;
  }

  private boolean checkMetadataMatchAndAppend(Table<String, Long> metadataTable,
      String key, Long currentValue, long expectedValue,
      StringBuilder messageBuilder) throws IOException {
    boolean match = (currentValue != null && currentValue == expectedValue);
    if (!match) {
      messageBuilder.append(String.format("!Value of metadata key %s " +
              "does not match DB total: %d != %d%n", key, currentValue,
          expectedValue));
      if (mode == Mode.REPAIR) {
        messageBuilder.append(String.format("!Repairing %s of %d to match " +
            "database total: %d%n", key, currentValue, expectedValue));
        metadataTable.put(key, expectedValue);
      }
    }

    return match;
  }

  private void countFilesAndAppend(String chunksDirStr,
      StringBuilder messageBuilder) throws IOException {
    File chunksDirFile = new File(chunksDirStr);
    if (!FileUtils.isDirectory(chunksDirFile)) {
      messageBuilder.append("!Missing chunks directory: ")
          .append(chunksDirStr);
      if (mode == Mode.REPAIR) {
        messageBuilder.append("!Creating empty chunks directory: ")
            .append(chunksDirStr);
        Files.createDirectories(chunksDirFile.toPath());
      }
    } else {
      // Chunks directory present. Count the number of files in it.
      try (Stream<Path> stream = Files.list(chunksDirFile.toPath())) {
        long fileCount = stream.count();
        messageBuilder.append(fileCount)
            .append(" files in chunks directory: ").append(chunksDirStr)
            .append("\n");
      }
    }
  }

  private static long getBlockLength(BlockData block) throws IOException {
    long blockLen = 0;
    List<ContainerProtos.ChunkInfo> chunkInfoList = block.getChunks();

    for (ContainerProtos.ChunkInfo chunk : chunkInfoList) {
      ChunkInfo info = ChunkInfo.getFromProtoBuf(chunk);
      blockLen += info.getLen();
    }

    return blockLen;
  }
}
