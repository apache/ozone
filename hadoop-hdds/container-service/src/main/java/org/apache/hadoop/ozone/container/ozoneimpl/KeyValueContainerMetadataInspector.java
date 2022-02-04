package org.apache.hadoop.ozone.container.ozoneimpl;

import jdk.internal.org.objectweb.asm.util.CheckFieldAdapter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class KeyValueContainerMetadataInspector {
  public static final Logger LOG =
      LoggerFactory.getLogger(KeyValueContainerMetadataInspector.class);

  private KeyValueContainerData kvContainerData;
  private DatanodeStore store;

  public enum Mode {
    REPAIR("repair"),
    INSPECT("inspect");

    private String name;

    Mode(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  private static Mode metadataMode = null;

  public static final String SYSTEM_PROPERTY = "ozone.datanode.container" +
      ".metadata";

  public KeyValueContainerMetadataInspector(KeyValueContainerData kvContainerData,
                                            DatanodeStore store) {
   this.kvContainerData = kvContainerData;
   this.store = store;
  }

  /**
   * Should be called at least once before calling processMetadata.
   *
   * Reads the value of the system property enabling metadata
   * inspection or repair for containers. This is done statically so that a
   * configuration error does not spam the logs with identical error messages
   * for each container.
   */
  public static boolean loadSystemProperty() {
    String propertyValue = System.getProperty(SYSTEM_PROPERTY);
    boolean propertySet = false;

    if (propertyValue != null && !propertyValue.isEmpty()) {
      propertySet = true;
      if (propertyValue.equals(Mode.REPAIR.getName())) {
        metadataMode = Mode.REPAIR;
      } else if (propertyValue.equals(Mode.INSPECT.getName())) {
        metadataMode = Mode.INSPECT;
      } else {
        LOG.error("{} system property specified with invalid mode {}. " +
                "Valid options are {} and {}. Container metadata repair will not " +
                "be performed.", SYSTEM_PROPERTY, propertyValue,
            Mode.REPAIR.getName(), Mode.INSPECT.getName());
        propertySet = false;
      }
    }

    return propertySet;
  }

    public static void unloadSystemProperty() {
      System.clearProperty(SYSTEM_PROPERTY);
    }

  public void processMetadata() {
    // If the system property to process container metadata was not
    // specified, this method is a no-op.
    if (metadataMode == null) {
      return;
    }

    StringBuilder messageBuilder = new StringBuilder();
    boolean passed = false;

    try {
      messageBuilder.append(String.format("Audit of container %d metadata\n",
          kvContainerData.getContainerID()));

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

      String schemaVersion = kvContainerData.getSchemaVersion();

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
        pendingDeleteBlockCountTotal = countPendingDeletesSchemaV2();
      } else {
        messageBuilder.append("Cannot process deleted blocks for unknown " +
            "container schema ").append(schemaVersion);
      }

      // Count number of files in chunks directory.
      countFilesAndAppend(messageBuilder);

      messageBuilder.append("Schema Version: ")
          .append(kvContainerData.getSchemaVersion()).append("\n");
      messageBuilder.append("Total block keys in DB: ").append(blockCountTotal)
          .append("\n");
      messageBuilder.append("Total pending delete block keys in DB: ")
          .append(pendingDeleteBlockCountTotal).append("\n");
      messageBuilder.append("Total used bytes in DB: ").append(usedBytesTotal)
          .append("\n");

      passed = checkMetadataMatchAndAppend(OzoneConsts.BLOCK_COUNT,
          blockCount, blockCountTotal, messageBuilder) &&
          checkMetadataMatchAndAppend(OzoneConsts.PENDING_DELETE_BLOCK_COUNT,
              pendingDeleteBlockCount, pendingDeleteBlockCountTotal,
              messageBuilder) &&
          checkMetadataMatchAndAppend(OzoneConsts.CONTAINER_BYTES_USED,
              bytesUsed, usedBytesTotal, messageBuilder);
    } catch(IOException ex) {
      LOG.error("Inspecting container {} failed",
          kvContainerData.getContainerID(), ex);
    }

    if (passed) {
      LOG.trace(messageBuilder.toString());
    } else {
      LOG.error(messageBuilder.toString());
    }
  }

  private long countPendingDeletesSchemaV2() throws IOException {
    long pendingDeleteBlockCountTotal = 0;
    DatanodeStoreSchemaTwoImpl schemaTwoStore =
        (DatanodeStoreSchemaTwoImpl) store;
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

  private boolean checkMetadataMatchAndAppend(String key, Long expected,
      long actual, StringBuilder messageBuilder) throws IOException {
    boolean match = (expected == null && actual == 0) ||
        (expected != null && expected == actual);
    if (!match) {
      messageBuilder.append(String.format("!Value of metadata key %s " +
          "does not match DB total: %d != %d\n", key, expected, actual));
      if (metadataMode == Mode.REPAIR) {
        messageBuilder.append(String.format("!Repairing %s of %d to match " +
           "database total: %d\n", key, actual, expected));
        store.getMetadataTable().put(key, expected);
      }
    }

    return match;
  }

  public void countFilesAndAppend(StringBuilder messageBuilder)
      throws IOException {
    String chunksDirStr = kvContainerData.getChunksPath();
    File chunksDirFile = new File(chunksDirStr);
    if (!FileUtils.isDirectory(chunksDirFile)) {
      messageBuilder.append("!Missing chunks directory. Expected ")
          .append(chunksDirStr);
      if (metadataMode == Mode.REPAIR) {
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
