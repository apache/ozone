package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class KeyValueContainerMetadataInspector {
  public static final Logger LOG =
      LoggerFactory.getLogger(KeyValueContainerMetadataInspector.class);

  private KeyValueContainerData kvContainerData;
  private DatanodeStore store;

  private enum Mode {
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

  private static final String SYSTEM_PROPERTY = "ozone.datanode.container" +
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
  public static void loadSystemProperty() {
    String propertyValue = System.getProperty(SYSTEM_PROPERTY);

    if (propertyValue != null && !propertyValue.isEmpty()) {
      if (propertyValue.equals(Mode.REPAIR.getName())) {
        metadataMode = Mode.REPAIR;
      } else if (propertyValue.equals(Mode.INSPECT.getName())) {
        metadataMode = Mode.INSPECT;
      } else {
        LOG.error("{} system property specified with invalid mode {}. " +
                "Valid options are {} and {}. Container metadata repair will not " +
                "be performed.", SYSTEM_PROPERTY, propertyValue,
            Mode.REPAIR.getName(), Mode.INSPECT.getName());
      }
    }
  }

    public static void unloadSystemProperty() {
      System.clearProperty(SYSTEM_PROPERTY);
    }

  public void processMetadata() throws IOException {
    // If the system property to process container metadata was not
    // specified, this method is a no-op.
    if (metadataMode == null) {
      return;
    }

    StringBuilder messageBuilder = new StringBuilder();

    messageBuilder.append(String.format("Audit of container %d metadata\n",
            kvContainerData.getContainerID()));

    // Read metadata values.
    Table<String, Long> metadataTable = store.getMetadataTable();
    long pendingDeleteBlockCount = getAndAppend(metadataTable,
        OzoneConsts.PENDING_DELETE_BLOCK_COUNT, messageBuilder);
    long blockCount = getAndAppend(metadataTable,
        OzoneConsts.BLOCK_COUNT, messageBuilder);
    long bytesUsed = getAndAppend(metadataTable,
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

    // Count pending delete blocks.
    // TODO: Check for schema v2
    try (BlockIterator<BlockData> blockIter =
             store.getBlockIterator(
                 MetadataKeyFilters.getDeletingKeyFilter())) {

      while (blockIter.hasNext()) {
        blockCountTotal++;
        pendingDeleteBlockCountTotal++;
        usedBytesTotal += getBlockLength(blockIter.nextBlock());
      }
    }

    // Count the number of block files on disk.

    messageBuilder.append("Total block keys in DB: ").append(blockCountTotal)
        .append("\n");
    messageBuilder.append("Total pending delete block keys in DB: ")
        .append(pendingDeleteBlockCountTotal).append("\n");
    messageBuilder.append("Total used bytes in DB: ").append(usedBytesTotal)
        .append("\n");

    boolean passed =
        checkMetadataMismatchAndAppend(OzoneConsts.BLOCK_COUNT,
          blockCount, blockCountTotal, messageBuilder) &&
        checkMetadataMismatchAndAppend(OzoneConsts.PENDING_DELETE_BLOCK_COUNT,
          pendingDeleteBlockCount, pendingDeleteBlockCountTotal,
            messageBuilder) &&
        checkMetadataMismatchAndAppend(OzoneConsts.CONTAINER_BYTES_USED,
          bytesUsed, usedBytesTotal, messageBuilder);

    if (passed) {
      LOG.trace(messageBuilder.toString());
    } else {
      LOG.error(messageBuilder.toString());
    }
  }

  private long getAndAppend(Table<String, Long> metadataTable, String key,
                            StringBuilder messageBuilder) throws IOException {
    long value = metadataTable.get(key);
    messageBuilder.append(key)
        .append(": ")
        .append(value)
        .append("\n");
    return value;
  }

  private boolean checkMetadataMismatchAndAppend(String key, long expected,
      long actual, StringBuilder messageBuilder) throws IOException {
    boolean mismatch = (expected != actual);
    if (mismatch) {
      messageBuilder.append(String.format("Value of metadata key %s " +
          "does not match DB total: %d != %d\n", expected, actual));
      if (metadataMode == Mode.REPAIR) {
        messageBuilder.append(String.format("Repairing %s of %d to match " +
           "database total: %d\n", key, actual, expected));
        store.getMetadataTable().put(key, expected);
      }
    }

    return mismatch;
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
