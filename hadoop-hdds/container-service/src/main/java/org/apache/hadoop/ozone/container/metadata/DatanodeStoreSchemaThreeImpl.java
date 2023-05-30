/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.metadata;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.FixedLengthStringCodec;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.bouncycastle.util.Strings;
import org.rocksdb.LiveFileMetaData;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition.getContainerKeyPrefix;

/**
 * Constructs a datanode store in accordance with schema version 3, which uses
 * three column families/tables:
 * 1. A block data table.
 * 2. A metadata table.
 * 3. A Delete Transaction Table.
 *
 * This is different from schema version 2 from these points:
 * - All keys have containerID as prefix.
 * - The table 3 has String as key instead of Long since we want to use prefix.
 */
public class DatanodeStoreSchemaThreeImpl extends AbstractDatanodeStore
    implements DeleteTransactionStore<String> {

  public static final String DUMP_FILE_SUFFIX = ".data";
  public static final String DUMP_DIR = "db";

  private final Table<String, DeletedBlocksTransaction> deleteTransactionTable;

  public DatanodeStoreSchemaThreeImpl(ConfigurationSource config,
      String dbPath, boolean openReadOnly) throws IOException {
    super(config, new DatanodeSchemaThreeDBDefinition(dbPath, config),
        openReadOnly);
    this.deleteTransactionTable = ((DatanodeSchemaThreeDBDefinition) getDbDef())
        .getDeleteTransactionsColumnFamily().getTable(getStore());
  }

  @Override
  public Table<String, DeletedBlocksTransaction> getDeleteTransactionTable() {
    return this.deleteTransactionTable;
  }

  @Override
  public BlockIterator<BlockData> getBlockIterator(long containerID)
      throws IOException {
    // Here we need to filter the keys with containerID as prefix
    // and followed by metadata prefixes such as #deleting#.
    return new KeyValueBlockIterator(containerID,
        getBlockDataTableWithIterator()
            .iterator(getContainerKeyPrefix(containerID)),
        new MetadataKeyFilters.KeyPrefixFilter().addFilter(
            getContainerKeyPrefix(containerID) + "#", true));
  }

  @Override
  public BlockIterator<BlockData> getBlockIterator(long containerID,
      MetadataKeyFilters.KeyPrefixFilter filter) throws IOException {
    return new KeyValueBlockIterator(containerID,
        getBlockDataTableWithIterator()
            .iterator(getContainerKeyPrefix(containerID)), filter);
  }

  public void removeKVContainerData(long containerID) throws IOException {
    String prefix = getContainerKeyPrefix(containerID);
    try (BatchOperation batch = getBatchHandler().initBatchOperation()) {
      getMetadataTable().deleteBatchWithPrefix(batch, prefix);
      getBlockDataTable().deleteBatchWithPrefix(batch, prefix);
      getDeletedBlocksTable().deleteBatchWithPrefix(batch, prefix);
      getDeleteTransactionTable().deleteBatchWithPrefix(batch, prefix);
      getBatchHandler().commitBatchOperation(batch);
    }
  }

  public void dumpKVContainerData(long containerID, File dumpDir)
      throws IOException {
    String prefix = getContainerKeyPrefix(containerID);
    getMetadataTable().dumpToFileWithPrefix(
        getTableDumpFile(getMetadataTable(), dumpDir), prefix);
    getBlockDataTable().dumpToFileWithPrefix(
        getTableDumpFile(getBlockDataTable(), dumpDir), prefix);
    getDeletedBlocksTable().dumpToFileWithPrefix(
        getTableDumpFile(getDeletedBlocksTable(), dumpDir), prefix);
    getDeleteTransactionTable().dumpToFileWithPrefix(
        getTableDumpFile(getDeleteTransactionTable(), dumpDir),
        prefix);
  }

  public void loadKVContainerData(File dumpDir)
      throws IOException {
    getMetadataTable().loadFromFile(
        getTableDumpFile(getMetadataTable(), dumpDir));
    getBlockDataTable().loadFromFile(
        getTableDumpFile(getBlockDataTable(), dumpDir));
    getDeletedBlocksTable().loadFromFile(
        getTableDumpFile(getDeletedBlocksTable(), dumpDir));
    getDeleteTransactionTable().loadFromFile(
        getTableDumpFile(getDeleteTransactionTable(), dumpDir));
  }

  public static File getTableDumpFile(Table<String, ?> table,
      File dumpDir) throws IOException {
    return new File(dumpDir, table.getName() + DUMP_FILE_SUFFIX);
  }

  public static File getDumpDir(File metaDir) {
    return new File(metaDir, DUMP_DIR);
  }

  public void compactionIfNeeded() throws Exception {
    // Calculate number of files per level and size per level
    RocksDatabase rocksDB = ((RDBStore)getStore()).getDb();
    List<LiveFileMetaData> liveFileMetaDataList =
        rocksDB.getLiveFilesMetaData();
    DatanodeConfiguration df =
        getDbDef().getConfig().getObject(DatanodeConfiguration.class);
    int numThreshold = df.getAutoCompactionSmallSstFileNum();
    long sizeThreshold = df.getAutoCompactionSmallSstFileSize();
    Map<String, Map<Integer, List<LiveFileMetaData>>> stat = new HashMap<>();
    Map<Integer, List<LiveFileMetaData>> map;

    for (LiveFileMetaData file: liveFileMetaDataList) {
      if (file.size() >= sizeThreshold) {
        continue;
      }
      String cf = Strings.fromByteArray(file.columnFamilyName());
      stat.computeIfAbsent(cf, k -> new HashMap<>());
      stat.computeIfPresent(cf, (k, v) -> {
        v.computeIfAbsent(file.level(), l -> new LinkedList<>());
        v.computeIfPresent(file.level(), (k1, v1) -> {
          v1.add(file);
          return v1;
        });
        return v;
      });
    }

    for (Map.Entry<String, Map<Integer, List<LiveFileMetaData>>> entry :
        stat.entrySet()) {
      for (Map.Entry<Integer, List<LiveFileMetaData>> innerEntry:
          entry.getValue().entrySet()) {
        if (innerEntry.getValue().size() > numThreshold) {
          ColumnFamily columnFamily = null;
          // Find CF Handler
          for (ColumnFamily cf : rocksDB.getExtraColumnFamilies()) {
            if (cf.getName().equals(entry.getKey())) {
              columnFamily = cf;
              break;
            }
          }
          if (columnFamily != null) {
            // Find the key range of these sst files
            long startCId = Long.MAX_VALUE;
            long endCId = Long.MIN_VALUE;
            for (LiveFileMetaData file: innerEntry.getValue()) {
              long firstCId = DatanodeSchemaThreeDBDefinition.getContainerId(
                  FixedLengthStringCodec.bytes2String(file.smallestKey()));
              long lastCId = DatanodeSchemaThreeDBDefinition.getContainerId(
                  FixedLengthStringCodec.bytes2String(file.largestKey()));
              startCId = Math.min(firstCId, startCId);
              endCId = Math.max(lastCId, endCId);
            }

            // Do the range compaction
            ManagedCompactRangeOptions options =
                new ManagedCompactRangeOptions();
            options.setBottommostLevelCompaction(
                ManagedCompactRangeOptions.BottommostLevelCompaction.kForce);
            LOG.info("CF {} level {} small file number {} exceeds threshold {}"
                    + ". Auto compact small sst files.", entry.getKey(),
                innerEntry.getKey(), innerEntry.getValue().size(),
                numThreshold);
            rocksDB.compactRange(columnFamily,
                DatanodeSchemaThreeDBDefinition
                    .getContainerKeyPrefixBytes(startCId),
                DatanodeSchemaThreeDBDefinition
                    .getContainerKeyPrefixBytes(endCId + 1),
                options);
          } else {
            LOG.warn("Failed to find cf {} in DB {}", entry.getKey(),
                getDbDef().getDBLocation(null));
          }
        }
      }
    }
  }
}
