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

package org.apache.hadoop.ozone.container.metadata;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link DatanodeStore} interface that contains
 * functionality common to all more derived datanode store implementations.
 */
public class AbstractDatanodeStore extends AbstractRDBStore<AbstractDatanodeDBDefinition> implements DatanodeStore {

  private Table<String, Long> metadataTable;

  private Table<String, BlockData> blockDataTable;

  private Table<String, BlockData> lastChunkInfoTable;

  private Table<String, BlockData> blockDataTableWithIterator;

  private Table<String, Long> finalizeBlocksTable;

  private Table<String, Long> finalizeBlocksTableWithIterator;

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractDatanodeStore.class);

  /**
   * Constructs the metadata store and starts the DB services.
   *
   * @param config - Ozone Configuration.
   */
  protected AbstractDatanodeStore(ConfigurationSource config,
      AbstractDatanodeDBDefinition dbDef, boolean openReadOnly)
      throws RocksDatabaseException, CodecException {
    super(dbDef, config, openReadOnly);
  }

  @Override
  protected DBStore initDBStore(DBStoreBuilder dbStoreBuilder, ManagedDBOptions options, ConfigurationSource config)
      throws RocksDatabaseException, CodecException {
    AbstractDatanodeDBDefinition dbDefinition = this.getDbDef();
    if (dbDefinition instanceof DatanodeSchemaOneDBDefinition ||
        dbDefinition instanceof DatanodeSchemaTwoDBDefinition) {
      long maxWalSize = DBProfile.toLong(StorageUnit.MB.toBytes(2));
      options.setMaxTotalWalSize(maxWalSize);
    }
    DatanodeConfiguration dc =
        config.getObject(DatanodeConfiguration.class);

    if (dbDefinition instanceof DatanodeSchemaThreeDBDefinition) {
      options.setDeleteObsoleteFilesPeriodMicros(
          dc.getRocksdbDeleteObsoleteFilesPeriod());

      // For V3, all Rocksdb dir has the same "container.db" name. So use
      // parentDirName(storage UUID)-dbDirName as db metrics name
      dbStoreBuilder.setDBJmxBeanNameName(dbDefinition.getDBLocation(config).getName() + "-" +
              dbDefinition.getName());
    }
    DBStore dbStore = dbStoreBuilder.setDBOptions(options).build();

    // Use the DatanodeTable wrapper to disable the table iterator on
    // existing Table implementations retrieved from the DBDefinition.
    // See the DatanodeTable's Javadoc for an explanation of why this is
    // necessary.
    metadataTable = new DatanodeTable<>(
        dbDefinition.getMetadataColumnFamily().getTable(dbStore));
    checkTableStatus(metadataTable, metadataTable.getName());

    // The block iterator this class returns will need to use the table
    // iterator internally, so construct a block data table instance
    // that does not have the iterator disabled by DatanodeTable.
    blockDataTableWithIterator =
        dbDefinition.getBlockDataColumnFamily().getTable(dbStore);

    blockDataTable = new DatanodeTable<>(blockDataTableWithIterator);
    checkTableStatus(blockDataTable, blockDataTable.getName());

    if (dbDefinition.getFinalizeBlocksColumnFamily() != null) {
      finalizeBlocksTableWithIterator =
          dbDefinition.getFinalizeBlocksColumnFamily().getTable(dbStore);

      finalizeBlocksTable = new DatanodeTable<>(
          finalizeBlocksTableWithIterator);
      checkTableStatus(finalizeBlocksTable, finalizeBlocksTable.getName());
    }

    if (dbDefinition.getLastChunkInfoColumnFamily() != null) {
      lastChunkInfoTable = new DatanodeTable<>(
          dbDefinition.getLastChunkInfoColumnFamily().getTable(dbStore));
      checkTableStatus(lastChunkInfoTable, lastChunkInfoTable.getName());
    }
    return dbStore;
  }

  @Override
  public Table<String, Long> getMetadataTable() {
    return metadataTable;
  }

  @Override
  public Table<String, BlockData> getBlockDataTable() {
    return blockDataTable;
  }

  @Override
  public Table<String, BlockData> getLastChunkInfoTable() {
    return lastChunkInfoTable;
  }

  @Override
  public Table<String, ChunkInfoList> getDeletedBlocksTable() {
    throw new UnsupportedOperationException("DeletedBlocksTable is only supported in Container Schema One");
  }

  @Override
  public Table<String, Long> getFinalizeBlocksTable() {
    return finalizeBlocksTable;
  }

  @Override
  public BlockIterator<BlockData> getBlockIterator(long containerID)
      throws IOException {
    return new KeyValueBlockIterator(containerID,
        blockDataTableWithIterator.iterator());
  }

  @Override
  public BlockIterator<BlockData> getBlockIterator(long containerID,
      KeyPrefixFilter filter) throws IOException {
    return new KeyValueBlockIterator(containerID,
        blockDataTableWithIterator.iterator(), filter);
  }

  @Override
  public BlockIterator<Long> getFinalizeBlockIterator(long containerID,
      KeyPrefixFilter filter) throws IOException {
    return new KeyValueBlockLocalIdIterator(containerID,
        finalizeBlocksTableWithIterator.iterator(), filter);
  }

  protected Table<String, BlockData> getBlockDataTableWithIterator() {
    return this.blockDataTableWithIterator;
  }

  protected Table<String, Long> getFinalizeBlocksTableWithIterator() {
    return this.finalizeBlocksTableWithIterator;
  }

  static void checkTableStatus(Table<?, ?> table, String name) throws RocksDatabaseException {
    if (table == null) {
      final RocksDatabaseException e = new RocksDatabaseException(
          "Failed to get table " + name + ": Please check the logs for more info.");
      LOG.error("", e);
      throw e;
    }
  }

  /**
   * Block Iterator for KeyValue Container. This block iterator returns blocks
   * which match with the {@link KeyPrefixFilter}.
   * The default filter is {@link #DEFAULT_BLOCK_FILTER}.
   */
  @InterfaceAudience.Public
  public static class KeyValueBlockIterator implements
          BlockIterator<BlockData>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(
            KeyValueBlockIterator.class);

    private final Table.KeyValueIterator<String, BlockData> blockIterator;
    private static final KeyPrefixFilter DEFAULT_BLOCK_FILTER =
            MetadataKeyFilters.getUnprefixedKeyFilter();
    private final KeyPrefixFilter blockFilter;
    private BlockData nextBlock;
    private final long containerID;

    /**
     * KeyValueBlockIterator to iterate unprefixed blocks in a container.
     * @param iterator - The underlying iterator to apply the block filter to.
     */
    KeyValueBlockIterator(long containerID, Table.KeyValueIterator<String, BlockData> iterator) {
      this.containerID = containerID;
      this.blockIterator = iterator;
      this.blockFilter = DEFAULT_BLOCK_FILTER;
    }

    /**
     * KeyValueBlockIterator to iterate blocks in a container.
     * @param iterator - The underlying iterator to apply the block filter to.
     * @param filter - Block filter, filter to be applied for blocks
     */
    KeyValueBlockIterator(long containerID,
        Table.KeyValueIterator<String, BlockData> iterator, KeyPrefixFilter filter) {
      this.containerID = containerID;
      this.blockIterator = iterator;
      this.blockFilter = filter;
    }

    /**
     * This method returns blocks matching with the filter.
     * @return next block or null if no more blocks
     * @throws IOException
     */
    @Override
    public BlockData nextBlock() throws IOException, NoSuchElementException {
      if (nextBlock != null) {
        BlockData currentBlock = nextBlock;
        nextBlock = null;
        return currentBlock;
      }
      if (hasNext()) {
        return nextBlock();
      }
      throw new NoSuchElementException("Block Iterator reached end for " +
              "ContainerID " + containerID);
    }

    @Override
    public boolean hasNext() throws IOException {
      if (nextBlock != null) {
        return true;
      }
      while (blockIterator.hasNext()) {
        Table.KeyValue<String, BlockData> keyValue = blockIterator.next();
        byte[] keyBytes = StringUtils.string2Bytes(keyValue.getKey());
        if (blockFilter.filterKey(keyBytes)) {
          nextBlock = keyValue.getValue();
          if (LOG.isTraceEnabled()) {
            LOG.trace("Block matching with filter found: blockID is : {} for " +
                    "containerID {}", nextBlock.getLocalID(), containerID);
          }
          return true;
        }
      }
      return false;
    }

    @Override
    public void seekToFirst() {
      nextBlock = null;
      blockIterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
      nextBlock = null;
      blockIterator.seekToLast();
    }

    @Override
    public void close() throws IOException {
      blockIterator.close();
    }
  }

  /**
   * Block localId Iterator for KeyValue Container.
   * This Block localId iterator returns localIds
   * which match with the {@link KeyPrefixFilter}.
   */
  @InterfaceAudience.Public
  public static class KeyValueBlockLocalIdIterator implements
      BlockIterator<Long>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(
        KeyValueBlockLocalIdIterator.class);

    private final Table.KeyValueIterator<String, Long> blockLocalIdIterator;
    private final KeyPrefixFilter localIdFilter;
    private Long nextLocalId;
    private final long containerID;

    /**
     * KeyValueBlockLocalIdIterator to iterate block localIds in a container.
     * @param iterator - The iterator to apply the blockLocalId filter to.
     * @param filter - BlockLocalId filter to be applied for block localIds.
     */
    KeyValueBlockLocalIdIterator(long containerID,
        Table.KeyValueIterator<String, Long> iterator, KeyPrefixFilter filter) {
      this.containerID = containerID;
      this.blockLocalIdIterator = iterator;
      this.localIdFilter = filter;
    }

    /**
     * This method returns blocks matching with the filter.
     * @return next block local Id or null if no more block localIds
     * @throws IOException
     */
    @Override
    public Long nextBlock() throws IOException, NoSuchElementException {
      if (nextLocalId != null) {
        Long currentLocalId = nextLocalId;
        nextLocalId = null;
        return currentLocalId;
      }
      if (hasNext()) {
        return nextBlock();
      }
      throw new NoSuchElementException("Block Local ID Iterator " +
          "reached end for ContainerID " + containerID);
    }

    @Override
    public boolean hasNext() throws IOException {
      if (nextLocalId != null) {
        return true;
      }
      while (blockLocalIdIterator.hasNext()) {
        Table.KeyValue<String, Long> keyValue = blockLocalIdIterator.next();
        byte[] keyBytes = StringUtils.string2Bytes(keyValue.getKey());
        if (localIdFilter.filterKey(keyBytes)) {
          nextLocalId = keyValue.getValue();
          if (LOG.isTraceEnabled()) {
            LOG.trace("Block matching with filter found: LocalID is : " +
                "{} for containerID {}", nextLocalId, containerID);
          }
          return true;
        }
      }
      return false;
    }

    @Override
    public void seekToFirst() {
      nextLocalId = null;
      blockLocalIdIterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
      nextLocalId = null;
      blockLocalIdIterator.seekToLast();
    }

    @Override
    public void close() throws IOException {
      blockLocalIdIterator.close();
    }
  }
}
