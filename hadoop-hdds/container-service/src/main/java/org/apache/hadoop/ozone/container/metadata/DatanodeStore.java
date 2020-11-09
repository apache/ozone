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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.BatchOperationHandler;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;

import java.io.IOException;

/**
 * Interface for interacting with datanode databases.
 */
public interface DatanodeStore {

  /**
   * Start datanode manager.
   *
   * @param configuration - Configuration
   * @throws IOException - Unable to start datanode store.
   */
  void start(ConfigurationSource configuration) throws IOException;

  /**
   * Stop datanode manager.
   */
  void stop() throws Exception;

  /**
   * Get datanode store.
   *
   * @return datanode store.
   */
  @VisibleForTesting
  DBStore getStore();

  /**
   * A Table that keeps the block data.
   *
   * @return Table
   */
  Table<String, BlockData> getBlockDataTable();

  /**
   * A Table that keeps the metadata.
   *
   * @return Table
   */
  Table<String, Long> getMetadataTable();

  /**
   * A Table that keeps IDs of blocks deleted from the block data table.
   *
   * @return Table
   */
  Table<String, ChunkInfoList> getDeletedBlocksTable();

  /**
   * Helper to create and write batch transactions.
   */
  BatchOperationHandler getBatchHandler();

  void flushLog(boolean sync) throws IOException;

  void flushDB() throws IOException;

  void compactDB() throws IOException;

  BlockIterator<BlockData> getBlockIterator();

  BlockIterator<BlockData>
      getBlockIterator(MetadataKeyFilters.KeyPrefixFilter filter);
}
