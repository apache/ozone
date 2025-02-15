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

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_BLOCK;

import java.io.IOException;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

/**
 * Interface for interacting with datanode databases.
 */
public interface DatanodeStore extends DBStoreManager {
  String NO_SUCH_BLOCK_ERR_MSG =
          "Unable to find the block.";

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
   * A Table that keeps finalize blocks requested from client.
   *
   * @return Table
   */
  Table<String, Long> getFinalizeBlocksTable();

  /**
   * A Table that keeps the metadata of the last chunk of blocks.
   *
   * @return Table
   */
  Table<String, BlockData> getLastChunkInfoTable();

  BlockIterator<BlockData> getBlockIterator(long containerID)
      throws IOException;

  BlockIterator<BlockData> getBlockIterator(long containerID,
      KeyPrefixFilter filter) throws IOException;

  BlockIterator<Long> getFinalizeBlockIterator(long containerID,
      KeyPrefixFilter filter) throws IOException;

  default BlockData getBlockByID(BlockID blockID,
      String blockKey) throws IOException {

    // check block data table
    BlockData blockData = getBlockDataTable().get(blockKey);

    return getCompleteBlockData(blockData, blockID, blockKey);
  }

  default BlockData getCompleteBlockData(BlockData blockData,
      BlockID blockID, String blockKey) throws IOException {
    if (blockData == null) {
      throw new StorageContainerException(
            NO_SUCH_BLOCK_ERR_MSG + " BlockID : " + blockID, NO_SUCH_BLOCK);
    }

    return blockData;
  }

  default void putBlockByID(BatchOperation batch, boolean incremental,
      long localID, BlockData data, KeyValueContainerData containerData,
      boolean endOfBlock)
      throws IOException {
    // old client: override chunk list.
    getBlockDataTable().putWithBatch(
        batch, containerData.getBlockKey(localID), data);
  }
}
