/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.keyvalue.interfaces;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;

import java.io.IOException;
import java.util.List;

/**
 * BlockManager is for performing key related operations on the container.
 */
public interface BlockManager {

  /**
   * Puts or overwrites a block.
   *
   * @param container - Container for which block need to be added.
   * @param data - Block Data.
   * @return length of the Block.
   */
  long putBlock(Container container, BlockData data) throws IOException;

  /**
   * Puts or overwrites a block.
   *
   * @param container - Container for which block need to be added.
   * @param data - Block Data.
   * @param endOfBlock - The last putBlock call for this block (when
   *                     all the chunks are written and stream is closed)
   * @return length of the Block.
   */
  long putBlock(Container container, BlockData data, boolean endOfBlock)
      throws IOException;

  /**
   * Gets an existing block.
   *
   * @param container - Container from which block needs to be fetched.
   * @param blockID - BlockID of the Block.
   * @return Block Data.
   * @throws IOException when BcsId is unknown or mismatched
   */
  BlockData getBlock(Container container, BlockID blockID) throws IOException;


  /**
   * Deletes an existing block.
   *
   * @param container - Container from which block need to be deleted.
   * @param blockID - ID of the block.
   */
  void deleteBlock(Container container, BlockID blockID) throws IOException;

  /**
   * List blocks in a container.
   *
   * @param container - Container from which blocks need to be listed.
   * @param startLocalID  - Block to start from, 0 to begin.
   * @param count - Number of blocks to return.
   * @return List of Blocks that match the criteria.
   */
  List<BlockData> listBlock(Container container, long startLocalID, int count)
      throws IOException;

  /**
   * Returns last committed length of the block.
   *
   * @param container - Container from which block need to be fetched.
   * @param blockID - BlockID of the block.
   * @return length of the block.
   * @throws IOException in case, the block key does not exist in db.
   */
  long getCommittedBlockLength(Container container, BlockID blockID)
      throws IOException;

  void finalizeBlock(Container container, BlockID blockId)
      throws IOException;

  int getDefaultReadBufferCapacity();

  /** @return the threshold to read using memory mapped buffers. */
  int getReadMappedBufferThreshold();

  /** @return the max count of memory mapped buffers to read. */
  int getReadMappedBufferMaxCount();

  /** @return true iff Netty ChunkedNioFile read is enabled. */
  boolean isReadNettyChunkedNioFile();

  /**
   * Shutdown ContainerManager.
   */
  void shutdown();
}
