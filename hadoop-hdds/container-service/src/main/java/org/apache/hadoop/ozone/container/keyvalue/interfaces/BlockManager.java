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

package org.apache.hadoop.ozone.container.keyvalue.interfaces;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;

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
   * Persists the block data for a closed container. The block data should have all the chunks and bcsId.
   * Overwrites the block if it already exists, The container's used bytes should be updated by the caller with
   * {@link ChunkManager#writeChunk(Container, BlockID, ChunkInfo, ByteBuffer, DispatcherContext)}.
   *
   * @param container      - Container for which block data need to be persisted.
   * @param data           - Block Data to be persisted (BlockData should have the chunks).
   * @param overwriteBcsId - To overwrite bcsId of the container.
   */
  long putBlockForClosedContainer(Container container, BlockData data, boolean overwriteBcsId)
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
   * Check if a block exists in the container.
   *
   * @param container - Container from which blocks need to be listed.
   * @param blockID - BlockID of the Block.
   * @return True if block exists, false otherwise.
   */
  boolean blockExists(Container container, BlockID blockID) throws IOException;

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
