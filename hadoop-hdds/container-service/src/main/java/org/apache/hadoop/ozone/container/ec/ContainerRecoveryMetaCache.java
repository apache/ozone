/**
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
package org.apache.hadoop.ozone.container.ec;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A cache for chunk-level & block-level metadata,
 * prepared for consolidating a container.
 */
public final class ContainerRecoveryMetaCache {

  private static ContainerRecoveryMetaCache cache;

  private final Map<Long, Map<BlockID, BlockData>> containerBlockDataMap;

  private ContainerRecoveryMetaCache() {
    this.containerBlockDataMap = new ConcurrentHashMap<>();
  }

  public static synchronized ContainerRecoveryMetaCache getInstance() {
    if (cache == null) {
      cache = new ContainerRecoveryMetaCache();
    }
    return cache;
  }

  void addChunkToBlock(BlockID blockID, ChunkInfo chunkInfo) {
    long containerID = blockID.getContainerID();

    containerBlockDataMap.putIfAbsent(containerID, new HashMap<>());
    containerBlockDataMap.get(containerID)
        .putIfAbsent(blockID, new BlockData(blockID));
    containerBlockDataMap.get(containerID).get(blockID)
        .addChunk(chunkInfo.getProtoBufMessage());
  }

  Iterator<BlockData> getBlockIterator(long containerID) {
    return containerBlockDataMap.getOrDefault(containerID,
        Collections.emptyMap()).values().iterator();
  }

  void dropContainerAll(long containerID) {
    containerBlockDataMap.remove(containerID);
  }
}
