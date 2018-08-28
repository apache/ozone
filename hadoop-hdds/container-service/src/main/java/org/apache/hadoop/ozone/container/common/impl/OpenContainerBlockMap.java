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

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Map: containerId -> (localId -> {@link KeyData}).
 * The outer container map does not entail locking for a better performance.
 * The inner {@link KeyDataMap} is synchronized.
 *
 * This class will maintain list of open keys per container when closeContainer
 * command comes, it should autocommit all open keys of a open container before
 * marking the container as closed.
 */
public class OpenContainerBlockMap {
  /**
   * Map: localId -> KeyData.
   *
   * In order to support {@link #getAll()}, the update operations are
   * synchronized.
   */
  static class KeyDataMap {
    private final ConcurrentMap<Long, KeyData> blocks =
        new ConcurrentHashMap<>();

    KeyData get(long localId) {
      return blocks.get(localId);
    }

    synchronized int removeAndGetSize(long localId) {
      blocks.remove(localId);
      return blocks.size();
    }

    synchronized KeyData computeIfAbsent(
        long localId, Function<Long, KeyData> f) {
      return blocks.computeIfAbsent(localId, f);
    }

    synchronized List<KeyData> getAll() {
      return new ArrayList<>(blocks.values());
    }
  }

  /**
   * TODO : We may construct the openBlockMap by reading the Block Layout
   * for each block inside a container listing all chunk files and reading the
   * sizes. This will help to recreate the openKeys Map once the DataNode
   * restarts.
   *
   * For now, we will track all open blocks of a container in the blockMap.
   */
  private final ConcurrentMap<Long, KeyDataMap> containers =
      new ConcurrentHashMap<>();

  /**
   * Removes the Container matching with specified containerId.
   * @param containerId containerId
   */
  public void removeContainer(long containerId) {
    Preconditions
        .checkState(containerId >= 0, "Container Id cannot be negative.");
    containers.remove(containerId);
  }

  public void addChunk(BlockID blockID, ChunkInfo info) {
    Preconditions.checkNotNull(info);
    containers.computeIfAbsent(blockID.getContainerID(), id -> new KeyDataMap())
        .computeIfAbsent(blockID.getLocalID(), id -> new KeyData(blockID))
        .addChunk(info);
  }

  /**
   * Removes the chunk from the chunkInfo list for the given block.
   * @param blockID id of the block
   * @param chunkInfo chunk info.
   */
  public void removeChunk(BlockID blockID, ChunkInfo chunkInfo) {
    Preconditions.checkNotNull(chunkInfo);
    Preconditions.checkNotNull(blockID);
    Optional.ofNullable(containers.get(blockID.getContainerID()))
        .map(blocks -> blocks.get(blockID.getLocalID()))
        .ifPresent(keyData -> keyData.removeChunk(chunkInfo));
  }

  /**
   * Returns the list of open to the openContainerBlockMap.
   * @param containerId container id
   * @return List of open Keys(blocks)
   */
  public List<KeyData> getOpenKeys(long containerId) {
    return Optional.ofNullable(containers.get(containerId))
        .map(KeyDataMap::getAll)
        .orElseGet(Collections::emptyList);
  }

  /**
   * removes the block from the block map.
   * @param blockID
   */
  public void removeFromKeyMap(BlockID blockID) {
    Preconditions.checkNotNull(blockID);
    containers.computeIfPresent(blockID.getContainerID(), (containerId, blocks)
        -> blocks.removeAndGetSize(blockID.getLocalID()) == 0? null: blocks);
  }

  /**
   * Returns true if the block exists in the map, false otherwise.
   *
   * @param blockID
   * @return True, if it exists, false otherwise
   */
  public boolean checkIfBlockExists(BlockID blockID) {
    KeyDataMap keyDataMap = containers.get(blockID.getContainerID());
    return keyDataMap != null && keyDataMap.get(blockID.getLocalID()) != null;
  }

  @VisibleForTesting
  KeyDataMap getKeyDataMap(long containerId) {
    return containers.get(containerId);
  }
}
