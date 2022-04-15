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
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;

import java.io.IOException;

/**
 * Interface for a temp store for containers under recovery,
 * NOTE: Specially for EC containers for now, but it could also
 * be used for common containers in the future.
 */
public interface ContainerRecoveryStore {

  /**
   * Write a recovered chunk to some temp location.
   * @param container
   * @param blockID
   * @param chunkInfo
   * @param data
   * @param checksum
   * @param last
   * @throws IOException
   */
  void writeChunk(KeyValueContainer container, BlockID blockID,
      ChunkInfo chunkInfo, ChunkBuffer data, Checksum checksum,
      boolean last) throws IOException;

  /**
   * Reconstruct a container from chunk files and metadata.
   * @param container
   * @throws IOException
   */
  void consolidateContainer(KeyValueContainer container)
      throws IOException;

  /**
   * Cleanup in-memory metadata and on-disk files for a container including
   * all the replicas.
   * Called on the CoordinatorDN.
   * @param container
   */
  void cleanupContainerAll(KeyValueContainer container);
}
