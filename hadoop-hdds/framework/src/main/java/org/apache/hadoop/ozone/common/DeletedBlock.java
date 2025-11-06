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

package org.apache.hadoop.ozone.common;

import org.apache.hadoop.hdds.client.BlockID;

/**
 * DeletedBlock of Ozone (BlockID + usedBytes).
 */
public class DeletedBlock {

  private BlockID blockID;
  private long size;
  private long replicatedSize;

  public DeletedBlock(BlockID blockID, long size, long replicatedSize) {
    this.blockID = blockID;
    this.size = size;
    this.replicatedSize = replicatedSize;
  }

  public BlockID getBlockID() {
    return this.blockID;
  }

  public long getSize() {
    return this.size;
  }

  public long getReplicatedSize() {
    return this.replicatedSize;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(64);
    sb.append(" localID: ").append(blockID.getContainerBlockID().getLocalID());
    sb.append(" containerID: ").append(blockID.getContainerBlockID().getContainerID());
    sb.append(" size: ").append(size);
    sb.append(" replicatedSize: ").append(replicatedSize);
    return sb.toString();
  }
}
