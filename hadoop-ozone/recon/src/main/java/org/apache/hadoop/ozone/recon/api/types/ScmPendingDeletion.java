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

package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents metadata related to pending deletions in the storage container manager (SCM).
 * This class encapsulates information such as the total block size, the total size of replicated blocks,
 * and the total number of blocks awaiting deletion.
 */
public class ScmPendingDeletion {
  @JsonProperty("totalBlocksize")
  private final long totalBlocksize;
  @JsonProperty("totalReplicatedBlockSize")
  private final long totalReplicatedBlockSize;
  @JsonProperty("totalBlocksCount")
  private final long totalBlocksCount;

  public ScmPendingDeletion() {
    this.totalBlocksize = 0;
    this.totalReplicatedBlockSize = 0;
    this.totalBlocksCount = 0;
  }

  public ScmPendingDeletion(long size, long replicatedSize, long totalBlocks) {
    this.totalBlocksize = size;
    this.totalReplicatedBlockSize = replicatedSize;
    this.totalBlocksCount = totalBlocks;
  }

  public long getTotalBlocksize() {
    return totalBlocksize;
  }

  public long getTotalReplicatedBlockSize() {
    return totalReplicatedBlockSize;
  }

  public long getTotalBlocksCount() {
    return totalBlocksCount;
  }
}
