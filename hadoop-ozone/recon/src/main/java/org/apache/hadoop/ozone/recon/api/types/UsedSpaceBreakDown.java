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
 * Represents a breakdown of storage space usage in a system by categorizing
 * the used space into open keys, committed bytes, and deletion-pending bytes.
 *
 * This class serves as a container for information about the allocation of
 * storage resources across different states of usage. It provides a detailed
 * view of three primary categories of used space:
 *
 * 1. Open keys: The total space occupied by keys that are in an open,
 *    uncommitted state.
 *
 * 2. Committed bytes: Information on space occupied by keys that have been
 *    committed. This is encapsulated in the {@link CommittedBytes} object,
 *    which provides further categorization based on key types (e.g., FSO, OBS, legacy).
 *
 * 3. Deletion-pending bytes: Information on the space occupied by keys that
 *    are marked for deletion but are still occupying storage. This is organized
 *    by deletion stages and is encapsulated within the
 *    {@link DeletionPendingBytesByStage} object.
 *
 * This class is typically used in scenarios where it is necessary to analyze
 * or monitor the storage utilization in a system to gain insights or plan
 * optimizations.
 */
public class UsedSpaceBreakDown {

  @JsonProperty("openKeysBytes")
  private long openKeysBytes;

  @JsonProperty("committedBytes")
  private CommittedBytes committedBytes;

  @JsonProperty("deletionPendingBytes")
  private DeletionPendingBytesByStage deletionPendingBytesByStage;

  public UsedSpaceBreakDown(long openKeysBytes, CommittedBytes committedBytes,
      DeletionPendingBytesByStage deletionPendingBytesByStage) {
    this.openKeysBytes = openKeysBytes;
    this.committedBytes = committedBytes;
    this.deletionPendingBytesByStage = deletionPendingBytesByStage;
  }

  public long getOpenKeysBytes() {
    return openKeysBytes;
  }

  public CommittedBytes getCommittedBytes() {
    return committedBytes;
  }

  public DeletionPendingBytesByStage getDeletionPendingBytesByStage() {
    return deletionPendingBytesByStage;
  }
}
