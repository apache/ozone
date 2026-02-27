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
 *    committed.
 *
 * 3. Deletion-pending bytes: Information on the space occupied by keys that
 *    are marked for deletion but are still occupying storage. This is organized
 *    by deletion stages and is encapsulated within the
 *    {@link DeletionPendingBytesByComponent} object.
 *
 * This class is typically used in scenarios where it is necessary to analyze
 * or monitor the storage utilization in a system to gain insights or plan
 * optimizations.
 */
public class UsedSpaceBreakDown {

  @JsonProperty("openKeyBytes")
  private OpenKeyBytesInfo openKeyBytes;

  @JsonProperty("committedKeyBytes")
  private long committedKeyBytes;

  @JsonProperty("preAllocatedContainerBytes")
  private long preAllocatedContainerBytes;

  public UsedSpaceBreakDown() {
  }

  public UsedSpaceBreakDown(OpenKeyBytesInfo openKeyBytes, long committedKeyBytes, long preAllocatedContainerBytes) {
    this.openKeyBytes = openKeyBytes;
    this.committedKeyBytes = committedKeyBytes;
    this.preAllocatedContainerBytes = preAllocatedContainerBytes;
  }

  public OpenKeyBytesInfo getOpenKeyBytes() {
    return openKeyBytes;
  }

  public long getCommittedKeyBytes() {
    return committedKeyBytes;
  }

  public long getPreAllocatedContainerBytes() {
    return preAllocatedContainerBytes;
  }
}
