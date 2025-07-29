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
 * Represents a data structure that encapsulates the various categories
 * of committed bytes in a storage system.
 *
 * This class is used to store and manage the distribution of bytes
 * across different key types in the storage system, such as:
 * - Total bytes committed.
 * - Bytes associated with file system object (FSO) keys.
 * - Bytes associated with object storage (OBS) keys.
 * - Bytes associated with legacy keys.
 *
 * Each of these types of bytes is represented as a long value.
 */
public class CommittedBytes {
  @JsonProperty("totalBytes")
  private long totalBytes;

  @JsonProperty("fsoKeyBytes")
  private long fsoKeyBytes;

  @JsonProperty("obsKeyBytes")
  private long obsKeyBytes;

  @JsonProperty("legacyKeyBytes")
  private long legacyKeyBytes;

  public CommittedBytes(long totalBytes, long fsoKeyBytes, long obsKeyBytes, long legacyKeyBytes) {
    this.totalBytes = totalBytes;
    this.fsoKeyBytes = fsoKeyBytes;
    this.obsKeyBytes = obsKeyBytes;
    this.legacyKeyBytes = legacyKeyBytes;
  }

  public long getTotalBytes() {
    return totalBytes;
  }

  public long getFsoKeyBytes() {
    return fsoKeyBytes;
  }

  public long getObsKeyBytes() {
    return obsKeyBytes;
  }

  public long getLegacyKeyBytes() {
    return legacyKeyBytes;
  }
}
