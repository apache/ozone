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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents pending deletion metrics for a datanode.
 * This class encapsulates information about blocks pending deletion on a specific datanode.
 */
public class DatanodePendingDeletionMetrics {

  @JsonProperty("hostName")
  private final String hostName;

  @JsonProperty("datanodeUuid")
  private final String datanodeUuid;

  @JsonProperty("pendingBlockSize")
  private final long pendingBlockSize;

  @JsonCreator
  public DatanodePendingDeletionMetrics(
      @JsonProperty("hostName") String hostName,
      @JsonProperty("datanodeUuid") String datanodeUuid,
      @JsonProperty("pendingBlockSize") long pendingBlockSize) {
    this.hostName = hostName;
    this.datanodeUuid = datanodeUuid;
    this.pendingBlockSize = pendingBlockSize;
  }

  public String getHostName() {
    return hostName;
  }

  public long getPendingBlockSize() {
    return pendingBlockSize;
  }

  public String getDatanodeUuid() {
    return datanodeUuid;
  }
}
