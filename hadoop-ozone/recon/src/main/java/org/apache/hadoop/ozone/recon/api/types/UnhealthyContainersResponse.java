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
import java.util.Collection;
import org.apache.ozone.recon.schema.ContainerSchemaDefinitionV2.UnHealthyContainerStates;

/**
 * Class that represents the API Response structure of Unhealthy Containers.
 */
public class UnhealthyContainersResponse {
  /**
   * Total count of the missing containers.
   */
  @JsonProperty("missingCount")
  private long missingCount = 0;

  /**
   * Total count of under replicated containers.
   */
  @JsonProperty("underReplicatedCount")
  private long underReplicatedCount = 0;

  /**
   * Total count of over replicated containers.
   */
  @JsonProperty("overReplicatedCount")
  private long overReplicatedCount = 0;

  /**
   * Total count of mis-replicated containers.
   */
  @JsonProperty("misReplicatedCount")
  private long misReplicatedCount = 0;

  /**
   * Total count of containers that have replicas with mismatched data checksums.
   */
  @JsonProperty("replicaMismatchCount")
  private long replicaMismatchCount = 0;

  /**
   * The smallest container ID in the current response batch.
   * Used for pagination to determine the lower bound for the next page.
   */
  @JsonProperty("firstKey")
  private long firstKey = 0;


  /**
   * The largest container ID in the current response batch.
   * Used for pagination to determine the upper bound for the next page.
   */
  @JsonProperty("lastKey")
  private long lastKey = 0;



  /**
   * A collection of unhealthy containers.
   */
  @JsonProperty("containers")
  private Collection<UnhealthyContainerMetadata> containers;

  public UnhealthyContainersResponse(Collection<UnhealthyContainerMetadata>
                                       containers) {
    this.containers = containers;
  }

  // Default constructor, used by jackson lib for object deserialization.
  public UnhealthyContainersResponse() {
  }

  public void setSummaryCount(String state, long count) {
    if (state.equals(UnHealthyContainerStates.MISSING.toString())) {
      this.missingCount = count;
    } else if (state.equals(
        UnHealthyContainerStates.OVER_REPLICATED.toString())) {
      this.overReplicatedCount = count;
    } else if (state.equals(
        UnHealthyContainerStates.UNDER_REPLICATED.toString())) {
      this.underReplicatedCount = count;
    } else if (state.equals(
        UnHealthyContainerStates.MIS_REPLICATED.toString())) {
      this.misReplicatedCount = count;
    } else if (state.equals(
        UnHealthyContainerStates.REPLICA_MISMATCH.toString())) {
      this.replicaMismatchCount = count;
    }
  }

  public long getMissingCount() {
    return missingCount;
  }

  public long getUnderReplicatedCount() {
    return underReplicatedCount;
  }

  public long getOverReplicatedCount() {
    return overReplicatedCount;
  }

  public long getMisReplicatedCount() {
    return misReplicatedCount;
  }

  public long getReplicaMismatchCount() {
    return replicaMismatchCount;
  }

  public long getLastKey() {
    return lastKey;
  }

  public long getFirstKey() {
    return firstKey;
  }

  public Collection<UnhealthyContainerMetadata> getContainers() {
    return containers;
  }

  public void setFirstKey(long firstKey) {
    this.firstKey = firstKey;
  }

  public void setLastKey(long lastKey) {
    this.lastKey = lastKey;
  }
}
