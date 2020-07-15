/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;

import java.util.Collection;

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
   * A collection of unhealthy containers.
   */
  @JsonProperty("containers")
  private Collection<UnhealthyContainerMetadata> containers;

  public UnhealthyContainersResponse(Collection<UnhealthyContainerMetadata>
                                       containers) {
    this.containers = containers;
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

  public Collection<UnhealthyContainerMetadata> getContainers() {
    return containers;
  }
}
