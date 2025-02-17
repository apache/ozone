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
 * Class that represents the datanode metrics captured during decommissioning.
 */
public class DatanodeMetrics {
  /**
   * Start time of decommission of datanode.
   */
  @JsonProperty("decommissionStartTime")
  private String decommissionStartTime;

  /**
   * Number of pipelines in unclosed status.
   */
  @JsonProperty("numOfUnclosedPipelines")
  private int numOfUnclosedPipelines;

  /**
   * Number of under replicated containers.
   */
  @JsonProperty("numOfUnderReplicatedContainers")
  private double numOfUnderReplicatedContainers;

  /**
   * Number of containers still not closed.
   */
  @JsonProperty("numOfUnclosedContainers")
  private double numOfUnclosedContainers;

  public String getDecommissionStartTime() {
    return decommissionStartTime;
  }

  public void setDecommissionStartTime(String decommissionStartTime) {
    this.decommissionStartTime = decommissionStartTime;
  }

  public int getNumOfUnclosedPipelines() {
    return numOfUnclosedPipelines;
  }

  public void setNumOfUnclosedPipelines(int numOfUnclosedPipelines) {
    this.numOfUnclosedPipelines = numOfUnclosedPipelines;
  }

  public double getNumOfUnderReplicatedContainers() {
    return numOfUnderReplicatedContainers;
  }

  public void setNumOfUnderReplicatedContainers(double numOfUnderReplicatedContainers) {
    this.numOfUnderReplicatedContainers = numOfUnderReplicatedContainers;
  }

  public double getNumOfUnclosedContainers() {
    return numOfUnclosedContainers;
  }

  public void setNumOfUnclosedContainers(double numOfUnclosedContainers) {
    this.numOfUnclosedContainers = numOfUnclosedContainers;
  }
}
