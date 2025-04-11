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
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;

/**
 * Class that represents the API Response of decommissioning status info of datanode.
 */
public class DecommissionStatusInfoResponse {
  /**
   * Metadata of a datanode when decommissioning of datanode is in progress.
   */
  @JsonProperty("datanodeDetails")
  private DatanodeDetails dataNodeDetails;

  /**
   * Metrics of datanode when decommissioning of datanode is in progress.
   */
  @JsonProperty("metrics")
  private DatanodeMetrics datanodeMetrics;

  /**
   * containers info of a datanode when decommissioning of datanode is in progress.
   */
  @JsonProperty("containers")
  private Map<String, List<ContainerID>> containers;

  public DatanodeDetails getDataNodeDetails() {
    return dataNodeDetails;
  }

  public void setDataNodeDetails(DatanodeDetails dataNodeDetails) {
    this.dataNodeDetails = dataNodeDetails;
  }

  public DatanodeMetrics getDatanodeMetrics() {
    return datanodeMetrics;
  }

  public void setDatanodeMetrics(DatanodeMetrics datanodeMetrics) {
    this.datanodeMetrics = datanodeMetrics;
  }

  public Map<String, List<ContainerID>> getContainers() {
    return containers;
  }

  public void setContainers(
      Map<String, List<ContainerID>> containers) {
    this.containers = containers;
  }
}
