/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import java.util.List;

/**
 * Metadata object that represents a Container Discrepancy Info.
 */
public class ContainerDiscrepancyInfo {

  @JsonProperty("containerId")
  private long containerID;

  @JsonProperty("numberOfKeys")
  private long numberOfKeys;

  @JsonProperty("pipelines")
  private List<Pipeline> pipelines;

  @JsonProperty("existsAt")
  private String existsAt;

  public ContainerDiscrepancyInfo() {

  }

  public long getContainerID() {
    return containerID;
  }

  public void setContainerID(long containerID) {
    this.containerID = containerID;
  }

  public long getNumberOfKeys() {
    return numberOfKeys;
  }

  public void setNumberOfKeys(long numberOfKeys) {
    this.numberOfKeys = numberOfKeys;
  }

  public List<Pipeline> getPipelines() {
    return pipelines;
  }

  public void setPipelines(
      List<Pipeline> pipelines) {
    this.pipelines = pipelines;
  }

  public String getExistsAt() {
    return existsAt;
  }

  public void setExistsAt(String existsAt) {
    this.existsAt = existsAt;
  }
}
