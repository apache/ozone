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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * This class wraps containers and their associated blocks information.
 */
public class ContainerBlocksInfoWrapper {
  @JsonProperty("containerId")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long containerID;
  @JsonProperty("localIDList")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<Long> localIDList;
  @JsonProperty("localIDCount")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private int localIDCount;
  @JsonProperty("txID")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long txID;

  public ContainerBlocksInfoWrapper() {
    this.containerID = 0;
    this.localIDList = new ArrayList<>();
    this.localIDCount = 0;
    this.txID = -1;
  }

  public long getContainerID() {
    return containerID;
  }

  public void setContainerID(long containerID) {
    this.containerID = containerID;
  }

  public List<Long> getLocalIDList() {
    return localIDList;
  }

  public void setLocalIDList(List<Long> localIDList) {
    this.localIDList = localIDList;
  }

  public int getLocalIDCount() {
    return localIDCount;
  }

  public void setLocalIDCount(int localIDCount) {
    this.localIDCount = localIDCount;
  }

  public long getTxID() {
    return txID;
  }

  public void setTxID(long txID) {
    this.txID = txID;
  }
}
