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

/**
 * API response wrapper for the quasi-closed containers endpoint.
 */
public class QuasiClosedContainersResponse {

  @JsonProperty("quasiClosedCount")
  private long quasiClosedCount = 0;

  @JsonProperty("firstKey")
  private long firstKey = 0;

  @JsonProperty("lastKey")
  private long lastKey = 0;

  @JsonProperty("containers")
  private List<QuasiClosedContainerMetadata> containers;

  public QuasiClosedContainersResponse() {
  }

  public QuasiClosedContainersResponse(long quasiClosedCount, long firstKey, long lastKey,
      List<QuasiClosedContainerMetadata> containers) {
    this.quasiClosedCount = quasiClosedCount;
    this.firstKey = firstKey;
    this.lastKey = lastKey;
    this.containers = containers;
  }

  public long getQuasiClosedCount() {
    return quasiClosedCount;
  }

  public void setQuasiClosedCount(long quasiClosedCount) {
    this.quasiClosedCount = quasiClosedCount;
  }

  public long getFirstKey() {
    return firstKey;
  }

  public void setFirstKey(long firstKey) {
    this.firstKey = firstKey;
  }

  public long getLastKey() {
    return lastKey;
  }

  public void setLastKey(long lastKey) {
    this.lastKey = lastKey;
  }

  public List<QuasiClosedContainerMetadata> getContainers() {
    return containers;
  }

  public void setContainers(List<QuasiClosedContainerMetadata> containers) {
    this.containers = containers;
  }
}
