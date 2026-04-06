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

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper response for the quasi-closed containers API.
 */
public class QuasiClosedContainersResponse {

  private long totalCount;
  private List<QuasiClosedContainerMetadata> containers;
  /** The highest container ID returned — for forward-pagination cursor. */
  private long lastKey;

  public QuasiClosedContainersResponse() {
    this.containers = new ArrayList<>();
  }

  public QuasiClosedContainersResponse(
      long totalCount, List<QuasiClosedContainerMetadata> containers, long lastKey) {
    this.totalCount = totalCount;
    this.containers = containers;
    this.lastKey = lastKey;
  }

  public long getTotalCount() { return totalCount; }
  public void setTotalCount(long totalCount) { this.totalCount = totalCount; }

  public List<QuasiClosedContainerMetadata> getContainers() { return containers; }
  public void setContainers(List<QuasiClosedContainerMetadata> containers) {
    this.containers = containers;
  }

  public long getLastKey() { return lastKey; }
  public void setLastKey(long lastKey) { this.lastKey = lastKey; }
}
