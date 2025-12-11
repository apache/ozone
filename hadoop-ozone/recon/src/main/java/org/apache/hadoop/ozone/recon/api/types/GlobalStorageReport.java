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
 * Represents a report detailing global storage usage metrics.
 *
 * This class provides information about the total used space,
 * total free space, and the total storage capacity. It is used
 * to encapsulate and transport these metrics, which can assist
 * in monitoring and analyzing storage allocation and usage.
 */
public class GlobalStorageReport {

  @JsonProperty("totalUsedSpace")
  private long totalUsedSpace;

  @JsonProperty("totalFreeSpace")
  private long totalFreeSpace;

  @JsonProperty("totalCapacity")
  private long totalCapacity;

  public GlobalStorageReport() {
  }

  public GlobalStorageReport(long totalUsedSpace, long totalFreeSpace, long totalCapacity) {
    this.totalUsedSpace = totalUsedSpace;
    this.totalFreeSpace = totalFreeSpace;
    this.totalCapacity = totalCapacity;
  }

  public long getTotalUsedSpace() {
    return totalUsedSpace;
  }

  public long getTotalFreeSpace() {
    return totalFreeSpace;
  }

  public long getTotalCapacity() {
    return totalCapacity;
  }
}
