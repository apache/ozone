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
import java.util.ArrayList;
import java.util.Collection;

/**
 * Class the represents the API response structure of Volumes.
 */
public class VolumesResponse {
  /**
   * Total count of volumes.
   */
  @JsonProperty("totalCount")
  private long totalCount;

  /**
   * An array of volumes.
   */
  @JsonProperty("volumes")
  private Collection<VolumeObjectDBInfo> volumes;

  public VolumesResponse() {
    this(0, new ArrayList<>());
  }

  public VolumesResponse(long totalCount,
                         Collection<VolumeObjectDBInfo> volumes) {
    this.totalCount = totalCount;
    this.volumes = volumes;
  }

  public long getTotalCount() {
    return totalCount;
  }

  public Collection<VolumeObjectDBInfo> getVolumes() {
    return volumes;
  }
}
