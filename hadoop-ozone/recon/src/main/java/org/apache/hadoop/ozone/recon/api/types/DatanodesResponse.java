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
import java.util.ArrayList;
import java.util.Collection;

/**
 * Class that represents the API Response structure of Datanodes.
 */
public class DatanodesResponse {
  /**
   * Total count of the datanodes.
   */
  @JsonProperty("totalCount")
  private long totalCount;

  /**
   * An array of datanodes.
   */
  @JsonProperty("datanodes")
  private Collection<DatanodeMetadata> datanodes;


  public DatanodesResponse() {
    this(0, new ArrayList<>());
  }

  public DatanodesResponse(long totalCount,
                           Collection<DatanodeMetadata> datanodes) {
    this.totalCount = totalCount;
    this.datanodes = datanodes;
  }

  public long getTotalCount() {
    return totalCount;
  }

  public Collection<DatanodeMetadata> getDatanodes() {
    return datanodes;
  }
}
