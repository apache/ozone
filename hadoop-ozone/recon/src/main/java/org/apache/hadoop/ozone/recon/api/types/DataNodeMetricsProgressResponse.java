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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.ozone.recon.api.DataNodeMetricsService;

/**
 * Response returned while metrics collection is still in progress.
 * Intentionally omits metric payload fields.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataNodeMetricsProgressResponse {

  @JsonProperty("status")
  private final DataNodeMetricsService.MetricCollectionStatus status;

  @JsonProperty("message")
  private final String message;

  @JsonCreator
  public DataNodeMetricsProgressResponse(
      @JsonProperty("status") DataNodeMetricsService.MetricCollectionStatus status,
      @JsonProperty("message") String message) {
    this.status = status;
    this.message = message;
  }

  public DataNodeMetricsService.MetricCollectionStatus getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }
}
