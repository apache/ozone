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
 * This is Solr Health Check response for healthCheck API.
 */
public final class HealthCheckResponse {

  /** Health check response message. */
  @JsonProperty("message")
  private String message;

  /** Health check status code. */
  @JsonProperty("status")
  private int status;

  // Private constructor to prevent direct instantiation
  private HealthCheckResponse(Builder builder) {
    this.message = builder.message;
    this.status = builder.status;
  }

  public String getMessage() {
    return message;
  }

  public int getStatus() {
    return status;
  }

  /**
   * Builder class.
   */
  public static class Builder {

    // Required parameters
    private String message;

    private int status;

    // Constructor with required parameters
    public Builder(String message, int status) {
      this.message = message;
      this.status = status;
    }

    // Build method to create a new Person instance
    public HealthCheckResponse build() {
      return new HealthCheckResponse(this);
    }
  }
}
