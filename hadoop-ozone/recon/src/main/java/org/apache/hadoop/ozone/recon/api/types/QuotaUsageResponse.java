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
 * HTTP Response wrapped for a quota usage request.
 */
public class QuotaUsageResponse {

  /** The quota in bytes that the namespace allows. */
  @JsonProperty("allowed")
  private long quota;

  /** The quota that the namespace has consumed in bytes. */
  @JsonProperty("used")
  private long quotaUsed;

  /** Path status. */
  @JsonProperty("status")
  private ResponseStatus responseCode;

  public QuotaUsageResponse() {
    responseCode = ResponseStatus.OK;
    quota = 0L;
    quotaUsed = 0L;
  }

  public long getQuota() {
    return quota;
  }

  public long getQuotaUsed() {
    return quotaUsed;
  }

  public ResponseStatus getResponseCode() {
    return responseCode;
  }

  public void setQuota(long quota) {
    this.quota = quota;
  }

  public void setQuotaUsed(long quotaUsed) {
    this.quotaUsed = quotaUsed;
  }

  public void setResponseCode(ResponseStatus responseCode) {
    this.responseCode = responseCode;
  }
}
