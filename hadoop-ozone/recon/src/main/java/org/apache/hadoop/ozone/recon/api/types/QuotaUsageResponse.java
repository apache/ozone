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

/**
 * HTTP Response wrapped for a quota usage request.
 * 'allowed': the quota in bytes that the namespace allows.
 * 'used': the quota that the namespace has consumed in bytes.
 * 'pathNotFound': invalid path request.
 * 'typeNA': the path exists, but refers to a namespace type (directory, key)
 * that are not applicable to a quota usage request.
 */
public class QuotaUsageResponse {

  @JsonProperty("allowed")
  private long quota;
  @JsonProperty("used")
  private long quotaUsed;
  @JsonProperty("pathNotFound")
  private boolean pathNotFound;
  @JsonProperty("typeNA")
  private boolean namespaceNotApplicable;

  public QuotaUsageResponse() {
    pathNotFound = false;
    namespaceNotApplicable = false;
    quota = 0L;
    quotaUsed = 0L;
  }

  public long getQuota() {
    return quota;
  }

  public long getQuotaUsed() {
    return quotaUsed;
  }

  public boolean isNamespaceNotApplicable() {
    return namespaceNotApplicable;
  }

  public boolean isPathNotFound() {
    return pathNotFound;
  }

  public void setQuota(long quota) {
    this.quota = quota;
  }

  public void setQuotaUsed(long quotaUsed) {
    this.quotaUsed = quotaUsed;
  }

  public void setNamespaceNotApplicable(boolean namespaceNotApplicable) {
    this.namespaceNotApplicable = namespaceNotApplicable;
  }

  public void setPathNotFound(boolean pathNotFound) {
    this.pathNotFound = pathNotFound;
  }
}
