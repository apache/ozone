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

import java.util.ArrayList;
import java.util.List;

/**
 * HTTP Response wrapped for Open keys insights.
 */
public class OpenKeyInsightInfoResp {

  /** Amount of data mapped to all open keys and files in
   * a cluster across all DNs. */
  @JsonProperty("replicatedTotal")
  private long replicatedTotal;

  /** Amount of data mapped to all open keys and files on a single DN. */
  @JsonProperty("unreplicatedTotal")
  private long unreplicatedTotal;

  /** List of all open non-fso keys. */
  @JsonProperty("non-fso")
  private List<KeyEntityInfo> nonFSOKeyInfoList;

  /** List of all open fso keys. */
  @JsonProperty("non-fso")
  private List<KeyEntityInfo> fsoKeyInfoList;

  /** Path status. */
  @JsonProperty("status")
  private ResponseStatus responseCode;

  public OpenKeyInsightInfoResp() {
    responseCode = ResponseStatus.OK;
    replicatedTotal = 0L;
    unreplicatedTotal = 0L;
    nonFSOKeyInfoList = new ArrayList<>();
    fsoKeyInfoList = new ArrayList<>();
  }

  public long getReplicatedTotal() {
    return replicatedTotal;
  }

  public void setReplicatedTotal(long replicatedTotal) {
    this.replicatedTotal = replicatedTotal;
  }

  public long getUnreplicatedTotal() {
    return unreplicatedTotal;
  }

  public void setUnreplicatedTotal(long unreplicatedTotal) {
    this.unreplicatedTotal = unreplicatedTotal;
  }

  public List<KeyEntityInfo> getNonFSOKeyInfoList() {
    return nonFSOKeyInfoList;
  }

  public void setNonFSOKeyInfoList(
      List<KeyEntityInfo> nonFSOKeyInfoList) {
    this.nonFSOKeyInfoList = nonFSOKeyInfoList;
  }

  public List<KeyEntityInfo> getFsoKeyInfoList() {
    return fsoKeyInfoList;
  }

  public void setFsoKeyInfoList(
      List<KeyEntityInfo> fsoKeyInfoList) {
    this.fsoKeyInfoList = fsoKeyInfoList;
  }

  public ResponseStatus getResponseCode() {
    return responseCode;
  }

  public void setResponseCode(ResponseStatus responseCode) {
    this.responseCode = responseCode;
  }
}
