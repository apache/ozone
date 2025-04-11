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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;

/**
 * HTTP Response wrapped for keys insights.
 */
public class KeyInsightInfoResponse {

  /** last key sent. */
  @JsonProperty("lastKey")
  private String lastKey;

  /** Amount of data mapped to all keys and files in
   * a cluster across all DNs. */
  @JsonProperty("replicatedDataSize")
  private long replicatedDataSize;

  /** Amount of data mapped to all keys and files on a single DN. */
  @JsonProperty("unreplicatedDataSize")
  private long unreplicatedDataSize;

  /** List of all non-fso keys. */
  @JsonProperty("nonFSO")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<KeyEntityInfo> nonFSOKeyInfoList;

  /** List of all fso keys. */
  @JsonProperty("fso")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<KeyEntityInfo> fsoKeyInfoList;

  /** List of all deleted and repeatedly deleted keys.  */
  @JsonProperty("deletedKeyInfo")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<RepeatedOmKeyInfo> repeatedOmKeyInfoList;

  @JsonProperty("deletedDirInfo")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<KeyEntityInfo> deletedDirInfoList;

  /** Path status. */
  @JsonProperty("status")
  private ResponseStatus responseCode;

  public KeyInsightInfoResponse() {
    responseCode = ResponseStatus.OK;
    lastKey = "";
    replicatedDataSize = 0L;
    unreplicatedDataSize = 0L;
    nonFSOKeyInfoList = new ArrayList<>();
    fsoKeyInfoList = new ArrayList<>();
    repeatedOmKeyInfoList = new ArrayList<>();
    deletedDirInfoList = new ArrayList<>();
  }

  public String getLastKey() {
    return lastKey;
  }

  public void setLastKey(String lastKey) {
    this.lastKey = lastKey;
  }

  public long getReplicatedDataSize() {
    return replicatedDataSize;
  }

  public void setReplicatedDataSize(long replicatedDataSize) {
    this.replicatedDataSize = replicatedDataSize;
  }

  public long getUnreplicatedDataSize() {
    return unreplicatedDataSize;
  }

  public void setUnreplicatedDataSize(long unreplicatedDataSize) {
    this.unreplicatedDataSize = unreplicatedDataSize;
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

  public List<RepeatedOmKeyInfo> getRepeatedOmKeyInfoList() {
    return repeatedOmKeyInfoList;
  }

  public void setRepeatedOmKeyInfoList(
      List<RepeatedOmKeyInfo> repeatedOmKeyInfoList) {
    this.repeatedOmKeyInfoList = repeatedOmKeyInfoList;
  }

  public List<KeyEntityInfo> getDeletedDirInfoList() {
    return deletedDirInfoList;
  }

  public void setDeletedDirInfoList(
      List<KeyEntityInfo> deletedDirInfoList) {
    this.deletedDirInfoList = deletedDirInfoList;
  }

  public ResponseStatus getResponseCode() {
    return responseCode;
  }

  public void setResponseCode(ResponseStatus responseCode) {
    this.responseCode = responseCode;
  }

}
