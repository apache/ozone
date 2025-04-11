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

import java.util.TimeZone;
import org.apache.hadoop.ozone.recon.ReconUtils;

/**
 * Wrapper object for statistics of records of a page in API response.
 */
public class ParamInfo {

  /**
   * start prefix path to start search.
   */
  private String startPrefix;

  /**
   * key size filter.
   */
  private long keySize;

  /**
   * creation date filter for keys to filter.
   */
  private String creationDate;

  private long creationDateEpoch = -1;

  /**
   *
   */
  private String replicationType;

  /**
   * limit the number of records to return in API response.
   */
  private int limit;

  private String prevKey;

  private String lastKey;

  private boolean skipPrevKeyDone = false;

  /**
   * counter to track the number of records added in API response.
   */
  private long currentCount;

  @SuppressWarnings("parameternumber")
  public ParamInfo(String replicationType, String creationDate, long keySize, String startPrefix, String prevKey,
                   int limit, boolean skipPrevKeyDone, String lastKey) {
    this.replicationType = replicationType;
    this.creationDate = creationDate;
    this.keySize = keySize;
    this.startPrefix = startPrefix;
    this.prevKey = prevKey;
    this.limit = limit;
    this.skipPrevKeyDone = skipPrevKeyDone;
    this.lastKey = lastKey;
  }

  public String getStartPrefix() {
    return startPrefix;
  }

  public void setStartPrefix(String startPrefix) {
    this.startPrefix = startPrefix;
  }

  public long getKeySize() {
    return keySize;
  }

  public String getCreationDate() {
    return creationDate;
  }

  public long getCreationDateEpoch() {
    if (creationDateEpoch == -1) {
      creationDateEpoch = ReconUtils.convertToEpochMillis(
          getCreationDate(), "MM-dd-yyyy HH:mm:ss", TimeZone.getDefault());
    }
    return creationDateEpoch;
  }

  public String getReplicationType() {
    return replicationType;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public long getCurrentCount() {
    return currentCount;
  }

  public void setCurrentCount(long currentCount) {
    this.currentCount = currentCount;
  }

  public String getPrevKey() {
    return prevKey;
  }

  public void setPrevKey(String prevKey) {
    this.prevKey = prevKey;
  }

  public boolean isSkipPrevKeyDone() {
    return skipPrevKeyDone;
  }

  public void setSkipPrevKeyDone(boolean skipPrevKeyDone) {
    this.skipPrevKeyDone = skipPrevKeyDone;
  }

  public String getLastKey() {
    return lastKey;
  }

  public void setLastKey(String lastKey) {
    this.lastKey = lastKey;
  }
}
